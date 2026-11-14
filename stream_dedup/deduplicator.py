from __future__ import annotations

from dataclasses import dataclass, field

from .bloom import BloomFilter
from .config import DedupConfig
from .fingerprint import fingerprint, partition_id
from .recheck import InMemoryRecheckBackend, RecheckBackend


class Status:
    NEW = "new"
    DUPLICATE = "duplicate"
    RECHECK = "recheck"
    LATE = "late"


@dataclass
class DedupResult:
    status: str
    record: dict
    partition: str
    bucket: int
    digest: bytes
    bloom_positive: bool = False
    late: bool = False
    resolved: bool = False


@dataclass
class BucketState:
    bucket_id: int
    exact: set[bytes] = field(default_factory=set)
    bloom: BloomFilter | None = None

    @property
    def is_bloom(self) -> bool:
        return self.bloom is not None


@dataclass
class PartitionState:
    watermark: int = 0
    buckets: dict[int, BucketState] = field(default_factory=dict)


class Deduplicator:
    """按分区 + 时间窗口做有界内存流式去重。

    每个分区内部按时间桶分层：

    - 近期桶：精确指纹集合（零误判）；
    - 较老但仍在保留窗口内的桶：有界 BloomFilter（只会误报）；
    - Bloom 阳性一律进入 :class:`RecheckBackend` 回查，绝不直接判重；
    - 超出保留窗口的迟到记录走 ``late`` 回查路径。
    """

    def __init__(self, config: DedupConfig,
                 recheck_backend: RecheckBackend | None | bool = None) -> None:
        self.config = config
        if recheck_backend is None:
            self.backend: RecheckBackend | None = InMemoryRecheckBackend()
        elif recheck_backend is False:
            self.backend = None
        else:
            self.backend = recheck_backend
        self.partitions: dict[str, PartitionState] = {}
        self.sequence = 0
        self.stats = {
            "total": 0,
            "new": 0,
            "duplicate": 0,
            "recheck": 0,
            "late": 0,
            "bloom_positive": 0,
            "bloom_false_positives": 0,
        }

    # ------------------------------------------------------------------
    # 生命周期与水位
    # ------------------------------------------------------------------
    def _exact_cutoff(self, watermark_bucket: int) -> int:
        return watermark_bucket - self.config.exact_age_buckets + 1

    def _retain_cutoff(self, watermark_bucket: int) -> int:
        return watermark_bucket - self.config.retained_buckets + 1

    def _apply_lifecycle(self, partition: str, state: PartitionState) -> None:
        watermark_bucket = self.config.bucket_id(state.watermark)
        exact_cutoff = self._exact_cutoff(watermark_bucket)
        retain_cutoff = self._retain_cutoff(watermark_bucket)

        for bucket_id in sorted(state.buckets):
            bucket = state.buckets[bucket_id]
            if bucket_id < retain_cutoff:
                del state.buckets[bucket_id]
                continue
            if not bucket.is_bloom and bucket_id < exact_cutoff:
                bloom = BloomFilter(
                    self.config.bloom_expected_items, self.config.bloom_fpp
                )
                for digest in bucket.exact:
                    bloom.add(digest)
                    if self.backend is not None:
                        self.backend.put(partition, bucket_id, digest)
                bucket.exact.clear()
                bucket.bloom = bloom

        if self.backend is not None:
            self.backend.evict_buckets_before(partition, retain_cutoff)

    def advance_watermark(self, timestamp: int) -> None:
        """手动推进全局水位（适合没有新数据的空闲分区）。"""
        for partition, state in self.partitions.items():
            if timestamp > state.watermark:
                state.watermark = timestamp
                self._apply_lifecycle(partition, state)

    # ------------------------------------------------------------------
    # 主流程
    # ------------------------------------------------------------------
    def process(self, record: dict) -> DedupResult:
        self.sequence += 1
        self.stats["total"] += 1

        partition = partition_id(record, self.config)
        digest = fingerprint(record, self.config)
        timestamp = record[self.config.timestamp_field]
        bucket_id = self.config.bucket_id(timestamp)
        state = self.partitions.setdefault(partition, PartitionState())

        if timestamp > state.watermark:
            state.watermark = timestamp
            self._apply_lifecycle(partition, state)

        retain_cutoff = self._retain_cutoff(self.config.bucket_id(state.watermark))
        result = DedupResult(
            status=Status.NEW,
            record=record,
            partition=partition,
            bucket=bucket_id,
            digest=digest,
        )

        if bucket_id < retain_cutoff:
            return self._handle_late(result, state)

        bucket = state.buckets.get(bucket_id)
        if bucket is None:
            bucket = BucketState(bucket_id)
            state.buckets[bucket_id] = bucket

        if bucket.is_bloom:
            if bucket.bloom.contains(digest):
                result.bloom_positive = True
                self.stats["bloom_positive"] += 1
                return self._handle_bloom_positive(result, bucket, state)
            bucket.bloom.add(digest)
            if self.backend is not None:
                self.backend.put(partition, bucket_id, digest)
            self._accept_new(result)
            return result

        if digest in bucket.exact:
            result.status = Status.DUPLICATE
            self.stats["duplicate"] += 1
            return result

        bucket.exact.add(digest)
        self._accept_new(result)
        return result

    def _accept_new(self, result: DedupResult) -> None:
        result.status = Status.NEW
        self.stats["new"] += 1

    def _handle_bloom_positive(self, result: DedupResult, bucket: BucketState,
                               state: PartitionState) -> DedupResult:
        # 概率结构阳性必须回查，不能直接当确定重复。
        if self.backend is None:
            result.status = Status.RECHECK
            self.stats["recheck"] += 1
            return result
        if self.backend.exists(result.partition, result.digest):
            result.status = Status.DUPLICATE
            self.stats["duplicate"] += 1
            return result
        # Bloom 误报：回查确认不存在，作为新记录接受并补写回查表。
        self.stats["bloom_false_positives"] += 1
        if self.backend is not None:
            self.backend.put(result.partition, result.bucket, result.digest)
        self._accept_new(result)
        return result

    def _handle_late(self, result: DedupResult, state: PartitionState) -> DedupResult:
        result.late = True
        if self.backend is None:
            result.status = Status.RECHECK
            self.stats["recheck"] += 1
            self.stats["late"] += 1
            return result
        if self.backend.exists(result.partition, result.digest):
            result.status = Status.DUPLICATE
            self.stats["duplicate"] += 1
        else:
            self.backend.put(result.partition, result.bucket, result.digest)
            self._accept_new(result)
        self.stats["late"] += 1
        return result

    def resolve_recheck(self, result: DedupResult, is_duplicate: bool) -> DedupResult:
        """由外部系统裁决 ``recheck`` 结果（仅在禁用内置回查后端时出现）。"""
        if result.status != Status.RECHECK:
            raise ValueError("只有 RECHECK 状态的结果需要外部裁决")
        result.resolved = True
        if is_duplicate:
            result.status = Status.DUPLICATE
            self.stats["duplicate"] += 1
        else:
            result.status = Status.NEW
            self.stats["new"] += 1
        return result

    # ------------------------------------------------------------------
    # 观测
    # ------------------------------------------------------------------
    def partition_stats(self) -> dict[str, dict]:
        report = {}
        for partition, state in self.partitions.items():
            exact_fps = sum(len(b.exact) for b in state.buckets.values())
            bloom_buckets = [b for b in state.buckets.values() if b.is_bloom]
            bloom_bytes = sum(b.bloom.byte_count for b in bloom_buckets)
            report[partition] = {
                "watermark": state.watermark,
                "bucket_count": len(state.buckets),
                "exact_fingerprints": exact_fps,
                "bloom_buckets": len(bloom_buckets),
                "bloom_bytes": bloom_bytes,
                "bloom_items": sum(b.bloom.count for b in bloom_buckets),
                "bloom_estimated_fpp": max(
                    (b.bloom.estimated_fpp() for b in bloom_buckets), default=0.0
                ),
            }
        return report

    def total_index_bytes(self) -> int:
        total = 0
        for state in self.partitions.values():
            for bucket in state.buckets.values():
                total += len(bucket.exact) * 16
                if bucket.bloom is not None:
                    total += bucket.bloom.byte_count
        return total
