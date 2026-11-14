from __future__ import annotations

from abc import ABC, abstractmethod


class RecheckBackend(ABC):
    """Bloom 阳性后的确定性回查接口。

    生产环境可实现为 Redis / RocksDB / KV 存储等外部查询；
    库自带一个有界的内存实现 :class:`InMemoryRecheckBackend`。
    """

    @abstractmethod
    def put(self, partition: str, bucket: int, digest: bytes) -> None: ...

    @abstractmethod
    def exists(self, partition: str, digest: bytes) -> bool: ...

    @abstractmethod
    def evict_buckets_before(self, partition: str, cutoff_bucket: int) -> None: ...

    @abstractmethod
    def evict_partition(self, partition: str) -> None: ...


class InMemoryRecheckBackend(RecheckBackend):
    """按 ``(partition, bucket)`` 组织的确定性回查表。

    仍是有界的：桶随水位推进被整体淘汰，与精确索引/Bloom 的窗口一致。
    """

    def __init__(self) -> None:
        self._buckets: dict[str, dict[int, set[bytes]]] = {}

    def put(self, partition: str, bucket: int, digest: bytes) -> None:
        self._buckets.setdefault(partition, {}).setdefault(bucket, set()).add(digest)

    def exists(self, partition: str, digest: bytes) -> bool:
        for bucket_fps in self._buckets.get(partition, {}).values():
            if digest in bucket_fps:
                return True
        return False

    def evict_buckets_before(self, partition: str, cutoff_bucket: int) -> None:
        part = self._buckets.get(partition)
        if not part:
            return
        for bucket in [b for b in part if b < cutoff_bucket]:
            del part[bucket]
        if not part:
            del self._buckets[partition]

    def evict_partition(self, partition: str) -> None:
        self._buckets.pop(partition, None)

    def fingerprint_count(self, partition: str | None = None) -> int:
        if partition is not None:
            return sum(len(fps) for fps in self._buckets.get(partition, {}).values())
        return sum(
            len(fps)
            for part in self._buckets.values()
            for fps in part.values()
        )

    def bucket_count(self, partition: str | None = None) -> int:
        if partition is not None:
            return len(self._buckets.get(partition, {}))
        return sum(len(part) for part in self._buckets.values())

    def snapshot(self) -> dict[str, dict[int, list[bytes]]]:
        return {
            partition: {
                bucket: sorted(fps) for bucket, fps in buckets.items()
            }
            for partition, buckets in self._buckets.items()
        }

    @classmethod
    def restore(cls, snapshot: dict[str, dict[int, list[bytes]]]) -> "InMemoryRecheckBackend":
        backend = cls()
        for partition, buckets in snapshot.items():
            backend._buckets[partition] = {
                int(bucket): set(fps) for bucket, fps in buckets.items()
            }
        return backend
