from __future__ import annotations

import json
import os
import struct

from .bloom import BloomFilter
from .config import DedupConfig
from .deduplicator import BucketState, Deduplicator, PartitionState
from .recheck import InMemoryRecheckBackend

MAGIC_META = b"SDMETA01"
MAGIC_PART = b"SDPART01"
MAGIC_RECH = b"SDRECH01"


class CheckpointError(RuntimeError):
    pass


def _pack_text(text: str) -> bytes:
    raw = text.encode("utf-8")
    return struct.pack("<I", len(raw)) + raw


def _unpack_text(buf: bytes, offset: int) -> tuple[str, int]:
    (length,) = struct.unpack_from("<I", buf, offset)
    offset += 4
    text = buf[offset : offset + length].decode("utf-8")
    return text, offset + length


def _atomic_write(path: str, payload: bytes) -> None:
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "wb") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def save_checkpoint(deduplicator: Deduplicator, directory: str) -> str:
    """把全部去重状态落盘。

    快照包含配置、水位、每个桶（精确集合或 Bloom 位向量）以及内置回查表，
    因此重放同一个 checkpoint 不会产生重复输出。
    """
    if not isinstance(deduplicator.backend, (InMemoryRecheckBackend, type(None))):
        raise CheckpointError(
            "自定义 RecheckBackend 无法由本库序列化，请自行持久化后用 "
            "Deduplicator(config, recheck_backend=...) 装配"
        )

    os.makedirs(directory, exist_ok=True)

    part_payload = bytearray(MAGIC_PART)
    part_payload += struct.pack("<I", len(deduplicator.partitions))
    for partition, state in deduplicator.partitions.items():
        part_payload += _pack_text(partition)
        part_payload += struct.pack("<QI", state.watermark, len(state.buckets))
        for bucket_id, bucket in sorted(state.buckets.items()):
            if bucket.is_bloom:
                bloom = bucket.bloom
                flags = 1
                part_payload += struct.pack("<QI", bucket_id, flags)
                part_payload += struct.pack(
                    "<IIQI",
                    bloom.expected_items,
                    bloom.hash_count,
                    bloom.count,
                    bloom.byte_count,
                )
                part_payload += bloom.to_bytes()
            else:
                part_payload += struct.pack("<QI", bucket_id, 0)
                part_payload += struct.pack("<I", len(bucket.exact))
                for digest in sorted(bucket.exact):
                    part_payload += digest
    _atomic_write(os.path.join(directory, "partition.state"), bytes(part_payload))

    rech_payload = bytearray(MAGIC_RECH)
    backend = deduplicator.backend
    if backend is None:
        rech_payload += struct.pack("<I", 0)
    else:
        snapshot = backend.snapshot()
        rech_payload += struct.pack("<I", len(snapshot))
        for partition, buckets in sorted(snapshot.items()):
            rech_payload += _pack_text(partition)
            rech_payload += struct.pack("<I", len(buckets))
            for bucket_id, digests in sorted(buckets.items()):
                rech_payload += struct.pack("<QI", bucket_id, len(digests))
                for digest in digests:
                    rech_payload += digest
    _atomic_write(os.path.join(directory, "recheck.state"), bytes(rech_payload))

    meta = {
        "magic": MAGIC_META.decode("ascii"),
        "config": deduplicator.config.to_dict(),
        "sequence": deduplicator.sequence,
        "has_backend": backend is not None,
    }
    _atomic_write(
        os.path.join(directory, "meta.json"),
        json.dumps(meta, ensure_ascii=False, sort_keys=True).encode("utf-8"),
    )
    return directory


def _read_bytes(path: str) -> bytes:
    with open(path, "rb") as fh:
        return fh.read()


def load_checkpoint(directory: str,
                    recheck_backend=None) -> Deduplicator:
    """从 checkpoint 恢复，窗口内的精确键和 Bloom 键都不会丢失。"""
    meta_path = os.path.join(directory, "meta.json")
    with open(meta_path, "rb") as fh:
        meta = json.loads(fh.read().decode("utf-8"))
    if meta.get("magic") != MAGIC_META.decode("ascii"):
        raise CheckpointError("meta.json 魔数不匹配，目录不是合法 checkpoint")

    config = DedupConfig.from_dict(meta["config"])
    has_backend = meta["has_backend"]
    if recheck_backend is not None:
        backend_arg = recheck_backend
    elif has_backend:
        backend_arg = None  # 构造后由 recheck.state 恢复内置后端
    else:
        backend_arg = False
    deduplicator = Deduplicator(config, recheck_backend=backend_arg)

    part_buf = _read_bytes(os.path.join(directory, "partition.state"))
    if part_buf[:8] != MAGIC_PART:
        raise CheckpointError("partition.state 魔数不匹配")
    offset = 8
    (partition_count,) = struct.unpack_from("<I", part_buf, offset)
    offset += 4
    for _ in range(partition_count):
        partition, offset = _unpack_text(part_buf, offset)
        watermark, bucket_count = struct.unpack_from("<QI", part_buf, offset)
        offset += 12
        state = PartitionState(watermark=watermark)
        for _ in range(bucket_count):
            bucket_id, flags = struct.unpack_from("<QI", part_buf, offset)
            offset += 12
            bucket = BucketState(bucket_id)
            if flags == 1:
                expected, hash_count, item_count, byte_count = struct.unpack_from(
                    "<IIQI", part_buf, offset
                )
                offset += 20
                bits = part_buf[offset : offset + byte_count]
                offset += byte_count
                bucket.bloom = BloomFilter.from_state(
                    expected, config.bloom_fpp, bits, hash_count, item_count
                )
            else:
                (fp_count,) = struct.unpack_from("<I", part_buf, offset)
                offset += 4
                for _ in range(fp_count):
                    bucket.exact.add(part_buf[offset : offset + 16])
                    offset += 16
            state.buckets[bucket_id] = bucket
        deduplicator.partitions[partition] = state

    rech_buf = _read_bytes(os.path.join(directory, "recheck.state"))
    if rech_buf[:8] != MAGIC_RECH:
        raise CheckpointError("recheck.state 魔数不匹配")
    offset = 8
    (part_count,) = struct.unpack_from("<I", rech_buf, offset)
    offset += 4
    snapshot: dict[str, dict[int, list[bytes]]] = {}
    for _ in range(part_count):
        partition, offset = _unpack_text(rech_buf, offset)
        (bucket_count,) = struct.unpack_from("<I", rech_buf, offset)
        offset += 4
        snapshot[partition] = {}
        for _ in range(bucket_count):
            bucket_id, digest_count = struct.unpack_from("<QI", rech_buf, offset)
            offset += 12
            digests = []
            for _ in range(digest_count):
                digests.append(rech_buf[offset : offset + 16])
                offset += 16
            snapshot[partition][bucket_id] = digests

    if has_backend and recheck_backend is None:
        deduplicator.backend = InMemoryRecheckBackend.restore(snapshot)

    deduplicator.sequence = meta["sequence"]
    return deduplicator
