from __future__ import annotations

import hashlib
import math

from .fingerprint import DIGEST_SIZE


def _optimal_bits(expected_items: int, fpp: float) -> int:
    bits = math.ceil(-expected_items * math.log(fpp) / (math.log(2) ** 2))
    return max(bits, 8)


def _optimal_hashes(expected_items: int, bit_count: int) -> int:
    return max(1, round(bit_count / expected_items * math.log(2)))


class BloomFilter:
    """固定容量位数组上的 Bloom Filter。

    空间与已插入元素数量无关，只由期望元素数和目标误判率决定，
    因此是“有界概率结构”：超容量插入只会抬高误判率，不会抬高内存。
    使用 Kirsch–Mitzenmacher 双哈希，从一个 16 字节指纹派生全部哈希。
    """

    def __init__(self, expected_items: int, fpp: float, bits: bytearray | None = None,
                 hash_count: int | None = None) -> None:
        self.expected_items = expected_items
        self.target_fpp = fpp
        bit_count = _optimal_bits(expected_items, fpp)
        self.bit_count = max(bit_count, (len(bits) * 8) if bits else 0)
        self.byte_count = (self.bit_count + 7) // 8
        self.hash_count = hash_count or _optimal_hashes(expected_items, self.bit_count)
        self.bits = bits if bits is not None else bytearray(self.byte_count)
        if len(self.bits) != self.byte_count:
            raise ValueError("恢复的位数组长度与配置不匹配")
        self.count = 0

    @staticmethod
    def _split_hash(digest: bytes) -> tuple[int, int]:
        # 对指纹做两次带不同域分隔的独立哈希派生，保证 h1/h2 相互独立，
        # 符合 Kirsch–Mitzenmacher 双哈希对独立性的要求
        h1 = int.from_bytes(hashlib.blake2b(digest, person=b"sdh1", digest_size=16).digest(), "little")
        h2 = int.from_bytes(hashlib.blake2b(digest, person=b"sdh2", digest_size=16).digest(), "little")
        if h2 == 0:
            h2 = 0x9E3779B97F4A7C15
        return h1, h2

    def _indexes(self, digest: bytes) -> list[int]:
        h1, h2 = self._split_hash(digest)
        return [(h1 + i * h2) % self.bit_count for i in range(self.hash_count)]

    def add(self, digest: bytes) -> None:
        for idx in self._indexes(digest):
            self.bits[idx >> 3] |= 1 << (idx & 7)
        self.count += 1

    def contains(self, digest: bytes) -> bool:
        for idx in self._indexes(digest):
            if not (self.bits[idx >> 3] & (1 << (idx & 7))):
                return False
        return True

    def estimated_fpp(self) -> float:
        """当前实际占用下的理论误判率 ``(1-e^(-kn/m))^k``。"""
        if self.count == 0:
            return 0.0
        fill = 1.0 - math.exp(-self.hash_count * self.count / self.bit_count)
        return fill ** self.hash_count

    def to_bytes(self) -> bytes:
        return bytes(self.bits)

    @classmethod
    def from_state(cls, expected_items: int, fpp: float, bits: bytes,
                   hash_count: int, count: int) -> "BloomFilter":
        filter_ = cls(expected_items, fpp, bits=bytearray(bits), hash_count=hash_count)
        filter_.count = count
        return filter_
