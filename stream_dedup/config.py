from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DedupConfig:
    """流式去重器的不可变配置。

    时间被切成长度为 ``window_seconds`` 的不相交桶（bucket）。

    - 最近的桶保存精确指纹集合，零误判；
    - 超过 ``exact_age_buckets`` 个桶但仍在保留窗口内的老数据，
      转为每个桶一个有界 BloomFilter，只可能误报、不可能漏报；
    - BloomFilter 报阳性时必须进入回查（recheck），不会被直接判定为重复；
    - 超过 ``retained_buckets`` 个桶的数据被淘汰，相关记录改走回查。
    """

    schema_version: int
    partition_key: tuple[str, ...]
    primary_key: tuple[str, ...]
    timestamp_field: str
    window_seconds: int = 60
    exact_age_buckets: int = 5
    retained_buckets: int = 60
    bloom_expected_items: int = 10_000
    bloom_fpp: float = 0.01

    def __post_init__(self) -> None:
        if not self.partition_key:
            raise ValueError("partition_key 不能为空")
        if not self.primary_key:
            raise ValueError("primary_key 不能为空")
        if not self.timestamp_field:
            raise ValueError("timestamp_field 不能为空")
        if self.window_seconds <= 0:
            raise ValueError("window_seconds 必须为正整数")
        if self.exact_age_buckets <= 0:
            raise ValueError("exact_age_buckets 必须为正整数")
        if self.retained_buckets < self.exact_age_buckets:
            raise ValueError("retained_buckets 不能小于 exact_age_buckets")
        if self.bloom_expected_items <= 0:
            raise ValueError("bloom_expected_items 必须为正整数")
        if not 0.0 < self.bloom_fpp < 1.0:
            raise ValueError("bloom_fpp 必须落在 (0, 1) 之间")

    def bucket_id(self, timestamp: int | float) -> int:
        return int(timestamp) // self.window_seconds

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "partition_key": list(self.partition_key),
            "primary_key": list(self.primary_key),
            "timestamp_field": self.timestamp_field,
            "window_seconds": self.window_seconds,
            "exact_age_buckets": self.exact_age_buckets,
            "retained_buckets": self.retained_buckets,
            "bloom_expected_items": self.bloom_expected_items,
            "bloom_fpp": self.bloom_fpp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DedupConfig":
        return cls(
            schema_version=data["schema_version"],
            partition_key=tuple(data["partition_key"]),
            primary_key=tuple(data["primary_key"]),
            timestamp_field=data["timestamp_field"],
            window_seconds=data["window_seconds"],
            exact_age_buckets=data["exact_age_buckets"],
            retained_buckets=data["retained_buckets"],
            bloom_expected_items=data["bloom_expected_items"],
            bloom_fpp=data["bloom_fpp"],
        )
