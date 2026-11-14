# bounded-memory-deduplicator
Python 有界内存流式去重库，处理分区窗口、重试重复、概率索引和 checkpoint 恢复

## 解决的问题

上游重试会反复投递同一批记录。用一个全局大 `set` 去重时，指纹只增不减，
进程跑得越久内存越高。本库按 **分区键 + 时间窗口** 组织有界状态，
把内存占用限制在“仍可能重试的时间窗口”之内。

## 分层索引

时间按 `window_seconds` 切成不相交的桶，每个分区独立维护：

| 数据年龄 | 结构 | 语义 |
| --- | --- | --- |
| 最近 `exact_age_buckets` 个桶 | 精确指纹集合 | 零误判，命中即确定重复 |
| 更老但仍在 `retained_buckets` 内 | 每桶一个有界 `BloomFilter` | 只会误报、不会漏报 |
| 超出保留窗口 | 桶整体淘汰 | 迟到记录走回查路径 |

**概率阳性必须回查**：Bloom 命中时不会直接判重，而是查询 `RecheckBackend`
（内置有界内存实现，也可替换成 Redis/RocksDB 等外部 KV）。
回查未命中即 Bloom 误报，记录作为新数据接受并补写回查表；
禁用内置后端时结果置为 `recheck`，由 `resolve_recheck()` 交外部裁决。

## 稳定指纹

`fingerprint = blake2b(schema版本 ‖ 主键规范化JSON ‖ 内容摘要)`，其中：

- dict key 排序、Unicode 做 NFC 归一化，记录字段顺序不影响指纹；
- 时间戳字段不参与内容摘要（同内容不同投递时间视为同一记录）；
- schema 版本或主键变化自然隔离，不会跨版本误判。

## 水位、乱序与淘汰

- 每条记录自动推进所属分区水位，也可用 `advance_watermark()` 手动推进；
- 水位推进时老精确桶整体降级为 Bloom，过期桶连同回查条目一起释放；
- 窗口内乱序记录照常精确去重；窗口外迟到记录标记 `late` 并走回查，
  保证不会被静默丢弃或误判。

## Checkpoint

`save_checkpoint()` 以二进制快照落盘（配置、水位、精确集合、Bloom 位向量、
回查表、序列号），临时文件 + 原子替换。`load_checkpoint()` 恢复后：

- 窗口内的精确键和 Bloom 键都不丢失；
- 重放同一 checkpoint 覆盖的输入全部判为 `duplicate`，不会重复输出。

## 用法

```python
from stream_dedup import DedupConfig, Deduplicator, save_checkpoint, load_checkpoint

config = DedupConfig(
    schema_version=1,
    partition_key=("region",),
    primary_key=("id",),
    timestamp_field="ts",
    window_seconds=60,        # 桶大小
    exact_age_buckets=5,      # 最近 5 个桶走精确索引
    retained_buckets=60,      # 总保留 60 个桶，更老的桶淘汰
    bloom_expected_items=10_000,
    bloom_fpp=0.01,
)
deduplicator = Deduplicator(config)
result = deduplicator.process(record)
if result.status == "new":
    ...  # 下游输出
save_checkpoint(deduplicator, "/data/checkpoint")
deduplicator = load_checkpoint("/data/checkpoint")
```

## 索引空间与误判率

运行 `python -m benchmark.bloom_fpp`（报告写入 `artifacts/bloom_fpp_report.txt`）。
期望容量 10000 时实测：

| 目标 FPP | 位/元素 | 索引字节 | 满载实测 FPP |
| --- | --- | --- | --- |
| 0.1 | 4.79 | 5,991 | 1.02e-01 |
| 0.01 | 9.59 | 11,982 | 1.04e-02 |
| 0.001 | 14.38 | 17,972 | 9.80e-04 |

- 索引大小只由期望容量和目标 FPP 决定，与实际插入量无关，超容量只抬高 FPP；
- 目标 FPP 每降 10 倍，每元素约增加 4.79 bit；
- 任何 Bloom 误报都由回查兜底，不会产生“确定重复”的错误判定。

## 测试

`python -m unittest`，覆盖高基数分区、数据倾斜、窗口边界、重复投递、
误报回查、过期淘汰、乱序记录与中断恢复。
