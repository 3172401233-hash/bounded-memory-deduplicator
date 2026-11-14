import os
import shutil
import tempfile
import unittest

from stream_dedup import (
    BloomFilter,
    DedupConfig,
    Deduplicator,
    InMemoryRecheckBackend,
    Status,
    load_checkpoint,
    save_checkpoint,
)


def make_config(**overrides):
    base = dict(
        schema_version=1,
        partition_key=("region",),
        primary_key=("id",),
        timestamp_field="ts",
        window_seconds=10,
        exact_age_buckets=2,
        retained_buckets=5,
        bloom_expected_items=1024,
        bloom_fpp=0.01,
    )
    base.update(overrides)
    return DedupConfig(**base)


def record(rid, ts, region="cn", **extra):
    payload = {"id": rid, "region": region, "ts": ts}
    payload.update(extra)
    return payload


class BasicDedupTest(unittest.TestCase):
    def test_exact_retry_is_duplicate(self):
        dedup = Deduplicator(make_config())
        first = dedup.process(record(1, 100))
        retry = dedup.process(record(1, 100))
        self.assertEqual(first.status, Status.NEW)
        self.assertEqual(retry.status, Status.DUPLICATE)
        self.assertEqual(dedup.stats["new"], 1)
        self.assertEqual(dedup.stats["duplicate"], 1)

    def test_partitions_are_isolated(self):
        dedup = Deduplicator(make_config())
        self.assertEqual(dedup.process(record(1, 100, region="cn")).status, Status.NEW)
        same_key_other_partition = dedup.process(record(1, 100, region="us"))
        self.assertEqual(same_key_other_partition.status, Status.NEW)
        self.assertEqual(
            dedup.process(record(1, 100, region="us")).status, Status.DUPLICATE
        )


class WindowBoundaryTest(unittest.TestCase):
    def test_exact_to_bloom_transition_and_recheck(self):
        config = make_config()
        dedup = Deduplicator(config)
        # 桶 10 (100-109)
        dedup.process(record("a", 100))
        # 水位推到桶 11，exact cutoff = 11-2+1 = 10，桶 10 仍在精确层
        dedup.process(record("marker", 115))
        self.assertFalse(dedup.partitions['["cn"]'].buckets[10].is_bloom)
        # 水位推到桶 12，exact cutoff = 11，桶 10 降级为 Bloom
        dedup.process(record("marker2", 125))
        bucket_10 = dedup.partitions['["cn"]'].buckets[10]
        self.assertTrue(bucket_10.is_bloom)
        self.assertEqual(len(bucket_10.exact), 0)

        # Bloom 命中 -> 回查内置后端 -> 确认是真重复
        retry = dedup.process(record("a", 100))
        self.assertTrue(retry.bloom_positive)
        self.assertEqual(retry.status, Status.DUPLICATE)

    def test_expired_bucket_eviction(self):
        config = make_config()
        dedup = Deduplicator(config)
        dedup.process(record("a", 100))  # 桶 10
        # 水位推到桶 14：retain cutoff = 14-5+1 = 10，桶 10 仍保留
        dedup.process(record("m1", 145))
        self.assertIn(10, dedup.partitions['["cn"]'].buckets)
        # 水位推到桶 15：cutoff = 11，桶 10 被淘汰
        dedup.process(record("m2", 155))
        self.assertNotIn(10, dedup.partitions['["cn"]'].buckets)

    def test_late_record_beyond_window_goes_recheck_path(self):
        config = make_config()
        dedup = Deduplicator(config)
        dedup.process(record("a", 100))
        dedup.process(record("m", 155))  # 桶 10 被淘汰
        late_retry = dedup.process(record("a", 100))
        self.assertTrue(late_retry.late)
        # 内置回查表仍持有该指纹（随桶一起淘汰过，但原指纹已被移除）
        self.assertEqual(late_retry.status, Status.NEW)
        # 再来一次相同的迟到记录，这次回查表能命中
        second_late = dedup.process(record("a", 100))
        self.assertTrue(second_late.late)
        self.assertEqual(second_late.status, Status.DUPLICATE)


class OutOfOrderTest(unittest.TestCase):
    def test_out_of_order_within_window_still_exact(self):
        config = make_config()
        dedup = Deduplicator(config)
        dedup.process(record("high", 135))  # 水位桶 13
        late = dedup.process(record("low", 105))  # 桶 10，仍在保留窗口内
        self.assertEqual(late.status, Status.NEW)
        self.assertFalse(late.late)
        self.assertEqual(dedup.process(record("low", 105)).status, Status.DUPLICATE)


class BloomFalsePositiveTest(unittest.TestCase):
    def _colliding_filter(self, config):
        # 极小 Bloom：1 字节、1 个哈希，插入一个指纹后很快出现位碰撞，
        # 从而确定性地制造 Bloom 阳性但回查未命中的误报场景。
        victim = BloomFilter(8, 0.5)
        victim.bits = bytearray(1)
        victim.bit_count = 8
        victim.byte_count = 1
        victim.hash_count = 1
        return victim

    def test_bloom_positive_without_backend_requires_resolution(self):
        config = make_config(bloom_expected_items=8, bloom_fpp=0.5)
        dedup = Deduplicator(config, recheck_backend=False)
        dedup.process(record("a", 100))
        # 手工把桶 10 降级成 1 字节 Bloom，写入 "a" 的指纹
        from stream_dedup.fingerprint import fingerprint
        state = dedup.partitions['["cn"]']
        digest_a = fingerprint(record("a", 100), config)
        bloom = self._colliding_filter(config)
        bloom.add(digest_a)
        state.buckets[10].exact.clear()
        state.buckets[10].bloom = bloom

        collided = None
        for idx in range(2000):
            candidate = record(f"fp{idx}", 100)
            if bloom.contains(fingerprint(candidate, config)):
                collided = candidate
                break
        self.assertIsNotNone(collided, "1 字节单哈希 Bloom 应当能制造碰撞")
        unknown = dedup.process(collided)
        self.assertTrue(unknown.bloom_positive)
        self.assertEqual(unknown.status, Status.RECHECK)
        dedup.resolve_recheck(unknown, is_duplicate=False)
        self.assertEqual(unknown.status, Status.NEW)
        # 外部裁决为真重复的路径
        duplicate = dedup.process(record("a", 100))
        self.assertEqual(duplicate.status, Status.RECHECK)
        dedup.resolve_recheck(duplicate, is_duplicate=True)
        self.assertEqual(duplicate.status, Status.DUPLICATE)

    def test_bloom_false_positive_is_accepted_via_recheck(self):
        config = make_config(bloom_expected_items=8, bloom_fpp=0.5)
        dedup = Deduplicator(config)
        dedup.process(record("a", 100))
        from stream_dedup.fingerprint import fingerprint
        state = dedup.partitions['["cn"]']
        digest_a = fingerprint(record("a", 100), config)
        bloom = self._colliding_filter(config)
        bloom.add(digest_a)
        state.buckets[10].exact.clear()
        state.buckets[10].bloom = bloom

        # 穷举若干主键，找到一个与 "a" 位碰撞、但回查表不存在的新指纹
        collided = None
        for idx in range(2000):
            candidate = record(f"fp{idx}", 100)
            candidate_digest = fingerprint(candidate, config)
            if bloom.contains(candidate_digest) and not dedup.backend.exists(
                '["cn"]', candidate_digest
            ):
                collided = candidate
                break
        self.assertIsNotNone(collided, "1 字节单哈希 Bloom 应当能制造碰撞")
        result = dedup.process(collided)
        self.assertTrue(result.bloom_positive)
        self.assertEqual(result.status, Status.NEW)
        self.assertEqual(dedup.stats["bloom_false_positives"], 1)
        # 误报被接受后，再次投递应被回查确认为重复
        self.assertEqual(dedup.process(collided).status, Status.DUPLICATE)


class HighCardinalityAndSkewTest(unittest.TestCase):
    def test_high_cardinality_partitions_keep_bounded_buckets(self):
        config = make_config()
        dedup = Deduplicator(config)
        for part in range(200):
            for rid in range(50):
                dedup.process(record(f"p{part}-{rid}", 100, region=f"r{part}"))
        report = dedup.partition_stats()
        self.assertEqual(len(report), 200)
        # 每个分区只有一个桶，桶数量与分区内基数无关
        self.assertTrue(all(row["bucket_count"] == 1 for row in report.values()))
        self.assertEqual(dedup.stats["duplicate"], 0)

    def test_skewed_partition_bloom_bytes_stay_bounded(self):
        config = make_config(bloom_expected_items=1000, bloom_fpp=0.02)
        dedup = Deduplicator(config)
        # 热分区先灌大量数据
        for rid in range(5000):
            dedup.process(record(f"hot{rid}", 100, region="hot"))
        # 推进水位让桶 10 降级为 Bloom
        dedup.advance_watermark(135)
        bloom_bucket = dedup.partitions['["hot"]'].buckets[10]
        self.assertTrue(bloom_bucket.is_bloom)
        bounded_bytes = bloom_bucket.bloom.byte_count
        # Bloom 空间由配置决定，与实际插入 5000 条（远超期望 1000）无关
        self.assertLessEqual(bounded_bytes, 10_000)
        # 超容量只抬高误判率，不抬高内存
        self.assertGreater(bloom_bucket.bloom.estimated_fpp(), config.bloom_fpp)

        # 冷分区同样只占一个 Bloom 桶
        for rid in range(10):
            dedup.process(record(f"cold{rid}", 100, region="cold"))
        dedup.advance_watermark(135)
        self.assertTrue(dedup.partitions['["cold"]'].buckets[10].is_bloom)


class RepeatedDeliveryTest(unittest.TestCase):
    def test_repeated_redelivery_across_tiers(self):
        config = make_config()
        dedup = Deduplicator(config)
        rec = record("k1", 100)
        self.assertEqual(dedup.process(rec).status, Status.NEW)
        for _ in range(3):
            self.assertEqual(dedup.process(rec).status, Status.DUPLICATE)
        dedup.advance_watermark(135)  # 降级到 Bloom
        for _ in range(3):
            result = dedup.process(rec)
            self.assertEqual(result.status, Status.DUPLICATE)
            self.assertTrue(result.bloom_positive)
        self.assertEqual(dedup.stats["new"], 1)
        self.assertEqual(dedup.stats["duplicate"], 6)


class CheckpointTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="stream-dedup-cp-")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_restore_keeps_in_window_keys(self):
        config = make_config()
        dedup = Deduplicator(config)
        dedup.process(record("a", 100))
        dedup.process(record("b", 105))
        dedup.advance_watermark(135)  # 桶 10 进入 Bloom 层
        self.assertTrue(dedup.partitions['["cn"]'].buckets[10].is_bloom)
        save_checkpoint(dedup, self.dir)

        restored = load_checkpoint(self.dir)
        self.assertEqual(restored.sequence, dedup.sequence)
        bucket = restored.partitions['["cn"]'].buckets[10]
        self.assertTrue(bucket.is_bloom)
        # Bloom 键 + 回查表都恢复，重试仍判重
        self.assertEqual(restored.process(record("a", 100)).status, Status.DUPLICATE)
        self.assertEqual(restored.process(record("b", 105)).status, Status.DUPLICATE)

    def test_replay_same_checkpoint_emits_no_duplicates(self):
        config = make_config()
        dedup = Deduplicator(config)
        batch = [record(rid, 100 + rid) for rid in range(20)]
        for rec in batch:
            self.assertEqual(dedup.process(rec).status, Status.NEW)
        save_checkpoint(dedup, self.dir)

        # 模拟崩溃后从同一 checkpoint 重放整批输入
        restored = load_checkpoint(self.dir)
        statuses = [restored.process(rec).status for rec in batch]
        self.assertTrue(all(s == Status.DUPLICATE for s in statuses))
        self.assertEqual(restored.stats["new"], 0)

    def test_checkpoint_after_eviction_and_late_replay(self):
        config = make_config()
        dedup = Deduplicator(config)
        dedup.process(record("old", 100))
        dedup.process(record("new", 155))  # 桶 10 淘汰，桶 15 精确
        save_checkpoint(dedup, self.dir)
        restored = load_checkpoint(self.dir)
        self.assertNotIn(10, restored.partitions['["cn"]'].buckets)
        self.assertEqual(restored.process(record("new", 155)).status, Status.DUPLICATE)

    def test_checkpoint_works_without_backend(self):
        config = make_config()
        dedup = Deduplicator(config, recheck_backend=False)
        dedup.process(record("a", 100))
        save_checkpoint(dedup, self.dir)
        restored = load_checkpoint(self.dir)
        self.assertIsNone(restored.backend)
        self.assertEqual(restored.process(record("a", 100)).status, Status.DUPLICATE)


if __name__ == "__main__":
    unittest.main()
