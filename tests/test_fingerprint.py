import unittest

from stream_dedup import DedupConfig, canonicalize, content_digest, fingerprint


def make_config(**overrides):
    base = dict(
        schema_version=1,
        partition_key=("region",),
        primary_key=("id",),
        timestamp_field="ts",
    )
    base.update(overrides)
    return DedupConfig(**base)


class FingerprintTest(unittest.TestCase):
    def test_key_order_and_unicode_equivalence(self):
        config = make_config()
        record_a = {"id": "u1", "name": "café", "nested": {"x": 1, "y": 2}, "ts": 100}
        record_b = {"nested": {"y": 2, "x": 1}, "name": "cafe\u0301", "id": "u1", "ts": 100}
        self.assertEqual(fingerprint(record_a, config), fingerprint(record_b, config))

    def test_content_change_changes_fingerprint(self):
        config = make_config()
        record_a = {"id": "u1", "v": 1, "ts": 100}
        record_b = {"id": "u1", "v": 2, "ts": 100}
        self.assertNotEqual(fingerprint(record_a, config), fingerprint(record_b, config))

    def test_schema_version_isolated(self):
        record = {"id": "u1", "v": 1, "ts": 100}
        self.assertNotEqual(
            fingerprint(record, make_config(schema_version=1)),
            fingerprint(record, make_config(schema_version=2)),
        )

    def test_primary_key_change_isolated(self):
        record_a = {"id": "u1", "v": 1, "ts": 100}
        record_b = {"id": "u2", "v": 1, "ts": 100}
        config = make_config()
        self.assertNotEqual(
            fingerprint(record_a, config), fingerprint(record_b, config)
        )

    def test_timestamp_excluded_from_content_digest(self):
        config = make_config()
        record_a = {"id": "u1", "v": 1, "ts": 100}
        record_b = {"id": "u1", "v": 1, "ts": 999}
        self.assertEqual(
            content_digest(record_a, config), content_digest(record_b, config)
        )
        self.assertEqual(
            fingerprint(record_a, config), fingerprint(record_b, config)
        )

    def test_canonicalize_sorts_nested_dicts(self):
        self.assertEqual(
            canonicalize({"b": [3, 2], "a": {"z": 1, "y": 2}}),
            canonicalize({"a": {"y": 2, "z": 1}, "b": [3, 2]}),
        )


if __name__ == "__main__":
    unittest.main()
