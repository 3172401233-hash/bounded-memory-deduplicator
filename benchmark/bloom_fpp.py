"""测量 BloomFilter 索引空间与误判率的关系。

直接运行：``python -m benchmark.bloom_fpp``，结果写入
``artifacts/bloom_fpp_report.txt``（也会打印到标准输出）。
"""

from __future__ import annotations

import os

from stream_dedup import BloomFilter
from stream_dedup.fingerprint import DIGEST_SIZE


def _digest(value: int) -> bytes:
    return value.to_bytes(DIGEST_SIZE, "little")


def measure(expected_items: int, target_fpp: float, load_ratios=(1.0, 2.0, 5.0),
            negative_probes: int = 100_000) -> list[dict]:
    rows = []
    template = BloomFilter(expected_items, target_fpp)
    bits_per_item = template.bit_count / expected_items
    for ratio in load_ratios:
        inserted = int(expected_items * ratio)
        bloom = BloomFilter(expected_items, target_fpp)
        for value in range(inserted):
            bloom.add(_digest(value))
        # 负样本取与插入区间不重叠的键，全部应当返回 False
        false_positives = 0
        for value in range(inserted, inserted + negative_probes):
            if bloom.contains(_digest(value)):
                false_positives += 1
        observed = false_positives / negative_probes
        rows.append(
            {
                "target_fpp": target_fpp,
                "expected_items": expected_items,
                "load_ratio": ratio,
                "inserted": inserted,
                "bytes": bloom.byte_count,
                "bits_per_item_configured": round(bits_per_item, 2),
                "theoretical_fpp": bloom.estimated_fpp(),
                "observed_fpp": observed,
            }
        )
    return rows


def main() -> None:
    lines = [
        "BloomFilter 索引空间与误判率实测",
        "=" * 96,
        f"{'目标FPP':>8} {'期望容量':>8} {'负载比':>6} {'实际插入':>8} "
        f"{'位/元素':>8} {'索引字节':>10} {'理论FPP':>10} {'实测FPP':>10}",
        "-" * 96,
    ]
    for target_fpp in (0.1, 0.01, 0.001):
        for row in measure(10_000, target_fpp):
            lines.append(
                f"{row['target_fpp']:>8.3g} {row['expected_items']:>8d} "
                f"{row['load_ratio']:>6.1f} {row['inserted']:>8d} "
                f"{row['bits_per_item_configured']:>8.2f} {row['bytes']:>10d} "
                f"{row['theoretical_fpp']:>10.2e} {row['observed_fpp']:>10.2e}"
            )
        lines.append("-" * 96)
    lines.append(
        "\n结论：\n"
        "1. 索引大小只由期望容量和目标误判率决定，与实际插入量无关；\n"
        "2. 目标误判率每降低 10 倍，每元素约增加 4.79 bit（-log2(10) 量级）；\n"
        "3. 超容量插入（负载比 >1）不增加内存，但实际误判率随填充率上升，"
        "此时由回查兜底，不会误判为确定重复。"
    )
    report = "\n".join(lines)
    print(report)
    os.makedirs("artifacts", exist_ok=True)
    with open(os.path.join("artifacts", "bloom_fpp_report.txt"), "w", encoding="utf-8") as fh:
        fh.write(report + "\n")


if __name__ == "__main__":
    main()
