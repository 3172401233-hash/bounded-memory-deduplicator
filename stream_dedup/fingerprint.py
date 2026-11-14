from __future__ import annotations

import hashlib
import json
import unicodedata
from typing import Any

from .config import DedupConfig

DIGEST_SIZE = 16
SEP = b"\x1f"


def canonicalize(value: Any) -> Any:
    """把值转成与输入顺序无关、Unicode 归一化后的规范形式。

    规则：

    - dict 按 key 排序后递归规范化；
    - list/tuple 保持顺序（顺序属于内容的一部分）；
    - str 做 NFC 归一化；
    - 其他 JSON 原生类型原样返回。
    """
    if isinstance(value, dict):
        return {key: canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [canonicalize(item) for item in value]
    if isinstance(value, tuple):
        return [canonicalize(item) for item in value]
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    raise TypeError(f"不支持纳入稳定指纹的类型: {type(value).__name__}")


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        canonicalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def content_digest(record: dict, config: DedupConfig) -> bytes:
    """规范化后的内容摘要，供测试与回查比对使用。"""
    content = {
        key: record[key] for key in record if key != config.timestamp_field
    }
    return hashlib.blake2b(canonical_json(content), digest_size=DIGEST_SIZE).digest()


def fingerprint(record: dict, config: DedupConfig) -> bytes:
    """生成稳定指纹：schema 版本 + 主键 + 规范化内容摘要。

    三段通过单元分隔符 ``0x1f`` 拼接，避免字段边界歧义。
    指纹是纯函数：语义相同的记录（dict key 顺序、Unicode 等价形式不同）
    得到相同指纹；schema 版本或主键变化会得到不同指纹。
    """
    primary = [record[key] for key in config.primary_key]
    parts = [
        str(config.schema_version).encode("utf-8"),
        canonical_json(primary),
        content_digest(record, config),
    ]
    joined = SEP.join(parts)
    return hashlib.blake2b(joined, digest_size=DIGEST_SIZE).digest()


def partition_id(record: dict, config: DedupConfig) -> str:
    values = [record[key] for key in config.partition_key]
    return canonical_json(values).decode("utf-8")
