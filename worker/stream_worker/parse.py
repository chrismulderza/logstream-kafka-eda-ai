"""Normalize PCP / raw-metrics JSON into logs and disk samples."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterator

USED_NAMES = (
    "filesys.used",
    "filesys.used_bytes",
    "disk.used",
    "disk.used_bytes",
    "fs.used",
    "used_bytes",
    "disk_used",
    "used",
)
CAPACITY_NAMES = (
    "filesys.capacity",
    "filesys.size",
    "disk.total",
    "disk.capacity",
    "fs.capacity",
    "capacity_bytes",
    "disk_total",
    "capacity",
    "total",
)
PERCENT_NAMES = ("filesys.full", "filesys.used_percent", "disk_percent", "used_percent")


def parse_timestamp(payload: dict[str, Any], fallback: float) -> float:
    for key in ("@timestamp", "timestamp", "time", "ts"):
        if key not in payload:
            continue
        value = payload[key]
        parsed = _coerce_ts(value)
        if parsed is not None:
            return parsed
    return fallback


def _coerce_ts(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            return ts / 1000.0
        return ts
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            numeric = float(text)
            return _coerce_ts(numeric)
        except ValueError:
            pass
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            return datetime.fromisoformat(text).timestamp()
        except ValueError:
            return None
    return None


def host_from(payload: dict[str, Any]) -> str:
    for key in ("host", "hostname", "@sourcehost", "source", "nodename", "node"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            name = value.get("name") or value.get("hostname")
            if isinstance(name, str) and name.strip():
                return name.strip()
    return "unknown"


def extract_log(payload: dict[str, Any]) -> dict[str, Any] | None:
    message = payload.get("message") or payload.get("msg") or payload.get("log")
    if not isinstance(message, str) or not message.strip():
        return None
    if _looks_like_metric_document(payload) and not payload.get("syslogtag"):
        return None
    return {
        "host": host_from(payload),
        "message": message.strip(),
        "syslogtag": str(payload.get("syslogtag") or payload.get("tag") or ""),
        "facility": str(payload.get("facility") or ""),
        "severity": str(payload.get("severity") or payload.get("level") or ""),
    }


def extract_disk_samples(
    payload: dict[str, Any], timestamp: float
) -> list[dict[str, Any]]:
    host = host_from(payload)
    samples: list[dict[str, Any]] = []

    for item in _named_metric_items(payload):
        sample = _sample_from_named(host, timestamp, item, payload)
        if sample:
            samples.append(sample)

    by_name = _metric_maps(payload)
    used_map = _first_map(by_name, USED_NAMES)
    cap_map = _first_map(by_name, CAPACITY_NAMES)
    pct_map = _first_map(by_name, PERCENT_NAMES)

    instances = set(used_map) | set(cap_map) | set(pct_map)
    if not instances and _scalar_pair(payload):
        used, capacity = _scalar_pair(payload)  # type: ignore[misc]
        samples.append(
            {
                "host": host,
                "instance": str(payload.get("instance") or payload.get("mount") or "/"),
                "used": used,
                "capacity": capacity,
                "timestamp": timestamp,
            }
        )
        return _dedupe(samples)

    for instance in instances or ("/"):
        used = used_map.get(instance)
        capacity = cap_map.get(instance)
        pct = pct_map.get(instance)
        if used is None and pct is not None:
            used, capacity = float(pct), 100.0 if float(pct) > 1.5 else 1.0
        avail = by_name.get("filesys.avail", {}).get(instance)
        if used is not None and capacity is None and avail is not None:
            capacity = float(used) + float(avail)
        if used is not None and capacity is not None and capacity < used and avail is not None:
            capacity = float(used) + float(avail)
        if used is None or capacity is None:
            continue
        if capacity <= 0:
            continue
        samples.append(
            {
                "host": host,
                "instance": str(instance),
                "used": float(used),
                "capacity": float(capacity),
                "timestamp": timestamp,
            }
        )
    return _dedupe(samples)


def _looks_like_metric_document(payload: dict[str, Any]) -> bool:
    keys = set(payload)
    metric_hints = {
        "metrics",
        "values",
        "used",
        "used_bytes",
        "capacity",
        "capacity_bytes",
        "filesys",
        "disk",
    }
    return bool(keys & metric_hints)


def _named_metric_items(payload: dict[str, Any]) -> Iterator[dict[str, Any]]:
    for key in ("values", "metrics", "data"):
        block = payload.get(key)
        if isinstance(block, list):
            for item in block:
                if isinstance(item, dict):
                    yield item


def _sample_from_named(
    host: str,
    timestamp: float,
    item: dict[str, Any],
    parent: dict[str, Any],
) -> dict[str, Any] | None:
    name = str(item.get("name") or item.get("metric") or "")
    instance = str(item.get("instance") or item.get("inst") or parent.get("mount") or "/")
    value = _to_float(item.get("value") if "value" in item else item.get("val"))
    if value is None:
        return None
    used = capacity = None
    lname = name.lower()
    if any(n in lname for n in ("used",)):
        used = value
    if any(n in lname for n in ("capacity", "size", "total")):
        capacity = value
    if used is not None and capacity is None:
        sibling_cap = _to_float(item.get("capacity") or item.get("total"))
        capacity = sibling_cap
    if used is None or capacity is None or capacity <= 0:
        return None
    return {
        "host": host,
        "instance": instance,
        "used": used,
        "capacity": capacity,
        "timestamp": timestamp,
    }


def _metric_maps(payload: dict[str, Any]) -> dict[str, dict[str, float]]:
    found: dict[str, dict[str, float]] = {}
    _walk_metrics(payload, found)
    for item in _named_metric_items(payload):
        name = str(item.get("name") or item.get("metric") or "")
        if not name:
            continue
        instance = str(item.get("instance") or item.get("inst") or "/")
        value = _to_float(item.get("value") if "value" in item else item.get("val"))
        if value is None:
            continue
        found.setdefault(name, {})[instance] = value
    return found


def _walk_metrics(node: Any, found: dict[str, dict[str, float]], path: str = "") -> None:
    if isinstance(node, dict):
        numeric_children = {
            str(k): _to_float(v) for k, v in node.items() if _to_float(v) is not None
        }
        if path and numeric_children and len(numeric_children) == len(node):
            found.setdefault(path, {}).update(
                {inst: val for inst, val in numeric_children.items() if val is not None}
            )
            return
        for key, value in node.items():
            if str(key).startswith("@"):
                continue
            child_path = f"{path}.{key}" if path else str(key)
            if _to_float(value) is not None and path in {"filesys", "disk", "fs", ""}:
                found.setdefault(child_path, {}).setdefault("/", _to_float(value))  # type: ignore[arg-type]
            _walk_metrics(value, found, child_path)
    elif isinstance(node, list):
        for item in node:
            _walk_metrics(item, found, path)


def _first_map(
    by_name: dict[str, dict[str, float]], names: tuple[str, ...]
) -> dict[str, float]:
    for name in names:
        if name in by_name and by_name[name]:
            return by_name[name]
        for key, value in by_name.items():
            if key.endswith(name) or key == name:
                return value
    return {}


def _scalar_pair(payload: dict[str, Any]) -> tuple[float, float] | None:
    used = None
    capacity = None
    for key, value in payload.items():
        lkey = str(key).lower()
        number = _to_float(value)
        if number is None:
            continue
        if lkey in {"used", "used_bytes", "disk_used", "filesys.used"}:
            used = number
        if lkey in {"capacity", "capacity_bytes", "total", "disk_total", "filesys.capacity"}:
            capacity = number
    if used is None or capacity is None or capacity <= 0:
        return None
    return used, capacity


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _dedupe(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[tuple[str, str], dict[str, Any]] = {}
    for sample in samples:
        seen[(sample["host"], sample["instance"])] = sample
    return list(seen.values())


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
