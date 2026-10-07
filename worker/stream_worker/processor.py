"""Turn Kafka messages into enriched-events."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from stream_worker.config import Config
from stream_worker.inference import InferenceClient
from stream_worker.parse import (
    extract_disk_samples,
    extract_log,
    parse_timestamp,
    utc_now_iso,
)
from stream_worker.predictor import ALERT_TYPE, RollingWindowStore
from stream_worker import prompts

LOG = logging.getLogger(__name__)


class EventProcessor:
    def __init__(self, cfg: Config, inference: InferenceClient) -> None:
        self.cfg = cfg
        self.inference = inference
        self.windows = RollingWindowStore(cfg.window_seconds, cfg.min_samples)
        self._last_alert: dict[tuple[str, str], float] = {}

    def process(self, raw: bytes | str, topic: str) -> list[dict[str, Any]]:
        payload = _decode(raw)
        if payload is None:
            return []
        now = time.time()
        ts = parse_timestamp(payload, now)
        events: list[dict[str, Any]] = []

        log_event = extract_log(payload)
        if log_event and self.cfg.classify_logs:
            classified = self._classify_log(log_event, topic)
            if classified:
                events.append(classified)

        for sample in extract_disk_samples(payload, ts):
            self.windows.add(
                sample["host"],
                sample["instance"],
                sample["timestamp"],
                sample["used"],
                sample["capacity"],
            )
            alert = self._maybe_storage_alert(sample, topic)
            if alert:
                events.append(alert)
        return events

    def _classify_log(self, log_event: dict[str, Any], topic: str) -> dict[str, Any] | None:
        if not self.inference.enabled:
            return None
        rca = self.inference.classify_json(
            prompts.rca_messages(
                host=log_event["host"],
                syslogtag=log_event["syslogtag"],
                facility=log_event["facility"],
                severity=log_event["severity"],
                message=log_event["message"],
            )
        )
        severity = self.inference.classify_json(
            prompts.severity_messages(
                host=log_event["host"],
                event_context=f"topic={topic} syslogtag={log_event['syslogtag']}",
                message=log_event["message"],
            )
        )
        if not rca and not severity:
            return None
        return {
            "@timestamp": utc_now_iso(),
            "event_type": "LOG_CLASSIFICATION",
            "host": log_event["host"],
            "source_topic": topic,
            "syslogtag": log_event["syslogtag"],
            "facility": log_event["facility"],
            "reported_severity": log_event["severity"],
            "message": log_event["message"],
            "severity": str(severity.get("severity") or "").upper() or None,
            "severity_score": _as_score(severity.get("score")),
            "severity_rationale": severity.get("rationale"),
            "root_cause": rca.get("root_cause"),
            "subsystem": rca.get("subsystem"),
            "summary": rca.get("summary"),
            "recommended_action": rca.get("recommended_action"),
            "worker": "predictive-ai-worker",
        }

    def _maybe_storage_alert(self, sample: dict[str, Any], topic: str) -> dict[str, Any] | None:
        estimate = self.windows.estimate(sample["host"], sample["instance"])
        if estimate is None:
            return None
        if estimate.tte_seconds > self.cfg.tte_alert_threshold_seconds:
            return None
        key = (sample["host"], sample["instance"])
        now = time.time()
        last = self._last_alert.get(key, 0.0)
        if now - last < self.cfg.alert_cooldown_seconds:
            return None
        self._last_alert[key] = now

        event: dict[str, Any] = {
            "@timestamp": utc_now_iso(),
            "event_type": "PREDICTIVE_ALERT",
            "alert_type": ALERT_TYPE,
            "type": ALERT_TYPE,
            "host": sample["host"],
            "instance": sample["instance"],
            "source_topic": topic,
            "tte_seconds": round(estimate.tte_seconds, 3),
            "rate_per_second": estimate.rate_per_second,
            "used": estimate.used_current,
            "capacity": estimate.capacity,
            "samples": estimate.samples,
            "window_span_seconds": round(estimate.window_span_seconds, 3),
            "window_seconds": self.cfg.window_seconds,
            "formula": "TTE = (Capacity - Used_current) / (dUsed/dt)",
            "worker": "predictive-ai-worker",
        }

        if self.inference.enabled and self.cfg.llm_on_metric_alerts:
            classified = self.inference.classify_json(
                prompts.metric_alert_messages(
                    host=sample["host"],
                    instance=sample["instance"],
                    used=estimate.used_current,
                    capacity=estimate.capacity,
                    rate_per_second=estimate.rate_per_second,
                    tte_seconds=estimate.tte_seconds,
                )
            )
            if classified:
                event["severity"] = str(classified.get("severity") or "").upper() or None
                event["severity_score"] = _as_score(classified.get("score"))
                event["root_cause"] = classified.get("root_cause")
                event["summary"] = classified.get("summary")
                event["recommended_action"] = classified.get("recommended_action")
        return event


def _decode(raw: bytes | str) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            LOG.debug("skipping non-utf8 payload")
            return None
    else:
        text = raw
    text = text.strip()
    if not text:
        return None
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        LOG.debug("skipping non-json payload")
        return None
    return data if isinstance(data, dict) else None


def _as_score(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
