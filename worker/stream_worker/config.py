from __future__ import annotations

import os
from dataclasses import dataclass


def _bool(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _csv(name: str, default: str) -> list[str]:
    raw = os.environ.get(name, default)
    return [part.strip() for part in raw.split(",") if part.strip()]


def _env(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value != "":
            return value
    return default


@dataclass
class Config:
    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    consume_topics: list[str]
    produce_topic: str
    consumer_group: str
    client_id: str
    auto_offset_reset: str
    inference_base_url: str
    inference_api_key: str
    inference_model: str
    inference_timeout: float
    inference_max_tokens: int
    window_seconds: float
    min_samples: int
    tte_alert_threshold_seconds: float
    alert_cooldown_seconds: float
    health_bind: str
    health_port: int
    log_level: str
    classify_logs: bool
    llm_on_metric_alerts: bool
    poll_timeout_ms: int

    @property
    def inference_enabled(self) -> bool:
        return bool(self.inference_base_url)

    @property
    def kafka_bootstrap(self) -> str:
        return self.kafka_bootstrap_servers


def load_config() -> Config:
    return Config(
        kafka_bootstrap_servers=_env(
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_BOOTSTRAP",
            default="telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092",
        ),
        kafka_security_protocol=_env("KAFKA_SECURITY_PROTOCOL", default="PLAINTEXT"),
        consume_topics=_csv("KAFKA_CONSUME_TOPICS", "rhel-pcp-metrics,raw-metrics")
        or _csv("CONSUME_TOPICS", "rhel-pcp-metrics,raw-metrics"),
        produce_topic=_env("KAFKA_PRODUCE_TOPIC", "PRODUCE_TOPIC", default="enriched-events"),
        consumer_group=_env(
            "KAFKA_CONSUMER_GROUP",
            "KAFKA_GROUP_ID",
            default="stream-worker",
        ),
        client_id=_env("KAFKA_CLIENT_ID", default="predictive-ai-worker"),
        auto_offset_reset=_env("KAFKA_AUTO_OFFSET_RESET", default="latest"),
        inference_base_url=_env("INFERENCE_BASE_URL", default="").rstrip("/"),
        inference_api_key=_env("INFERENCE_API_KEY", default=""),
        inference_model=_env("INFERENCE_MODEL", default="default"),
        inference_timeout=float(_env("INFERENCE_TIMEOUT_SECONDS", default="15")),
        inference_max_tokens=int(_env("INFERENCE_MAX_TOKENS", default="400")),
        window_seconds=float(_env("WINDOW_SECONDS", default="900")),
        min_samples=int(_env("MIN_SAMPLES", default="4")),
        tte_alert_threshold_seconds=float(
            _env("TTE_ALERT_THRESHOLD_SECONDS", "ALERT_TTE_SECONDS", default="3600")
        ),
        alert_cooldown_seconds=float(_env("ALERT_COOLDOWN_SECONDS", default="300")),
        health_bind=_env("HEALTH_BIND", default="0.0.0.0"),
        health_port=int(_env("HEALTH_PORT", default="8080")),
        log_level=_env("LOG_LEVEL", default="INFO"),
        classify_logs=_bool("CLASSIFY_LOGS", "false")
        or _bool("ENABLE_LOG_CLASSIFICATION", "false"),
        llm_on_metric_alerts=_bool("LLM_ON_METRIC_ALERTS", "true"),
        poll_timeout_ms=int(_env("KAFKA_POLL_TIMEOUT_MS", default="1000")),
    )
