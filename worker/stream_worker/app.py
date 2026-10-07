"""Kafka consume/produce loop with graceful shutdown."""

from __future__ import annotations

import json
import logging
import signal
import sys
import threading
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from stream_worker.config import Config, load_config
from stream_worker.health import HealthState, start_health_server
from stream_worker.inference import InferenceClient
from stream_worker.processor import EventProcessor

LOG = logging.getLogger(__name__)


def main() -> None:
    cfg = load_config()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    LOG.info(
        "starting predictive-ai-worker group=%s consume=%s produce=%s inference=%s",
        cfg.consumer_group,
        ",".join(cfg.consume_topics),
        cfg.produce_topic,
        "enabled" if cfg.inference_enabled else "disabled",
    )
    run(cfg)


def run(cfg: Config) -> None:
    stop = threading.Event()
    state = HealthState()

    def _handle(signum: int, _frame: Any) -> None:
        LOG.info("received signal %s, shutting down", signum)
        state.alive = False
        stop.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    health = start_health_server(cfg.health_bind, cfg.health_port, state)
    inference = InferenceClient(cfg)
    processor = EventProcessor(cfg, inference)

    consumer: KafkaConsumer | None = None
    producer: KafkaProducer | None = None
    try:
        consumer = _consumer(cfg)
        producer = _producer(cfg)
        state.ready = True
        LOG.info("subscribed and ready")
        while not stop.is_set():
            try:
                batches = consumer.poll(timeout_ms=cfg.poll_timeout_ms)
            except KafkaError as exc:
                state.consume_error = str(exc)
                LOG.error("kafka poll failed: %s", exc)
                stop.wait(1.0)
                continue
            state.consume_error = None
            for tp, records in batches.items():
                for record in records:
                    if stop.is_set():
                        break
                    _handle_record(processor, producer, cfg, tp.topic, record.value)
    except Exception:
        LOG.exception("worker crashed")
        raise
    finally:
        state.ready = False
        state.alive = False
        try:
            health.shutdown()
        except Exception:
            LOG.debug("health server shutdown failed", exc_info=True)
        if producer is not None:
            try:
                producer.flush(timeout=10)
                producer.close(timeout=10)
            except Exception:
                LOG.warning("producer close failed", exc_info=True)
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                LOG.warning("consumer close failed", exc_info=True)
        LOG.info("shutdown complete")


def _handle_record(
    processor: EventProcessor,
    producer: KafkaProducer,
    cfg: Config,
    topic: str,
    value: Any,
) -> None:
    try:
        events = processor.process(value, topic)
    except Exception:
        LOG.exception("failed to process message from %s", topic)
        return
    for event in events:
        key = (event.get("host") or "").encode("utf-8")
        producer.send(
            cfg.produce_topic,
            key=key,
            value=json.dumps(event, default=str).encode("utf-8"),
        )
        LOG.info(
            "emitted %s host=%s instance=%s tte=%s",
            event.get("alert_type") or event.get("event_type"),
            event.get("host"),
            event.get("instance"),
            event.get("tte_seconds"),
        )


def _consumer(cfg: Config) -> KafkaConsumer:
    return KafkaConsumer(
        *cfg.consume_topics,
        bootstrap_servers=cfg.kafka_bootstrap_servers.split(","),
        security_protocol=cfg.kafka_security_protocol,
        group_id=cfg.consumer_group,
        client_id=cfg.client_id,
        enable_auto_commit=True,
        auto_offset_reset=cfg.auto_offset_reset,
        value_deserializer=lambda v: v,
        key_deserializer=lambda v: v,
        consumer_timeout_ms=-1,
    )


def _producer(cfg: Config) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap_servers.split(","),
        security_protocol=cfg.kafka_security_protocol,
        client_id=f"{cfg.client_id}-producer",
        acks="all",
        linger_ms=20,
        retries=5,
    )


if __name__ == "__main__":
    main()
