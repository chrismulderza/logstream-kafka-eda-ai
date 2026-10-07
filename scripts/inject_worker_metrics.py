#!/usr/bin/env python3
"""Inject rising PCP filesystem samples for Quarkus predictive-worker TTE tests.

Preferred invocation (from repo root, with worker/mise.toml installed):

  mise exec -C worker -- uv run --with kafka-python scripts/inject_worker_metrics.py --consume

Or via the shell wrapper / mise task:

  ./scripts/inject-worker-metrics.sh --consume
  cd worker && mise run inject-metrics -- --consume

Requires: kafka-python (pulled by uv --with), and Podman when discovering
Quarkus Dev Services Kafka (unless -b / KAFKA_BOOTSTRAP_SERVERS).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def discover_devservices_bootstrap() -> str | None:
    """Resolve Dev Services Kafka via Podman only (never Docker Desktop)."""
    if not shutil.which("podman"):
        return None
    try:
        listed = subprocess.check_output(
            ["podman", "ps", "-q", "--filter", "label=quarkus-dev-service-kafka"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    cid = listed.splitlines()[0].strip() if listed else ""
    if not cid:
        return None
    try:
        mapped = subprocess.check_output(
            ["podman", "port", cid, "9092"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        if mapped:
            port = mapped.splitlines()[0].rsplit(":", 1)[-1]
            if port.isdigit():
                return f"localhost:{port}"
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    try:
        port = subprocess.check_output(
            [
                "podman",
                "inspect",
                "-f",
                '{{(index (index .NetworkSettings.Ports "9092/tcp") 0).HostPort}}',
                cid,
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        if port.isdigit():
            return f"localhost:{port}"
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Produce rising filesys.used samples for the predictive worker."
    )
    p.add_argument(
        "-b",
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
        help="Kafka bootstrap (default: env or Dev Services discovery)",
    )
    p.add_argument("-t", "--topic", default=os.environ.get("TOPIC", "rhel-pcp-metrics"))
    p.add_argument(
        "--enriched-topic",
        default=os.environ.get("ENRICHED_TOPIC", "enriched-events"),
    )
    p.add_argument("-n", "--samples", type=int, default=int(os.environ.get("SAMPLES", "6")))
    p.add_argument(
        "-i",
        "--interval",
        type=float,
        default=float(os.environ.get("INTERVAL_SEC", "1")),
    )
    p.add_argument("-H", "--host", default=os.environ.get("HOST_NAME", "testhost"))
    p.add_argument("-m", "--mount", default=os.environ.get("MOUNT", "/var"))
    p.add_argument(
        "-c",
        "--capacity",
        type=int,
        default=int(os.environ.get("CAPACITY", "1000")),
    )
    p.add_argument(
        "--used-start",
        type=int,
        default=int(os.environ.get("USED_START", "100")),
    )
    p.add_argument(
        "--used-step",
        type=int,
        default=int(os.environ.get("USED_STEP", "100")),
    )
    p.add_argument(
        "--consume",
        action="store_true",
        help="After inject, print recent enriched-events messages",
    )
    return p.parse_args()


def main() -> int:
    try:
        from kafka import KafkaConsumer, KafkaProducer
    except ImportError:
        print(
            "ERROR: kafka-python is required.\n"
            "  Prefer: cd worker && mise install && mise run inject-metrics -- [options]\n"
            "  Or:     uv run --with kafka-python scripts/inject_worker_metrics.py",
            file=sys.stderr,
        )
        return 1

    args = parse_args()
    bootstrap = args.bootstrap.strip()
    if not bootstrap:
        discovered = discover_devservices_bootstrap()
        if discovered:
            bootstrap = discovered
            print(f"Using Quarkus Dev Services Kafka at {bootstrap}")
        else:
            print(
                "ERROR: set KAFKA_BOOTSTRAP_SERVERS or -b HOST:PORT,\n"
                "  or start 'mise run dev' (Podman Dev Services) so Kafka is running.\n"
                "  Bootstrap discovery uses podman only (label quarkus-dev-service-kafka).",
                file=sys.stderr,
            )
            return 1

    print(
        f"Producing {args.samples} samples to {args.topic} @ {bootstrap} "
        f"(host={args.host} mount={args.mount})"
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
    )
    used = args.used_start
    try:
        for i in range(args.samples):
            ts = utc_ts()
            payload = {
                "@timestamp": ts,
                "host": args.host,
                "metrics": {
                    "filesys.used": {args.mount: used},
                    "filesys.capacity": {args.mount: args.capacity},
                },
            }
            body = json.dumps(payload, separators=(",", ":"))
            print(f"  [{i}] used={used} capacity={args.capacity} t={ts}")
            producer.send(args.topic, key=args.host, value=body)
            producer.flush()
            used += args.used_step
            if i < args.samples - 1:
                time.sleep(args.interval)
    finally:
        producer.close()

    print(
        f"Done. With default MIN_SAMPLES=4 and a positive fill rate, "
        f"expect an alert on {args.enriched_topic}."
    )
    print("Watch worker logs for: emitted PREEMPTIVE_STORAGE_EXHAUSTION_RISK")

    if args.consume:
        print(f"Consuming {args.enriched_topic} (up to 20 messages from end)...")
        consumer = KafkaConsumer(
            args.enriched_topic,
            bootstrap_servers=bootstrap,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            group_id=f"inject-worker-verify-{os.getpid()}",
        )
        # Seek near end so we see freshly emitted alerts.
        assigned = False
        deadline = time.time() + 8
        while time.time() < deadline and not assigned:
            consumer.poll(timeout_ms=500)
            if consumer.assignment():
                assigned = True
                break
        if assigned:
            for tp in consumer.assignment():
                end = consumer.end_offsets([tp])[tp]
                start = max(0, end - 20)
                consumer.seek(tp, start)
            for msg in consumer:
                value = msg.value.decode("utf-8", errors="replace") if msg.value else ""
                print(value)
        else:
            print("(no partition assignment; broker may still be settling)", file=sys.stderr)
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
