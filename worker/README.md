# Predictive AI stream worker

Python Kafka consumer that:

1. Reads PCP / raw metric JSON from `rhel-pcp-metrics` and/or `raw-metrics`.
2. Estimates filesystem **time to exhaustion** over a rolling window:
   `TTE = (Capacity - Used_current) / (dUsed/dt)`.
3. If `dUsed/dt <= 0`, it emits **no** exhaustion alert.
4. Optionally classifies logs (RCA + severity) via one OpenAI-compatible HTTP client.
5. Writes enriched JSON to `enriched-events`.

When `INFERENCE_BASE_URL` is empty, metric alerts still emit. LLM classification is skipped.

Alert type string (do not change): `PREEMPTIVE_STORAGE_EXHAUSTION_RISK`.

## Local run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_CONSUME_TOPICS=rhel-pcp-metrics,raw-metrics
export KAFKA_PRODUCE_TOPIC=enriched-events
export KAFKA_CONSUMER_GROUP=stream-worker
# Leave empty to disable inference:
export INFERENCE_BASE_URL=
python -m stream_worker
```

## Container build

```bash
podman build -t IMAGE_REF -f Dockerfile .
```

OpenShift packaging and inference backend choices are in `docs/06-optional-predictive-ai-worker.md`.

## Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092` | In-cluster plaintext bootstrap |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | kafka-python security protocol |
| `KAFKA_CONSUME_TOPICS` | `rhel-pcp-metrics,raw-metrics` | Comma-separated consume list |
| `KAFKA_PRODUCE_TOPIC` | `enriched-events` | Output topic |
| `KAFKA_CONSUMER_GROUP` | `stream-worker` | Consumer group |
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | `latest` or `earliest` |
| `INFERENCE_BASE_URL` | empty | OpenAI-compatible base including `/v1` (empty disables LLM) |
| `INFERENCE_API_KEY` | empty | Bearer token if the backend requires it |
| `INFERENCE_MODEL` | `default` | Model name |
| `WINDOW_SECONDS` | `900` | Rolling window for dUsed/dt |
| `MIN_SAMPLES` | `4` | Minimum points before TTE is computed |
| `TTE_ALERT_THRESHOLD_SECONDS` | `3600` | Emit when TTE is at or below this |
| `ALERT_COOLDOWN_SECONDS` | `300` | Per host+instance suppress window |
| `HEALTH_PORT` | `8080` | `/healthz` and `/readyz` |

Keep replicas at 1 unless producers key metric records by host (in-memory windows are not shared).
