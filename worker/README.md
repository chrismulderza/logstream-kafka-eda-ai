# Predictive AI stream worker (Quarkus)

Java 21 / Quarkus Kafka consumer that:

1. Reads PCP / raw metric JSON from `rhel-pcp-metrics` and/or `raw-metrics`.
2. Estimates filesystem **time to exhaustion** over a rolling window:
   `TTE = (Capacity - Used_current) / (dUsed/dt)`.
3. If `dUsed/dt <= 0`, it emits **no** exhaustion alert.
4. Optionally classifies logs (RCA + severity) via an OpenAI-compatible HTTP client.
5. Writes enriched JSON to `enriched-events` (Kafka key = host).

When `INFERENCE_BASE_URL` is **unset**, metric alerts still emit and LLM classification is skipped.

Alert type string (do not change): `PREEMPTIVE_STORAGE_EXHAUSTION_RISK`.

**Full workstation setup (macOS / Fedora / RHEL), first-run steps, inject scripts, and expected output:** [docs chapter 6 §6.2](../docs/06-optional-predictive-ai-worker.md#62-local-development-quarkus-dev-mode).

## Quick start

### Dependencies

| Need | How |
| --- | --- |
| **OpenJDK 21**, Maven, Quarkus CLI, Python, uv | `cd worker && mise install` ([mise](https://mise.jdx.dev/getting-started.html) + [`mise.toml`](mise.toml) pins `openjdk-21.0.2`) |
| **Podman** (required; not Docker Desktop) | macOS: [Podman Desktop](https://podman-desktop.io/) + `podman machine start`. Fedora/RHEL: `dnf install podman` + `systemctl --user enable --now podman.socket` |
| bash, curl | OS defaults |

### Start dev mode

```bash
cd worker
mise trust && mise install
mise run dev
```

Expect Kafka Dev Services to start via Podman and:

```text
Listening on: http://0.0.0.0:8080
Profile dev activated. Live Coding activated.
```

Health: `curl -s http://127.0.0.1:8080/healthz` → `"status":"UP"`.

### Inject TTE test samples

In a second terminal (dev mode still running):

```bash
cd worker && mise run inject-metrics -- --consume
# or: ../scripts/inject-worker-metrics.sh --consume
```

Expect discovery of `localhost:<port>`, six rising samples, then JSON with `"alert_type":"PREEMPTIVE_STORAGE_EXHAUSTION_RISK"`.

| Task | Command |
| --- | --- |
| Dev mode | `mise run dev` |
| Unit tests | `mise run test` |
| Package | `mise run package` |
| Inject metrics | `mise run inject-metrics -- --consume` |

`dev` / `test` / `package` / `inject-metrics` source [`scripts/podman-env.sh`](scripts/podman-env.sh).

Stop: press `q` in the Quarkus console, or terminate the `mise run dev` process.

## Local LLM testing (Ollama)

Step-by-step procedure (start Ollama → pull `granite3.3:2b` → export `INFERENCE_*` → `mise run dev` → inject → `ollama rm` cleanup): [docs §6.2.10](../docs/06-optional-predictive-ai-worker.md#6210-local-inference-testing-with-ollama).

```bash
ollama serve   # if needed
ollama pull granite3.3:2b
export INFERENCE_BASE_URL=http://127.0.0.1:11434/v1
export INFERENCE_MODEL=granite3.3:2b
export LLM_ON_METRIC_ALERTS=true
export INFERENCE_TIMEOUT_SECONDS=60
mise run dev
# other terminal:
../scripts/inject-worker-metrics.sh --consume -H ollama-dev
# when finished:
ollama rm granite3.3:2b
```

## Container / OpenShift build

Prefer the in-cluster BuildConfig (continuous):

```bash
oc apply -k openshift/worker/
oc start-build predictive-ai-worker --from-dir=worker --follow -n logstream-kafka
```

Optional local image (Red Hat UBI 9 bases only):

```bash
podman build -t predictive-ai-worker:latest -f Dockerfile .
```

JVM images: `registry.access.redhat.com/ubi9/openjdk-21*:1.24` (OpenJDK). Native micro: `quay.io/quarkus/ubi9-quarkus-micro-image:2.0`. GitHub Actions verifies allowed bases and builds docs in `ubi9/python-312`.

Details: [`openshift/worker/README.md`](../openshift/worker/README.md) and [chapter 6](../docs/06-optional-predictive-ai-worker.md).

## Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092` | Bootstrap brokers |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Kafka security protocol |
| `KAFKA_CONSUME_TOPICS` | `rhel-pcp-metrics,raw-metrics` | Comma-separated consume list |
| `KAFKA_PRODUCE_TOPIC` | `enriched-events` | Output topic |
| `KAFKA_CONSUMER_GROUP` | `stream-worker` | Consumer group |
| `KAFKA_CLIENT_ID` | `predictive-ai-worker` | Client id |
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | `latest` or `earliest` |
| `INFERENCE_BASE_URL` | unset | OpenAI-compatible base including `/v1` (omit to disable LLM) |
| `INFERENCE_API_KEY` | unset | Bearer token if the backend requires it |
| `INFERENCE_MODEL` | `default` | Model name |
| `INFERENCE_TIMEOUT_SECONDS` | `15` | HTTP timeout |
| `INFERENCE_MAX_TOKENS` | `400` | Max completion tokens |
| `WINDOW_SECONDS` | `900` | Rolling window for dUsed/dt |
| `MIN_SAMPLES` | `4` | Minimum points before TTE is computed |
| `TTE_ALERT_THRESHOLD_SECONDS` | `3600` | Emit when TTE is at or below this |
| `ALERT_COOLDOWN_SECONDS` | `300` | Per host+instance suppress window |
| `CLASSIFY_LOGS` | `false` | Enable log RCA/severity via LLM |
| `LLM_ON_METRIC_ALERTS` | `true` | Enrich TTE alerts with LLM fields |
| `LOG_LEVEL` | `INFO` | Quarkus log level |

Health: `GET /healthz` (liveness), `GET /readyz` (readiness) on port `8080`.

Keep replicas at 1 unless producers key metric records by host (in-memory windows are not shared).
