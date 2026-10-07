# 6. Optional: Predictive AI stream worker

This chapter is **optional**. The core pipeline is syslog on `rhel-system-logs` plus Event-Driven Ansible in [chapter 5](05-event-driven-ansible.md). Deploy this worker only if you want PCP/raw-metric time-to-exhaustion (TTE) alerts on `enriched-events`. Skip image build, Secret, and the optional EDA rulebook if you do not need predictive storage alerts.

EDA for TTE uses `ansible/eda/rulebook-optional-predictive.yml` (CLI) or `aap-rulebook-optional-predictive.yml` (AAP). Do not add those sources unless this worker is running. Keep consumer group `stream-worker` exclusive to the worker.

This chapter deploys the Python Kafka worker that computes **time-to-exhaustion (TTE)** from PCP / raw metrics and optionally classifies logs with an OpenAI-compatible inference API.

Source: [`worker/`](../worker/). Manifests: [`openshift/worker/`](../openshift/worker/).

There is **no single default inference backend**. Choose a row in the decision matrix, or run with inference disabled and still emit predictive metric alerts.

## 6.1 What the worker does

| Direction | Name | Notes |
| --- | --- | --- |
| Namespace | `logstream-kafka` | Same project as Kafka |
| Kafka cluster | `telemetry` | Internal plaintext listener |
| Bootstrap | `telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092` | In-cluster **plaintext** |
| Consume | `rhel-pcp-metrics` and/or `raw-metrics` | `KAFKA_CONSUME_TOPICS` (default: both) |
| Produce | `enriched-events` | JSON keyed by host |
| Consumer group | `stream-worker` | Do not reuse this group for EDA or SIEM |

**TTE formula** (rolling window; `dUsed/dt` is a numpy least-squares slope):

```
TTE = (Capacity - Used_current) / (dUsed/dt)
```

- If the fill rate is **less than or equal to zero** (flat or shrinking usage), the worker **does not** emit an exhaustion alert.
- If TTE is at or below `TTE_ALERT_THRESHOLD_SECONDS`, it produces an event whose `alert_type` and `type` are exactly:

```
PREEMPTIVE_STORAGE_EXHAUSTION_RISK
```

The optional EDA rulebook matches that string. Do not rename it.

If `INFERENCE_BASE_URL` is empty, log RCA / severity scoring is skipped. Metric alerts still go to `enriched-events`.

Implementation: [`worker/stream_worker/predictor.py`](../worker/stream_worker/predictor.py), [`worker/stream_worker/processor.py`](../worker/stream_worker/processor.py).

## 6.2 Inference backend decision matrix

Pick **one** path. Do not treat any row as the project default. The worker uses a single HTTP client: `POST {INFERENCE_BASE_URL}/chat/completions` with `Authorization: Bearer {INFERENCE_API_KEY}` when the key is non-empty.

| Criterion | A. vLLM in-cluster | B. RHOAI model serving | C. External OpenAI-compatible API | D. Inference disabled |
| --- | --- | --- | --- | --- |
| When to choose | You already run vLLM next to Kafka; lowest latency; prompts stay in-cluster | You already operate OpenShift AI / KServe InferenceServices | You have a corporate or public Chat Completions endpoint and cannot host a model | You only need predictive disk TTE for EDA |
| Typical `INFERENCE_BASE_URL` | `http://vllm.<namespace>.svc:8000/v1` (often `http://vllm:8000/v1` if Service `vllm` is in the same namespace) | `https://<inference-service-host>/v1` from the serving Route or in-cluster Service | `https://api.openai.com/v1` or your gateway `/v1` | empty (`""`) |
| `INFERENCE_API_KEY` | Often empty for in-cluster vLLM | Token if the route is authenticated | Required | unused |
| `INFERENCE_MODEL` | vLLM `--served-model-name` | Deployed serving name | Provider model id | unused |
| Network | ClusterIP HTTP, typically port 8000 | Route (TLS) and/or Service. OCP 4.20 Gateway API Inference Extension is optional; this worker does not require it | Egress; may need proxy / EgressFirewall | none |
| Data handling | Prompts never leave the cluster | Stay in the OpenShift AI project | Log snippets and metric summaries leave the cluster — review policy | no prompts |
| Failure mode | Inference errors are logged; TTE alerts still emit | Same | Same | TTE alerts only |

Configure the chosen row in [`openshift/worker/configmap.yaml`](../openshift/worker/configmap.yaml) and the Secret **before** apply. Switching backends later is a ConfigMap/Secret change plus a rollout; the container image does not change.

Prompt templates (log RCA + severity scoring): [`worker/stream_worker/prompts.py`](../worker/stream_worker/prompts.py). Client: [`worker/stream_worker/inference.py`](../worker/stream_worker/inference.py).

## 6.3 Prerequisites

1. Namespace `logstream-kafka` exists.
2. Kafka cluster `telemetry` is Ready (Streams for Apache Kafka / Strimzi). Brokers advertise the internal plaintext listener on port 9092.
3. Topics exist: `rhel-pcp-metrics` and/or `raw-metrics` (consume), `enriched-events` (produce).
4. You can `oc` as a project admin on `logstream-kafka`.
5. `podman` (or compatible) to build the UBI9 image, and a registry the cluster can pull.
6. Build hosts can pull `registry.access.redhat.com/ubi9/python-311`. Runtime nodes only need your `IMAGE_REF`.
7. If Kafka authorization is enabled, grant group `stream-worker` read on the consume topics and write on `enriched-events`. These manifests assume the internal plaintext listener is reachable from pods in the namespace.

### 6.3.1 Security context (restricted-v2)

The Deployment is written for the OpenShift **restricted-v2** SCC (default on OpenShift 4.20+):

- `serviceAccountName: predictive-ai-worker` (no extra SCC binding)
- `runAsNonRoot: true`, `allowPrivilegeEscalation: false`
- all capabilities dropped, `seccompProfile: RuntimeDefault`
- `readOnlyRootFilesystem: true` with `emptyDir` on `/tmp`

Do **not** assign `privileged`, `anyuid`, or `hostaccess` unless a real constraint blocks startup. A missing Kafka bootstrap is not an SCC problem.

## 6.4 Build the image with podman

From the repository root:

```bash
cd /path/to/logstream-kafka-eda-ai

# Choose a real image reference (examples, not project defaults):
#   image-registry.openshift-image-registry.svc:5000/logstream-kafka/predictive-ai-worker:1.0.0
#   quay.io/<org>/predictive-ai-worker:1.0.0
export IMAGE_REF='<registry>/<namespace-or-org>/predictive-ai-worker:1.0.0'

podman build -t "${IMAGE_REF}" -f worker/Dockerfile worker
```

The Dockerfile is multi-stage UBI9 (`ubi9/python-311`): the builder installs `kafka-python`, `numpy`, and `requests`; the runtime copies the venv and `stream_worker` and runs as UID 1001 (OpenShift still injects an arbitrary UID in group 0).

### 6.4.1 Push to the integrated registry

```bash
oc login --server <api>
oc project logstream-kafka
oc registry login

HOST="$(oc get route default-route -n openshift-image-registry -o jsonpath='{.spec.host}')"
podman login -u "$(oc whoami)" -p "$(oc whoami -t)" "${HOST}"
podman tag "${IMAGE_REF}" "${HOST}/logstream-kafka/predictive-ai-worker:1.0.0"
podman push "${HOST}/logstream-kafka/predictive-ai-worker:1.0.0"

# In-cluster pull reference for the Deployment:
export IMAGE_REF="image-registry.openshift-image-registry.svc:5000/logstream-kafka/predictive-ai-worker:1.0.0"
```

Alternative: binary BuildConfig from the `worker/` directory:

```bash
oc new-build --name predictive-ai-worker --binary --strategy docker -n logstream-kafka || true
oc start-build predictive-ai-worker --from-dir=worker --follow -n logstream-kafka
export IMAGE_REF="image-registry.openshift-image-registry.svc:5000/logstream-kafka/predictive-ai-worker:latest"
```

If you push to Quay or another external registry, create an `imagePullSecret` and attach it to ServiceAccount `predictive-ai-worker` before the first rollout.

## 6.5 Fill placeholders (image, inference, secret)

1. Set the Deployment image. Edit `image: IMAGE_REF` in [`openshift/worker/deployment.yaml`](../openshift/worker/deployment.yaml) or substitute at apply time (next section).

2. Edit [`openshift/worker/configmap.yaml`](../openshift/worker/configmap.yaml):
   - Confirm bootstrap `telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092`.
   - Set `KAFKA_CONSUME_TOPICS` to `rhel-pcp-metrics`, `raw-metrics`, or both.
   - Apply **one** inference row from section 5.2 to `INFERENCE_BASE_URL` and `INFERENCE_MODEL`.

3. Do not store real keys in git. Create the Secret out of band:

```bash
# TTE only / vLLM without auth
oc -n logstream-kafka create secret generic predictive-ai-worker \
  --from-literal=INFERENCE_API_KEY='' \
  --dry-run=client -o yaml | oc apply -f -

# RHOAI or external API
oc -n logstream-kafka create secret generic predictive-ai-worker \
  --from-literal=INFERENCE_API_KEY='YOUR_KEY_HERE' \
  --dry-run=client -o yaml | oc apply -f -
```

[`openshift/worker/secret.yaml`](../openshift/worker/secret.yaml) uses the placeholder `CHANGE_ME`. [`openshift/worker/secret.example.yaml`](../openshift/worker/secret.example.yaml) uses an empty key.

4. Keep `replicas: 1`. TTE windows are in-process. Horizontal scale is safe only if metric producers key records by host so each host stays on one partition.

## 6.6 Apply manifests

```bash
oc project logstream-kafka

oc apply -f openshift/worker/serviceaccount.yaml
oc apply -f openshift/worker/configmap.yaml
# Skip if you already created the Secret with oc create in 5.5:
oc apply -f openshift/worker/secret.yaml
oc apply -f openshift/worker/service.yaml

sed "s|image: IMAGE_REF|image: ${IMAGE_REF}|" openshift/worker/deployment.yaml | oc apply -f -
```

Kustomize (replace `IMAGE_REF` in the Deployment first):

```bash
oc apply -k openshift/worker/
```

Wait until Ready:

```bash
oc -n logstream-kafka rollout status deployment/predictive-ai-worker --timeout=180s
oc -n logstream-kafka get pods -l app.kubernetes.io/name=predictive-ai-worker
oc -n logstream-kafka logs -l app.kubernetes.io/name=predictive-ai-worker --tail=50
```

Healthy startup logs include group `stream-worker`, the consume topic list, `produce=enriched-events`, and `inference=enabled` or `inference=disabled`.

## 6.7 Verify Kafka plumbing

```bash
# Consumer group (from a Kafka tools / debug pod in logstream-kafka)
bin/kafka-consumer-groups.sh \
  --bootstrap-server telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092 \
  --group stream-worker --describe
```

Watch output (from a jump pod with `kcat`):

```bash
kcat -b telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092 \
  -t enriched-events -C -o end -q
```

Synthetic increasing usage (repeat with higher `used` at least `MIN_SAMPLES` times inside `WINDOW_SECONDS`):

```bash
kcat -b telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092 \
  -t rhel-pcp-metrics -P <<'EOF'
{"@timestamp":"2026-10-06T10:00:00Z","host":"testhost","metrics":{"filesys.used":{"/var":100},"filesys.capacity":{"/var":1000}}}
EOF
```

On a lab host, the storage-fill test in [chapter 7](07-validation-runbook.md) is the end-to-end check.

To automate TTE in EDA, start a **second** CLI process or AAP activation on `rulebook-optional-predictive.yml` / `aap-rulebook-optional-predictive.yml`. Do not merge it into the syslog rulebook unless you intend both Kafka sources in group `ansible-eda`. Keep `allow_podman_prune` and `allow_lvextend` false.

Probes: `GET /healthz` and `GET /readyz` on port 8080. Service `predictive-ai-worker` exposes that port; it is not on the Kafka data path.

```bash
oc -n logstream-kafka port-forward svc/predictive-ai-worker 8080:8080
curl -sS http://127.0.0.1:8080/readyz
```

## 6.8 Tuning after deploy

| Knob | ConfigMap key | Effect |
| --- | --- | --- |
| Rolling window | `WINDOW_SECONDS` | Longer window smooths noise |
| Minimum points | `MIN_SAMPLES` | Must be ≥ 2; default 4 |
| Alert horizon | `TTE_ALERT_THRESHOLD_SECONDS` | Default 3600 (exhaustion within one hour) |
| Duplicate suppression | `ALERT_COOLDOWN_SECONDS` | Per host+instance |
| Log classification | `CLASSIFY_LOGS` | LLM RCA + severity when inference is enabled |
| LLM on TTE alerts | `LLM_ON_METRIC_ALERTS` | Extra severity/RCA fields on metric alerts |
| Offset policy | `KAFKA_AUTO_OFFSET_RESET` | `latest` for live; `earliest` only for replay tests |

After ConfigMap or Secret edits:

```bash
oc -n logstream-kafka rollout restart deployment/predictive-ai-worker
oc -n logstream-kafka rollout status deployment/predictive-ai-worker
```

## 6.9 Operational notes

- **Graceful shutdown:** SIGTERM stops polling, flushes the producer, and closes the consumer within `terminationGracePeriodSeconds: 30`.
- **LLM outage:** HTTP errors are logged. Predictive metric alerts are still produced.
- **Payload shape:** PCP-style maps (`filesys.used` / `filesys.capacity` per mount), list metrics (`name` / `instance` / `value`), and flat `used`/`capacity` or `used_bytes`/`capacity_bytes`. Host from `host`, `hostname`, `@sourcehost`, or `source`.
- **Log documents:** JSON with a `message` (typically `syslogtag`) can be classified when inference is on. Add `rhel-system-logs` to `KAFKA_CONSUME_TOPICS` if you want that path; the packaged default is metrics-only topics so EDA can remain the primary syslog consumer.

## 6.10 Troubleshooting

| Symptom | What to check |
| --- | --- |
| CrashLoop / not Ready | `oc describe pod`; bootstrap DNS; topic names; SCC only if events mention security context |
| `ImagePullBackOff` | `IMAGE_REF` not pushed, pull secret missing, wrong registry hostname |
| Group exists but lag grows | Logs for JSON parse skips; producers must send JSON |
| No TTE alerts | Need ≥ `MIN_SAMPLES` in-window points with **positive** fill rate; TTE ≤ threshold; cooldown |
| Alerts without `severity` | Expected when inference is disabled or the LLM call failed |
| `401/403` from inference | Wrong `INFERENCE_API_KEY` or RHOAI route auth |
| Connection refused to vLLM | Service name/namespace/port; URL must include `/v1` so the client posts `/v1/chat/completions` |

Delete (does not delete Kafka topics):

```bash
oc -n logstream-kafka delete -k openshift/worker/
```

## 6.11 Related files

| Path | Role |
| --- | --- |
| [`worker/stream_worker/app.py`](../worker/stream_worker/app.py) | Consume/produce loop, SIGTERM |
| [`worker/requirements.txt`](../worker/requirements.txt) | `kafka-python`, `numpy`, `requests` |
| [`worker/Dockerfile`](../worker/Dockerfile) | UBI9 multi-stage |
| [`openshift/worker/`](../openshift/worker/) | ServiceAccount, ConfigMap, Secret template, Deployment, optional Service |
| [`ansible/eda/rulebook-optional-predictive.yml`](../ansible/eda/rulebook-optional-predictive.yml) | CLI EDA for `PREEMPTIVE_STORAGE_EXHAUSTION_RISK` |
| [`ansible/eda/aap-rulebook-optional-predictive.yml`](../ansible/eda/aap-rulebook-optional-predictive.yml) | AAP activation for the same alert |
