# 6. Optional Predictive Worker

This chapter is **optional**. The core pipeline is syslog on `rhel-system-logs` plus Event-Driven Ansible in [Event-Driven Ansible](05-event-driven-ansible.md). Deploy this worker only if you want PCP/raw-metric time-to-exhaustion (TTE) alerts on `enriched-events`. Skip image build, Secret, and the optional EDA rulebook if you do not need predictive storage alerts.

EDA for TTE uses `ansible/eda/rulebook-optional-predictive.yml` (CLI) or `aap-rulebook-optional-predictive.yml` (AAP). Do not add those sources unless this worker is running. Keep consumer group `stream-worker` exclusive to the worker.

This chapter deploys a **Quarkus** Kafka worker that computes **time-to-exhaustion (TTE)** from PCP / raw metrics and optionally classifies events with an OpenAI-compatible inference API. Quarkus **dev mode** via `mise run dev` (Podman Dev Services) is the supported local development loop — see [§6.2](#62-local-development-quarkus-dev-mode).

Source: [`worker/`](../worker/). Manifests: [`openshift/worker/`](../openshift/worker/) (includes ImageStream + BuildConfig for continuous builds).

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
| Runtime | Quarkus 3 / Java 21 | SmallRye Reactive Messaging + REST Client |

**TTE formula** (rolling window; `dUsed/dt` is a least-squares slope):

```
TTE = (Capacity - Used_current) / (dUsed/dt)
```

- If the fill rate is **less than or equal to zero** (flat or shrinking usage), the worker **does not** emit an exhaustion alert.
- If TTE is at or below `TTE_ALERT_THRESHOLD_SECONDS`, it produces an event whose `alert_type` and `type` are exactly:

```
PREEMPTIVE_STORAGE_EXHAUSTION_RISK
```

The optional EDA rulebook matches that string. Do not rename it.

If `INFERENCE_BASE_URL` is unset (omit the env var; do not set it to `""`), log RCA / severity scoring is skipped. Metric alerts still go to `enriched-events`.

Implementation lives under `worker/src/main/java/ai/logstream/worker/` (`predict`, `process`, `parse`, `infer`, `messaging`).

## 6.2 Local development (Quarkus dev mode)

Use this section on a **developer workstation** before deploying to OpenShift. Kafka Dev Services runs through **Podman only** (not Docker Desktop). Language/tool versions come from [`worker/mise.toml`](../worker/mise.toml).

### 6.2.1 Workstation dependency overview

| Dependency | Required? | Provided by | Used for |
| --- | --- | --- | --- |
| [mise](https://mise.jdx.dev/) | Yes | OS package / install script | Pins Java, Maven, Quarkus CLI, Python, uv |
| Java 21 (Temurin) | Yes | mise | Compile / run Quarkus |
| Maven 3.9.x | Yes | mise (+ `./mvnw`) | `quarkus:dev`, tests, package |
| Quarkus CLI 3.40.x | Optional | mise | Helpers only; daily work uses `./mvnw` |
| Python 3.12 + uv | Yes (for inject) | mise | Metric inject via `kafka-python` |
| **Podman** | Yes | OS / Podman Desktop | Kafka Dev Services + inject bootstrap discovery |
| bash | Yes | OS | Shell wrappers under `scripts/` |
| curl | Yes (Podman helper) | OS | API ping in [`podman-env.sh`](../worker/scripts/podman-env.sh) |
| `kcat` | Optional | OS package | Legacy inject fallback; cluster scripts in chapter 7 |
| `oc` | Optional | OpenShift client | Port-forward cluster Kafka instead of Dev Services |

**Not used for Dev Services:** Docker Desktop. Pointing Quarkus at Docker will produce warnings such as `Docker isn't working, please configure the Kafka bootstrap servers property` and Kafka will not start.

Host scripts [`inject-oom-log.sh`](../scripts/inject-oom-log.sh) and [`storage-fill-test.sh`](../scripts/storage-fill-test.sh) run on **RHEL endpoints**, not on this workstation (see [Validation](07-validation-runbook.md)).

### 6.2.2 Install on macOS

1. **Podman** — install [Podman Desktop](https://podman-desktop.io/) (recommended) or `brew install podman`. Create and start a machine:

```bash
podman machine init        # once
podman machine start
podman machine list        # Last Up should include "Currently running"
```

Enable Docker **compatibility** in Podman Desktop if prompted (helps `/var/run/docker.sock` helpers). This project still forces Podman via [`podman-env.sh`](../worker/scripts/podman-env.sh).

2. **mise** — [install mise](https://mise.jdx.dev/getting-started.html), then activate in your shell (`~/.bashrc` / `~/.zshrc`):

```bash
curl https://mise.run | sh
echo 'eval "$(mise activate zsh)"' >> ~/.zshrc   # or bash
source ~/.zshrc
```

3. **Optional:** `brew install kcat` (only if you skip the mise/uv inject path). **Optional:** install `oc` from the OpenShift mirror if you will port-forward cluster Kafka.

4. Continue at [§6.2.5](#625-install-the-mise-toolchain).

### 6.2.3 Install on Fedora

1. **Podman** (usually already present):

```bash
sudo dnf install -y podman curl
systemctl --user enable --now podman.socket
podman info >/dev/null
```

Rootless socket path is typically `unix:///run/user/$UID/podman/podman.sock`. `mise run dev` sets `DOCKER_HOST` to that socket.

2. **mise:**

```bash
curl https://mise.run | sh
echo 'eval "$(mise activate bash)"' >> ~/.bashrc
source ~/.bashrc
```

Or install from Fedora/Copr if your site prefers packages.

3. **Optional:** `sudo dnf install -y kcat`. **Optional:** `oc` RPM / tarball for cluster port-forward.

4. Continue at [§6.2.5](#625-install-the-mise-toolchain).

### 6.2.4 Install on RHEL

Use a RHEL **8.10 / 9** workstation or jump host with subscription entitlements available for AppStream (and EPEL if you need `kcat`).

1. **Podman:**

```bash
sudo dnf install -y podman curl
systemctl --user enable --now podman.socket
loginctl enable-linger "$(whoami)"   # keep user podman.socket after logout (optional but useful)
podman info >/dev/null
```

On RHEL, prefer the distribution `podman` package — do not install Docker Desktop for this workflow. Quarkus guide: [Using Podman with Quarkus](https://quarkus.io/guides/podman).

2. **mise** (same as Fedora):

```bash
curl https://mise.run | sh
echo 'eval "$(mise activate bash)"' >> ~/.bashrc
source ~/.bashrc
```

3. **Optional `kcat`:** try AppStream first; if missing, enable EPEL per site policy then `sudo dnf install -y kcat`. Do not copy random binaries onto managed hosts.

4. **Optional `oc`:** OpenShift client matching your cluster version.

5. Continue at [§6.2.5](#625-install-the-mise-toolchain).

### 6.2.5 Install the mise toolchain

From the repository clone:

```bash
cd worker
mise trust          # first time only, if mise asks
mise install        # java, maven, quarkus, python, uv from mise.toml
mise ls
java -version       # openjdk 21.x
mvn -version        # Apache Maven 3.9.16
quarkus --version   # 3.40.1
python --version    # 3.12.x
uv --version        # 0.12.x
```

| Task (from `worker/`) | What it does |
| --- | --- |
| `mise run dev` | Source [`podman-env.sh`](../worker/scripts/podman-env.sh), then `./mvnw quarkus:dev` |
| `mise run test` | Same Podman env, then `./mvnw -B test` |
| `mise run package` | Same Podman env, then `./mvnw -B package` |
| `mise run inject-metrics -- [flags]` | Inject rising PCP samples (`uv` + `kafka-python`) |

[`podman-env.sh`](../worker/scripts/podman-env.sh) sets `DOCKER_HOST` to the Podman API socket, disables Testcontainers Ryuk, starts `podman machine` on macOS when needed, and puts a `docker`→Podman shim on `PATH` so Quarkus does not talk to Docker Desktop.

Sanity check:

```bash
cd worker
source scripts/podman-env.sh
# Expect a line like: Podman Dev Services env: DOCKER_HOST=unix://… (docker → …/logstream-podman-docker-shim/docker)
docker info >/dev/null
```

### 6.2.6 First-time development loop

Do this once toolchain + Podman are installed.

**Terminal A — start Quarkus**

```bash
cd worker
mise run dev
```

**Expected output (success):**

1. `Podman Dev Services env: DOCKER_HOST=unix://…`
2. Maven/`quarkus:dev` starts; debugger line `Listening for transport dt_socket at address: 5005` is normal.
3. Testcontainers connects to Podman (`Connected to docker:` / Server Version from Podman).
4. `Dev Services for Kafka started` and a bootstrap such as `localhost:<ephemeral-port>`.
5. Topics listed: `{raw-metrics=1, enriched-events=1, rhel-pcp-metrics=1}`.
6. Final lines similar to:

```text
worker 1.0.0-SNAPSHOT on JVM (powered by Quarkus 3.40.1) started in …. Listening on: http://0.0.0.0:8080
Profile dev activated. Live Coding activated.
Installed features: [… kafka-client, messaging-kafka, … smallrye-health …]
```

**Health check:**

```bash
curl -s http://127.0.0.1:8080/healthz
# Expect JSON with "status":"UP" (and messaging checks OK)
```

Leave Terminal A running. Hot-reload applies Java changes automatically.

**If startup fails:**

| Symptom | Likely cause | Action |
| --- | --- | --- |
| `Could not find a valid Docker environment` / Podman API `Status 500` + `registries.conf.d/999-podman-desktop…` | Corrupt Podman Desktop registries drop-in inside the machine | See [§6.2.9](#629-podman-troubleshooting-and-external-kafka) |
| `SRCFG00040` / empty `worker.inference.base-url` | Empty-string config (fixed in current tree; leave `INFERENCE_BASE_URL` unset) | Pull latest worker; do not export `INFERENCE_BASE_URL=` |
| Port `8080` in use | Another process bound | Stop the other process or change `quarkus.http.port` |

**Stop dev mode:** in Terminal A press `q`, or send SIGTERM to the `mise run dev` / `quarkus:dev` process.

### 6.2.7 Metric inject scripts (usage and expected output)

With Terminal A still running Dev Services Kafka:

**Terminal B — inject rising filesystem samples**

```bash
# Preferred (from worker/)
cd worker
mise run inject-metrics -- --consume

# Equivalent from repo root
chmod +x scripts/inject-worker-metrics.sh   # once
./scripts/inject-worker-metrics.sh --consume
```

| Script | Role |
| --- | --- |
| [`scripts/inject_worker_metrics.py`](../scripts/inject_worker_metrics.py) | Produce/consume via `kafka-python` |
| [`scripts/inject-worker-metrics.sh`](../scripts/inject-worker-metrics.sh) | Wrapper: mise/`uv` first, optional `kcat` fallback |
| `mise run inject-metrics` | Same Python path with Podman env loaded |

**Useful flags** (pass after `--` for the mise task):

| Flag | Default | Meaning |
| --- | --- | --- |
| `-b` / `--bootstrap` | Dev Services discovery | Kafka `host:port` |
| `-n` / `--samples` | `6` | Points to send (≥ `MIN_SAMPLES`, default 4) |
| `-i` / `--interval` | `1` | Seconds between samples |
| `-H` / `--host` | `testhost` | Kafka key / `host` field |
| `-m` / `--mount` | `/var` | Filesystem instance |
| `-c` / `--capacity` | `1000` | Capacity value |
| `--consume` | off | After produce, print recent `enriched-events` |

**Expected inject output:**

```text
Using Quarkus Dev Services Kafka at localhost:<port>
Producing 6 samples to rhel-pcp-metrics @ localhost:<port> (host=testhost mount=/var)
  [0] used=100 capacity=1000 t=…
  [1] used=200 capacity=1000 t=…
  …
  [5] used=600 capacity=1000 t=…
Done. With default MIN_SAMPLES=4 and a positive fill rate, expect an alert on enriched-events.
Watch worker logs for: emitted PREEMPTIVE_STORAGE_EXHAUSTION_RISK
Consuming enriched-events (up to 20 messages from end)...
{"@timestamp":"…","event_type":"PREDICTIVE_ALERT","alert_type":"PREEMPTIVE_STORAGE_EXHAUSTION_RISK","type":"PREEMPTIVE_STORAGE_EXHAUSTION_RISK","host":"testhost","instance":"/var",…,"tte_seconds":…,"used":…,"capacity":1000.0,…}
```

**Expected worker log (Terminal A):** a line mentioning emission of `PREEMPTIVE_STORAGE_EXHAUSTION_RISK` for `testhost` / `/var`.

**Pass criteria:**

| Check | Pass |
| --- | --- |
| Bootstrap discovery | Script prints `Using Quarkus Dev Services Kafka at localhost:…` without requiring `-b` |
| Produce | Six `[n] used=…` lines; no connection errors |
| Alert | JSON on stdout (with `--consume`) and/or worker log contains `PREEMPTIVE_STORAGE_EXHAUSTION_RISK` |
| Identity | `alert_type` and `type` are exactly `PREEMPTIVE_STORAGE_EXHAUSTION_RISK`; `host` matches `-H` |

**Fail / empty consume:** usually Dev Services not up yet, wrong bootstrap, fewer than `MIN_SAMPLES` with positive slope, or alert still in cooldown (`ALERT_COOLDOWN_SECONDS`). Re-run with `-n 8` or wait for cooldown.

Unit tests (separate from inject):

```bash
cd worker && mise run test
```

Expect Maven `BUILD SUCCESS`. Package: `mise run package` (or `./mvnw -B package -DskipTests` to skip tests).

### 6.2.8 Pointing at cluster Kafka (optional)

Exporting `KAFKA_BOOTSTRAP_SERVERS` **disables** Dev Services. Example with OpenShift port-forward:

```bash
oc -n logstream-kafka port-forward svc/telemetry-kafka-plain-bootstrap 9092:9092
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
cd worker && mise run dev
# inject with explicit bootstrap:
mise run inject-metrics -- --consume -b localhost:9092
```

### 6.2.9 Podman troubleshooting and external Kafka

**Corrupt registries drop-in (macOS / Podman Desktop):** Testcontainers fails with `Status 500` / `toml: … key name appears blank` mentioning `/etc/containers/registries.conf.d/999-podman-desktop-registries-from-host.conf`. Fix inside the VM:

```bash
podman machine ssh -- sudo tee /etc/containers/registries.conf.d/999-podman-desktop-registries-from-host.conf <<'EOF'
[[registry]]
location = "quay.io"
insecure = false
EOF
```

Then retry `mise run dev`. Until the file is valid, the Podman API returns 500 on `/info` and Kafka Dev Services cannot start.

**Running Maven without mise tasks:**

```bash
cd worker
source scripts/podman-env.sh
./mvnw quarkus:dev
```

## 6.3 Inference backend decision matrix

Pick **one** path. Do not treat any row as the project default. The worker uses a single HTTP client: `POST {INFERENCE_BASE_URL}/chat/completions` with `Authorization: Bearer {INFERENCE_API_KEY}` when the key is non-empty.

| Criterion | A. vLLM in-cluster | B. RHOAI model serving | C. External OpenAI-compatible API | D. Inference disabled |
| --- | --- | --- | --- | --- |
| When to choose | You already run vLLM next to Kafka; lowest latency; prompts stay in-cluster | You already operate OpenShift AI / KServe InferenceServices | You have a corporate or public Chat Completions endpoint and cannot host a model | You only need predictive disk TTE for EDA |
| Typical `INFERENCE_BASE_URL` | `http://vllm.<namespace>.svc:8000/v1` | `https://<inference-service-host>/v1` | `https://api.openai.com/v1` or your gateway `/v1` | unset (omit the key) |
| `INFERENCE_API_KEY` | Often empty for in-cluster vLLM | Token if the route is authenticated | Required | unused |
| `INFERENCE_MODEL` | vLLM `--served-model-name` | Deployed serving name | Provider model id | unused |
| Network | ClusterIP HTTP, typically port 8000 | Route (TLS) and/or Service | Egress; may need proxy / EgressFirewall | none |
| Failure mode | Inference errors are logged; TTE alerts still emit | Same | Same | TTE alerts only |

Configure the chosen row in [`openshift/worker/configmap.yaml`](../openshift/worker/configmap.yaml) and the Secret **before** apply. Switching backends later is a ConfigMap/Secret change plus a rollout; the container image does not change.

## 6.4 Prerequisites

1. Namespace `logstream-kafka` exists.
2. Kafka cluster `telemetry` is Ready. Brokers advertise the internal plaintext listener on port 9092.
3. Topics exist: `rhel-pcp-metrics` and/or `raw-metrics` (consume), `enriched-events` (produce).
4. You can `oc` as a project admin on `logstream-kafka`.
5. Cluster can pull builder images `registry.access.redhat.com/ubi9/openjdk-21` (and runtime) for the Docker strategy BuildConfig, **or** you push a pre-built image.
6. If Kafka authorization is enabled, grant group `stream-worker` read on the consume topics and write on `enriched-events`.

### 6.4.1 Security context (restricted-v2)

The Deployment is written for the OpenShift **restricted-v2** SCC:

- `serviceAccountName: predictive-ai-worker`
- `runAsNonRoot: true`, `allowPrivilegeEscalation: false`
- all capabilities dropped, `seccompProfile: RuntimeDefault`
- `readOnlyRootFilesystem: true` with `emptyDir` on `/tmp`

Do **not** assign `privileged`, `anyuid`, or `hostaccess` unless a real constraint blocks startup.

## 6.5 Continuous build on OpenShift

Manifests include an **ImageStream** and **BuildConfig** so the Quarkus image builds in-cluster and the Deployment rolls when `:latest` changes.

| Object | Name | Role |
| --- | --- | --- |
| ImageStream | `predictive-ai-worker` | Holds built tags |
| BuildConfig | `predictive-ai-worker` | Docker strategy, `contextDir: worker` |
| Deployment | `predictive-ai-worker` | ImageChange trigger on `predictive-ai-worker:latest` |

### 6.5.1 Apply worker manifests

```bash
oc project logstream-kafka

# Secret first (not committed with a real key)
oc -n logstream-kafka create secret generic predictive-ai-worker \
  --from-literal=INFERENCE_API_KEY='' \
  --dry-run=client -o yaml | oc apply -f -

oc apply -k openshift/worker/
```

[`openshift/worker/kustomization.yaml`](../openshift/worker/kustomization.yaml) applies ServiceAccount, ImageStream, BuildConfig, ConfigMap, Secret, Deployment, and Service.

### 6.5.2 Binary build (fastest for a laptop)

From the repository root, after manifests exist:

```bash
oc start-build predictive-ai-worker --from-dir=worker --follow -n logstream-kafka
oc -n logstream-kafka rollout status deployment/predictive-ai-worker --timeout=300s
```

The BuildConfig Docker strategy uses [`worker/Dockerfile`](../worker/Dockerfile): multi-stage UBI9 OpenJDK 21 Maven package → `quarkus-app` runtime image.

### 6.5.3 Git-triggered continuous builds

The BuildConfig Git source points at `https://github.com/chrismulderza/logstream-kafka-eda-ai.git` (`main`, `contextDir: worker`). Wire a webhook so pushes rebuild automatically:

```bash
# Show GitHub webhook URL (create a secret in the BuildConfig if you harden it)
oc -n logstream-kafka describe bc/predictive-ai-worker | grep -A2 Webhook
```

In the GitHub repo: **Settings → Webhooks → Add webhook**, paste the GitHub webhook URL from the BuildConfig, content type `application/json`, events **Just the push event**.

After a successful build, the Deployment’s ImageChange trigger rolls out the new `:latest` image.

### 6.5.4 Optional: build with podman outside the cluster

```bash
cd worker
./mvnw -B package -DskipTests
podman build -t image-registry.openshift-image-registry.svc:5000/logstream-kafka/predictive-ai-worker:latest -f Dockerfile .
# push after oc registry login, then tag/push to the ImageStream
```

Prefer the in-cluster BuildConfig for continuous delivery.

## 6.6 Configure inference and Kafka

1. Edit [`openshift/worker/configmap.yaml`](../openshift/worker/configmap.yaml):
   - Confirm bootstrap `telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092`.
   - Set `KAFKA_CONSUME_TOPICS` to `rhel-pcp-metrics`, `raw-metrics`, or both.
   - Apply **one** inference row from section 6.3 to `INFERENCE_BASE_URL` and `INFERENCE_MODEL`.

2. Keep real API keys in the Secret only (section 6.5.1).

3. Keep `replicas: 1`. TTE windows are in-process. Horizontal scale is safe only if metric producers key records by host so each host stays on one partition.

After ConfigMap or Secret edits:

```bash
oc -n logstream-kafka rollout restart deployment/predictive-ai-worker
oc -n logstream-kafka rollout status deployment/predictive-ai-worker
```

## 6.7 Verify

```bash
oc -n logstream-kafka get pods -l app.kubernetes.io/name=predictive-ai-worker
oc -n logstream-kafka logs -l app.kubernetes.io/name=predictive-ai-worker --tail=50
oc -n logstream-kafka get builds,is,bc -l app.kubernetes.io/name=predictive-ai-worker
```

Healthy startup should show consumer group `stream-worker`, subscribed topics, produce topic `enriched-events`, and inference enabled or disabled.

```bash
# Consumer group (from a Kafka tools / debug pod)
bin/kafka-consumer-groups.sh \
  --bootstrap-server telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092 \
  --group stream-worker --describe

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

On a lab host, the storage-fill test in [Validation](07-validation-runbook.md) is the end-to-end check.

To automate TTE in EDA, start a **second** CLI process or AAP activation on `rulebook-optional-predictive.yml` / `aap-rulebook-optional-predictive.yml`. Keep `allow_podman_prune` and `allow_lvextend` false.

Probes: `GET /healthz` and `GET /readyz` on port 8080.

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

## 6.9 Operational notes

- **Graceful shutdown:** Quarkus / SmallRye shut down messaging within `terminationGracePeriodSeconds: 30`.
- **LLM outage:** HTTP errors are logged. Predictive metric alerts are still produced.
- **Payload shape:** PCP-style maps (`filesys.used` / `filesys.capacity` per mount), list metrics (`name` / `instance` / `value`), and flat `used`/`capacity` or `used_bytes`/`capacity_bytes`.
- **Log documents:** JSON with a `message` can be classified when inference is on. Add `rhel-system-logs` to `KAFKA_CONSUME_TOPICS` if you want that path; the packaged default is metrics-only so EDA remains the primary syslog consumer.

## 6.10 Troubleshooting

| Symptom | What to check |
| --- | --- |
| Local: Docker / Testcontainers cannot start Kafka | Podman machine/socket; run `mise run dev` (not bare Docker); [§6.2.9](#629-podman-troubleshooting-and-external-kafka) registries drop-in |
| Local: inject cannot discover bootstrap | Dev mode running? `podman ps --filter label=quarkus-dev-service-kafka`; or pass `-b host:port` |
| Local: `--consume` prints nothing | Wait for ≥ `MIN_SAMPLES` with rising used; check worker log; cooldown; see [§6.2.7](#627-metric-inject-scripts-usage-and-expected-output) |
| CrashLoop / not Ready | `oc describe pod`; bootstrap DNS; topic names; SCC only if events mention security context |
| Build fails | Build log (`oc logs -f bc/predictive-ai-worker`); Maven deps; Dockerfile context is `worker/` |
| `ImagePullBackOff` | ImageStream tag empty — run a build first |
| Group exists but lag grows | Logs for JSON parse skips; producers must send JSON |
| No TTE alerts | Need ≥ `MIN_SAMPLES` in-window points with **positive** fill rate; TTE ≤ threshold; cooldown |
| Alerts without `severity` | Expected when inference is disabled or the LLM call failed |
| `401/403` from inference | Wrong `INFERENCE_API_KEY` or RHOAI route auth |
| Connection refused to vLLM | Service name/namespace/port; URL must include `/v1` |

Delete (does not delete Kafka topics or ImageStream history unless you remove those objects):

```bash
oc -n logstream-kafka delete -k openshift/worker/
```

## 6.11 Related files

| Path | Role |
| --- | --- |
| [`worker/pom.xml`](../worker/pom.xml) | Quarkus Maven project |
| [`worker/mise.toml`](../worker/mise.toml) | mise pins + tasks (`dev`, `test`, `package`, `inject-metrics`) |
| [`worker/scripts/podman-env.sh`](../worker/scripts/podman-env.sh) | Podman-only Dev Services env (`DOCKER_HOST`, Ryuk off, docker shim) |
| [`worker/src/main/java/ai/logstream/worker/`](../worker/src/main/java/ai/logstream/worker/) | Messaging, TTE, parse, inference |
| [`worker/Dockerfile`](../worker/Dockerfile) | UBI9 OpenJDK 21 multi-stage image |
| [`scripts/inject_worker_metrics.py`](../scripts/inject_worker_metrics.py) | Preferred inject (uv + kafka-python) |
| [`scripts/inject-worker-metrics.sh`](../scripts/inject-worker-metrics.sh) | Wrapper: mise/uv first, kcat fallback |
| [`openshift/worker/buildconfig.yaml`](../openshift/worker/buildconfig.yaml) | Continuous in-cluster Docker builds |
| [`openshift/worker/imagestream.yaml`](../openshift/worker/imagestream.yaml) | ImageStream `predictive-ai-worker` |
| [`openshift/worker/`](../openshift/worker/) | Deployment, ConfigMap, Secret, Service, SA |
| [`ansible/eda/rulebook-optional-predictive.yml`](../ansible/eda/rulebook-optional-predictive.yml) | CLI EDA for `PREEMPTIVE_STORAGE_EXHAUSTION_RISK` |
| [`ansible/eda/aap-rulebook-optional-predictive.yml`](../ansible/eda/aap-rulebook-optional-predictive.yml) | AAP activation for the same alert |

## Next

Prove the worker path with the storage-fill test in [Validation](07-validation-runbook.md), then wire [SIEM and dashboards](08-siem-dashboards.md) if required.
