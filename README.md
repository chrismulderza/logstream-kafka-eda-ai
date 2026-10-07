# Logstream Kafka EDA

Ship RHEL system logs into Kafka on OpenShift, keep your existing ArcSight path, and automate responses with Event-Driven Ansible — without guessing at remediations.

This repository is an **administrator pack**: a step-by-step deployment guide plus the OpenShift manifests, Ansible roles, rulebooks, playbooks, and validation scripts you apply to an estate you already run. It does **not** create an OpenShift cluster for you.

**Published guide:** [chrismulderza.github.io/logstream-kafka-eda-ai](https://chrismulderza.github.io/logstream-kafka-eda-ai/)

## What problem this solves

Many RHEL fleets already forward syslog to ArcSight. You often also want those same events in Kafka so automation and SIEM tools can react in parallel — without ripping out ArcSight or turning every noisy log line into a destructive playbook.

This pack gives you:

1. **Dual-home rsyslog** — add Kafka (`omkafka`) while ArcSight `omfwd` / `omrelp` stays untouched.
2. **KRaft Kafka on OpenShift** — Streams for Apache Kafka topics and listeners you can point producers and consumers at.
3. **Event-Driven Ansible on ten high-value syslog patterns** — OOM, SSH brute force, sudo failures, SELinux AVCs, systemd failures, disk I/O errors, account changes, kernel panic / MCE, link down, and package installs.
4. **Fail-closed remediations** — by default playbooks diagnose or notify; firewall bans, restarts, IPMI, and similar actions stay off until you opt in.
5. **Optional predictive disk alerts** — a stream worker that computes time-to-exhaustion from PCP metrics and can call an OpenAI-compatible LLM. Skip it if you only care about log-driven EDA.

## How the pipeline fits together

```text
RHEL hosts
  ├── rsyslog ──► ArcSight          (existing, unchanged)
  ├── rsyslog ──► Kafka             (rhel-system-logs)
  └── PCP ──────► Kafka             (rhel-pcp-metrics, optional)

Kafka
  ├── Event-Driven Ansible          (group: ansible-eda)
  ├── SIEM (Logstash / Splunk)      (groups: siem-logstash, siem-splunk)
  └── Optional stream worker        (group: stream-worker → enriched-events)
```

Consumer groups are exclusive. Sharing `ansible-eda` with SIEM or the worker will steal partitions and drop events.

## Who should use this

Platform, logging, and automation administrators who operate:

- OpenShift **4.20–4.21**
- RHEL **8.10 / 9** endpoints
- Ansible Automation Platform **2.6** Event-Driven Ansible (or **2.5** only on OCP 4.20), **or** `ansible-rulebook` on a jump host

You should be comfortable applying Operator manifests, running Ansible against RHEL, and validating with `oc` / `kcat` / `logger`.

## What’s in the repository

| Path | Purpose |
| --- | --- |
| [`docs/`](docs/) | MkDocs deployment book (architecture through SIEM) |
| [`openshift/kafka/`](openshift/kafka/) | Namespace, Streams operator, KRaft node pools, Kafka cluster, topics |
| [`ansible/telemetry/`](ansible/telemetry/) | Role that adds `omkafka` + PCP exporters; never replaces ArcSight drop-ins |
| [`ansible/eda/`](ansible/eda/) | Rulebooks, gated playbooks, EE definition, example inventory and extra vars |
| [`extensions/eda/rulebooks/`](extensions/eda/rulebooks/) | AAP-scanned copies of the AAP rulebooks |
| [`worker/`](worker/) + [`openshift/worker/`](openshift/worker/) | Optional predictive AI stream worker |
| [`scripts/`](scripts/) | Synthetic OOM inject, storage-fill test, pipeline checks |
| [`siem/`](siem/) + [`grafana/`](grafana/) | Parallel SIEM consumers and Grafana dashboards |

## Read the guide

| Format | How |
| --- | --- |
| GitHub Pages | https://chrismulderza.github.io/logstream-kafka-eda-ai/ |
| Markdown in Git | Start at [`docs/index.md`](docs/index.md) |
| Local preview | `pip install -r docs/requirements-docs.txt && mkdocs serve` → http://127.0.0.1:8000 |

Suggested reading order:

1. [Architecture](docs/01-architecture.md) — topics, listeners, consumer groups
2. [Prerequisites](docs/02-prerequisites.md) — versions, network, TLS, SELinux
3. [Kafka on OpenShift](docs/03-kafka-openshift.md)
4. [RHEL telemetry](docs/04-rhel-telemetry.md) — keep ArcSight; add Kafka
5. [Event-Driven Ansible](docs/05-event-driven-ansible.md) — the ten-event catalog
6. [Optional predictive worker](docs/06-optional-predictive-ai-worker.md) — only if you need PCP TTE
7. [Validation](docs/07-validation-runbook.md) and [SIEM / Grafana](docs/08-siem-dashboards.md)

## Default names (keep them consistent)

Changing a name means updating every producer and consumer that uses it.

| Object | Value |
| --- | --- |
| Namespace | `logstream-kafka` |
| Kafka cluster | `telemetry` |
| Internal bootstrap | `telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092` |
| External listener | Route TLS `tls-external` (clients use port **443**) |
| Topics | `rhel-system-logs`, `rhel-pcp-metrics`, `raw-metrics`, `enriched-events` |
| EDA group | `ansible-eda` |
| SIEM groups | `siem-logstash`, `siem-splunk` |
| Worker group | `stream-worker` (only if chapter 6 is deployed) |

## Safety model

Destructive actions are **off by default**. Extra vars such as `allow_firewall_ban`, `allow_service_restart`, `allow_ipmi_reboot`, `allow_lb_isolate`, `allow_podman_prune`, and `allow_lvextend` must be set explicitly, and you should restart the EDA activation or CLI process after changing them.

Open gates only in a change window, on a limited inventory, after the validation chapter passes.

## Runtime choices

There is no single prescribed stack beyond Kafka + dual-home syslog + EDA:

- **EDA:** AAP Event-Driven Ansible activations, or `ansible-rulebook` CLI — same playbooks, different control plane ([chapter 5](docs/05-event-driven-ansible.md)).
- **Inference (optional):** none, vLLM, OpenShift AI, or an external OpenAI-compatible API ([chapter 6](docs/06-optional-predictive-ai-worker.md)).

## Note on `plan.md`

[`plan.md`](plan.md) is the original research brief that shaped this pack. Where current Red Hat docs differ (especially Streams for Apache Kafka 3.x KRaft-only and SELinux for `omkafka`), the book and artifacts follow current practice.
