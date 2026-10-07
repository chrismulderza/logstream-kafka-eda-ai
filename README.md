# Logstream Kafka EDA

Ship RHEL system logs into Kafka on OpenShift, keep your existing ArcSight path, and automate responses with Event-Driven Ansible — without guessing at remediations.

This repository is an **administrator pack**: a step-by-step deployment guide plus the OpenShift manifests, Ansible roles, rulebooks, playbooks, and validation scripts you apply to an estate you already run. It does **not** create an OpenShift cluster for you.

## Read the guide

The published book opens with a full [Introduction](docs/index.md) (problem statement, pipeline, safety model, chapter map), then walks through architecture to SIEM.

| Format | How |
| --- | --- |
| GitHub Pages | https://chrismulderza.github.io/logstream-kafka-eda-ai/ |
| Markdown in Git | Start at [`docs/index.md`](docs/index.md) |
| Local preview | `pip install -r docs/requirements-docs.txt && mkdocs serve` → http://127.0.0.1:8000 |

## What this pack provides

1. **Dual-home rsyslog** — add Kafka (`omkafka`) while ArcSight `omfwd` / `omrelp` stays untouched.
2. **KRaft Kafka on OpenShift** — Streams for Apache Kafka topics and listeners for producers and consumers.
3. **Event-Driven Ansible on ten high-value syslog patterns** — OOM, SSH brute force, sudo failures, SELinux AVCs, systemd failures, disk I/O errors, account changes, kernel panic / MCE, link down, and package installs.
4. **Fail-closed remediations** — diagnose or notify by default; destructive actions stay off until you opt in.
5. **Optional predictive disk alerts** — PCP time-to-exhaustion (and optional LLM). Skip if you only need log-driven EDA.

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

## Repository layout

| Path | Purpose |
| --- | --- |
| [`docs/`](docs/) | MkDocs deployment book |
| [`openshift/kafka/`](openshift/kafka/) | Streams operator, KRaft node pools, Kafka cluster, topics |
| [`ansible/telemetry/`](ansible/telemetry/) | Dual-home rsyslog + PCP exporters |
| [`ansible/eda/`](ansible/eda/) | Rulebooks, gated playbooks, EE, example vars |
| [`extensions/eda/rulebooks/`](extensions/eda/rulebooks/) | AAP-scanned rulebook copies |
| [`worker/`](worker/) + [`openshift/worker/`](openshift/worker/) | Optional predictive stream worker |
| [`scripts/`](scripts/) | Synthetic tests and pipeline checks |
| [`siem/`](siem/) + [`grafana/`](grafana/) | Parallel SIEM consumers and dashboards |

## Default names

| Object | Value |
| --- | --- |
| Namespace | `logstream-kafka` |
| Kafka cluster | `telemetry` |
| Internal bootstrap | `telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092` |
| External listener | Route TLS `tls-external` (clients use port **443**) |
| Topics | `rhel-system-logs`, `rhel-pcp-metrics`, `raw-metrics`, `enriched-events` |
| EDA group | `ansible-eda` |
| SIEM groups | `siem-logstash`, `siem-splunk` |
| Worker group | `stream-worker` (only if optional chapter 6 is deployed) |

## Note on `plan.md`

[`plan.md`](plan.md) is the original research brief. Where current Red Hat docs differ (especially Streams for Apache Kafka 3.x KRaft-only and SELinux for `omkafka`), the book and artifacts follow current practice.
