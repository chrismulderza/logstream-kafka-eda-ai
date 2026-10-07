# Event-Driven Telemetry and Ansible EDA

This repository is an **administrator implementation pack**: a chaptered deployment book plus the OpenShift, Ansible, optional worker, Grafana, and SIEM artifacts the book tells you to apply.

The **required** path is RHEL syslog (kept on ArcSight, added to Kafka) and **Event-Driven Ansible** matching ten operational and security events. Predictive analytics is optional.

It does **not** provision an OpenShift cluster. You apply these files to a cluster and RHEL estate you already operate.

## Read the book

| Format | How |
| --- | --- |
| Git markdown | Start at [docs/index.md](docs/index.md) |
| GitHub Pages | https://chrismulderza.github.io/logstream-kafka-eda-ai/ (built from `main` by `.github/workflows/docs.yml`) |
| Local MkDocs | `pip install -r docs/requirements-docs.txt && mkdocs serve` then open `http://127.0.0.1:8000` |

## Apply order

Follow the chapters in order unless you already have a KRaft Kafka cluster that matches the topic and listener contracts in [docs/01-architecture.md](docs/01-architecture.md).

1. [Prerequisites](docs/02-prerequisites.md) — versions, ports, TLS, SELinux, RBAC
2. [Kafka on OpenShift (KRaft)](docs/03-kafka-openshift.md) — `openshift/kafka/`
3. [RHEL host telemetry](docs/04-rhel-telemetry.md) — `ansible/telemetry/` (adds Kafka; keeps ArcSight rsyslog)
4. [Event-Driven Ansible](docs/05-event-driven-ansible.md) — `ansible/eda/` (ten syslog events)
5. [Optional predictive AI stream worker](docs/06-optional-predictive-ai-worker.md) — `worker/` and `openshift/worker/`
6. [Validation](docs/07-validation-runbook.md) — `scripts/`
7. [SIEM and Grafana](docs/08-siem-dashboards.md) — `siem/` and `grafana/`

## Shared identifiers

Use these names everywhere unless you change them in **all** manifests, playbooks, and consumer groups:

| Object | Value |
| --- | --- |
| OpenShift namespace | `logstream-kafka` |
| Kafka cluster | `telemetry` |
| Internal bootstrap | `telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092` |
| External listener | Route TLS `tls-external` (client port **443** on the Route) |
| Topics | `rhel-system-logs`, `rhel-pcp-metrics`, `raw-metrics`, `enriched-events` |
| Worker group | `stream-worker` (only if chapter 6 is deployed) |
| EDA group | `ansible-eda` |
| SIEM groups | `siem-logstash`, `siem-splunk` |
| Optional predictive alert type | `PREEMPTIVE_STORAGE_EXHAUSTION_RISK` |

## Decision matrices (no single default)

- **EDA runtime:** Ansible Automation Platform 2.6 (2.5 only on OCP 4.20) Event-Driven Ansible, or `ansible-rulebook` CLI — see [chapter 5](docs/05-event-driven-ansible.md).
- **Inference (optional):** vLLM, Red Hat OpenShift AI (OpenAI-compatible), or an external OpenAI-compatible API — see [chapter 6](docs/06-optional-predictive-ai-worker.md).

Destructive remediations (firewall bans, service restarts, IPMI, `podman system prune`, `lvextend`) default to **off**.

## Research note

[plan.md](plan.md) is the original research brief. Where current Red Hat documentation differs (especially Streams for Apache Kafka **3.x** KRaft-only and SELinux for `omkafka`), the book and artifacts follow current practice.
