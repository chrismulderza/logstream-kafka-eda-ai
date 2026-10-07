# 7. Validation

Prove the event-driven pipeline on OpenShift namespace `logstream-kafka`, Kafka cluster `telemetry`, and attached RHEL hosts. Assume Kafka, host telemetry, and Event-Driven Ansible from chapters 3–5 are already deployed. Section 7.6 (storage fill) applies only if you deployed the [optional predictive worker](06-optional-predictive-ai-worker.md).

Companion SIEM and Grafana work is in [SIEM and dashboards](08-siem-dashboards.md).

## 7.1 Scope and naming

| Item | Value |
|------|--------|
| Namespace | `logstream-kafka` |
| Kafka cluster | `telemetry` |
| Internal bootstrap (placeholder) | `telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092` |
| External listener | OpenShift Route TLS (clients use **443**) |
| Topics | `rhel-system-logs`, `rhel-pcp-metrics`, `raw-metrics`, `enriched-events` |
| EDA consumer group | `ansible-eda` |
| Stream worker group | `stream-worker` |
| SIEM groups (must differ) | `siem-logstash`, `siem-splunk` |
| Verification consumer group | `verify-pipeline` (scripts only; do not reuse EDA/worker/SIEM groups) |

Scripts in this repository:

| Script | Where to run | Tools required on that machine |
| --- | --- | --- |
| [scripts/inject-oom-log.sh](../scripts/inject-oom-log.sh) | Dual-homed **RHEL** endpoint | `logger` (util-linux) |
| [scripts/storage-fill-test.sh](../scripts/storage-fill-test.sh) | Lab **RHEL** host with free space on `/var/log` | `fallocate`, `df`, `rm` |
| [scripts/verify-pipeline.sh](../scripts/verify-pipeline.sh) | Workstation or jump host with cluster access | `oc` and/or `kcat` (script prints placeholders if missing) |
| [scripts/inject-worker-metrics.sh](../scripts/inject-worker-metrics.sh) / [inject_worker_metrics.py](../scripts/inject_worker_metrics.py) | **Developer workstation** (Quarkus Dev Services) | macOS/Fedora/RHEL setup, `mise run inject-metrics -- --consume`, and expected `PREEMPTIVE_STORAGE_EXHAUSTION_RISK` output — [chapter 6 §6.2](06-optional-predictive-ai-worker.md#62-local-development-quarkus-dev-mode) |

Dashboards used during validation:

- [grafana/kafka-throughput-lag.json](../grafana/kafka-throughput-lag.json)
- [grafana/pcp-system-metrics.json](../grafana/pcp-system-metrics.json)

## 7.2 Safety

1. Run synthetic tests on a lab host first.
2. The storage-fill test **allocates 5 GiB** on the log filesystem (`fallocate -l 5G /var/log/test_fill.img`). On a small `/var` this can cause a **real** outage. Always run cleanup (`rm` of the image) in the same change window.
3. `kcat` in [scripts/verify-pipeline.sh](../scripts/verify-pipeline.sh) uses group `verify-pipeline`. Never point `kcat -G` at `ansible-eda`, `stream-worker`, `siem-logstash`, or `siem-splunk`.
4. Do not leave `test_fill.img` in place on production.

## 7.3 Prerequisites checklist

| Step | Action | Pass | Fail |
|------|--------|------|------|
| P1 | `oc` login works; `oc project logstream-kafka` (or `-n logstream-kafka`) | Current context is the cluster; you can list namespaced objects | `Unauthorized`, wrong cluster, or namespace missing |
| P2 | `oc get kafka telemetry -n logstream-kafka` shows Ready | `Ready` / listeners present | CR missing or `NotReady` |
| P3 | `oc get kafkatopic -n logstream-kafka` lists all four topics | Names match the table in 7.1 | Topic missing or wrong retention |
| P4 | Broker pods in `logstream-kafka` are Running | 3 brokers (and 3 controllers if using node pools) Running | CrashLoop, pending PVCs |
| P5 | RHEL host: `rsyslog` and `pcp2kafka.service` (or equivalent) active | `systemctl is-active` is `active` | Unit failed; no Kafka produce |
| P5b | Existing ArcSight rsyslog drop-ins still present and unchanged | `ls /etc/rsyslog.d` plus connector still receiving | Kafka onboarding overwrote SIEM forwarding |
| P6 | Host can reach bootstrap (`9092` in-cluster or Route **443**) | `kcat -b <bootstrap> -L` lists brokers | Timeout, TLS mismatch, NetworkPolicy |
| P7 | `logger` and `fallocate` installed on the test host | Commands exist | Missing util-linux / util-linux-core |

Print required group names:

```bash
./scripts/verify-pipeline.sh groups
```

**Pass:** output lists `ansible-eda`, `stream-worker`, `siem-logstash`, `siem-splunk`, and `verify-pipeline`.  
**Fail:** script not executable or you plan to reuse `ansible-eda` for SIEM.

Make scripts executable once:

```bash
chmod +x scripts/inject-oom-log.sh scripts/storage-fill-test.sh scripts/verify-pipeline.sh scripts/inject-worker-metrics.sh
```

## 7.4 Baseline cluster and topic inspection

Set placeholders for your environment:

```bash
export NAMESPACE=logstream-kafka
export KAFKA_CLUSTER=telemetry
export BOOTSTRAP=telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092
# External example:
# export BOOTSTRAP=telemetry-kafka-bootstrap-logstream-kafka.apps.example.com:443
```

Run:

```bash
./scripts/verify-pipeline.sh oc
./scripts/verify-pipeline.sh kcat
# or
./scripts/verify-pipeline.sh all
```

If `oc` or `kcat` is not on `PATH`, the script prints the same commands as copy-paste placeholders.

Manual equivalents:

```bash
oc get kafka telemetry -n logstream-kafka -o wide
oc get kafkanodepool -n logstream-kafka
oc get kafkatopic -n logstream-kafka
oc get pods -n logstream-kafka -l strimzi.io/cluster=telemetry

kcat -b "${BOOTSTRAP}" -L
kcat -b "${BOOTSTRAP}" -t rhel-system-logs -L
```

| Step | Pass | Fail |
|------|------|------|
| B1 | Kafka CR Ready; node pools present | Operator not reconciling |
| B2 | Topics `rhel-system-logs`, `rhel-pcp-metrics`, `raw-metrics`, `enriched-events` exist | Missing KafkaTopic |
| B3 | `kcat -L` shows brokers and the four topics | Empty metadata, auth error |
| B4 | Grafana Kafka dashboard (after import per [chapter 08](08-siem-dashboards.md)) shows bytes/messages in | All panels `No data` and brokers idle |

## 7.5 Synthetic test A — OOM log injection

**Purpose:** prove `omkafka` JSON on `rhel-system-logs` **and** that existing ArcSight forwarding still receives the same line. EDA group `ansible-eda` matches an OOM rule; SIEM on Kafka uses **their own** groups.

On the **RHEL endpoint** that ships syslog to Kafka:

```bash
./scripts/inject-oom-log.sh
```

The script runs:

```bash
logger -p kern.err -t test_oom -- "Out of memory: Kill process 1234"
```

Confirm local syslog (optional):

```bash
journalctl -t test_oom -n 5 --no-pager
# or grep in /var/log/messages
```

Consume **without** stealing EDA offsets:

```bash
kcat -b "${BOOTSTRAP}" -t rhel-system-logs -C -o -20 -e -c 20 -G verify-pipeline
```

Look for `"syslogtag":"test_oom"` (or equivalent) and message `Out of memory: Kill process 1234`.

EDA:

```bash
# AAP: activation log in EDA controller
# CLI lab:
# ansible-rulebook --rulebook ansible/eda/rulebook.yml ...
```

Expect a match on OOM and a run of `remediate_oom.yml` (or a logged skip if the host is already healthy).

| Step | Pass | Fail |
|------|------|------|
| O1 | `logger` exits 0 | `logger` missing or permission denied |
| O2 | Line appears in local journal/messages with tag `test_oom` | rsyslog dropped the line |
| O2b | Same line still reaches ArcSight (connector or Logger) | Kafka dual-home broke SIEM forwarding |
| O3 | Same payload on `rhel-system-logs` within the SLA (typically seconds) | Topic silent: omkafka, TLS, SELinux, or Route; ArcSight `stop` before Kafka |
| O4 | Consumer group `ansible-eda` lag does not grow unbounded | EDA not subscribed or stuck |
| O5 | Rule match / diagnostic playbook evidence in EDA logs | Rulebook pattern mismatch |
| O6 | SIEM (if enabled) shows the event via `siem-logstash` / `siem-splunk` only | SIEM using `ansible-eda` (incorrect) |

Cleanup: none required (syslog is append-only). Repeat injection if Kafka was down.

Optional extra catalog lines (same dual-home host; gates remain closed):

```bash
logger -t sshd -- "Failed password for invalid user admin from 203.0.113.10 port 22 ssh2"
logger -t sudo -- "user : 1 incorrect password attempt ; TTY=pts/0 ; PWD=/home/user ; USER=root ; COMMAND=/bin/id"
logger -t systemd -- "Failed to start sshd.service"
logger -t dnf -- "Installed: tree-1.8.0-1.el9.x86_64"
```

Full catalog: [chapter 5](05-event-driven-ansible.md).

## 7.6 Optional synthetic test B — storage fill (predictive worker)

**Purpose:** raise filesystem used so PCP (`rhel-pcp-metrics` / `raw-metrics`) trends up and `stream-worker` publishes `PREEMPTIVE_STORAGE_EXHAUSTION_RISK` on `enriched-events`.

**Risk (mandatory read):** `fallocate -l 5G /var/log/test_fill.img` reserves 5 GiB on the log volume. If free space is less than ~5 GiB plus headroom, you can fill `/var/log`, break logging, and impact the node. Prefer a host with ≥15 GiB free on that filesystem.

Status and create:

```bash
df -h /var/log
./scripts/storage-fill-test.sh status
./scripts/storage-fill-test.sh create
```

Watch PCP / Grafana [grafana/pcp-system-metrics.json](../grafana/pcp-system-metrics.json) filesys used %. Tail worker output:

```bash
oc logs -n logstream-kafka -l app.kubernetes.io/name=predictive-ai-worker --tail=100 -f
kcat -b "${BOOTSTRAP}" -t raw-metrics -C -o -5 -e -c 5 -G verify-pipeline
kcat -b "${BOOTSTRAP}" -t enriched-events -C -o -20 -e -c 20 -G verify-pipeline
```

EDA should see enriched events on group `ansible-eda` and may run `proactive_disk_mitigation.yml`.

**Cleanup (required):**

```bash
./scripts/storage-fill-test.sh cleanup
# equivalent: rm -f /var/log/test_fill.img
df -h /var/log
./scripts/storage-fill-test.sh status
```

| Step | Pass | Fail |
|------|------|------|
| S1 | `df` shows ≥15 GiB free (lab heuristic) before create | Insufficient space — **do not create** |
| S2 | `create` produces `/var/log/test_fill.img` (~5 GiB) | `fallocate` error; file already exists |
| S3 | `rhel-pcp-metrics` or `raw-metrics` show increased filesys used | pcp2kafka/kcat producer down |
| S4 | `enriched-events` contains a preemptive storage risk event | Worker formula/window not warming; wait one window then recheck |
| S5 | Group `stream-worker` is the producer path; lag on that group stays bounded | Worker crash / OOMKill |
| S6 | After `cleanup`, image is gone and `df` recovers | Leftover file — run `rm -f /var/log/test_fill.img` as root |

If create fails because the file exists, run `cleanup` then `create`.

## 7.7 Consumer group and lag verification

Lag belongs on [grafana/kafka-throughput-lag.json](../grafana/kafka-throughput-lag.json) (Prometheus/Strimzi kafka-exporter). Groups that **must** appear as four distinct series:

- `ansible-eda`
- `stream-worker`
- `siem-logstash` (only after Logstash is deployed — [siem/logstash-kafka.conf](../siem/logstash-kafka.conf))
- `siem-splunk` (only after Splunk Connect — [siem/splunk-connect-kafka.yaml](../siem/splunk-connect-kafka.yaml))

| Step | Pass | Fail |
|------|------|------|
| L1 | EDA and worker groups exist after traffic | No members — process not consuming |
| L2 | SIEM groups are **not** named `ansible-eda` or `stream-worker` | Shared group (split-brain / stolen offsets) |
| L3 | Lag returns toward zero after each synthetic test | Stuck consumer; check TLS and max.poll.interval |
| L4 | `verify-pipeline` group is unused except this runbook | Operators using `-G ansible-eda` in kcat |

Kafka Exporter / Strimzi metrics placeholders (Prometheus):

```text
sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup=~"ansible-eda|stream-worker|siem-logstash|siem-splunk"})
```

## 7.8 End-to-end pass/fail (sign-off)

Record date, operator, cluster, and host. All of **Must-pass** must be Pass before production SIEM cutover ([chapter 08](08-siem-dashboards.md)).

| ID | Control | Pass | Fail |
|----|---------|------|------|
| E1 | Kafka `telemetry` Ready in `logstream-kafka` | | |
| E2 | Four topics present and receiving | | |
| E3 | OOM injection visible on `rhel-system-logs` | | |
| E3b | Same OOM line still received by ArcSight | | |
| E4 | EDA matched OOM using group `ansible-eda` | | |
| E5 | Storage fill created, worker enriched, then **cleaned up** | | |
| E6 | No SIEM consumer shares EDA/worker groups | | |
| E7 | Grafana Kafka lag dashboard imported and populated (or documented N/A if metrics not scraped yet) | | |

**Rollback:** `./scripts/storage-fill-test.sh cleanup`; stop extra `kcat -G` processes; do not reset production consumer groups.

## 7.9 Related files

| File | Role |
|------|------|
| [scripts/inject-oom-log.sh](../scripts/inject-oom-log.sh) | OOM synthetic |
| [scripts/storage-fill-test.sh](../scripts/storage-fill-test.sh) | Disk synthetic + cleanup |
| [scripts/verify-pipeline.sh](../scripts/verify-pipeline.sh) | oc/kcat placeholders |
| [grafana/kafka-throughput-lag.json](../grafana/kafka-throughput-lag.json) | Throughput and lag |
| [grafana/pcp-system-metrics.json](../grafana/pcp-system-metrics.json) | Host PCP / filesys |
| [siem/logstash-kafka.conf](../siem/logstash-kafka.conf) | Logstash group `siem-logstash` |
| [siem/splunk-connect-kafka.yaml](../siem/splunk-connect-kafka.yaml) | Splunk group `siem-splunk` |
| [08-siem-dashboards.md](08-siem-dashboards.md) | SIEM and dashboard import |

## Next

Continue with [SIEM and dashboards](08-siem-dashboards.md) for parallel Logstash/Splunk consumers and Grafana.
