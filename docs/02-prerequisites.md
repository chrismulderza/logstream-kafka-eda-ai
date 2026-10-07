# 2. Prerequisites and Environment Verification

Complete this chapter before applying Kafka or host playbooks. Commands assume `oc` is logged in as a user who can create namespaces and Operator subscriptions, and that Ansible can SSH to RHEL endpoints as a privileged user.

## 2.1 Version matrix

Pin to versions supported in *your* subscription. The table is the floor this pack was written against.

| Layer | Minimum | Notes |
| --- | --- | --- |
| OpenShift Container Platform | **4.20** (4.2x) | Target 4.20–4.21. Kubernetes 1.33 on 4.20, 1.34 on 4.21. See [§2.1.1](#211-openshift-42x-compatibility). |
| Red Hat Streams for Apache Kafka | 3.2 on `stable` | KRaft only. Tested by Red Hat on OCP 4.16–4.21 **except 4.17**. Operator catalog: `amq-streams` |
| KafkaNodePool + Kafka CRs | Cluster Operator 3.2 | `kafka.strimzi.io/v1`; annotations `strimzi.io/kraft: enabled` and `strimzi.io/node-pools: enabled` |
| RHEL endpoints (syslog/PCP) | 8.10 or 9.x | Outside the cluster. OCP 4.2x **cluster** workers are RHCOS, not RHEL 8 |
| Ansible Automation Platform | 2.6 on OCP 4.21; 2.5 or 2.6 on 4.20 | Operator AAP 2.5 supports OCP through **4.20** only. CLI `ansible-rulebook` has no OCP pin |
| ansible-rulebook + `ansible.eda` | Current supported collection | CLI path; Python 3.9+ |
| Grafana | 9+ | Optional dashboards |
| Logstash / Splunk Connect for Kafka | Your SIEM standard | Optional parallel consumers |

### 2.1.1 OpenShift 4.2x compatibility

This pack targets the **4.20 / 4.21** line (4.2x), not historic 4.12.

| Area | Change vs 4.12-era pack | Action in this repo |
| --- | --- | --- |
| Streams for Apache Kafka | 3.2 requires OCP 4.16–4.21 excluding 4.17 | Keep `stable` Subscription; do not pin CSV. CRs stay `kafka.strimzi.io/v1` |
| Kafka Routes | OpenShift still serves `type: route` listeners. Gateway API is extra, not a replacement | No listener YAML change. Clients still use Route **443** |
| SCC | `restricted-v2` remains the default for new workloads | Worker Deployment already matches 4.20 (`runAsNonRoot`, drop ALL, seccomp) |
| User namespaces | Optional on 4.20+ | Do not enable for this worker |
| CatalogSource | Still `redhat-operators` in `openshift-marketplace` | Subscription YAML unchanged |
| Cluster compute OS | RHEL 8 workers not supported on OCP 4.19+ | Telemetry hosts remain RHEL 8/9 **outside** the cluster |
| AAP on the cluster | 2.5 Operator: OCP 4.12–4.20. 2.6 Operator: 4.14–4.22 | Use AAP **2.6** DE/EE images on 4.2x; 2.5 only if the cluster is 4.20 |
| OpenShift AI | 4.20 adds Gateway API Inference Extension (optional) | Worker still uses OpenAI-compatible HTTP; GIE not required |
| `oc` client | Match the cluster minor (4.20 or 4.21) | `oc version` should show a 4.2x client or Kubernetes 1.33/1.34 |

No Kafka, worker, or Route manifests required a 4.2x API bump. Confirm OperatorHub lists **Streams for Apache Kafka 3.2** (or the current `stable` CSV) after you subscribe.

### RHEL packages

| Package | Channel (typical) | Purpose |
| --- | --- | --- |
| `rsyslog` | BaseOS / AppStream | Syslog daemon |
| `rsyslog-kafka` | AppStream | Provides `omkafka` / `imkafka` |
| `pcp` | AppStream | Performance Co-Pilot |
| `pcp-system-tools` | AppStream | `pmstat` and related tools |
| `pcp-export-pcp2json` | AppStream | `pcp2json` exporter (package name can vary slightly by release; the playbook tries this name first) |
| `kcat` | AppStream on some 9.x releases; otherwise **EPEL** | Kafka CLI produce/consume |

If `kcat` is missing from AppStream, enable EPEL per Red Hat guidance or install from a supported internal mirror. Do not copy binaries from the public internet onto production hosts without change control.

Confirm on a sample host:

```bash
subscription-manager repos --list-enabled
dnf list rsyslog rsyslog-kafka pcp pcp-system-tools pcp-export-pcp2json kcat
```

### OpenShift CLI and Ansible control node

```bash
oc version
# Server should be OpenShift 4.20 or 4.21 (Kubernetes 1.33 / 1.34).
oc auth can-i create subscriptions.operators.coreos.com -n logstream-kafka
ansible --version
python3 -c "import sys; print(sys.version)"
```

Install MkDocs only if you want the HTML book locally: `pip install -r docs/requirements-docs.txt`.

## 2.2 Cluster capacity

| Resource | Requested by this pack |
| --- | --- |
| Namespace | `logstream-kafka` |
| Controller PVCs | 3 × 20Gi |
| Broker PVCs | 3 × 100Gi |
| Worker | 1 Deployment (CPU/memory in [openshift/worker/](../openshift/worker/)) |
| StorageClass | Must support `ReadWriteOnce` block or file volumes suitable for Kafka |

Identify a StorageClass before you apply node pools:

```bash
oc get storageclass
```

Set `storageClassName` in the KafkaNodePool files if the default class is wrong for Kafka (for example, do not use a weakly consistent RWX NFS class).

## 2.3 Network and ports

| Path | Port | Direction |
| --- | --- | --- |
| Kafka internal `plain` | 9092/TCP | Worker, in-cluster EDA, in-cluster SIEM → brokers |
| Kafka external Route | **443/TCP** (listener 9094 in the CR) | RHEL hosts and jump hosts → OpenShift ingress |
| `pmcd` | 44321/TCP | Optional remote PCP clients → RHEL |
| `pmproxy` | 44322/TCP | Optional HTTP/metrics proxy → RHEL |
| Inference | 8000/TCP or 443/TCP | Worker → vLLM / RHOAI / external API |
| AAP / ansible-rulebook | As deployed | EDA runtime → Kafka (9092 in-cluster or 443 via Route) |

Firewall on RHEL:

- Egress to the Kafka Route hostname, TCP 443.
- Egress to inference if the worker runs on RHEL (this pack runs the worker on OpenShift).
- Ingress 44321/44322 only if you intentionally expose PCP.

DNS: every RHEL producer must resolve the Kafka bootstrap Route. After chapter 3:

```bash
oc get routes -n logstream-kafka
getent hosts <bootstrap-route-host>
openssl s_client -connect <bootstrap-route-host>:443 -servername <bootstrap-route-host> </dev/null
```

### TLS strategy for the external listener

| Mode | Use |
| --- | --- |
| **Passthrough** (this pack) | Kafka TLS end-to-end. Clients trust the **Kafka cluster CA**, not the OpenShift ingress cert. |
| Reencrypt / edge | Not used here. Edge TLS would terminate Kafka TLS at the router and break Kafka client TLS hostname verification. |

## 2.4 Security baselines

### SELinux on RHEL producers

`omkafka` and `kcat` make outbound TCP connections. That is **not** the same as "syslog sending mail."

Do **not** enable `logging_syslogd_can_send_mail` as a Kafka workaround.

Procedure:

1. Put SELinux in enforcing (production) and reproduce a send.
2. Check denials:

```bash
sudo ausearch -m avc -ts recent
sudo grep rsyslog /var/log/audit/audit.log | tail
```

3. Prefer a **targeted policy module** generated from those AVCs (`audit2allow -M rsyslog-kafka` after review).
4. Common boolean that sometimes unblocks generic client TCP (last resort, wide blast radius): `nis_enabled`. Treat it as a temporary diagnostic, not the design.

rsyslog may also need to read the Kafka CA file. Put the CA in a location rsyslog can read (the telemetry role uses a certs directory under `/etc/rsyslog.d/` or the trust anchor path) and restore context with `restorecon`.

Existing **ArcSight** forwarding stays. The telemetry role only adds `/etc/rsyslog.d/05-omkafka-additional.conf`. Do not replace `/etc/rsyslog.conf` as part of this project.

Details: [chapter 4](04-rhel-telemetry.md).

### OpenShift RBAC and SCC

| Identity | Needs |
| --- | --- |
| Cluster Operator ServiceAccount (created by the Operator) | Operator-install RBAC from the `amq-streams` CSV; typically `privileged` SCC for Kafka pods as shipped by Red Hat |
| `stream-worker` ServiceAccount | `restricted-v2` SCC is enough: no hostPath, no privileged |
| Human installer | `create` on Namespace, OperatorGroup, Subscription, Kafka, KafkaNodePool, KafkaTopic |

Do not grant `cluster-admin` to the worker. Do not run the worker as `privileged`.

## 2.5 Credentials and secrets

| Secret | Where it lives | Git |
| --- | --- | --- |
| Kafka cluster CA | `telemetry-cluster-ca-cert` in `logstream-kafka` | Extract at deploy time; not committed |
| Inference API key (optional chapter 6) | `predictive-ai-worker` Secret | Use the example Secret; fill on cluster |
| AAP credentials | AAP credential store | Not in Git |
| Ansible vault | Optional for bootstrap hostnames | `.example` files only in Git |

This repository gitignores `openshift/worker/secret.yaml` if you copy the example to that name. Prefer `oc create secret` as documented in [optional chapter 6](06-optional-predictive-ai-worker.md).

## 2.6 Pre-flight checklist

Print this list and tick it in the change window.

- [ ] Cluster is OpenShift **4.20 or 4.21** (`oc version`)
- [ ] `oc` login works against the target cluster
- [ ] You can create namespaces and OperatorHub subscriptions
- [ ] StorageClass for Kafka is chosen; 3×20Gi + 3×100Gi is available
- [ ] RHEL hosts are subscribed (BaseOS + AppStream)
- [ ] Ansible control node can SSH to hosts with become
- [ ] Inventory of existing rsyslog ArcSight drop-ins is recorded (files will not be replaced)
- [ ] Firewall/DNS will allow RHEL → OpenShift ingress 443 **and** existing ArcSight connector ports still work
- [ ] You have chosen EDA: AAP 2.6 (or 2.5 on OCP 4.20 only) or `ansible-rulebook`
- [ ] Predictive worker / inference is in scope **or** explicitly skipped
- [ ] SIEM owners know they must use `siem-logstash` / `siem-splunk` groups
- [ ] Destructive remediation flags stay `false` until validation passes

## 2.7 Next step

Deploy Kafka: [chapter 3](03-kafka-openshift.md).
