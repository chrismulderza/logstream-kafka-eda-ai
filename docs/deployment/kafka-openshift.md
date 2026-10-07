# Kafka on OpenShift

This chapter installs **Red Hat Streams for Apache Kafka** from OperatorHub and deploys a production KRaft cluster named `telemetry` in namespace `logstream-kafka`. Manifests live in [`openshift/kafka/`](../../openshift/kafka). Apply-order notes are also in [`openshift/kafka/README.md`](../../openshift/kafka/README.md).

Streams for Apache Kafka **3.x is KRaft-only**. ZooKeeper is not supported. Do not pin an operator CSV or Kafka broker version in Git; install the **latest CSV on the `stable` channel** and let that CSV choose the Kafka version it ships.

Custom resources in this repository use `kafka.strimzi.io/v1` (current in the Streams for Apache Kafka 3.2 API). The older `v1beta2` API is deprecated.

## Topology

| Object | Name | Notes |
| --- | --- | --- |
| Namespace | `logstream-kafka` | Operator and Kafka cluster |
| OperatorGroup | `logstream-kafka` | Namespace-scoped: `targetNamespaces: [logstream-kafka]` |
| Subscription | `amq-streams` | Package `amq-streams`, CatalogSource `redhat-operators`, `sourceNamespace: openshift-marketplace`, channel `stable` |
| Kafka | `telemetry` | Annotations `strimzi.io/kraft: enabled`, `strimzi.io/node-pools: enabled` |
| KafkaNodePool | `controller` | 3 replicas, role `controller`, 20Gi PVC, `kraftMetadata: shared` |
| KafkaNodePool | `broker` | 3 replicas, role `broker`, 100Gi PVC |
| Listeners | `plain` | Internal, plaintext, port `9092` |
| Listeners | `tls-external` | OpenShift Route, TLS, broker port `9094` (clients use router port **443**) |
| Entity Operator | topic + user | Both enabled |
| Topics | see below | Topic Operator (`KafkaTopic` CRs) |

Topics (partitions `3`, replication `3`):

| Topic | Retention |
| --- | --- |
| `rhel-system-logs` | 7 days (`604800000` ms) |
| `rhel-pcp-metrics` | 3 days (`259200000` ms) |
| `raw-metrics` | 1 day (`86400000` ms) |
| `enriched-events` | 3 days (`259200000` ms) |

---

## Prerequisites

1. OpenShift **4.20 or 4.21** (4.2x) with a default StorageClass that can provision RWO volumes (or a class you will set on the node pools). Streams for Apache Kafka 3.2 is tested on 4.16–4.21 except 4.17; this pack does not target pre-4.20.
2. `cluster-admin` (or equivalent rights to create namespaces, OperatorGroups, Subscriptions, and Streams CRs).
3. `oc` logged in: `oc whoami` and `oc project` succeed.
4. On a workstation used for health checks: `kcat` (or `kcat` from a debug pod). RHEL AppStream package name is typically `kcat`.
5. Outbound access from the cluster to `openshift-marketplace` / Red Hat Operators (or a mirrored CatalogSource with the same package name).

---

## Path A — `oc apply` (preferred for GitOps)

Work from the repository root unless a command `cd`s into `openshift/kafka`.

### Step 1 — Create the namespace

```bash
oc apply -f openshift/kafka/namespace.yaml
```

Expected:

```text
namespace/logstream-kafka created
```

Verify:

```bash
oc get ns logstream-kafka
```

Expected: `STATUS` is `Active`.

**If it fails:** `AlreadyExists` is safe to ignore. `Forbidden` means the user lacks project-create rights.

### Step 2 — Create the namespace-scoped OperatorGroup

```bash
oc apply -f openshift/kafka/operator-group.yaml
```

Expected:

```text
operatorgroup.operators.coreos.com/logstream-kafka created
```

Verify:

```bash
oc get operatorgroup -n logstream-kafka -o yaml
```

Confirm `spec.targetNamespaces` contains only `logstream-kafka`.

**If it fails:** A second OperatorGroup in the same namespace is invalid. `oc get og -n logstream-kafka` and reuse the existing group if it already targets this namespace.

### Step 3 — Subscribe to Streams for Apache Kafka (`stable`)

```bash
oc apply -f openshift/kafka/subscription.yaml
```

Expected:

```text
subscription.operators.coreos.com/amq-streams created
```

The CatalogSource **object name** is `redhat-operators`; it lives in namespace `openshift-marketplace`. That matches “from openshift-marketplace” on a connected cluster.

Verify the Subscription exists:

```bash
oc get subscription amq-streams -n logstream-kafka -o jsonpath='{.spec.channel}{" "}{.spec.source}{" "}{.spec.sourceNamespace}{"\n"}'
```

Expected:

```text
stable redhat-operators openshift-marketplace
```

**If it fails:** `spec.source` not found — list catalog sources with `oc get catalogsource -n openshift-marketplace`. Disconnected clusters often use a custom CatalogSource name; update [`openshift/kafka/subscription.yaml`](../../openshift/kafka/subscription.yaml) to match, keep `name: amq-streams` and `channel: stable`.

### Step 4 — Approve the InstallPlan and wait for the CSV

The Subscription uses **Manual** approval so the `stable` channel cannot auto-upgrade across minor/major releases without an administrator. That follows Red Hat’s OperatorHub guidance for `stable`.

```bash
oc get installplan -n logstream-kafka
```

Example:

```text
NAME            CSV                           APPROVAL   APPROVED
install-abcde   amq-streams.v3.x.y            Manual     false
```

Approve the pending plan (replace the name):

```bash
oc patch installplan install-abcde -n logstream-kafka --type merge -p '{"spec":{"approved":true}}'
```

Wait until the ClusterServiceVersion is healthy:

```bash
oc get csv -n logstream-kafka
```

Expected (version string will match whatever `stable` currently ships — **do not** treat a specific `v3.x.y` as required):

```text
NAME                 DISPLAY                         PHASE
amq-streams.v3.x.y   Streams for Apache Kafka        Succeeded
```

Confirm the Cluster Operator deployment is up:

```bash
oc get deploy -n logstream-kafka
```

Expected: a deployment whose name includes `amq-streams-cluster-operator` (OLM install) with `READY` `1/1`.

Wait helper:

```bash
oc wait csv -n logstream-kafka --for=jsonpath='{.status.phase}'=Succeeded --timeout=600s -l operators.coreos.com/amq-streams.logstream-kafka
```

If the label selector is empty on your cluster, wait by name from `oc get csv`.

**If PHASE is Failed:**

```bash
oc describe csv -n logstream-kafka
oc get events -n logstream-kafka --sort-by=.lastTimestamp | tail -n 50
```

Typical causes: missing `redhat-operators` catalog, OperatorGroup install-mode mismatch, or SCC/RBAC denials creating the Cluster Operator.

### Step 5 — Optional: set StorageClass on the node pools

PVCs use `type: persistent-claim`. Class is **not** set so OpenShift uses the default StorageClass.

To pin a class, uncomment and set `class` on the volume in:

- [`openshift/kafka/kafka-nodepool-controller.yaml`](../../openshift/kafka/kafka-nodepool-controller.yaml)
- [`openshift/kafka/kafka-nodepool-broker.yaml`](../../openshift/kafka/kafka-nodepool-broker.yaml)

Example:

```yaml
class: gp3-csi   # replaces the ${STORAGE_CLASS} placeholder
```

Confirm a default exists if you leave `class` unset:

```bash
oc get sc
```

Look for `(default)` on one class. If none is default and `class` is omitted, PVCs stay `Pending`.

### Step 6 — Apply KafkaNodePools and the Kafka cluster

Apply **controller pool, broker pool, and Kafka together**. Node pools must be labeled `strimzi.io/cluster: telemetry`.

```bash
oc apply -f openshift/kafka/kafka-nodepool-controller.yaml \
  -f openshift/kafka/kafka-nodepool-broker.yaml \
  -f openshift/kafka/kafka.yaml
```

Expected:

```text
kafkanodepool.kafka.strimzi.io/controller created
kafkanodepool.kafka.strimzi.io/broker created
kafka.kafka.strimzi.io/telemetry created
```

[`openshift/kafka/kafka.yaml`](../../openshift/kafka/kafka.yaml) sets:

- `strimzi.io/kraft: enabled`
- `strimzi.io/node-pools: enabled`
- listener `plain` (`internal`, `9092`, TLS off)
- listener `tls-external` (`route`, `9094`, TLS on)
- `entityOperator.topicOperator` and `entityOperator.userOperator`

Wait for Ready (often 5–15 minutes while 6 PVCs bind and 6 Kafka pods start):

```bash
oc wait kafka/telemetry -n logstream-kafka --for=condition=Ready --timeout=900s
```

Expected:

```text
kafka.kafka.strimzi.io/telemetry condition met
```

**If it fails:** see [Troubleshooting](#troubleshooting).

### Step 7 — Create topics (Topic Operator)

Create topics only after the Kafka cluster (and Entity Operator) is Ready:

```bash
oc apply -f openshift/kafka/kafka-topic-rhel-system-logs.yaml \
  -f openshift/kafka/kafka-topic-rhel-pcp-metrics.yaml \
  -f openshift/kafka/kafka-topic-raw-metrics.yaml \
  -f openshift/kafka/kafka-topic-enriched-events.yaml
```

Expected: four `kafkatopic.kafka.strimzi.io/... created` lines.

---

## Path B — OpenShift console (OperatorHub)

Use this path when you cannot apply the Subscription YAML, or to cross-check Path A.

### Step 1 — Create the project

1. **Home → Projects → Create Project**.
2. Name: `logstream-kafka`.
3. Create.

Alternatively apply [`openshift/kafka/namespace.yaml`](../../openshift/kafka/namespace.yaml) as in Path A.

### Step 2 — Install from OperatorHub

1. Switch to project `logstream-kafka` (project dropdown).
2. **Operators → OperatorHub**.
3. Filter: `Streams for Apache Kafka` (package `amq-streams`; formerly branded AMQ Streams).
4. Open the **Red Hat** offering (not a community-only Strimzi tile, unless that is an explicit exception).
5. **Install**.
6. **Update channel:** `stable` (latest stable; includes 3.x KRaft-only releases).
7. **Installation mode:** A specific namespace → `logstream-kafka`.
8. **Installed Namespace:** `logstream-kafka` (operator namespace equals the Kafka namespace).
9. **Update approval:** **Manual**.
10. **Install**.

The console creates an `OperatorGroup` targeting `logstream-kafka` and a `Subscription` named for the package. Prefer the Git manifests if you need the exact object names (`OperatorGroup` `logstream-kafka`, `Subscription` `amq-streams`).

### Step 3 — Approve the InstallPlan

1. **Operators → Installed Operators → Streams for Apache Kafka**.
2. If the operator sits on **Upgrade available** / pending approval: **Installed Operators** → open the operator → **InstallPlan** (or **Operators → Installed Operators** kebab → **Approve**).
3. Wait until **Status** is **Succeeded**.

### Step 4 — Create KafkaNodePool `controller`

1. **Installed Operators → Streams for Apache Kafka**.
2. Tab **Kafka Node Pool → Create KafkaNodePool**.
3. **YAML view**. Replace the sample with [`openshift/kafka/kafka-nodepool-controller.yaml`](../../openshift/kafka/kafka-nodepool-controller.yaml).
4. Create.

### Step 5 — Create KafkaNodePool `broker`

Repeat with [`openshift/kafka/kafka-nodepool-broker.yaml`](../../openshift/kafka/kafka-nodepool-broker.yaml).

### Step 6 — Create Kafka `telemetry`

1. Tab **Kafka → Create Kafka**.
2. **YAML view**. Paste [`openshift/kafka/kafka.yaml`](../../openshift/kafka/kafka.yaml).
3. Create.
4. Wait until the Kafka resource **Status** is **Ready** (Conditions `Ready=True`).

### Step 7 — Create KafkaTopic resources

1. Tab **Kafka Topic → Create KafkaTopic**.
2. For each file, paste YAML:
   - [`openshift/kafka/kafka-topic-rhel-system-logs.yaml`](../../openshift/kafka/kafka-topic-rhel-system-logs.yaml)
   - [`openshift/kafka/kafka-topic-rhel-pcp-metrics.yaml`](../../openshift/kafka/kafka-topic-rhel-pcp-metrics.yaml)
   - [`openshift/kafka/kafka-topic-raw-metrics.yaml`](../../openshift/kafka/kafka-topic-raw-metrics.yaml)
   - [`openshift/kafka/kafka-topic-enriched-events.yaml`](../../openshift/kafka/kafka-topic-enriched-events.yaml)

---

## Health checks

### Cluster Operator and CRs

```bash
oc get csv,deploy,po -n logstream-kafka
oc get kafka,kafkaNodePool,kafkaTopic -n logstream-kafka
```

Expected Kafka:

```text
NAME                 DESIRED KAFKA REPLICAS   DESIRED ZK REPLICAS   READY   WARNINGS
telemetry            3                                              True
```

Column headers vary by `oc`/CRD printer. What matters: `READY` is `True` (or `status.conditions[type=Ready].status=True`). ZooKeeper replica columns, if present, should be empty on 3.x.

Expected node pools:

```text
NAME         DESIRED REPLICAS   ROLES        READY
broker       3                  ["broker"]   True
controller   3                  ["controller"] True
```

Expected topics (all `READY True`):

```text
NAME                CLUSTER      PARTITIONS   REPLICATION FACTOR   READY
enriched-events     telemetry    3            3                    True
raw-metrics         telemetry    3            3                    True
rhel-pcp-metrics    telemetry    3            3                    True
rhel-system-logs    telemetry    3            3                    True
```

Pods (names include the pool):

```bash
oc get po -n logstream-kafka -l strimzi.io/cluster=telemetry
```

Expected: three `telemetry-controller-*` pods, three `telemetry-broker-*` pods, plus Entity Operator (`telemetry-entity-operator-*`), all `Running` / `Ready`.

PVCs:

```bash
oc get pvc -n logstream-kafka
```

Expected: six `Bound` claims (20Gi × 3 controllers, 100Gi × 3 brokers).

### Internal plaintext produce / consume (`kcat`)

Internal bootstrap DNS (listener `plain`):

```text
telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092
```

From a pod **in the cluster** (example: a debug pod with `kcat` on `PATH`):

```bash
oc exec -n logstream-kafka -it deploy/telemetry-entity-operator -- bash
```

If that image has no `kcat`, run a one-shot client from a host that can reach the Service (another pod in `logstream-kafka`):

```bash
printf 'healthcheck %s\n' "$(date -Iseconds)" | kcat -P \
  -b telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092 \
  -t rhel-system-logs

kcat -C -e \
  -b telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092 \
  -t rhel-system-logs -o -1
```

Expected consume line: the JSON/text you produced, then `kcat` exits (`-e` = exit at end of partition).

Metadata check:

```bash
kcat -L -b telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092
```

Expected: brokers listed, topics `rhel-system-logs`, `rhel-pcp-metrics`, `raw-metrics`, `enriched-events` each with 3 partitions.

### External Route TLS produce / consume (`kcat`)

Route listeners terminate on the OpenShift router. Clients connect to **port 443**, not 9094. TLS is passthrough: trust the **Kafka cluster CA**, not the ingress wildcard.

1. Bootstrap host:

```bash
oc get kafka telemetry -n logstream-kafka \
  -o jsonpath='{.status.listeners[?(@.name=="tls-external")].bootstrapServers}{"\n"}'
```

Expected shape:

```text
telemetry-kafka-tls-external-bootstrap-logstream-kafka.apps.<cluster-domain>:443
```

Equivalent:

```bash
oc get route telemetry-kafka-tls-external-bootstrap -n logstream-kafka \
  -o jsonpath='{.status.ingress[0].host}{"\n"}'
```

2. Extract the cluster CA (RHEL producers use this file):

```bash
oc extract secret/telemetry-cluster-ca-cert -n logstream-kafka --keys=ca.crt --to=. --confirm
```

Expected: `ca.crt` extracted. PEM check:

```bash
openssl x509 -in ca.crt -noout -subject -dates
```

3. Produce and consume from a workstation that can resolve the Route:

```bash
BOOTSTRAP="$(oc get kafka telemetry -n logstream-kafka \
  -o jsonpath='{.status.listeners[?(@.name=="tls-external")].bootstrapServers}')"

printf 'route-healthcheck %s\n' "$(date -Iseconds)" | kcat -P \
  -b "${BOOTSTRAP}" \
  -X security.protocol=SSL \
  -X ssl.ca.location="$(pwd)/ca.crt" \
  -t rhel-system-logs

kcat -C -e \
  -b "${BOOTSTRAP}" \
  -X security.protocol=SSL \
  -X ssl.ca.location="$(pwd)/ca.crt" \
  -t rhel-system-logs -o -1
```

Expected: produce returns `0`; consume prints the message.

---

## Cluster CA for RHEL producers

Secret name is always `<cluster-name>-cluster-ca-cert` → `telemetry-cluster-ca-cert`.

```bash
oc extract secret/telemetry-cluster-ca-cert -n logstream-kafka --keys=ca.crt --to=. --confirm
```

PKCS#12 variant (Java clients):

```bash
oc get secret telemetry-cluster-ca-cert -n logstream-kafka -o jsonpath='{.data.ca\.p12}' | base64 -d > ca.p12
oc get secret telemetry-cluster-ca-cert -n logstream-kafka -o jsonpath='{.data.ca\.password}' | base64 -d; echo
```

Distribute `ca.crt` to RHEL hosts that produce to the Route (`rsyslog`/`omkafka`, `kcat`, `pcp2kafka`). Trust this CA; do not use the OpenShift ingress certificate for passthrough Routes.

The CA rotates when Streams renews cluster certificates. Re-extract after renewal if producers start failing TLS verify.

---

## Troubleshooting

### InstallPlan / CSV never Succeeded

```bash
oc describe subscription amq-streams -n logstream-kafka
oc get installplan -n logstream-kafka
oc describe csv -n logstream-kafka
```

| Symptom | Action |
| --- | --- |
| Subscription `ResolutionFailed` | CatalogSource name/namespace wrong; `oc get catsrc -n openshift-marketplace` |
| CSV `UnsupportedOperatorGroup` | Only one OperatorGroup per namespace; `targetNamespaces` must be exactly `logstream-kafka` |
| Image pull errors | Entitlement / pull secret missing for `registry.redhat.io` |
| Manual InstallPlan `APPROVED=false` | Patch `spec.approved: true` |

### Kafka stays NotReady; pods Pending

```bash
oc describe kafka telemetry -n logstream-kafka
oc get pvc -n logstream-kafka
oc describe pvc -n logstream-kafka | grep -A5 -E 'Name:|Events:'
oc get events -n logstream-kafka --field-selector type=Warning
```

| Symptom | Action |
| --- | --- |
| PVC `Pending`, no default StorageClass | Set `class` on both node pools or mark a default SC |
| `FailedScheduling` / insufficient CPU/memory | Free worker capacity; 6 Kafka pods plus operator |
| `CreateContainerConfigError` | Operator too old for `kafka.strimzi.io/v1` — wait until the 3.x CSV is Succeeded |
| Missing `kraftMetadata: shared` on controller volume | Cluster Operator rejects controller-only pools; re-apply [`kafka-nodepool-controller.yaml`](../../openshift/kafka/kafka-nodepool-controller.yaml) |
| Applied Kafka without node pools | Apply both `KafkaNodePool` CRs labeled `strimzi.io/cluster: telemetry` |

### Topics NotReady

```bash
oc describe kafkatopic -n logstream-kafka
oc get po -n logstream-kafka -l strimzi.io/name=telemetry-entity-operator
```

| Symptom | Action |
| --- | --- |
| No Entity Operator pod | `spec.entityOperator.topicOperator` missing — re-apply [`kafka.yaml`](../../openshift/kafka/kafka.yaml) |
| `Resource not ready` / cluster not Ready | Wait for Kafka `Ready=True`, then re-apply topics |
| Wrong `strimzi.io/cluster` label | Must be `telemetry` |

### Route / kcat TLS failures

```bash
oc get route -n logstream-kafka
oc get kafka telemetry -n logstream-kafka -o yaml | sed -n '/status:/,$p'
```

| Symptom | Action |
| --- | --- |
| Connection to `:9094` from outside the cluster | Use **443** (router), not the broker listener port |
| `SSL handshake failed` / unknown CA | Re-run `oc extract secret/telemetry-cluster-ca-cert ... --keys=ca.crt` |
| Timeout to the Route | Router, DNS, or firewall; `curl -vkI https://<bootstrap-host>` still fails for Kafka binary, but DNS/TCP:443 must work |
| Hostname verification failed | Bootstrap must match `status.listeners[name=tls-external].bootstrapServers`; SNI is required for passthrough |
| `PLAINTEXT` to the Route | Route listeners **require** `tls: true` (already set) |

### kustomize applied everything too early

`oc apply -k openshift/kafka` is valid **after** the operator CSV is Succeeded. On an empty cluster it creates topic CRs before the Topic Operator exists; they reconcile once Entity Operator is up. Prefer the numbered `oc apply -f` order in Path A.

---

## Kustomize reference

[`openshift/kafka/kustomization.yaml`](../../openshift/kafka/kustomization.yaml) lists every resource and sets `namespace: logstream-kafka`.

```bash
oc apply -k openshift/kafka
```

Use only when the operator is already installed, or accept that topics will be `NotReady` until Step 6 completes.

---

## Related files

| Path | Role |
| --- | --- |
| [`openshift/kafka/namespace.yaml`](../../openshift/kafka/namespace.yaml) | Namespace `logstream-kafka` |
| [`openshift/kafka/operator-group.yaml`](../../openshift/kafka/operator-group.yaml) | Namespace-scoped OperatorGroup |
| [`openshift/kafka/subscription.yaml`](../../openshift/kafka/subscription.yaml) | `amq-streams` / `stable` / marketplace |
| [`openshift/kafka/kafka-nodepool-controller.yaml`](../../openshift/kafka/kafka-nodepool-controller.yaml) | Controller pool |
| [`openshift/kafka/kafka-nodepool-broker.yaml`](../../openshift/kafka/kafka-nodepool-broker.yaml) | Broker pool |
| [`openshift/kafka/kafka.yaml`](../../openshift/kafka/kafka.yaml) | Kafka `telemetry` |
| [`openshift/kafka/kafka-topic-*.yaml`](../../openshift/kafka) | Topic Operator CRs |
| [`openshift/kafka/kustomization.yaml`](../../openshift/kafka/kustomization.yaml) | Resource list |
| [`openshift/kafka/README.md`](../../openshift/kafka/README.md) | Short apply-order card |

## Next

Continue with [RHEL telemetry](rhel-telemetry.md) to dual-home syslog onto Kafka while keeping the existing syslog destination.
