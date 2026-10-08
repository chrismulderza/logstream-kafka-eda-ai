# OpenShift Kafka (KRaft) — apply order

These manifests deploy **Red Hat Streams for Apache Kafka** (OperatorHub package `amq-streams`, channel `stable`) into namespace `logstream-kafka`, then a KRaft cluster named `telemetry`. Target cluster: **OpenShift 4.20–4.21**.

Do **not** pin an operator CSV or Kafka broker version in Git. Install the latest CSV on the `stable` channel. Streams for Apache Kafka **3.x is KRaft-only** (no ZooKeeper). Custom resources use `kafka.strimzi.io/v1` (the current API in the 3.2 documentation; `v1beta2` is deprecated).

Full administrator procedure: [docs/deployment/kafka-openshift.md](../../docs/deployment/kafka-openshift.md).

## Apply order

Apply in this sequence. Do not create `Kafka` / `KafkaNodePool` until the Cluster Operator CSV is `Succeeded`. Do not create topics until the `Kafka` resource is `Ready`.

| Step | File | Why |
| --- | --- | --- |
| 1 | [namespace.yaml](namespace.yaml) | Target project for the operator and the cluster |
| 2 | [operator-group.yaml](operator-group.yaml) | Namespace-scoped `OperatorGroup` (`targetNamespaces: [logstream-kafka]`) |
| 3 | [subscription.yaml](subscription.yaml) | `amq-streams` from CatalogSource `redhat-operators` in `openshift-marketplace`, channel `stable` |
| 4 | Approve the InstallPlan (Manual) | Wait until CSV `PHASE=Succeeded` |
| 5 | [kafka-nodepool-controller.yaml](kafka-nodepool-controller.yaml) and [kafka-nodepool-broker.yaml](kafka-nodepool-broker.yaml) | Node pools must exist (or be applied together with `Kafka`) |
| 6 | [kafka.yaml](kafka.yaml) | Cluster `telemetry` with KRaft + node-pool annotations, listeners, Entity Operator |
| 7 | Topic CRs | After `Kafka` is `Ready` |

Topic files:

- [kafka-topic-rhel-system-logs.yaml](kafka-topic-rhel-system-logs.yaml) — 7d retention
- [kafka-topic-rhel-pcp-metrics.yaml](kafka-topic-rhel-pcp-metrics.yaml) — 3d retention
- [kafka-topic-raw-metrics.yaml](kafka-topic-raw-metrics.yaml) — 1d retention
- [kafka-topic-enriched-events.yaml](kafka-topic-enriched-events.yaml) — 3d retention

Optional, for [Automated Remediation](../../docs/optional/automated-remediation.md) only. `oc apply -k .` includes these. They are not part of the four-topic syslog path:

- [kafka-topic-security-advisories.yaml](kafka-topic-security-advisories.yaml) — 7d retention
- [kafka-bridge.yaml](kafka-bridge.yaml) — HTTP Bridge `telemetry-bridge`
- [kafka-bridge-route.yaml](kafka-bridge-route.yaml) — edge Route, path `/topics`

## Commands

```bash
# 1–3: namespace, OperatorGroup, Subscription
oc apply -f namespace.yaml
oc apply -f operator-group.yaml
oc apply -f subscription.yaml

# 4: approve the latest InstallPlan, then wait for the CSV
oc get installplan -n logstream-kafka
oc patch installplan <installplan-name> -n logstream-kafka --type merge -p '{"spec":{"approved":true}}'
oc get csv -n logstream-kafka -w

# 5–6: node pools + Kafka (apply together)
oc apply -f kafka-nodepool-controller.yaml -f kafka-nodepool-broker.yaml -f kafka.yaml
oc wait kafka/telemetry -n logstream-kafka --for=condition=Ready --timeout=900s

# 7: topics
oc apply -f kafka-topic-rhel-system-logs.yaml \
  -f kafka-topic-rhel-pcp-metrics.yaml \
  -f kafka-topic-raw-metrics.yaml \
  -f kafka-topic-enriched-events.yaml
```

`kustomize` / `oc apply -k .` applies **all** resources at once, including topics and the optional advisory Bridge. That is convenient only after the operator is already installed; otherwise topic CRs sit `NotReady` until the Topic Operator starts. Prefer the staged `oc apply -f` order above on a new cluster.

Optional advisory path, after `Kafka` is `Ready`. Procedure: [Automated Remediation](../../docs/optional/automated-remediation.md).

```bash
oc apply -f kafka-topic-security-advisories.yaml \
  -f kafka-bridge.yaml \
  -f kafka-bridge-route.yaml
oc wait kafkabridge/telemetry-bridge -n logstream-kafka --for=condition=Ready --timeout=300s
```

## StorageClass

PVCs use `type: persistent-claim`. The `class` field is commented as `${STORAGE_CLASS}` in each node pool. Leave it unset to use the cluster default StorageClass, or uncomment and set a class before apply.

## Cluster CA for RHEL producers

```bash
oc extract secret/telemetry-cluster-ca-cert -n logstream-kafka --keys=ca.crt --to=. --confirm
```
