# Research Plan: Automated Deployment Guide for Event-Driven Telemetry, Predictive AI, and Remediation Architecture

## Objective

To direct an automated execution agent (such as `cursor-cli`) in researching context, validating API schemas, generating declarative manifests, authoring automation playbooks, and assembling a complete, step-by-step deployment guide for an event-driven telemetry and predictive AI infrastructure across Red Hat Enterprise Linux (RHEL) and OpenShift.

---

## Phase 1: Environment Prerequisites & Version Matrix Research

### Task 1.1: Platform & OS Compatibility Audit

* **RHEL Endpoints:** Identify exact package names, version constraints, and repository channels (BaseOS / AppStream) for `rsyslog`, `rsyslog-kafka`, `pcp`, `pcp-system-tools`, `pcp-export-pcp2json`, and `kcat` across RHEL 8.x and 9.x.
* **Red Hat OpenShift (OCP):** Validate cluster requirements for OCP 4.12+.
* **Kafka Operator:** Verify Red Hat Streams for Apache Kafka (AMQ Streams 2.7+ / Strimzi 0.40+) in KRaft mode.
* **Ansible Automation Platform (AAP):** Verify AAP 2.4+ / `ansible-rulebook` CLI dependencies (`aiokafka`, `ansible.eda`, `requests`, `numpy`).

### Task 1.2: Network Topology & Port Mapping

* **Ingress/Egress Ports:** Map required network paths:
* Kafka internal cluster listener (`9092/TCP`).
* External OpenShift Route TLS listener (`9094/TCP`).
* Performance Co-Pilot daemons (`pmcd` on `44321/TCP`, `pmproxy` on `44322/TCP`).
* Local/Remote AI inference API endpoints (`8000/TCP` or `443/TCP`).


* **Route & Certificate Handling:** Determine edge/passthrough TLS route strategies for external RHEL host ingestion.

### Task 1.3: Security & Policy Baselines

* Document SELinux booleans required for network logging (`logging_syslogd_can_send_mail`, `nis_enabled` or custom policy modules for custom ports).
* Establish ServiceAccount RBAC permissions and Security Context Constraints (SCCs) on OpenShift for Strimzi and stream-worker pods.

---

## Phase 2: OpenShift Kafka Cluster Blueprinting (KRaft Mode)

### Task 2.1: Operator Installation Manifests

* Generate declarative YAML for `Namespace`, `OperatorGroup`, and `Subscription` targeting `amq-streams` from `openshift-marketplace`.

### Task 2.2: KRaft Custom Resource Architecture

* Generate `KafkaNodePool` resources:
* **Controller Pool:** 3 replicas (Quorum metadata, 20Gi PVC).
* **Broker Pool:** 3 replicas (Data storage & ingestion, 100Gi PVC).


* Draft the primary `Kafka` custom resource incorporating:
* Strimzi KRaft annotations (`strimzi.io/node-pools: enabled`, `strimzi.io/kraft: enabled`).
* Listener configurations for internal plaintext (`9092`) and external OpenShift Route TLS (`9094`).



### Task 2.3: Topic Declarations

* Draft `KafkaTopic` manifests for required event streams:
* `rhel-system-logs` (7-day retention).
* `rhel-pcp-metrics` (3-day retention).
* `raw-metrics` (1-day retention).
* `enriched-events` (3-day retention).



---

## Phase 3: Host Telemetry Onboarding Playbook Development

### Task 3.1: Package & Service Provisioning

* Write idempotent Ansible tasks to install `rsyslog`, `rsyslog-kafka`, `pcp`, `pcp-export-pcp2json`, and `kcat`.
* Configure and enable `pmcd` and `rsyslog` systemd services.

### Task 3.2: Structured Log Exporter (`rsyslog` / `omkafka`)

* Author the `/etc/rsyslog.d/10-kafka.conf` Jinja2 template using the `omkafka` module.
* Ensure log fields are serialized into RFC-3339 compliant JSON containing `@timestamp`, `host`, `severity`, `facility`, `syslogtag`, and `message`.

### Task 3.3: Metric Streaming Pipeline (`pcp2kafka.service`)

* Create a systemd unit `/etc/systemd/system/pcp2kafka.service` that pipes output from `pcp2json` directly to `kcat` targeting the OpenShift Kafka Route endpoint.
* Include parameters for metric selection, sampling intervals, and auto-restart policies (`RestartSec=5s`).

---

## Phase 4: Stream Processing & Predictive AI Worker Microservice

### Task 4.1: Predictive Calculation Algorithm Design

* Author a Python microservice using `kafka-python` (or `faust-streaming`) and `numpy` to calculate time-to-exhaustion over rolling metric windows:

$$\text{Time to Exhaustion} = \frac{\text{Capacity} - \text{Used}_{\text{current}}}{\left(\frac{\Delta \text{Used}}{\Delta t}\right)}$$

### Task 4.2: AI Inference Integration Interface

* Implement an HTTP REST client wrapper connecting to vLLM, Red Hat OpenShift AI (RHOAI), or OpenAI-compatible endpoints for real-time log classification.
* Define prompt templates for system log root-cause analysis and severity scoring.

### Task 4.3: OpenShift Container Packaging

* Construct a multi-stage `Dockerfile` based on Red Hat Universal Base Image (UBI9).
* Write Kubernetes deployment manifests (`Deployment`, `ConfigMap`, `Secret`, `ServiceAccount`) to deploy the microservice inside OpenShift.

---

## Phase 5: Event-Driven Ansible (EDA) & Remediation Automation

### Task 5.1: Execution Environment (EE) Definition

* Author an `execution-environment.yml` file specifying required collections (`ansible.eda`, `community.general`) and Python dependencies (`aiokafka`, `requests`, `numpy`).

### Task 5.2: EDA Rulebook Architecture

* Construct `rulebook.yml` utilizing the `ansible.eda.kafka` source plugin listening on `rhel-system-logs` and `enriched-events`.
* Write rule conditions targeting log patterns (OOM events, SSH failures) and predictive alerts (`PREEMPTIVE_STORAGE_EXHAUSTION_RISK`).

### Task 5.3: Automated Remediation Playbook Suite

* Author production-grade Ansible playbooks:
* `remediate_oom.yml`: Diagnostic collection and service recovery.
* `proactive_disk_mitigation.yml`: Automated log rotation, container storage pruning (`podman system prune`), and dynamic LVM volume expansion (`lvextend`).



---

## Phase 6: Downstream SIEM & Dashboard Integration

### Task 6.1: SIEM Ingestion Pipelines

* Document configuration patterns for Logstash (Elasticsearch) and Splunk Connect for Kafka to consume topics in parallel with EDA.

### Task 6.2: Operational Dashboards

* Construct Grafana JSON dashboard templates for:
* Strimzi Kafka cluster throughput and consumer group lag.
* PCP system metric time-series visualizations.



---

## Phase 7: Validation, Synthetic Testing & Runbook Generation

### Task 7.1: Synthetic Event Generators

* Author test scripts to validate end-to-end functionality:
* **Storage Fill Test:** `fallocate -l 5G /var/log/test_fill.img` to test predictive calculation workers.
* **Log Injection Test:** `logger -t test_oom "Out of memory: Kill process 1234"` to test `omkafka` ingestion and EDA rule triggers.



### Task 7.2: Pipeline Verification Checklist

* Provide specific CLI verification commands (`oc`, `ansible-rulebook`, `kcat`) to inspect partition offsets, rule matches, and playbook execution status.

---

## Agent Deliverable Assembly Structure

The execution agent must assemble all research outputs into a single deployment guide structured as follows:

```
# Comprehensive Deployment Guide: Event-Driven Telemetry, Predictive AI, and Automated Remediation Architecture

## 1. Architectural Overview & Component Topology
## 2. Prerequisites & Environment Verification
## 3. Deployment Phase 1: Deploying Apache Kafka on OpenShift (KRaft)
## 4. Deployment Phase 2: Provisioning RHEL Host Telemetry (Ansible)
## 5. Deployment Phase 3: Deploying the Predictive AI Stream Worker
## 6. Deployment Phase 4: Configuring Event-Driven Ansible (EDA)
## 7. Operational Validation & Synthetic Testing Runbook
## 8. SIEM & Dashboard Integration Reference

```
