# AAP helper: project, decision environment, and rulebook activation

Use this when running Event-Driven Ansible on Ansible Automation Platform 2.5 (OCP 4.20 only) or 2.6 (OCP 4.20–4.21). The CLI path is documented in `docs/deployment/event-driven-ansible.md` with equal weight.

## Layout AAP actually scans

EDA controller imports rulebooks only from **one** of these git-root paths (first match wins):

1. `extensions/eda/rulebooks/` (supported collection layout)
2. `rulebooks/` (legacy)

This repository keeps the working copy under `ansible/eda/`. AAP also reads `extensions/eda/rulebooks/` at the git root (`aap-rulebook.yml` is already copied there). Do not leave an empty `extensions/eda/rulebooks/` if you relocate files.

Automation controller (job templates) can use the same git repo and run `ansible/eda/playbooks/*.yml` directly.

## 1. Credentials

In **Event-Driven Ansible controller**:

1. SCM credential for the git project (token or username/password over HTTPS).
2. Registry credential if the decision environment image is private (`registry.redhat.io` or an internal registry).
3. Automation controller token: create a token in AAP controller (user **Tokens**), then add it in EDA controller so activations can call `run_job_template`.

Optional Kafka credential type (AAP 2.5+ custom EDA credentials) can inject `host`, `port`, TLS files, and SASL without putting secrets in activation variables.

## 2. Project

1. Open **Projects** → **Create project**.
2. Name: `logstream-kafka-eda`.
3. SCM type: Git. URL: HTTPS clone URL of this repository (or the dedicated EDA content repo).
4. Branch: the branch that contains `extensions/eda/rulebooks/`.
5. Attach the SCM credential. Leave **Verify SSL** enabled unless you use a private CA and have installed it on the controller.
6. Save, confirm status is successful, then **Sync** after every rulebook change.

Activations keep the git hash from the last sync. Restart the activation after a project sync to pick up rulebook changes.

## 3. Decision environment

Build from `ansible/eda/execution-environment.yml`:

```bash
ansible-builder build \
  -f ansible/eda/execution-environment.yml \
  -t registry.example.com/aiops/logstream-eda-de:1.0
podman push registry.example.com/aiops/logstream-eda-de:1.0
```

In EDA controller **Decision Environments** → **Create decision environment**:

| Field | Value |
| --- | --- |
| Name | `logstream-eda-de` |
| Image | full path including tag, e.g. `registry.example.com/aiops/logstream-eda-de:1.0` |
| Credential | registry pull credential |

AAP always pulls the image on activation start (pull policy `Always`). Pin a digest or immutable tag for production.

Controller also needs an **execution environment** that contains `community.general` (and Python deps if job templates need them) to run the playbooks. You can reuse this definition with an `ee-minimal-rhel9` base image for job templates, or keep a separate EE.

## 4. Controller project and job templates

On **automation controller** (not EDA):

1. Create a Project pointing at the same git repo.
2. Inventory: import `ansible/eda/inventory/hosts.example.yml` (group `rhel_telemetry`).
3. Create one job template per playbook under `ansible/eda/playbooks/` (see `docs/deployment/event-driven-ansible.md`, Shared artifacts). Extra vars: every `allow_*` flag `false`.
4. Optional TTE: job template `proactive-disk-mitigation` only if you enable `aap-rulebook-optional-predictive.yml`.
5. Disable privilege escalation only if the inventory already sets `ansible_become`. Keep jobs as `run` (not `check`) only after a dry-run review.
6. Grant the EDA controller token permission to launch those templates.

Names must match `job_template_*` keys in `vars/extra_vars.example.yml`.

## 5. Rulebook activation

1. **Rulebook Activations** → **Create rulebook activation**.
2. Project: `logstream-kafka-eda`.
3. Rulebook: `aap-rulebook.yml` (not `rulebook.yml`; that file uses CLI-only `run_playbook`).
4. Decision environment: `logstream-eda-de`.
5. Restart policy: **On failure** for production; **Always** only if the rulebook is expected to exit cleanly and should loop.
6. Variables: paste YAML from `vars/extra_vars.example.yml`, substituting real Kafka host/port. Keep every `allow_*` flag **false** until a change window.
7. Enable the activation.

Confirm the activation log shows the syslog Kafka source connecting with consumer group `ansible-eda`. If the group already has SIEM or worker members, stop and fix group IDs before enabling rules.

## Dual / optional predictive rulebook on AAP

Core activations use `aap-rulebook.yml` only (`rhel-system-logs`). If you deploy the optional worker:

1. Create a second activation on `aap-rulebook-optional-predictive.yml`.
2. Share `group_id: ansible-eda` and keep extra vars identical so safety gates cannot drift.
