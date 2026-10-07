# 4. RHEL Telemetry

This chapter **adds** rsyslog `omkafka` and PCP → Kafka exporters on RHEL 8/9 with Ansible. It does **not** replace existing syslog forwarding. Hosts that already ship logs to **ArcSight** keep that path; Kafka is a second destination.

Playbooks live in [`ansible/telemetry/`](../ansible/telemetry/). End-to-end synthetic tests are in [chapter 7](07-validation-runbook.md).

Producers use the OpenShift **Route** for listener `tls-external`. Connect on **TCP 443**. Do not point RHEL hosts at broker port **9094**.

## Topology and naming

| Object | Value |
| --- | --- |
| Inventory group | `rhel_telemetry` |
| Kafka cluster / namespace | `telemetry` / `logstream-kafka` |
| External listener | `tls-external` (broker listen `9094`, Route listen **443**) |
| Bootstrap (RHEL) | `telemetry-kafka-tls-external-bootstrap-logstream-kafka.apps.<cluster-domain>:443` |
| Log topic | `rhel-system-logs` |
| Metric topic | `rhel-pcp-metrics` |
| Cluster CA secret | `telemetry-cluster-ca-cert` (`ca.crt`) |
| CA on host (preferred) | `/etc/pki/ca-trust/source/anchors/kafka-cluster-ca.pem` |
| Kafka rsyslog drop-in | `/etc/rsyslog.d/05-omkafka-additional.conf` (additive; no `stop`) |

Confirm the live bootstrap string (must end in `:443`):

```bash
oc get kafka telemetry -n logstream-kafka \
  -o jsonpath='{.status.listeners[?(@.name=="tls-external")].bootstrapServers}{"\n"}'
```

Expected shape:

```text
telemetry-kafka-tls-external-bootstrap-logstream-kafka.apps.<cluster-domain>:443
```

TLS is **passthrough**. Hosts trust the **Kafka cluster CA**, not the OpenShift ingress wildcard. Extract once on the Ansible controller:

```bash
oc extract secret/telemetry-cluster-ca-cert -n logstream-kafka --keys=ca.crt --to=. --confirm
cp ca.crt ansible/telemetry/roles/rhel_telemetry/files/kafka-cluster-ca.pem
openssl x509 -in ansible/telemetry/roles/rhel_telemetry/files/kafka-cluster-ca.pem -noout -subject -dates
```

## Artifacts in this repository

| Path | Purpose |
| --- | --- |
| [`ansible/telemetry/site.yml`](../ansible/telemetry/site.yml) | Entry playbook (imports onboard) |
| [`ansible/telemetry/playbooks/onboard.yml`](../ansible/telemetry/playbooks/onboard.yml) | Play targeting group `rhel_telemetry` |
| [`ansible/telemetry/inventory/hosts.example.yml`](../ansible/telemetry/inventory/hosts.example.yml) | Example inventory |
| [`ansible/telemetry/group_vars/all.yml.example`](../ansible/telemetry/group_vars/all.yml.example) | Bootstrap, `SSL`, topics, PCP interval |
| [`ansible/telemetry/ansible.cfg`](../ansible/telemetry/ansible.cfg) | `roles_path` and inventory defaults |
| [`ansible/telemetry/collections/requirements.yml`](../ansible/telemetry/collections/requirements.yml) | `ansible.posix` |
| [`ansible/telemetry/roles/rhel_telemetry/tasks/`](../ansible/telemetry/roles/rhel_telemetry/tasks/) | Packages, CA, SELinux, firewall, rsyslog, PCP |
| [`ansible/telemetry/roles/rhel_telemetry/templates/omkafka-additional.conf.j2`](../ansible/telemetry/roles/rhel_telemetry/templates/omkafka-additional.conf.j2) | Extra `omkafka` destination; does not replace ArcSight |
| [`ansible/telemetry/roles/rhel_telemetry/templates/pcp2kafka.service.j2`](../ansible/telemetry/roles/rhel_telemetry/templates/pcp2kafka.service.j2) | `pcp2json` piped to `kcat`, `RestartSec=5s` |
| [`ansible/telemetry/roles/rhel_telemetry/templates/hotproc.conf.j2`](../ansible/telemetry/roles/rhel_telemetry/templates/hotproc.conf.j2) | Hot-process RSS floor for `pmdaproc` |
| [`ansible/telemetry/roles/rhel_telemetry/files/rsyslog_omkafka.te`](../ansible/telemetry/roles/rhel_telemetry/files/rsyslog_omkafka.te) | Targeted SELinux module (connect to port 443) |
| [`ansible/telemetry/roles/rhel_telemetry/files/kafka-cluster-ca.pem.example`](../ansible/telemetry/roles/rhel_telemetry/files/kafka-cluster-ca.pem.example) | Placeholder; replace with real PEM |

The role installs `rsyslog-kafka` (and PCP packages), enables `rsyslog` if it is not already enabled, and adds **one** drop-in. It does not rewrite `/etc/rsyslog.conf` or other files in `/etc/rsyslog.d/`.

## Preserve ArcSight forwarding

Treat Kafka as a **dual-home**, not a cutover.

| Keep | Do not |
| --- | --- |
| Existing ArcSight SmartConnector / Logger `omfwd` or `omrelp` drop-ins | Replace `/etc/rsyslog.conf` |
| CEF/legacy forwarding host, port, and filters | Add `stop` or `& ~` in the Kafka file |
| Local files such as `/var/log/messages` | Delete or overwrite files named `*arcsight*`, `*cef*` |

How the pack enforces that:

1. Writes only [`/etc/rsyslog.d/05-omkafka-additional.conf`](../ansible/telemetry/roles/rhel_telemetry/templates/omkafka-additional.conf.j2). The `05-` prefix loads **before** typical `10-` / `50-` ArcSight files so a later `stop` after ArcSight forward still leaves Kafka a copy.
2. The Kafka action has **no** `stop` / `& ~`, so later ArcSight rules still see the message.
3. After apply, the role checksums `/etc/rsyslog.conf` and every pre-existing `rsyslog.d` drop-in and **fails** if any of those files changed.
4. If an earlier file already discards with `stop` (so Kafka would never run), the role **fails** with `rsyslog_fail_on_early_stop` (default `true`). Keep the ArcSight file; move the discard to the **last** drop-in, or set `rsyslog_kafka_conf` to a name that sorts before that discard file (for example `/etc/rsyslog.d/00-omkafka-additional.conf`).

Verify ArcSight is still configured after onboarding:

```bash
ls -l /etc/rsyslog.d
grep -nEi 'arcsight|omfwd|omrelp|@@|@' /etc/rsyslog.conf /etc/rsyslog.d/*.conf
rsyslogd -N1
```

Expected: original ArcSight files still present and byte-identical to pre-change; Kafka drop-in exists; `rsyslogd -N1` is clean. Confirm the connector or Logger still receives events (ArcSight console or SmartConnector status) in the same window you confirm Kafka with `kcat`.


## Prerequisites

1. Chapter 3 complete: Kafka Ready, topics present, Route bootstrap on **443**.
2. Ansible control node: Ansible 2.14+, SSH to hosts with `become`.
3. Collection: `ansible-galaxy collection install -r ansible/telemetry/collections/requirements.yml`
4. Hosts subscribed to RHEL BaseOS/AppStream (see [prerequisites](02-prerequisites.md)).
5. Cluster CA PEM on the controller (previous section).
6. DNS from each RHEL host to the Route hostname; egress **TCP 443** to the OpenShift ingress.

Optional: inbound **44321/tcp** (`pmcd`) and **44322/tcp** (`pmproxy`) only if you collect PCP from another host. Kafka export does **not** need those ports.

## Step 1 — Inventory

```bash
cd ansible/telemetry
cp inventory/hosts.example.yml inventory/hosts.yml
```

Edit `inventory/hosts.yml`. Hosts **must** be in group `rhel_telemetry`:

```yaml
all:
  children:
    rhel_telemetry:
      hosts:
        rhel-app-01:
          ansible_host: 192.0.2.11
          ansible_user: ansible
      vars:
        ansible_become: true
```

Verify:

```bash
ansible -i inventory/hosts.yml rhel_telemetry -m ping
```

Expected: `pong` from every host. **If it fails:** SSH keys, `ansible_user`, or sudo.

## Step 2 — Group vars and extra-vars

```bash
cp group_vars/all.yml.example group_vars/all.yml
```

Set at least:

| Variable | Example | Notes |
| --- | --- | --- |
| `kafka_bootstrap` | `telemetry-kafka-tls-external-bootstrap-logstream-kafka.apps.example.com:443` | Route host **and** `:443` |
| `kafka_security_protocol` | `SSL` | Templates emit librdkafka `ssl` |
| `kafka_logs_topic` | `rhel-system-logs` | |
| `kafka_pcp_topic` | `rhel-pcp-metrics` | |
| `kafka_ca_path` | `/etc/pki/ca-trust/source/anchors/kafka-cluster-ca.pem` | Or `/etc/rsyslog.d/certs/kafka-cluster-ca.pem` |
| `kafka_ca_src` | `kafka-cluster-ca.pem` | File in `roles/rhel_telemetry/files/` |
| `rsyslog_kafka_conf` | `/etc/rsyslog.d/05-omkafka-additional.conf` | Additive drop-in only |
| `rsyslog_fail_on_early_stop` | `true` | Fail if a `stop` rule would skip Kafka |
| `pcp_interval` | `15s` | `pcp2json -t` sampling interval |
| `pcp2json_names_change` | `update` | `pcp2json -4`. Follows processes that cross the hotproc floor after `pcp2kafka` starts |
| `hotproc_predicate` | `residentsize > 102400` | Kilobytes (about 100 MB). Written to `hotproc.conf` |
| `hotproc_conf_path` | `/var/lib/pcp/pmdas/proc/hotproc.conf` | Predicate file for the `proc` PMDA |

You can override on the command line instead of editing `group_vars/all.yml`:

```bash
export BOOTSTRAP="$(oc get kafka telemetry -n logstream-kafka \
  -o jsonpath='{.status.listeners[?(@.name=="tls-external")].bootstrapServers}')"
```

`kcat` is often **not** in AppStream. Fallbacks:

| Extra-var | Effect |
| --- | --- |
| `-e telemetry_enable_epel=true` | Install EPEL, then `kcat` / `kafkacat` |
| `-e telemetry_kcat_pip_fallback=true` | `pip install kafkacat` (may **not** provide a binary; last resort) |

If both fail, install `kcat` from a supported internal mirror or build [kcat](https://github.com/edenhill/kcat) against `librdkafka`. Do not copy random binaries onto production hosts.

SELinux extra-vars:

| Extra-var | Default | Effect |
| --- | --- | --- |
| `telemetry_selinux_install_module` | `true` | Compile/install `rsyslog_omkafka` |
| `telemetry_selinux_nis_fallback` | `false` | Sets `nis_enabled` — last resort only |

Do **not** set `logging_syslogd_can_send_mail`. That boolean is for SMTP (port 25) and is **not** a Kafka/`omkafka` fix.

Remote PCP collectors:

```text
-e pcp_expose_remote=true
```

opens firewalld **44321/tcp** and **44322/tcp**. Outbound Kafka is **443**; default firewalld egress already allows it.

## Step 3 — Run the playbook

From `ansible/telemetry/`:

```bash
ansible-playbook -i inventory/hosts.yml site.yml \
  -e "kafka_bootstrap=${BOOTSTRAP}" \
  -e kafka_security_protocol=SSL \
  -e kafka_logs_topic=rhel-system-logs \
  -e kafka_pcp_topic=rhel-pcp-metrics \
  -e kafka_ca_path=/etc/pki/ca-trust/source/anchors/kafka-cluster-ca.pem \
  -e kafka_ca_src=kafka-cluster-ca.pem
```

Equivalent:

```bash
ansible-playbook -i inventory/hosts.yml playbooks/onboard.yml \
  -e "kafka_bootstrap=${BOOTSTRAP}"
```

Expected: play recap `failed=0` on `rhel_telemetry`. Handlers restart `rsyslog` and `pcp2kafka`.

**If it fails on kcat:** re-run with `-e telemetry_enable_epel=true` (and change control for EPEL), or provision `kcat` first.

**If it fails on CA missing:** `kafka-cluster-ca.pem` is still the placeholder, or `kafka_ca_src` is wrong. Re-extract `telemetry-cluster-ca-cert`.

**If it fails on `rsyslogd -N1`:** inspect `/etc/rsyslog.d/05-omkafka-additional.conf` (from [`omkafka-additional.conf.j2`](../ansible/telemetry/roles/rhel_telemetry/templates/omkafka-additional.conf.j2)). Do not edit ArcSight drop-ins to “make Kafka work” unless a `stop` rule is documented as blocking dual-home.

**If it fails because an existing drop-in checksum changed:** the role refused to clobber ArcSight. Investigate unexpected writes; do not re-run with a destination of `/etc/rsyslog.conf`.

**If it fails on early `stop`:** an existing discard loads before the Kafka file. See [Preserve ArcSight](#preserve-arcsight-forwarding).

## What the role configures

### rsyslog / omkafka (additional destination)

[`omkafka-additional.conf.j2`](../ansible/telemetry/roles/rhel_telemetry/templates/omkafka-additional.conf.j2) writes `/etc/rsyslog.d/05-omkafka-additional.conf`:

- Module `omkafka`, broker `kafka_bootstrap`, topic `rhel-system-logs`.
- JSON object: `@timestamp` (RFC-3339), `host`, `severity`, `facility`, `syslogtag`, `message`.
- `confParam`: `security.protocol=ssl`, `ssl.ca.location=<kafka_ca_path>`.
- Disk-assisted queue so a Kafka outage does not stall ArcSight or local logging.
- **No** `stop` / `& ~`. ArcSight `omfwd`/`omrelp` rules continue to run.

A previous pack filename `/etc/rsyslog.d/10-kafka.conf` is removed if present so `omkafka` is not loaded twice.

### PCP / pcp2kafka

[`pcp2kafka.service.j2`](../ansible/telemetry/roles/rhel_telemetry/templates/pcp2kafka.service.j2) installs `/etc/systemd/system/pcp2kafka.service`:

- `pcp2json -t <pcp_interval> -4 update` piped to `kcat -P` on topic `rhel-pcp-metrics`. `-4 update` refreshes the instance list when a process crosses the hotproc floor after the unit has started.
- TLS: `-X security.protocol=ssl -X ssl.ca.location=<kafka_ca_path>`.
- Metrics: filesystem used/capacity, host memory, CPU idle/user/sys/wait, plus `hotproc.psinfo.rss` and `hotproc.psinfo.cmd`.
- `Restart=always`, `RestartSec=5s`.
- `Requires=pmcd.service`.

[`hotproc.conf.j2`](../ansible/telemetry/roles/rhel_telemetry/templates/hotproc.conf.j2) writes `hotproc_conf_path` (default `/var/lib/pcp/pmdas/proc/hotproc.conf`) and reloads it with `pmstore hotproc.control.reload_config 1`. The `proc` PMDA already ships in `pcp-system-tools`. The predicate `residentsize > 102400` keeps processes at or above about 100 MB. `hotproc.psinfo.rss` and `mem.util.free` are both kilobytes. The unfiltered `proc.psinfo.rss` series is not exported. No process above the floor means an empty hotproc instance domain, which is a successful onboard. If `group_vars/all.yml` sets `pcp_metrics`, include `hotproc.psinfo.rss` and `hotproc.psinfo.cmd` in that list. A copied group_vars file replaces the role default. These series are the input for the process-growth pattern in [chapter 6](06-optional-predictive-ai-worker.md#611-further-preemptive-patterns). The predictive worker does not yet emit that alert.

### firewalld

No inbound Kafka ports on the RHEL host. Optional `pmcd`/`pmproxy` as above. Confirm egress:

```bash
getent hosts telemetry-kafka-tls-external-bootstrap-logstream-kafka.apps.<cluster-domain>
timeout 5 bash -c 'echo >/dev/tcp/<bootstrap-host>/443' && echo 'tcp 443 reachable'
```

## SELinux

`syslogd_t` cannot `name_connect` to `http_port_t` (TCP **443**) by default. The role installs the targeted module [`rsyslog_omkafka.te`](../ansible/telemetry/roles/rhel_telemetry/files/rsyslog_omkafka.te) (`allow syslogd_t http_port_t:tcp_socket name_connect`).

After a send failure, inspect AVCs:

```bash
sudo ausearch -m avc -ts recent
sudo ausearch -m avc -ts recent --raw | audit2allow -m rsyslog_omkafka
sudo grep syslog /var/log/audit/audit.log | tail
```

| Approach | Use |
| --- | --- |
| Targeted module (`rsyslog_omkafka`) | **Preferred.** Matches 443 → Kafka Route |
| `nis_enabled` | Last resort diagnostic; overly broad (`telemetry_selinux_nis_fallback=true`) |
| `logging_syslogd_can_send_mail` | **Do not use** as a Kafka fix (SMTP only) |

Rebuild the module from live AVCs if your port type differs:

```bash
sudo ausearch -m avc -ts recent --raw | audit2allow -M rsyslog_omkafka
sudo semodule -i rsyslog_omkafka.pp
```

The `rsyslog_omkafka` module only **adds** `name_connect` to TCP 443 for Kafka. It does not change allow rules for ArcSight SmartConnector ports (typically 514/tcp or 6514/tcp). Do not disable existing syslog SELinux booleans that ArcSight already needs.

## Step 4 — Verify on the host

```bash
systemctl is-active rsyslog pmcd pcp2kafka
systemctl is-enabled rsyslog pmcd pcp2kafka
rsyslogd -N1
test -f /etc/pki/ca-trust/source/anchors/kafka-cluster-ca.pem
journalctl -u rsyslog -u pcp2kafka -n 50 --no-pager
```

Expected: all three units `active`; `rsyslogd -N1` reports no errors; CA PEM present; ArcSight drop-ins still listed under `/etc/rsyslog.d/`.

```bash
test -f /etc/rsyslog.d/05-omkafka-additional.conf
ls /etc/rsyslog.d
```

Inject a log:

```bash
logger -t telemetry-onboard "rhel telemetry probe $(hostname -f) $(date -Iseconds)"
```

Wait a few seconds. `journalctl -u rsyslog` must not show repeating `omkafka` SSL or `name_connect` denials. Local `/var/log/messages` (or journal) and the ArcSight connector should still show the same line. Kafka is extra, not a replacement.

## Step 5 — Verify with kcat consume

From a workstation (or the RHEL host) that has `kcat` and the cluster CA:

```bash
BOOTSTRAP="$(oc get kafka telemetry -n logstream-kafka \
  -o jsonpath='{.status.listeners[?(@.name=="tls-external")].bootstrapServers}')"
CA=/etc/pki/ca-trust/source/anchors/kafka-cluster-ca.pem
# workstation: CA=./ca.crt after oc extract

kcat -L -b "${BOOTSTRAP}" \
  -X security.protocol=ssl \
  -X ssl.ca.location="${CA}"
```

Expected: brokers listed; topics include `rhel-system-logs` and `rhel-pcp-metrics`.

Consume the logger probe (exit after current end of partition):

```bash
kcat -C -e -o -10 \
  -b "${BOOTSTRAP}" \
  -t rhel-system-logs \
  -X security.protocol=ssl \
  -X ssl.ca.location="${CA}"
```

Expected: a JSON object with `"syslogtag"` containing `telemetry-onboard` and RFC-3339 `"@timestamp"`.

Consume a PCP sample:

```bash
kcat -C -e -o -1 \
  -b "${BOOTSTRAP}" \
  -t rhel-pcp-metrics \
  -X security.protocol=ssl \
  -X ssl.ca.location="${CA}"
```

Expected: JSON from `pcp2json` including filesystem, host memory, and CPU names from `pcp_metrics`. `hotproc.psinfo.rss` and `hotproc.psinfo.cmd` appear for processes at or above `hotproc_predicate`. If the topic is empty, `journalctl -u pcp2kafka -n 80` and confirm `kcat` is on `PATH` as installed by the role.

Do **not** use consumer group `ansible-eda`, `stream-worker`, or SIEM groups for these checks (see [chapter 7](07-validation-runbook.md)).

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| Timeout to `:9094` | Client used broker port | Set `kafka_bootstrap` to the Route **`:443`** |
| `SSL handshake failed` / unknown CA | Ingress CA or missing cluster CA | Re-extract `telemetry-cluster-ca-cert`; `ssl.ca.location` → PEM on the host |
| Hostname verification failed | Bootstrap host ≠ advertised Route | Use `status.listeners[name=tls-external].bootstrapServers` exactly |
| `omkafka` silent / AVCs on `name_connect` | SELinux blocked 443 | `ausearch`; keep `rsyslog_omkafka`; do not enable `logging_syslogd_can_send_mail` |
| ArcSight stopped receiving after onboard | Kafka drop-in used `stop`, or `/etc/rsyslog.conf` was replaced | Restore ArcSight files from backup; this role must only add `05-omkafka-additional.conf` |
| Kafka empty but ArcSight works | Early `stop`/`& ~` before the `05-` drop-in | See [Preserve ArcSight](#preserve-arcsight-forwarding) |
| `pcp2kafka` crash loop | `kcat` missing | EPEL/pip fallback or internal `kcat` package |
| Empty `rhel-pcp-metrics` | `pmcd` down or metric names | `systemctl status pmcd`; `pminfo filesys.used mem.physmem` |
| `hotproc.psinfo.rss` missing | Predicate not loaded | `pminfo -f hotproc.control.config`; rerun onboard so `hotproc.conf` is installed and `pmstore` reloads it |
| `hotproc.psinfo.rss` has no instances | No process at or above the RSS floor | Expected. Lower `hotproc_predicate` only when you need smaller processes |
| firewalld dropped remote PCP | Ports closed | `-e pcp_expose_remote=true` or do not collect PCP remotely |

## Next

Continue with [Event-Driven Ansible](05-event-driven-ansible.md) for the ten syslog events. Skip the [optional predictive worker](06-optional-predictive-ai-worker.md) unless you need PCP TTE. Prove the syslog path with `logger` in [Validation](07-validation-runbook.md).
