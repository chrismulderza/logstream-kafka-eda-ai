#!/usr/bin/env bash
# Inject a synthetic OOM syslog event into the host pipeline.
# Expected path: rsyslog continues to ArcSight *and* omkafka ->
# topic rhel-system-logs -> EDA group ansible-eda
# and SIEM groups siem-logstash / siem-splunk (never reuse EDA or stream-worker groups).
set -euo pipefail

LOGGER_TAG="${LOGGER_TAG:-test_oom}"
LOGGER_MESSAGE="${LOGGER_MESSAGE:-Out of memory: Kill process 1234}"
PRIORITY="${PRIORITY:-kern.err}"

usage() {
  cat <<'EOF'
Usage: inject-oom-log.sh

Injects a synthetic kernel-style OOM line via logger(1) so rsyslog copies it
to Kafka (omkafka) without replacing existing ArcSight forwarding.

Environment:
  LOGGER_TAG       Syslog tag (default: test_oom)
  LOGGER_MESSAGE   Message body (default: Out of memory: Kill process 1234)
  PRIORITY         logger -p facility.level (default: kern.err)

Verification: docs/07-validation-runbook.md (OOM injection checklist)
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if ! command -v logger >/dev/null 2>&1; then
  echo "ERROR: logger(1) is required (util-linux)." >&2
  exit 1
fi

echo "Injecting synthetic OOM log: tag=${LOGGER_TAG} priority=${PRIORITY}"
logger -p "${PRIORITY}" -t "${LOGGER_TAG}" -- "${LOGGER_MESSAGE}"
echo "OK: logger accepted the message."
echo "Next: confirm ingestion with scripts/verify-pipeline.sh or kcat on rhel-system-logs."
echo "See docs/07-validation-runbook.md"
