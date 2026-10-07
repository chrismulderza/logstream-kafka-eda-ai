#!/usr/bin/env bash
# Synthetic storage-pressure test for the predictive stream-worker path.
# Creates a sparse 5Gi file under /var/log so PCP disk/filesys metrics rise and
# the worker can emit PREEMPTIVE_STORAGE_EXHAUSTION_RISK on enriched-events.
#
# RISK: This consumes 5Gi of filesystem capacity on the log volume. On small
# disks this can itself trigger real exhaustion, journald rate-limits, or
# service failure. Always run cleanup after the test. Do not use on production
# hosts without a maintenance window and a rollback plan.
set -euo pipefail

FILL_PATH="${FILL_PATH:-/var/log/test_fill.img}"
FILL_SIZE="${FILL_SIZE:-5G}"

usage() {
  cat <<'EOF'
Usage: storage-fill-test.sh <create|cleanup|status>

Modes:
  create   fallocate a 5Gi image at /var/log/test_fill.img (override FILL_PATH / FILL_SIZE)
  cleanup  remove the test image (rm -f)
  status   report whether the image exists and df for its filesystem

RISK: occupy 5Gi on the log filesystem. If /var/log is nearly full, this can
cause real outages. Prefer a lab host. Always run cleanup when finished.

Environment:
  FILL_PATH   default /var/log/test_fill.img
  FILL_SIZE   default 5G (fallocate -l syntax)

Related: scripts/verify-pipeline.sh, docs/07-validation-runbook.md
EOF
}

require_root_hint() {
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "WARN: not running as root; fallocate/rm under /var/log may fail." >&2
  fi
}

cmd_create() {
  require_root_hint
  if [[ -e "${FILL_PATH}" ]]; then
    echo "ERROR: ${FILL_PATH} already exists. Run cleanup first or set FILL_PATH." >&2
    exit 1
  fi
  local parent
  parent="$(dirname "${FILL_PATH}")"
  if [[ ! -d "${parent}" ]]; then
    echo "ERROR: parent directory ${parent} does not exist." >&2
    exit 1
  fi
  echo "RISK: allocating ${FILL_SIZE} at ${FILL_PATH} (may fill the log filesystem)."
  df -h "${parent}" || true
  if ! command -v fallocate >/dev/null 2>&1; then
    echo "ERROR: fallocate is required." >&2
    exit 1
  fi
  fallocate -l "${FILL_SIZE}" "${FILL_PATH}"
  ls -lh "${FILL_PATH}"
  df -h "${parent}"
  echo "OK: fill file created. PCP/pcp2kafka should now show rising filesys usage."
  echo "When finished: $0 cleanup"
}

cmd_cleanup() {
  require_root_hint
  if [[ ! -e "${FILL_PATH}" ]]; then
    echo "OK: ${FILL_PATH} not present; nothing to remove."
    return 0
  fi
  echo "Removing ${FILL_PATH}"
  rm -f "${FILL_PATH}"
  echo "OK: cleanup complete."
  df -h "$(dirname "${FILL_PATH}")" || true
}

cmd_status() {
  if [[ -e "${FILL_PATH}" ]]; then
    echo "PRESENT: ${FILL_PATH}"
    ls -lh "${FILL_PATH}"
  else
    echo "ABSENT: ${FILL_PATH}"
  fi
  df -h "$(dirname "${FILL_PATH}")" || true
}

case "${1:-}" in
  create) cmd_create ;;
  cleanup) cmd_cleanup ;;
  status) cmd_status ;;
  -h|--help|"") usage; [[ -n "${1:-}" ]] || exit 1 ;;
  *) echo "ERROR: unknown mode '${1}'" >&2; usage; exit 1 ;;
esac
