#!/usr/bin/env bash
# Placeholder-driven pipeline verification for logstream-kafka.
# Uses oc and kcat; replace environment variables for your cluster.
# Does not consume as ansible-eda or stream-worker. Optional kcat consumer
# uses group verify-pipeline so those offsets are not stolen.
set -euo pipefail

NAMESPACE="${NAMESPACE:-logstream-kafka}"
KAFKA_CLUSTER="${KAFKA_CLUSTER:-telemetry}"
BOOTSTRAP="${BOOTSTRAP:-telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092}"
KCAT_BIN="${KCAT_BIN:-kcat}"
OC_BIN="${OC_BIN:-oc}"
VERIFY_GROUP="${VERIFY_GROUP:-verify-pipeline}"

TOPICS=(rhel-system-logs rhel-pcp-metrics raw-metrics enriched-events)
EDA_GROUP="ansible-eda"
WORKER_GROUP="stream-worker"

usage() {
  cat <<'EOF'
Usage: verify-pipeline.sh [oc|kcat|groups|all]

Subcommands:
  oc       OpenShift Kafka/topic/pod health (placeholder oc commands)
  kcat     Topic metadata and a non-destructive tail of rhel-system-logs
  groups   Print required consumer group names (pass/fail reminder)
  all      Run oc then kcat (default)

Environment:
  NAMESPACE       default logstream-kafka
  KAFKA_CLUSTER   default telemetry
  BOOTSTRAP       default telemetry-kafka-plain-bootstrap.logstream-kafka.svc:9092
  KCAT_BIN        default kcat
  OC_BIN          default oc
  VERIFY_GROUP    kcat consumer group default verify-pipeline

See docs/validation/runbook.md and docs/optional/metrics-dashboard-pcp.md
EOF
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "MISSING: $1 (install or set ${2} and re-run). Skipping related checks." >&2
    return 1
  fi
}

cmd_groups() {
  cat <<EOF
Required consumer groups (do not overlap):
  EDA:           ${EDA_GROUP}
  stream-worker: ${WORKER_GROUP}
  This script:   ${VERIFY_GROUP}

Topics:
  ${TOPICS[*]}

Namespace / cluster:
  ${NAMESPACE} / ${KAFKA_CLUSTER}
EOF
}

cmd_oc() {
  echo "=== oc placeholders (namespace=${NAMESPACE} cluster=${KAFKA_CLUSTER}) ==="
  if ! need_cmd "${OC_BIN}" OC_BIN; then
    echo "PASS CRITERION (manual): ${OC_BIN} whoami succeeds and project ${NAMESPACE} exists."
    cat <<EOF
# Placeholder commands — run after logging in:
${OC_BIN} whoami
${OC_BIN} project ${NAMESPACE}
${OC_BIN} get kafka ${KAFKA_CLUSTER} -n ${NAMESPACE} -o wide
${OC_BIN} get kafkanodepool -n ${NAMESPACE}
${OC_BIN} get kafkatopic -n ${NAMESPACE}
${OC_BIN} get pods -n ${NAMESPACE} -l strimzi.io/cluster=${KAFKA_CLUSTER}
${OC_BIN} get kafka ${KAFKA_CLUSTER} -n ${NAMESPACE} -o jsonpath='{.status.listeners}' ; echo
${OC_BIN} get deploy -n ${NAMESPACE}
${OC_BIN} logs -n ${NAMESPACE} -l app.kubernetes.io/name=predictive-ai-worker --tail=50
EOF
    return 0
  fi

  echo "-> ${OC_BIN} whoami"
  "${OC_BIN}" whoami
  echo "-> project / kafka / topics / pods"
  "${OC_BIN}" get kafka "${KAFKA_CLUSTER}" -n "${NAMESPACE}" -o wide || true
  "${OC_BIN}" get kafkanodepool -n "${NAMESPACE}" || true
  "${OC_BIN}" get kafkatopic -n "${NAMESPACE}" || true
  "${OC_BIN}" get pods -n "${NAMESPACE}" -l "strimzi.io/cluster=${KAFKA_CLUSTER}" || true
  echo "Expected KafkaTopic names: ${TOPICS[*]}"
}

cmd_kcat() {
  echo "=== kcat placeholders (bootstrap=${BOOTSTRAP}) ==="
  if ! need_cmd "${KCAT_BIN}" KCAT_BIN; then
    cat <<EOF
# Placeholder commands — run from a host that can reach Kafka:
${KCAT_BIN} -b ${BOOTSTRAP} -L
${KCAT_BIN} -b ${BOOTSTRAP} -t rhel-system-logs -C -o -10 -e -c 10 -G ${VERIFY_GROUP}
${KCAT_BIN} -b ${BOOTSTRAP} -t rhel-pcp-metrics -C -o -5 -e -c 5 -G ${VERIFY_GROUP}
${KCAT_BIN} -b ${BOOTSTRAP} -t raw-metrics -C -o -5 -e -c 5 -G ${VERIFY_GROUP}
${KCAT_BIN} -b ${BOOTSTRAP} -t enriched-events -C -o -10 -e -c 10 -G ${VERIFY_GROUP}
# Metadata only (no consume):
${KCAT_BIN} -b ${BOOTSTRAP} -t rhel-system-logs -L
EOF
    return 0
  fi

  echo "-> broker metadata"
  "${KCAT_BIN}" -b "${BOOTSTRAP}" -L || true
  echo "-> last messages on rhel-system-logs (group ${VERIFY_GROUP}, bounded)"
  "${KCAT_BIN}" -b "${BOOTSTRAP}" -t rhel-system-logs -C -o -10 -e -c 10 -G "${VERIFY_GROUP}" || true
}

main() {
  local mode="${1:-all}"
  case "${mode}" in
    -h|--help) usage; exit 0 ;;
    oc) cmd_oc ;;
    kcat) cmd_kcat ;;
    groups) cmd_groups ;;
    all) cmd_groups; echo; cmd_oc; echo; cmd_kcat ;;
    *) echo "ERROR: unknown subcommand '${mode}'" >&2; usage; exit 1 ;;
  esac
}

main "${@:-}"
