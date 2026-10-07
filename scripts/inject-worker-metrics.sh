#!/usr/bin/env bash
# Inject rising PCP filesystem samples so the Quarkus predictive worker emits
# PREEMPTIVE_STORAGE_EXHAUSTION_RISK on enriched-events.
#
# Preferred path (mise + uv + kafka-python — no kcat required):
#   cd worker && mise install && mise run inject-metrics -- --consume
#   ./scripts/inject-worker-metrics.sh --consume
#
# Typical flow (dev mode with Kafka Dev Services):
#   cd worker && mise run dev
#   ./scripts/inject-worker-metrics.sh --consume
#
# Bootstrap resolution order:
#   1. -b / --bootstrap
#   2. KAFKA_BOOTSTRAP_SERVERS
#   3. Quarkus Dev Services container (podman label quarkus-dev-service-kafka)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_SCRIPT="${ROOT}/scripts/inject_worker_metrics.py"

usage() {
  cat <<'EOF'
Usage: inject-worker-metrics.sh [options]

Produce rising filesys.used samples on rhel-pcp-metrics for the predictive worker.

Options:
  -b, --bootstrap HOST:PORT   Kafka bootstrap (default: env or Dev Services)
  -t, --topic TOPIC           Produce topic (default: rhel-pcp-metrics)
  -n, --samples N             Number of samples (default: 6; need >= MIN_SAMPLES)
  -i, --interval SEC          Sleep between samples (default: 1)
  -H, --host NAME             host field (default: testhost)
  -m, --mount PATH            mount/instance (default: /var)
  -c, --capacity N            capacity (default: 1000)
  --consume                   After inject, print enriched-events briefly
  -h, --help                  Show this help

Workstation requirements (see docs/06 §6.2):
  Preferred (via mise in worker/):
    - java, maven, quarkus, python, uv  —  mise install
    - Podman (required) for Kafka Dev Services — not Docker Desktop
  Optional fallback:
    - kcat (brew/dnf) if mise/uv/python path is unavailable

Example (Quarkus Dev Services):
  cd worker && mise install && mise run dev
  # in another terminal:
  ./scripts/inject-worker-metrics.sh --consume
  # or:
  cd worker && mise run inject-metrics -- --consume
EOF
}

ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    *) ARGS+=("$1"); shift ;;
  esac
done

# --- Preferred: mise-pinned uv + kafka-python ---
# Once a path is selected, propagate the injector exit code (do not fall through on failure).
if command -v mise >/dev/null 2>&1 && [[ -f "${ROOT}/worker/mise.toml" ]]; then
  if (cd "${ROOT}/worker" && mise which uv >/dev/null 2>&1 && mise which python >/dev/null 2>&1); then
    (cd "${ROOT}/worker" && mise exec -- uv run --with kafka-python "${PY_SCRIPT}" "${ARGS[@]+"${ARGS[@]}"}")
    exit $?
  fi
fi
if command -v uv >/dev/null 2>&1; then
  uv run --with kafka-python "${PY_SCRIPT}" "${ARGS[@]+"${ARGS[@]}"}"
  exit $?
fi
if command -v python3 >/dev/null 2>&1 && python3 -c "import kafka" >/dev/null 2>&1; then
  python3 "${PY_SCRIPT}" "${ARGS[@]+"${ARGS[@]}"}"
  exit $?
fi

# --- Fallback: kcat + python3 timestamps (legacy) ---
if ! command -v kcat >/dev/null 2>&1; then
  echo "ERROR: could not run the Python injector, and kcat is not on PATH." >&2
  echo "  Install the mise toolchain:  cd worker && mise install" >&2
  echo "  Then:  mise run inject-metrics -- --consume" >&2
  echo "  Or install kcat (macOS: brew install kcat)." >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required for the kcat fallback path (ISO-8601 timestamps)." >&2
  echo "  Prefer: cd worker && mise install  (pins python via mise.toml)" >&2
  exit 1
fi

TOPIC="${TOPIC:-rhel-pcp-metrics}"
ENRICHED_TOPIC="${ENRICHED_TOPIC:-enriched-events}"
HOST_NAME="${HOST_NAME:-testhost}"
MOUNT="${MOUNT:-/var}"
CAPACITY="${CAPACITY:-1000}"
SAMPLES="${SAMPLES:-6}"
INTERVAL_SEC="${INTERVAL_SEC:-1}"
USED_START="${USED_START:-100}"
USED_STEP="${USED_STEP:-100}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-}"
CONSUME=0

# Re-parse for kcat fallback (ARGS already collected).
set -- "${ARGS[@]+"${ARGS[@]}"}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    -b|--bootstrap) BOOTSTRAP="${2:-}"; shift 2 ;;
    -t|--topic) TOPIC="${2:-}"; shift 2 ;;
    -n|--samples) SAMPLES="${2:-}"; shift 2 ;;
    -i|--interval) INTERVAL_SEC="${2:-}"; shift 2 ;;
    -H|--host) HOST_NAME="${2:-}"; shift 2 ;;
    -m|--mount) MOUNT="${2:-}"; shift 2 ;;
    -c|--capacity) CAPACITY="${2:-}"; shift 2 ;;
    --consume) CONSUME=1; shift ;;
    *) echo "ERROR: unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

echo "NOTE: using kcat fallback (install mise tools for the preferred path)." >&2

discover_devservices_bootstrap() {
  local cid="" port=""
  if ! command -v podman >/dev/null 2>&1; then
    return 1
  fi
  cid="$(podman ps -q --filter label=quarkus-dev-service-kafka 2>/dev/null | head -n1 || true)"
  if [[ -z "${cid}" ]]; then
    return 1
  fi
  port="$(podman port "${cid}" 9092 2>/dev/null | head -n1 | awk -F: '{print $NF}' || true)"
  if [[ -z "${port}" ]]; then
    port="$(podman inspect -f '{{(index (index .NetworkSettings.Ports "9092/tcp") 0).HostPort}}' "${cid}" 2>/dev/null || true)"
  fi
  if [[ -z "${port}" ]]; then
    return 1
  fi
  echo "localhost:${port}"
}

if [[ -z "${BOOTSTRAP}" ]]; then
  if BOOTSTRAP="$(discover_devservices_bootstrap)"; then
    echo "Using Quarkus Dev Services Kafka at ${BOOTSTRAP}"
  else
    echo "ERROR: set KAFKA_BOOTSTRAP_SERVERS or -b HOST:PORT," >&2
    echo "  or start './mvnw quarkus:dev' so Dev Services Kafka is running." >&2
    exit 1
  fi
fi

echo "Producing ${SAMPLES} samples to ${TOPIC} @ ${BOOTSTRAP} (host=${HOST_NAME} mount=${MOUNT})"
used="${USED_START}"
for ((i = 0; i < SAMPLES; i++)); do
  ts="$(python3 - <<'PY'
from datetime import datetime, timezone
print(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")
PY
)"
  json="$(cat <<EOF
{"@timestamp":"${ts}","host":"${HOST_NAME}","metrics":{"filesys.used":{"${MOUNT}":${used}},"filesys.capacity":{"${MOUNT}":${CAPACITY}}}}
EOF
)"
  echo "  [${i}] used=${used} capacity=${CAPACITY} t=${ts}"
  printf '%s\n' "${json}" | kcat -P -b "${BOOTSTRAP}" -t "${TOPIC}" -k "${HOST_NAME}"
  used=$((used + USED_STEP))
  if (( i < SAMPLES - 1 )); then
    sleep "${INTERVAL_SEC}"
  fi
done

echo "Done. With default MIN_SAMPLES=4 and a positive fill rate, expect an alert on ${ENRICHED_TOPIC}."
echo "Watch worker logs for: emitted PREEMPTIVE_STORAGE_EXHAUSTION_RISK"

if [[ "${CONSUME}" -eq 1 ]]; then
  echo "Consuming ${ENRICHED_TOPIC} (up to 20 messages from end)..."
  kcat -b "${BOOTSTRAP}" -t "${ENRICHED_TOPIC}" -C -o -20 -e -c 20 -G inject-worker-verify || true
fi
