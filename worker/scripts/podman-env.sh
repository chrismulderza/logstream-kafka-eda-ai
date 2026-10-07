#!/usr/bin/env bash
# Configure the shell so Quarkus Dev Services / Testcontainers talk to Podman only.
# Source from mise tasks or:  source worker/scripts/podman-env.sh
#
# Sets:
#   DOCKER_HOST                       → Podman API socket (unix://…)
#   TESTCONTAINERS_RYUK_DISABLED=true  → Ryuk is incompatible with rootless Podman
#   PATH                              → docker→podman shim (always, so Docker Desktop is unused)
#
# Requires: podman on PATH. On macOS/Windows, a running `podman machine`.
set -euo pipefail

if ! command -v podman >/dev/null 2>&1; then
  echo "ERROR: podman is required for Quarkus Kafka Dev Services on this project." >&2
  echo "  Install Podman Desktop (recommended) or the Podman CLI, then re-run." >&2
  echo "  Docs: https://quarkus.io/guides/podman" >&2
  return 1 2>/dev/null || exit 1
fi

start_podman_machine_if_needed() {
  if ! podman machine list --format '{{.Name}}' 2>/dev/null | grep -q .; then
    return 0
  fi
  # Prefer listing "Currently running"; fall back to start (ignore already-running).
  if podman machine list --format '{{.Name}} {{.LastUp}}' 2>/dev/null | grep -qi 'currently running'; then
    return 0
  fi
  echo "Starting podman machine..."
  podman machine start >/dev/null 2>&1 || true
  local attempt
  for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    if podman machine list --format '{{.LastUp}}' 2>/dev/null | grep -qi 'currently running'; then
      return 0
    fi
    sleep 1
  done
  return 1
}

resolve_podman_socket() {
  local sock=""
  # macOS/Windows: machine API socket (works even when `podman info` fails on bad registries.conf)
  if sock="$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null | head -n1)"; then
    if [[ -n "${sock}" && -S "${sock}" ]]; then
      echo "${sock}"
      return 0
    fi
  fi
  if sock="$(podman info --format '{{.Host.RemoteSocket.Path}}' 2>/dev/null)"; then
    if [[ -n "${sock}" ]]; then
      echo "${sock}"
      return 0
    fi
  fi
  for sock in \
    "${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/podman/podman.sock" \
    "${HOME}/.local/share/containers/podman/machine/podman.sock"
  do
    if [[ -S "${sock}" ]]; then
      echo "${sock}"
      return 0
    fi
  done
  return 1
}

ping_docker_api() {
  local sock="$1"
  if command -v curl >/dev/null 2>&1; then
    curl -sf --unix-socket "${sock}" http://localhost/_ping >/dev/null 2>&1 && return 0
  fi
  # Fallback: docker shim against DOCKER_HOST (may still fail if CLI registries.conf is broken)
  DOCKER_HOST="unix://${sock}" docker info >/dev/null 2>&1
}

if ! start_podman_machine_if_needed; then
  echo "ERROR: podman machine did not become ready." >&2
  echo "  On macOS/Windows: podman machine init && podman machine start" >&2
  return 1 2>/dev/null || exit 1
fi

sock="$(resolve_podman_socket || true)"
if [[ -z "${sock}" ]]; then
  echo "ERROR: could not resolve a Podman API socket." >&2
  echo "  On Linux: systemctl --user enable --now podman.socket" >&2
  echo "  On macOS/Windows: podman machine start" >&2
  return 1 2>/dev/null || exit 1
fi

# Quarkus / Testcontainers speak the Docker API; point them at Podman.
export DOCKER_HOST="unix://${sock}"
export TESTCONTAINERS_RYUK_DISABLED=true
# Prefer the Podman socket even if a stale /var/run/docker.sock exists.
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="${sock}"

# Quarkus IsDockerWorking probes the `docker` CLI. Always put a Podman shim
# first on PATH so Dev Services never talks to Docker Desktop. Probes use the
# Docker API on DOCKER_HOST so a broken `podman` CLI (e.g. bad registries.conf)
# still reports a working engine.
shim_dir="${TMPDIR:-/tmp}/logstream-podman-docker-shim"
mkdir -p "${shim_dir}"
cat > "${shim_dir}/docker" <<EOF
#!/usr/bin/env bash
# Project-local shim: Quarkus Dev Services must use Podman, not Docker Desktop.
set -euo pipefail
sock="\${TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE:-}"
if [[ -z "\${sock}" && "\${DOCKER_HOST:-}" == unix://* ]]; then
  sock="\${DOCKER_HOST#unix://}"
fi
api() {
  curl -sf --unix-socket "\${sock}" "\$1"
}
case "\${1:-}" in
  -v|--version)
    echo "Docker version 24.0.0-podman, build logstream-shim"
    exit 0
    ;;
  version)
    if [[ -n "\${sock}" ]] && api http://localhost/_ping >/dev/null 2>&1; then
      echo "Client: Docker Engine - Community (podman shim)"
      echo "Server: Podman API"
      exit 0
    fi
    ;;
  info)
    if [[ -n "\${sock}" ]] && api http://localhost/_ping >/dev/null 2>&1; then
      api http://localhost/info 2>/dev/null || echo "Server: Podman (API reachable)"
      exit 0
    fi
    ;;
esac
exec podman "\$@"
EOF
chmod +x "${shim_dir}/docker"
export PATH="${shim_dir}:${PATH}"

if ! ping_docker_api "${sock}"; then
  echo "ERROR: Podman API is not responding at ${sock}" >&2
  echo "  DOCKER_HOST=${DOCKER_HOST}" >&2
  echo "  Try: podman machine stop && podman machine start" >&2
  echo "  If \`podman info\` fails on registries.conf, fix/remove the broken drop-in under" >&2
  echo "  /etc/containers/registries.conf.d/ (Podman Desktop host-sync files)." >&2
  return 1 2>/dev/null || exit 1
fi

echo "Podman Dev Services env: DOCKER_HOST=${DOCKER_HOST} (docker → $(command -v docker))"
