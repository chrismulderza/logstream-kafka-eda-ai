# Agent guidance — logstream-kafka-eda-ai

## Product context

Optional Quarkus predictive worker + OpenShift Kafka/EDA book. Core pipeline is dual-home rsyslog (ArcSight untouched) → Kafka → EDA. Spell **ArcSight** correctly (not Arc Sight / arcsight in prose).

## Worker development (Quarkus)

- Toolchain: [`worker/mise.toml`](worker/mise.toml) — **OpenJDK 21** (not Temurin), Maven, Quarkus CLI, Python, uv.
- Prefer `cd worker && mise run …` (`dev`, `test`, `package`, `inject-metrics`).
- Kafka **Dev Services use Podman only** (not Docker Desktop). Tasks source [`worker/scripts/podman-env.sh`](worker/scripts/podman-env.sh).
- Metric inject: `mise run inject-metrics -- --consume` or `./scripts/inject-worker-metrics.sh` (Podman discovery; uv + kafka-python preferred over kcat).
- Container bases: Red Hat **UBI 9 OpenJDK** (`registry.access.redhat.com/ubi9/openjdk-21*`). Native micro may use `quay.io/quarkus/ubi9-quarkus-micro-image:2.0`. No Temurin/Corretto/Docker Hub JVM bases.
- Sync inference HTTP must run `@Blocking` on the Kafka consumer (not the Vert.x event loop).

## Inference

- Optional enrichment only. TTE / alert emission is deterministic.
- No hardcoded severity/score/root-cause rules — see prompts in `worker/.../infer/Prompts.java` and [`worker/README.md`](worker/README.md).
- Local Ollama procedure + model bench: docs §6.2.10; recommended model `granite3.3:2b`. Prefer IBM Granite. Clean up with `ollama rm` when done.
- Do not set `INFERENCE_BASE_URL` to `""` (omit the var). OpenShift ConfigMap should omit empty keys.

## Docs

- MkDocs book under `docs/`. Plain-text formulas (no LaTeX). Diagrams in Mermaid only.
- OCP target **4.20–4.21**. Fail-closed EDA safety gates.
- Do not edit `plan.md` unless the user asks.

## Rules and skills

- Project rules: `.cursor/rules/*.mdc`
- Project skills: `.cursor/skills/*/SKILL.md`
