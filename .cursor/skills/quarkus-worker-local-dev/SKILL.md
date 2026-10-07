---
name: quarkus-worker-local-dev
description: >-
  Run and test the Quarkus predictive worker locally with mise, Podman Dev
  Services, metric inject, and optional Ollama inference. Use when working on
  worker/, inject-worker-metrics, Kafka Dev Services, or local Granite/Ollama
  enrichment.
---

# Quarkus worker local development

## Toolchain

```bash
cd worker
mise trust && mise install   # OpenJDK 21, Maven, Quarkus, Python, uv
```

## Dev Services (Podman)

```bash
mise run dev                 # sources scripts/podman-env.sh
# Expect: Listening on http://0.0.0.0:8080 and Dev Services for Kafka
```

If Podman API fails on corrupt `999-podman-desktop-registries-from-host.conf`, fix inside the machine (see docs §6.2.9).

## Inject TTE samples

```bash
mise run inject-metrics -- --consume
# or from repo root: ./scripts/inject-worker-metrics.sh --consume
```

Expect `PREEMPTIVE_STORAGE_EXHAUSTION_RISK` on `enriched-events`.

## Optional Ollama inference

```bash
ollama serve
ollama pull granite3.3:2b
export INFERENCE_BASE_URL=http://127.0.0.1:11434/v1
export INFERENCE_MODEL=granite3.3:2b
export LLM_ON_METRIC_ALERTS=true
export INFERENCE_TIMEOUT_SECONDS=60
mise run dev
# other terminal — use a fresh -H host to avoid cooldown:
./scripts/inject-worker-metrics.sh --consume -H ollama-dev
# cleanup:
ollama rm granite3.3:2b
```

Prefer IBM Granite; avoid granite4.2 / qwen3 with the current client (empty OpenAI `content`). Bench: `scripts/bench_ollama_inference.py`.

## Inference semantics

LLM fields are annotations only. TTE math and alert emission are deterministic. See `worker/README.md` § What inference does.
