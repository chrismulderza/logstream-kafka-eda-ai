#!/usr/bin/env python3
"""Benchmark Ollama OpenAI-compatible models against worker inference prompts."""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:11434/v1"

METRIC_SYSTEM = (
    "Explain a predictive storage exhaustion alert.\n"
    "Respond with compact JSON only:\n"
    '{"severity":"WARNING|CRITICAL","score":<0-100 integer>,'
    '"root_cause":"...","summary":"...","recommended_action":"..."}'
)
METRIC_USER = (
    "host=testhost instance=/var used=600.0 capacity=1000.0 "
    "rate_per_second=100.0 tte_seconds=4.0\n"
)

RCA_SYSTEM = (
    "You are a platform SRE assistant. Analyze one RHEL syslog line.\n"
    "Respond with compact JSON only:\n"
    '{"root_cause":"...","subsystem":"...","summary":"...","recommended_action":"..."}'
    "\nDo not invent host facts that are not in the message."
)
RCA_USER = (
    "host=web01\nsyslogtag=kernel\nfacility=kern\nseverity=err\n"
    "message=Out of memory: Kill process 1234 (java) score 900 or sacrifice child\n"
)

SEV_SYSTEM = (
    "Score operational severity of one event.\n"
    "Respond with compact JSON only:\n"
    '{"severity":"INFO|WARNING|CRITICAL","score":<0-100 integer>,"rationale":"..."}'
)
SEV_USER = "host=web01\ncontext=oom\nmessage=Out of memory: Kill process 1234 (java)\n"

TASKS = [
    ("metric_alert", METRIC_SYSTEM, METRIC_USER, {"severity", "score", "root_cause", "summary", "recommended_action"}),
    ("rca", RCA_SYSTEM, RCA_USER, {"root_cause", "subsystem", "summary", "recommended_action"}),
    ("severity", SEV_SYSTEM, SEV_USER, {"severity", "score", "rationale"}),
]


def chat(model: str, system: str, user: str, timeout: float = 180.0) -> tuple[str, float, str | None]:
    body = json.dumps(
        {
            "model": model,
            "temperature": 0,
            "max_tokens": 400,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
    ).encode()
    req = urllib.request.Request(
        f"{BASE}/chat/completions",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        elapsed = time.perf_counter() - t0
        content = data["choices"][0]["message"]["content"]
        return content or "", elapsed, None
    except Exception as ex:  # noqa: BLE001
        return "", time.perf_counter() - t0, str(ex)


def extract_json(text: str) -> dict | None:
    text = (text or "").strip()
    if not text:
        return None
    # Strip optional markdown fences / thinking wrappers
    if "```" in text:
        parts = text.split("```")
        for p in parts:
            p = p.strip()
            if p.startswith("json"):
                p = p[4:].strip()
            if p.startswith("{"):
                text = p
                break
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return None


def score_task(name: str, content: str, required: set[str]) -> dict:
    obj = extract_json(content)
    result = {
        "task": name,
        "valid_json": obj is not None,
        "required_keys": False,
        "severity_ok": None,
        "score_ok": None,
        "raw_preview": (content or "")[:240].replace("\n", "\\n"),
    }
    if not obj:
        return result
    result["required_keys"] = required.issubset(obj.keys())
    if "severity" in required:
        sev = str(obj.get("severity", "")).upper()
        allowed = {"WARNING", "CRITICAL"} if name == "metric_alert" else {"INFO", "WARNING", "CRITICAL"}
        result["severity_ok"] = sev in allowed
    if "score" in required:
        try:
            s = int(obj.get("score"))
            result["score_ok"] = 0 <= s <= 100
        except (TypeError, ValueError):
            result["score_ok"] = False
    result["parsed"] = obj
    return result


def points(r: dict) -> int:
    p = 0
    if r["valid_json"]:
        p += 2
    if r["required_keys"]:
        p += 2
    if r.get("severity_ok") is True:
        p += 1
    if r.get("score_ok") is True:
        p += 1
    if r.get("severity_ok") is None and r.get("score_ok") is None and r["required_keys"]:
        p += 1  # rca has no severity/score
    return p


def main(models: list[str]) -> int:
    rows = []
    for model in models:
        print(f"\n=== {model} ===", flush=True)
        total = 0
        max_pts = 0
        task_results = []
        for name, system, user, required in TASKS:
            content, elapsed, err = chat(model, system, user)
            if err:
                print(f"  {name}: ERROR {err} ({elapsed:.1f}s)", flush=True)
                tr = {"task": name, "valid_json": False, "required_keys": False, "error": err, "elapsed": elapsed}
                task_results.append(tr)
                max_pts += 6
                continue
            tr = score_task(name, content, required)
            tr["elapsed"] = round(elapsed, 2)
            pts = points(tr)
            # metric_alert max 6, rca max 5, severity max 6 — normalize later via sum
            total += pts
            max_pts += 6 if name != "rca" else 5
            task_results.append(tr)
            print(
                f"  {name}: json={tr['valid_json']} keys={tr['required_keys']} "
                f"sev={tr.get('severity_ok')} score={tr.get('score_ok')} "
                f"{elapsed:.1f}s pts={pts}",
                flush=True,
            )
            print(f"    preview: {tr['raw_preview']}", flush=True)
        rows.append({"model": model, "points": total, "max_points": max_pts, "tasks": task_results})

    rows.sort(key=lambda r: (-r["points"], r["model"]))
    print("\n=== RANKING ===")
    for r in rows:
        print(f"{r['points']}/{r['max_points']}\t{r['model']}")
    out = "/Users/cmulder/code/work/aiops/logstream-kafka-eda-ai/docs/ollama-model-bench.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
    print(f"wrote {out}")
    if rows:
        print(f"BEST={rows[0]['model']}")
    return 0


if __name__ == "__main__":
    models = sys.argv[1:] or [
        "granite3.3:2b",
        "granite3.3:8b",
        "granite4.2:3b",
        "granite4.2:8b",
    ]
    raise SystemExit(main(models))
