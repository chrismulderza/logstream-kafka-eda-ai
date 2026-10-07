from __future__ import annotations

import json
import logging
from typing import Any

import requests

from stream_worker.config import Config

logger = logging.getLogger(__name__)


class InferenceClient:
    """OpenAI-compatible Chat Completions client (vLLM, RHOAI, or external)."""

    def __init__(self, cfg: Config):
        self.base_url = cfg.inference_base_url.rstrip("/")
        self.api_key = cfg.inference_api_key
        self.model = cfg.inference_model
        self.timeout = cfg.inference_timeout
        self.max_tokens = cfg.inference_max_tokens

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    def classify_json(self, messages: list[dict[str, str]]) -> dict[str, Any]:
        if not self.enabled:
            return {}
        url = f"{self.base_url}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        body = {
            "model": self.model,
            "temperature": 0,
            "max_tokens": self.max_tokens,
            "messages": messages,
        }
        try:
            response = requests.post(url, json=body, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            content = (
                response.json()
                .get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                or ""
            )
            return _parse_json_object(content)
        except requests.RequestException:
            logger.exception("Inference request failed; continuing without LLM fields")
            return {}


def _parse_json_object(content: str) -> dict[str, Any]:
    text = content.strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                data = json.loads(text[start : end + 1])
                return data if isinstance(data, dict) else {}
            except json.JSONDecodeError:
                return {"summary": text}
        return {"summary": text}
