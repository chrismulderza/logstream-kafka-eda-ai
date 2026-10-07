"""HTTP health endpoint for OpenShift probes."""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = logging.getLogger(__name__)


class HealthState:
    def __init__(self) -> None:
        self.ready = False
        self.alive = True
        self.consume_error: str | None = None

    def snapshot(self) -> dict:
        return {
            "alive": self.alive,
            "ready": self.ready,
            "consume_error": self.consume_error,
        }


def start_health_server(bind: str, port: int, state: HealthState) -> ThreadingHTTPServer:
    snapshot = state

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: object) -> None:
            LOG.debug("health %s", fmt % args)

        def _send(self, code: int, body: dict) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:  # noqa: N802
            if self.path in {"/healthz", "/livez", "/"}:
                self._send(200 if snapshot.alive else 503, snapshot.snapshot())
                return
            if self.path in {"/readyz", "/ready"}:
                self._send(200 if snapshot.ready else 503, snapshot.snapshot())
                return
            self._send(404, {"error": "not found"})

    server = ThreadingHTTPServer((bind, port), Handler)
    thread = threading.Thread(target=server.serve_forever, name="health", daemon=True)
    thread.start()
    LOG.info("health server listening on %s:%s", bind, port)
    return server
