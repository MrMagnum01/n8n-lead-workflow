#!/usr/bin/env python3
"""Mock alert sink for the n8n lead-intake demo.

Stands in for a Slack/email notification for a newly-upserted lead. No
real Slack or email is used anywhere in this demo. Stdlib-only HTTP
server, 127.0.0.1 only. Appends each alert to a JSON array file, written
atomically (temp file + os.replace) on every write, same pattern as the
CRM mock, so the file is never observed half-written.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOCK = threading.Lock()
STATE_PATH: str = ""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load() -> list:
    if not os.path.exists(STATE_PATH):
        return []
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _atomic_write(alerts: list) -> None:
    directory = os.path.dirname(STATE_PATH) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".alerts_", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(alerts, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, STATE_PATH)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


class Handler(BaseHTTPRequestHandler):
    server_version = "MockAlertSink/1.0"

    def log_message(self, fmt, *args):
        pass

    def _send_json(self, code: int, payload) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw or b"{}")

    def do_GET(self):
        if self.path == "/alerts":
            with LOCK:
                alerts = _load()
            self._send_json(200, {"alerts": alerts})
        elif self.path == "/health":
            self._send_json(200, {"ok": True})
        else:
            self._send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path != "/alert":
            self._send_json(404, {"error": "not_found"})
            return
        body = self._read_json()
        with LOCK:
            alerts = _load()
            entry = dict(body)
            entry["received_at"] = _now()
            alerts.append(entry)
            _atomic_write(alerts)
        self._send_json(200, {"status": "ok"})


def main() -> None:
    global STATE_PATH
    if len(sys.argv) < 3:
        print("usage: alert_server.py <port> <state_file>", file=sys.stderr)
        sys.exit(2)
    port = int(sys.argv[1])
    STATE_PATH = sys.argv[2]
    if not os.path.exists(STATE_PATH):
        _atomic_write([])
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"alert mock listening on 127.0.0.1:{port} state={STATE_PATH}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
