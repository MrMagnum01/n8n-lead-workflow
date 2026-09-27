#!/usr/bin/env python3
"""Mock CRM server for the n8n lead-intake demo.

Stdlib-only HTTP server (no third-party dependencies). Binds to 127.0.0.1
on a caller-supplied port. This is the single system of record for the
demo: it holds three collections so the whole pipeline can be reconciled
against one source of truth --

  - contacts    : upserted leads (keyed by lowercased email; a repeat
                  email updates the existing record rather than creating
                  a second one -- "upsert", not "insert")
  - rejected    : leads the workflow's validation step refused, with a
                  reason
  - dead_letter : leads that failed enrichment or CRM upsert after
                  retries, with the failing stage and reason

Every write is atomic: build the new state in memory, serialize it, write
to a temp file in the same directory, then os.replace() it over the real
file. A reader (or a crashed writer) never observes a half-written file.
A single process-wide lock serializes writes so concurrent requests can't
interleave read-modify-write and lose an update.

Also exposes an outage toggle (POST /admin/outage) so the test harness can
simulate the CRM being down, and GET /admin/state for one-shot
reconciliation reads.

The outage toggle affects only the /contacts upsert endpoint -- the real
"CRM" surface a lead pipeline calls to do its main job. /rejected and
/dead-letter are deliberately NOT affected by it: a real dead-letter/
exception queue has to stay reachable precisely when the primary
downstream system is down, or every failure during an outage becomes a
second, silent failure (the thing this demo's error branch exists to
prevent). Modelling all three as one mock process is a synthetic-demo
simplification; the two failure domains are still kept logically and
operationally separate.
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
OUTAGE = {"on": False}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_state() -> dict:
    return {"contacts": {}, "rejected": [], "dead_letter": []}


def _load() -> dict:
    if not os.path.exists(STATE_PATH):
        return _empty_state()
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _atomic_write(state: dict) -> None:
    directory = os.path.dirname(STATE_PATH) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".crm_state_", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
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
    server_version = "MockCRM/1.0"

    def log_message(self, fmt, *args):  # quiet; harness reads state via API
        pass

    def _send_json(self, code: int, payload: dict) -> None:
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

    def _outage_check(self) -> bool:
        if OUTAGE["on"]:
            self._send_json(503, {"error": "crm_outage", "detail": "mock CRM is simulating an outage"})
            return True
        return False

    def do_GET(self):
        if self.path == "/contacts":
            with LOCK:
                state = _load()
            self._send_json(200, {"contacts": list(state["contacts"].values())})
        elif self.path == "/rejected":
            with LOCK:
                state = _load()
            self._send_json(200, {"rejected": state["rejected"]})
        elif self.path == "/dead-letter":
            with LOCK:
                state = _load()
            self._send_json(200, {"dead_letter": state["dead_letter"]})
        elif self.path == "/admin/state":
            with LOCK:
                state = _load()
            self._send_json(200, state)
        elif self.path == "/admin/outage":
            self._send_json(200, {"on": OUTAGE["on"]})
        elif self.path == "/health":
            self._send_json(200, {"ok": True})
        else:
            self._send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path == "/admin/outage":
            body = self._read_json()
            OUTAGE["on"] = bool(body.get("on", False))
            self._send_json(200, {"on": OUTAGE["on"]})
            return
        if self.path == "/admin/reset":
            with LOCK:
                _atomic_write(_empty_state())
            OUTAGE["on"] = False
            self._send_json(200, {"reset": True})
            return

        if self.path == "/contacts":
            if self._outage_check():
                return
            body = self._read_json()
            email = str(body.get("email", "")).strip().lower()
            if not email:
                self._send_json(400, {"error": "email_required"})
                return
            with LOCK:
                state = _load()
                existing = state["contacts"].get(email)
                record = dict(body)
                record["email"] = email
                record["updated_at"] = _now()
                if existing:
                    record["created_at"] = existing.get("created_at", _now())
                    record["upsert_count"] = existing.get("upsert_count", 1) + 1
                else:
                    record["created_at"] = record["updated_at"]
                    record["upsert_count"] = 1
                state["contacts"][email] = record
                _atomic_write(state)
            self._send_json(200, {"status": "ok", "contact": record})
        elif self.path == "/rejected":
            body = self._read_json()
            with LOCK:
                state = _load()
                entry = dict(body)
                entry["recorded_at"] = _now()
                state["rejected"].append(entry)
                _atomic_write(state)
            self._send_json(200, {"status": "ok"})
        elif self.path == "/dead-letter":
            body = self._read_json()
            with LOCK:
                state = _load()
                entry = dict(body)
                entry["recorded_at"] = _now()
                state["dead_letter"].append(entry)
                _atomic_write(state)
            self._send_json(200, {"status": "ok"})
        else:
            self._send_json(404, {"error": "not_found"})


def main() -> None:
    global STATE_PATH
    if len(sys.argv) < 3:
        print("usage: crm_server.py <port> <state_file>", file=sys.stderr)
        sys.exit(2)
    port = int(sys.argv[1])
    STATE_PATH = sys.argv[2]
    if not os.path.exists(STATE_PATH):
        _atomic_write(_empty_state())
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"crm mock listening on 127.0.0.1:{port} state={STATE_PATH}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
