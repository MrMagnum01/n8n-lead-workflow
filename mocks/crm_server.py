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

Idempotency: a /contacts POST may carry an "idempotency_key" (the n8n
workflow derives one from the lead's own lead_id). The first request
under a given key performs the write as normal; a later request under
the SAME key, with an unchanged payload, is recognized as a retry of
that same logical write -- it returns the original cached response and
does NOT increment upsert_count again. A later request under the same
key but a CHANGED payload is a conflict (409), not silently accepted as
a new write. This is what makes retries of an already-committed write
safe: a client that sent the request, then lost the reply and retried,
gets back the true prior result instead of writing a second time.

POST /admin/drop-reply-once arms a one-shot fault: the very next
/contacts write is performed and committed normally, but the HTTP
response is never sent -- the connection is dropped instead, so the
caller experiences a request timeout/reset for a write that in fact
already succeeded. This models "the write lands, the acknowledgement is
lost", the specific fault an idempotency key is for; the disarmed flag
resets to off after firing once.

POST /admin/malformed-once arms a one-shot fault on the OTHER side of the
same problem: the next /contacts write returns HTTP 200 with an empty
body ({}) instead of the normal {"status":"ok","contact":{...}} -- no
write is performed. A gateway/proxy bug that returns success with a
garbled body is a different fault than a lost acknowledgement, and must
not be accepted as a valid contact.

The outage toggle affects only the /contacts upsert endpoint -- the real
"CRM" surface a lead pipeline calls to do its main job. /rejected and
/dead-letter are deliberately NOT affected by it: a real dead-letter/
exception queue has to stay reachable precisely when the primary
downstream system is down, or every failure during an outage becomes a
second, silent failure (the thing this demo's error branch exists to
prevent). Modelling all three as one mock process is a synthetic-demo
simplification; the two failure domains are still kept logically and
operationally separate.

A SEPARATE toggle, POST /admin/recording-outage, exists only to test
what happens when the recording path ITSELF is unavailable (a real
exception-queue outage, not modelled by the /contacts toggle above): it
makes /rejected and /dead-letter return 503 while leaving /contacts
alone. The workflow must not report a lead as rejected/dead-lettered
when this recording write itself failed -- that would be the exact
false-durability bug this demo's error branch exists to avoid.
"""
from __future__ import annotations

import hashlib
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
DROP_REPLY_ONCE = {"armed": False}
MALFORMED_ONCE = {"armed": False}
RECORDING_OUTAGE = {"on": False}


def _fingerprint(body: dict) -> str:
    # Everything that identifies THIS logical write, excluding the
    # idempotency key itself. Two requests under the same key with a
    # different fingerprint are a genuine conflict, not a retry.
    material = {k: v for k, v in body.items() if k != "idempotency_key"}
    blob = json.dumps(material, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_state() -> dict:
    return {"contacts": {}, "rejected": [], "dead_letter": [], "idempotency": {}}


def _load() -> dict:
    if not os.path.exists(STATE_PATH):
        return _empty_state()
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        state = json.load(f)
    state.setdefault("idempotency", {})
    return state


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
            DROP_REPLY_ONCE["armed"] = False
            MALFORMED_ONCE["armed"] = False
            RECORDING_OUTAGE["on"] = False
            self._send_json(200, {"reset": True})
            return
        if self.path == "/admin/drop-reply-once":
            body = self._read_json()
            DROP_REPLY_ONCE["armed"] = bool(body.get("on", True))
            self._send_json(200, {"armed": DROP_REPLY_ONCE["armed"]})
            return
        if self.path == "/admin/malformed-once":
            body = self._read_json()
            MALFORMED_ONCE["armed"] = bool(body.get("on", True))
            self._send_json(200, {"armed": MALFORMED_ONCE["armed"]})
            return
        if self.path == "/admin/recording-outage":
            body = self._read_json()
            RECORDING_OUTAGE["on"] = bool(body.get("on", False))
            self._send_json(200, {"on": RECORDING_OUTAGE["on"]})
            return

        if self.path == "/contacts":
            if self._outage_check():
                return
            if MALFORMED_ONCE["armed"]:
                MALFORMED_ONCE["armed"] = False
                self._send_json(200, {})
                return
            body = self._read_json()
            email = str(body.get("email", "")).strip().lower()
            if not email:
                self._send_json(400, {"error": "email_required"})
                return
            idempotency_key = body.get("idempotency_key")
            fingerprint = _fingerprint(body)
            drop_this_reply = False
            with LOCK:
                state = _load()

                if idempotency_key:
                    prior = state["idempotency"].get(idempotency_key)
                    if prior is not None:
                        if prior["fingerprint"] == fingerprint:
                            # Same logical write retried (request resent, or
                            # the original reply was lost) -- return the
                            # original result, do not write or count again.
                            self._send_json(200, prior["response"])
                            return
                        # Same key, different payload: a genuine conflict,
                        # not a retry -- never silently applied as a write.
                        self._send_json(409, {
                            "error": "idempotency_key_conflict",
                            "detail": "idempotency_key reused with a different payload",
                        })
                        return

                existing = state["contacts"].get(email)
                record = dict(body)
                record.pop("idempotency_key", None)
                record["email"] = email
                record["updated_at"] = _now()
                if existing:
                    record["created_at"] = existing.get("created_at", _now())
                    record["upsert_count"] = existing.get("upsert_count", 1) + 1
                else:
                    record["created_at"] = record["updated_at"]
                    record["upsert_count"] = 1
                state["contacts"][email] = record

                response = {"status": "ok", "contact": record}
                if idempotency_key:
                    state["idempotency"][idempotency_key] = {
                        "fingerprint": fingerprint,
                        "response": response,
                    }

                _atomic_write(state)

                if DROP_REPLY_ONCE["armed"]:
                    DROP_REPLY_ONCE["armed"] = False
                    drop_this_reply = True

            if drop_this_reply:
                # The write above is already committed and (if an
                # idempotency_key was sent) cached -- only the acknowledgement
                # is lost, modelling a timeout-after-commit. A retry under the
                # same key will hit the cache branch above, not write again.
                self.close_connection = True
                return

            self._send_json(200, response)
        elif self.path == "/rejected":
            if RECORDING_OUTAGE["on"]:
                self._send_json(503, {"error": "recording_outage", "detail": "mock rejection store is simulating an outage"})
                return
            body = self._read_json()
            with LOCK:
                state = _load()
                entry = dict(body)
                entry["recorded_at"] = _now()
                state["rejected"].append(entry)
                _atomic_write(state)
            self._send_json(200, {"status": "ok"})
        elif self.path == "/dead-letter":
            if RECORDING_OUTAGE["on"]:
                self._send_json(503, {"error": "recording_outage", "detail": "mock dead-letter store is simulating an outage"})
                return
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
