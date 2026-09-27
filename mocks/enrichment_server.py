#!/usr/bin/env python3
"""Mock lead-enrichment API for the n8n lead-intake demo.

Stdlib-only HTTP server, 127.0.0.1 only. Given {"email", "company"} it
returns a deterministic synthetic firmographic record (industry, size
band, a 0-100 fit score) derived from a hash of the input -- same input
always produces the same output, so test assertions are reproducible.
No real enrichment vendor, network call, or data is involved anywhere.

Supports an outage toggle (POST /admin/outage) so the test harness can
simulate the enrichment vendor being down (a 503), exercised by the
workflow's retry-with-backoff and dead-letter branch.

Also supports POST /admin/malformed-once, a one-shot fault: the next
/enrich call still returns HTTP 200, but with an empty body ({}) instead
of the expected fields -- a vendor bug this pipeline must not treat as a
usable success, distinct from a transport-level outage.
"""
from __future__ import annotations

import hashlib
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

OUTAGE = {"on": False}
MALFORMED_ONCE = {"armed": False}

INDUSTRIES = ["software", "retail", "manufacturing", "healthcare", "logistics", "finance"]
SIZE_BANDS = ["1-10", "11-50", "51-200", "201-1000", "1000+"]


def _deterministic(seed: str, n: int) -> int:
    return int(hashlib.sha256(seed.encode("utf-8")).hexdigest(), 16) % n


class Handler(BaseHTTPRequestHandler):
    server_version = "MockEnrichment/1.0"

    def log_message(self, fmt, *args):
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

    def do_GET(self):
        if self.path == "/admin/outage":
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
        if self.path == "/admin/malformed-once":
            body = self._read_json()
            MALFORMED_ONCE["armed"] = bool(body.get("on", True))
            self._send_json(200, {"armed": MALFORMED_ONCE["armed"]})
            return
        if self.path != "/enrich":
            self._send_json(404, {"error": "not_found"})
            return
        if OUTAGE["on"]:
            self._send_json(503, {"error": "enrichment_outage", "detail": "mock enrichment vendor is simulating an outage"})
            return
        if MALFORMED_ONCE["armed"]:
            MALFORMED_ONCE["armed"] = False
            self._send_json(200, {})
            return

        body = self._read_json()
        email = str(body.get("email", "")).strip().lower()
        company = str(body.get("company", "")).strip().lower()
        if not email or not company:
            self._send_json(400, {"error": "email_and_company_required"})
            return

        seed = f"{email}|{company}"
        industry = INDUSTRIES[_deterministic(seed + "industry", len(INDUSTRIES))]
        size = SIZE_BANDS[_deterministic(seed + "size", len(SIZE_BANDS))]
        score = _deterministic(seed + "score", 101)
        self._send_json(200, {"industry": industry, "company_size": size, "fit_score": score, "source": "mock-enrichment"})


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: enrichment_server.py <port>", file=sys.stderr)
        sys.exit(2)
    port = int(sys.argv[1])
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"enrichment mock listening on 127.0.0.1:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
