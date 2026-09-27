#!/usr/bin/env python3
"""End-to-end test harness for the n8n lead-intake demo.

Brings up, entirely on 127.0.0.1:
  - the three mock services (enrichment, CRM, alert sink) as local
    Python subprocesses;
  - n8n itself in a podman container (pinned image, random free port,
    a temp data directory), imported with the workflow under
    ``workflow/lead-intake.json`` via the n8n CLI (``n8n import:workflow`` /
    ``n8n update:workflow --active``) -- no REST login/API key is used;

then fires a fixed set of synthetic leads at the workflow's webhook,
covering: valid leads, a duplicate (same lead resubmitted -> one CRM
upsert_count increment, not a second contact), several invalid-input
classes (missing/wrong-typed fields, bad email/phone format), and a
mock outage on each of the enrichment and CRM services (exercising the
retry+backoff path and the dead-letter branch).

It then reads the mock CRM's state and the alert sink's log and checks
them against known ground truth recorded alongside each synthetic lead,
including the reconciliation identity:

    leads_in == upserts + len(rejected) + len(dead_letter) + len(outcome_unknown)

where upserts = sum(contact.upsert_count) over CRM contacts whose lead is
NOT recorded as outcome_unknown (an unknown outcome may or may not have
committed; it is counted once, as unknown, never also as an upsert).

It also exercises the ambiguous-write, response-identity and
record/alert-idempotency cases from the 2026-09-27 rereview: every CRM
acknowledgement lost after commit (reconciles to upserted), malformed
acknowledgement after a durable write with the reconciliation lookup down
(outcome_unknown), a well-formed 200 for the wrong contact
(crm_response_identity_mismatch), and a lost acknowledgement on each of
Record Rejected / Record Dead Letter / Send Alert (retry must not
duplicate the row/alert).

Everything is torn down at the end (container removed, mock processes
killed, temp dirs removed) whether the run passes or fails.

Usage: python3 tests/harness.py
Exit code 0 on success, 1 on any assertion failure.
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOW_PATH = os.path.join(REPO_ROOT, "workflow", "lead-intake.json")
N8N_IMAGE = "docker.io/n8nio/n8n:1.114.3"
# Per-run container name (not a fixed constant): two concurrent rehearsals
# must not be able to remove each other's container. `podman rm -f` below
# is still safe -- it can only ever match THIS run's own never-before-used
# name -- rather than a collision-prone fixed name shared across runs.
CONTAINER_NAME_PREFIX = "n8n-lead-workflow-demo"

FAILURES: list[str] = []


def check(cond: bool, msg: str) -> None:
    if not cond:
        FAILURES.append(msg)
        print(f"FAIL: {msg}")
    else:
        print(f"ok:   {msg}")


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_http_ok(url: str, timeout_s: float = 20.0) -> bool:
    import urllib.request
    import urllib.error

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def http_json(method: str, url: str, body: dict | None = None, timeout: float = 10.0):
    import urllib.request
    import urllib.error

    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw.decode("utf-8", "replace")}


@dataclass
class Env:
    tmp_root: str
    crm_port: int
    enrich_port: int
    alert_port: int
    n8n_port: int
    crm_state: str
    alert_state: str
    n8n_data_dir: str
    container_name: str
    procs: list = field(default_factory=list)


def start_mocks(env: Env) -> None:
    def spawn(script: str, args: list[str]) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, os.path.join(REPO_ROOT, "mocks", script), *args],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    env.procs.append(spawn("crm_server.py", [str(env.crm_port), env.crm_state]))
    env.procs.append(spawn("enrichment_server.py", [str(env.enrich_port)]))
    env.procs.append(spawn("alert_server.py", [str(env.alert_port), env.alert_state]))

    for port in (env.crm_port, env.enrich_port, env.alert_port):
        ok = wait_http_ok(f"http://127.0.0.1:{port}/health", timeout_s=10)
        check(ok, f"mock service on port {port} came up")


def run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def start_n8n(env: Env) -> None:
    subprocess.run(["podman", "rm", "-f", env.container_name], capture_output=True)
    r = run([
        "podman", "run", "-d", "--name", env.container_name,
        "--network=host",
        "-e", "N8N_LISTEN_ADDRESS=127.0.0.1",
        "-e", f"N8N_PORT={env.n8n_port}",
        "-e", "N8N_BLOCK_ENV_ACCESS_IN_NODE=false",
        "-e", "N8N_DIAGNOSTICS_ENABLED=false",
        "-e", "N8N_VERSION_NOTIFICATIONS_ENABLED=false",
        "-e", "N8N_TEMPLATES_ENABLED=false",
        "-e", "N8N_USER_FOLDER=/home/node/.n8n",
        "-e", f"ENRICH_URL=http://127.0.0.1:{env.enrich_port}",
        "-e", f"CRM_URL=http://127.0.0.1:{env.crm_port}",
        "-e", f"ALERT_URL=http://127.0.0.1:{env.alert_port}",
        "-v", f"{env.n8n_data_dir}:/home/node/.n8n:Z",
        N8N_IMAGE,
    ])
    if r.returncode != 0:
        print(r.stdout, r.stderr)
        raise SystemExit("podman run failed")

    ok = wait_http_ok(f"http://127.0.0.1:{env.n8n_port}/healthz", timeout_s=60)
    check(ok, "n8n container became healthy")
    if not ok:
        print(run(["podman", "logs", env.container_name]).stdout[-4000:])
        raise SystemExit("n8n never became healthy; aborting")

    r = run(["podman", "cp", WORKFLOW_PATH, f"{env.container_name}:/tmp/lead-intake.json"])
    check(r.returncode == 0, "copied workflow json into container")

    r = run(["podman", "exec", env.container_name, "n8n", "import:workflow", "--input=/tmp/lead-intake.json"])
    check(r.returncode == 0, "n8n import:workflow succeeded")
    if r.returncode != 0:
        print(r.stdout, r.stderr)
        raise SystemExit("import:workflow failed; aborting")

    r = run(["podman", "exec", env.container_name, "n8n", "list:workflow"])
    check(r.returncode == 0, "n8n list:workflow succeeded")
    wf_id = None
    for line in r.stdout.strip().splitlines():
        if "|" in line:
            wid, name = line.split("|", 1)
            if name.strip() == "lead-intake-pipeline":
                wf_id = wid.strip()
    check(wf_id is not None, "found lead-intake-pipeline workflow id")
    if wf_id is None:
        raise SystemExit("workflow id not found after import; aborting")

    r = run(["podman", "exec", env.container_name, "n8n", "update:workflow", f"--id={wf_id}", "--active=true"])
    check(r.returncode == 0, "n8n update:workflow --active=true succeeded")

    # Restarting registers the webhook route for the now-active workflow.
    # podman's rootless (pasta) networking can race the just-freed port on
    # restart; retry a few times.
    restarted = False
    for _ in range(6):
        r = run(["podman", "restart", env.container_name])
        if r.returncode == 0:
            restarted = True
            break
        time.sleep(2)
    check(restarted, "container restarted to register active webhook")

    ok = wait_http_ok(f"http://127.0.0.1:{env.n8n_port}/healthz", timeout_s=60)
    check(ok, "n8n became healthy again after restart")

    # Poll the webhook itself (not just healthz) since route registration
    # can briefly lag readiness.
    webhook_url = f"http://127.0.0.1:{env.n8n_port}/webhook/lead-intake"
    deadline = time.time() + 20
    reached = False
    while time.time() < deadline:
        status, _ = http_json("POST", webhook_url, {"lead_id": "__warmup__"})
        if status in (200, 422, 502):
            reached = True
            break
        time.sleep(0.5)
    check(reached, "webhook route responded (not 404) after restart")


def teardown(env: Env) -> None:
    subprocess.run(["podman", "rm", "-f", env.container_name], capture_output=True)
    for p in env.procs:
        p.terminate()
    for p in env.procs:
        try:
            p.wait(timeout=5)
        except Exception:
            p.kill()
    # Files n8n created inside the bind-mounted data dir are owned by
    # rootless podman's mapped (sub-uid) owner, not this process's uid, so
    # a plain rmtree silently leaves them behind. `podman unshare` re-enters
    # that user namespace so removal actually works; fall back to a plain
    # rmtree for anything outside the mount (or if podman unshare itself is
    # unavailable) rather than leaking the whole tree.
    r = subprocess.run(["podman", "unshare", "rm", "-rf", env.n8n_data_dir], capture_output=True)
    if r.returncode != 0:
        print(f"warning: podman unshare cleanup of {env.n8n_data_dir} failed: {r.stderr.decode(errors='replace')[:500]}")
    shutil.rmtree(env.tmp_root, ignore_errors=True)


def send_lead(env: Env, lead: dict) -> tuple[int, dict]:
    url = f"http://127.0.0.1:{env.n8n_port}/webhook/lead-intake"
    return http_json("POST", url, lead, timeout=15)


def toggle_outage(port: int, on: bool) -> None:
    http_json("POST", f"http://127.0.0.1:{port}/admin/outage", {"on": on})


def crm_state(env: Env) -> dict:
    status, body = http_json("GET", f"http://127.0.0.1:{env.crm_port}/admin/state")
    check(status == 200, "read CRM admin/state")
    return body


def alerts(env: Env) -> list:
    status, body = http_json("GET", f"http://127.0.0.1:{env.alert_port}/alerts")
    check(status == 200, "read alert sink /alerts")
    return body.get("alerts", [])


def main() -> int:
    tmp_root = tempfile.mkdtemp(prefix="n8n-lead-demo-")
    env = Env(
        tmp_root=tmp_root,
        crm_port=free_port(),
        enrich_port=free_port(),
        alert_port=free_port(),
        n8n_port=free_port(),
        crm_state=os.path.join(tmp_root, "crm_state.json"),
        alert_state=os.path.join(tmp_root, "alerts.json"),
        n8n_data_dir=os.path.join(tmp_root, "n8n-data"),
        container_name=f"{CONTAINER_NAME_PREFIX}-{uuid.uuid4().hex[:10]}",
    )
    os.makedirs(env.n8n_data_dir, exist_ok=True)
    # The n8n image runs as its own in-container "node" uid, which does not
    # line up with the host uid under rootless podman's default id mapping;
    # world-writable on this private temp dir (mode 0700 parent, single-use,
    # removed on teardown) is what lets the container's process create its
    # subdirectories in the bind mount.
    os.chmod(env.n8n_data_dir, 0o777)

    try:
        start_mocks(env)
        start_n8n(env)

        # start_n8n's readiness probe fired one warmup request at the real
        # webhook (the only reliable way to confirm the route is live); wipe
        # the CRM/dead-letter state it may have produced before counting.
        status, _ = http_json("POST", f"http://127.0.0.1:{env.crm_port}/admin/reset")
        check(status == 200, "CRM state reset after warmup probe")

        leads_in = 0
        expected_rejected = 0
        expected_dead_letter = 0
        expected_unknown = 0
        expected_accepted_events = 0  # each accepted send is one upsert event
        expected_alert_fired = 0  # accepted sends where the alert sink itself succeeded

        # --- 1. valid leads, distinct emails ---
        valid_leads = [
            {"lead_id": "L1", "name": "Ada Lovelace", "email": "ada@example.com", "company": "Analytical Engines Ltd", "phone": "+1 202-555-0101", "message": "Interested in a quote"},
            {"lead_id": "L2", "name": "Grace Hopper", "email": "grace@example.com", "company": "Compile Corp", "phone": None, "message": None},
            {"lead_id": "L3", "name": "Alan Turing", "email": "alan@example.com", "company": "Enigma Systems"},
        ]
        for lead in valid_leads:
            status, body = send_lead(env, lead)
            leads_in += 1
            expected_accepted_events += 1
            expected_alert_fired += 1
            check(status == 200 and body.get("status") == "accepted", f"valid lead {lead['lead_id']} accepted (status={status}, body={body})")

        # --- 2. duplicate: resubmit L1's email under a new lead_id ---
        dup = {"lead_id": "L1-DUP", "name": "Ada L.", "email": "ada@example.com", "company": "Analytical Engines Ltd"}
        status, body = send_lead(env, dup)
        leads_in += 1
        expected_accepted_events += 1
        expected_alert_fired += 1
        check(status == 200 and body.get("status") == "accepted", f"duplicate-email lead accepted (status={status}, body={body})")

        # --- 3. invalid leads: one per validation class ---
        invalid_cases = [
            ("missing_email", {"lead_id": "I1", "name": "No Email", "company": "Acme"}),
            ("missing_name", {"lead_id": "I2", "email": "x@example.com", "company": "Acme"}),
            ("missing_company", {"lead_id": "I3", "name": "No Company", "email": "y@example.com"}),
            ("bad_email_format", {"lead_id": "I4", "name": "Bad Email", "email": "not-an-email", "company": "Acme"}),
            ("wrong_type_name", {"lead_id": "I5", "name": 12345, "email": "z@example.com", "company": "Acme"}),
            ("bad_phone_type", {"lead_id": "I6", "name": "Bad Phone", "email": "w@example.com", "company": "Acme", "phone": 5551234}),
            ("bad_phone_format", {"lead_id": "I7", "name": "Bad Phone Fmt", "email": "v@example.com", "company": "Acme", "phone": "call-me-maybe"}),
            ("missing_lead_id", {"name": "No Lead Id", "email": "u@example.com", "company": "Acme"}),
            ("empty_company", {"lead_id": "I9", "name": "Empty Co", "email": "t@example.com", "company": "   "}),
            # digits-required phone shape (MUST-FIX/NARROW 5): a punctuation-only
            # string must not pass as a phone -- it has the right characters and
            # length but not a single digit.
            ("phone_no_digits", {"lead_id": "I10", "name": "No Digit Phone", "email": "s@example.com", "company": "Acme", "phone": "-------"}),
        ]
        for name, lead in invalid_cases:
            status, body = send_lead(env, lead)
            leads_in += 1
            expected_rejected += 1
            check(status == 422 and body.get("status") == "rejected", f"invalid lead [{name}] rejected (status={status}, body={body})")

        # --- 4. enrichment outage: lead should retry, exhaust, dead-letter ---
        toggle_outage(env.enrich_port, True)
        de_lead = {"lead_id": "D1", "name": "Down Enrich", "email": "d1@example.com", "company": "Outage Co"}
        status, body = send_lead(env, de_lead)
        leads_in += 1
        expected_dead_letter += 1
        check(status == 502 and body.get("status") == "dead_lettered" and body.get("stage") == "enrichment",
              f"enrichment-outage lead dead-lettered with stage=enrichment (status={status}, body={body})")
        toggle_outage(env.enrich_port, False)

        # --- 5. CRM outage: enrichment succeeds, CRM upsert fails -> dead-letter ---
        toggle_outage(env.crm_port, True)
        dc_lead = {"lead_id": "D2", "name": "Down CRM", "email": "d2@example.com", "company": "Outage Co"}
        status, body = send_lead(env, dc_lead)
        leads_in += 1
        expected_dead_letter += 1
        check(status == 502 and body.get("status") == "dead_lettered" and body.get("stage") == "crm_upsert",
              f"CRM-outage lead dead-lettered with stage=crm_upsert (status={status}, body={body})")
        toggle_outage(env.crm_port, False)

        # --- 6. one more valid lead after recovery, to prove the pipeline
        #        is healthy again post-outage ---
        recovered = {"lead_id": "L4", "name": "Post Outage", "email": "post@example.com", "company": "Recovery Inc"}
        status, body = send_lead(env, recovered)
        leads_in += 1
        expected_accepted_events += 1
        expected_alert_fired += 1
        check(status == 200 and body.get("status") == "accepted", f"post-outage lead accepted (status={status}, body={body})")

        # --- 7. malformed enrichment response: HTTP 200 but body {} ---
        # A vendor bug, not a transport error -- must not be accepted as a
        # usable success. Categorised as its own dead-letter stage.
        status, _ = http_json("POST", f"http://127.0.0.1:{env.enrich_port}/admin/malformed-once", {"on": True})
        check(status == 200, "armed enrichment malformed-once")
        me_lead = {"lead_id": "ME1", "name": "Malformed Enrich", "email": "me1@example.com", "company": "Bad Response Co"}
        status, body = send_lead(env, me_lead)
        leads_in += 1
        expected_dead_letter += 1
        check(status == 502 and body.get("status") == "dead_lettered" and body.get("stage") == "enrichment_response_invalid",
              f"malformed-enrichment lead dead-lettered as enrichment_response_invalid, not accepted (status={status}, body={body})")

        # --- 8. malformed CRM response: HTTP 200 but body {} (no write) ---
        status, _ = http_json("POST", f"http://127.0.0.1:{env.crm_port}/admin/malformed-once", {"on": True})
        check(status == 200, "armed CRM malformed-once")
        mc_lead = {"lead_id": "MC1", "name": "Malformed CRM", "email": "mc1@example.com", "company": "Bad Response Co"}
        status, body = send_lead(env, mc_lead)
        leads_in += 1
        expected_dead_letter += 1
        check(status == 502 and body.get("status") == "dead_lettered" and body.get("stage") == "crm_response_invalid",
              f"malformed-CRM-response lead dead-lettered as crm_response_invalid, not accepted (status={status}, body={body})")

        # --- 9. idempotent retry: CRM commits the write but the ack is lost
        #        (armed by /admin/drop-reply-once) -- the workflow's retry
        #        must replay under the SAME idempotency key and land on the
        #        cached result, not a second commit. This is the ambiguous-
        #        retry case (MUST-FIX 3): leads_in == upserts+rejected+dead
        #        -lettered must still hold, and upsert_count must stay 1. ---
        status, _ = http_json("POST", f"http://127.0.0.1:{env.crm_port}/admin/drop-reply-once", {"on": True})
        check(status == 200, "armed CRM drop-reply-once")
        id_lead = {"lead_id": "ID1", "name": "Idempotent Retry", "email": "idem@example.com", "company": "Retry Co"}
        status, body = send_lead(env, id_lead)
        leads_in += 1
        expected_accepted_events += 1
        expected_alert_fired += 1
        check(status == 200 and body.get("status") == "accepted", f"ack-lost-then-retried lead still accepted exactly once (status={status}, body={body})")

        # --- 10. alert sink outage: CRM upsert succeeds, alert delivery
        #        fails after retries -- MUST-FIX 2: this must NOT continue
        #        on the success edge; response must be non-200 with an
        #        explicit notification-failed status, contact still saved. ---
        toggle_outage(env.alert_port, True)
        al_lead = {"lead_id": "AL1", "name": "Alert Failure", "email": "alertfail@example.com", "company": "Notify Co"}
        status, body = send_lead(env, al_lead)
        leads_in += 1
        expected_accepted_events += 1  # CRM write still committed
        check(status != 200 and body.get("status") == "accepted_notification_failed" and body.get("crm_contact", {}).get("email") == "alertfail@example.com",
              f"alert-outage lead: CRM accepted but response is non-200/failed, not silent success (status={status}, body={body})")
        toggle_outage(env.alert_port, False)

        crm = f"http://127.0.0.1:{env.crm_port}"

        # --- 12. ALL CRM acknowledgements lost after the write commits:
        #        retries exhaust, the workflow reconciles by idempotency key,
        #        finds the committed write -> upserted, not dead-lettered. ---
        status, _ = http_json("POST", f"{crm}/admin/drop-reply-once", {"on": True, "count": 3})
        check(status == 200, "armed CRM drop-reply x3 (every ack lost)")
        status, body = send_lead(env, {"lead_id": "AK1", "name": "All Acks Lost", "email": "ak1@example.com", "company": "Retry Co"})
        leads_in += 1
        expected_accepted_events += 1
        expected_alert_fired += 1
        check(status == 200 and body.get("status") == "accepted",
              f"all-acks-lost lead reconciles to upserted/accepted, not dead_lettered (status={status}, body={body})")

        # --- 13. malformed ack AFTER a durable write, reconciliation lookup
        #        down -> outcome_unknown (own record), never dead_lettered. ---
        http_json("POST", f"{crm}/admin/malformed-after-commit-once", {"on": True})
        http_json("POST", f"{crm}/admin/lookup-outage", {"on": True})
        status, body = send_lead(env, {"lead_id": "UK1", "name": "Unknown Outcome", "email": "uk1@example.com", "company": "Ambiguous Co"})
        http_json("POST", f"{crm}/admin/lookup-outage", {"on": False})
        leads_in += 1
        expected_unknown += 1
        check(status == 502 and body.get("status") == "outcome_unknown" and body.get("stage") == "crm_response_invalid",
              f"malformed-ack-after-commit + lookup down -> outcome_unknown (status={status}, body={body})")

        # --- 14. malformed ack after a durable write, lookup up -> the
        #        committed write is found, reconciled to upserted. ---
        http_json("POST", f"{crm}/admin/malformed-after-commit-once", {"on": True})
        status, body = send_lead(env, {"lead_id": "MA1", "name": "Malformed After Commit", "email": "ma1@example.com", "company": "Ambiguous Co"})
        leads_in += 1
        expected_accepted_events += 1
        expected_alert_fired += 1
        check(status == 200 and body.get("status") == "accepted",
              f"malformed-ack-after-commit + lookup up -> reconciled to accepted (status={status}, body={body})")

        # --- 15. response identity: well-formed 200 for a DIFFERENT contact
        #        (exact rereview reproduction) -> dead-lettered as
        #        crm_response_identity_mismatch, no accepted notification. ---
        http_json("POST", f"{crm}/admin/wrong-identity-once", {"on": True})
        status, body = send_lead(env, {"lead_id": "A1", "name": "Synthetic", "email": "a@example.com", "company": "Example"})
        leads_in += 1
        expected_dead_letter += 1
        check(status == 502 and body.get("status") == "dead_lettered" and body.get("stage") == "crm_response_identity_mismatch",
              f"wrong-contact 200 dead-lettered as crm_response_identity_mismatch, not accepted (status={status}, body={body})")

        # --- 16-18. lost ack on each retried side effect: the node's retry
        #        must be deduped by its record/alert key -> exactly one row. ---
        http_json("POST", f"{crm}/admin/drop-record-reply-once", {"path": "/rejected"})
        status, body = send_lead(env, {"lead_id": "RR1", "name": "Rejected Ack Lost", "company": "Acme"})
        leads_in += 1
        expected_rejected += 1
        check(status == 422 and body.get("status") == "rejected",
              f"rejected-record ack lost: retry succeeds as normal rejection (status={status}, body={body})")

        http_json("POST", f"{crm}/admin/drop-record-reply-once", {"path": "/dead-letter"})
        toggle_outage(env.enrich_port, True)
        status, body = send_lead(env, {"lead_id": "RD1", "name": "Dead Letter Ack Lost", "email": "rd1@example.com", "company": "Acme"})
        toggle_outage(env.enrich_port, False)
        leads_in += 1
        expected_dead_letter += 1
        check(status == 502 and body.get("status") == "dead_lettered",
              f"dead-letter-record ack lost: retry succeeds as normal dead_lettered (status={status}, body={body})")

        http_json("POST", f"http://127.0.0.1:{env.alert_port}/admin/drop-reply-once", {"on": True})
        status, body = send_lead(env, {"lead_id": "RA1", "name": "Alert Ack Lost", "email": "ra1@example.com", "company": "Notify Co"})
        leads_in += 1
        expected_accepted_events += 1
        expected_alert_fired += 1
        check(status == 200 and body.get("status") == "accepted",
              f"alert ack lost: retry succeeds as normal accepted (status={status}, body={body})")

        # --- 11. recording-store outage: MUST-FIX 1. If /rejected or
        #        /dead-letter itself cannot be written, the response must
        #        say so truthfully -- never the normal rejected/dead_lettered
        #        success shape implying a row was recorded. Run against a
        #        dedicated toggle so the CRM's main outage semantics (which
        #        deliberately leave /rejected and /dead-letter reachable)
        #        are untouched. These two leads are intentionally excluded
        #        from leads_in/expected_* below: by design nothing is
        #        recorded for them -- that is the behaviour under test. ---
        status, _ = http_json("POST", f"http://127.0.0.1:{env.crm_port}/admin/recording-outage", {"on": True})
        check(status == 200, "armed CRM recording-outage")

        pre_state = crm_state(env)
        rj_lead = {"lead_id": "RJ1", "name": "Rejected Store Down", "company": "Acme"}  # missing email -> rejected
        status, body = send_lead(env, rj_lead)
        check(status == 502 and body.get("status") == "rejection_not_recorded",
              f"rejection-store-down: truthful storage failure, not a normal 'rejected' response (status={status}, body={body})")

        toggle_outage(env.enrich_port, True)  # force dead-letter path too
        dl_lead = {"lead_id": "DL1", "name": "Dead Letter Store Down", "email": "dl1@example.com", "company": "Acme"}
        status, body = send_lead(env, dl_lead)
        check(status != 200 and body.get("status") == "dead_letter_not_recorded",
              f"dead-letter-store-down: truthful storage failure, not a normal 'dead_lettered' response (status={status}, body={body})")
        toggle_outage(env.enrich_port, False)

        status, _ = http_json("POST", f"http://127.0.0.1:{env.crm_port}/admin/recording-outage", {"on": False})
        check(status == 200, "disarmed CRM recording-outage")

        post_state = crm_state(env)
        check(len(post_state.get("rejected", [])) == len(pre_state.get("rejected", [])),
              "rejection-store-down: no rejected row was actually recorded (no false durability claim)")
        check(len(post_state.get("dead_letter", [])) == len(pre_state.get("dead_letter", [])),
              "dead-letter-store-down: no dead-letter row was actually recorded (no false durability claim)")

        # --- reconciliation against CRM ground truth ---
        state = crm_state(env)
        contacts = state.get("contacts", {})
        rejected = state.get("rejected", [])
        dead_letter = state.get("dead_letter", [])
        unknown = state.get("outcome_unknown", [])
        unknown_ids = {u.get("lead_id") for u in unknown}

        actual_upsert_events = sum(c.get("upsert_count", 0) for c in contacts.values() if c.get("lead_id") not in unknown_ids)
        check(actual_upsert_events == expected_accepted_events,
              f"CRM upsert events == accepted leads ({actual_upsert_events} == {expected_accepted_events})")
        check(len(rejected) == expected_rejected, f"rejected records == expected ({len(rejected)} == {expected_rejected})")
        check(len(dead_letter) == expected_dead_letter, f"dead-letter records == expected ({len(dead_letter)} == {expected_dead_letter})")

        check(len(unknown) == expected_unknown, f"outcome_unknown records == expected ({len(unknown)} == {expected_unknown})")

        total_out = actual_upsert_events + len(rejected) + len(dead_letter) + len(unknown)
        check(total_out == leads_in, f"reconciliation: leads_in ({leads_in}) == upserts+rejected+dead_lettered+unknown ({total_out})")

        # ambiguous writes: each lead has exactly ONE terminal outcome
        check(unknown_ids == {"UK1"}, f"outcome_unknown rows are exactly UK1 (got {unknown_ids})")
        check(contacts.get("uk1@example.com") is not None,
              "UK1's write really did commit (the case is genuinely ambiguous, not a plain failure)")
        dl_ids = [d.get("lead_id") for d in dead_letter]
        for lid in ("AK1", "MA1", "UK1"):
            check(lid not in dl_ids, f"{lid} not also dead-lettered (no double terminal outcome)")
        for email in ("ak1@example.com", "ma1@example.com"):
            c = contacts.get(email)
            check(c is not None and c.get("upsert_count") == 1, f"{email} committed exactly once (upsert_count == 1)")
        check("a@example.com" not in contacts and "other@example.com" not in contacts,
              "identity-mismatch case wrote no contact")

        # record/alert idempotency: exactly one row per logical event
        check(sum(1 for r in rejected if r.get("lead_id") == "RR1") == 1, "RR1: exactly one rejected row after ack-lost retry")
        check(dl_ids.count("RD1") == 1, "RD1: exactly one dead-letter row after ack-lost retry")

        # duplicate-email lead must collapse into ONE contact with upsert_count==2
        ada = contacts.get("ada@example.com")
        check(ada is not None, "ada@example.com present in CRM contacts")
        check(ada is not None and ada.get("upsert_count") == 2, f"ada@example.com upsert_count == 2 (duplicate collapsed, not duplicated) (got {ada.get('upsert_count') if ada else None})")
        check(len(contacts) == 10, f"exactly 10 distinct CRM contacts (3 valid + post-outage + idempotent-retry + alert-failure + AK1 + UK1 + MA1 + RA1, dup collapsed) (got {len(contacts)})")

        # idempotent-retry lead: the lost-ack retry must NOT have double-committed
        idem = contacts.get("idem@example.com")
        check(idem is not None and idem.get("upsert_count") == 1,
              f"idem@example.com upsert_count == 1 (retry under the same idempotency key did not write twice) (got {idem.get('upsert_count') if idem else None})")

        # rejected reasons spot-check
        rejected_by_id = {r.get("lead_id"): r for r in rejected}
        check("email_missing_or_invalid_type" in rejected_by_id.get("I1", {}).get("reasons", []), "I1 rejected for missing email")
        check("email_format_invalid" in rejected_by_id.get("I4", {}).get("reasons", []), "I4 rejected for bad email format")
        check("name_missing_or_invalid_type" in rejected_by_id.get("I5", {}).get("reasons", []), "I5 rejected: numeric name is not a valid string type")
        check("phone_invalid_type_or_format" in rejected_by_id.get("I6", {}).get("reasons", []), "I6 rejected: numeric phone is not a valid string type")
        check("phone_invalid_type_or_format" in rejected_by_id.get("I7", {}).get("reasons", []), "I7 rejected: malformed phone string")
        check("phone_invalid_type_or_format" in rejected_by_id.get("I10", {}).get("reasons", []), "I10 rejected: punctuation-only phone has no digit")

        # dead-letter reason/stage spot-check
        dl_by_id = {d.get("lead_id"): d for d in dead_letter}
        check(dl_by_id.get("D1", {}).get("stage") == "enrichment", "D1 dead-lettered at enrichment stage")
        check(dl_by_id.get("D2", {}).get("stage") == "crm_upsert", "D2 dead-lettered at crm_upsert stage")
        check(dl_by_id.get("D1", {}).get("attempts") == 3, f"D1 exhausted 3 retry attempts (got {dl_by_id.get('D1', {}).get('attempts')})")
        check(dl_by_id.get("ME1", {}).get("stage") == "enrichment_response_invalid", "ME1 dead-lettered at enrichment_response_invalid stage")
        check(dl_by_id.get("MC1", {}).get("stage") == "crm_response_invalid", "MC1 dead-lettered at crm_response_invalid stage (reconciliation confirmed nothing written)")
        check(dl_by_id.get("A1", {}).get("stage") == "crm_response_identity_mismatch", "A1 dead-letter row carries stage crm_response_identity_mismatch")

        # alerts: fired only where CRM accepted AND the alert sink itself
        # succeeded -- never for rejected/dead-lettered, and never claimed
        # for the alert-outage lead whose notification genuinely failed
        fired = alerts(env)
        alert_lead_ids = {a.get("lead_id") for a in fired}
        check(len(fired) == expected_alert_fired, f"alert fired exactly where notification succeeded ({len(fired)} == {expected_alert_fired})")
        check(alert_lead_ids == {"L1", "L2", "L3", "L1-DUP", "L4", "ID1", "AK1", "MA1", "RA1"}, f"alerts fired for exactly the notification-succeeded lead_ids (got {alert_lead_ids})")
        check("AL1" not in alert_lead_ids, "AL1 (alert-outage lead) never recorded a successful alert")
        check("A1" not in alert_lead_ids and "UK1" not in alert_lead_ids, "no accepted notification for identity-mismatch (A1) or unknown (UK1)")
        check(sum(1 for a in fired if a.get("lead_id") == "RA1") == 1, "RA1: exactly one alert after ack-lost retry")

    finally:
        teardown(env)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S):")
        for f in FAILURES:
            print(f" - {f}")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
