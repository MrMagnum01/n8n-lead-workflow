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

    leads_in == sum(contact.upsert_count for contact in CRM contacts)
                + len(rejected) + len(dead_letter)

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
from dataclasses import dataclass, field

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOW_PATH = os.path.join(REPO_ROOT, "workflow", "lead-intake.json")
N8N_IMAGE = "docker.io/n8nio/n8n:1.114.3"
CONTAINER_NAME = "n8n-lead-workflow-demo"

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
    subprocess.run(["podman", "rm", "-f", CONTAINER_NAME], capture_output=True)
    r = run([
        "podman", "run", "-d", "--name", CONTAINER_NAME,
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
        print(run(["podman", "logs", CONTAINER_NAME]).stdout[-4000:])
        raise SystemExit("n8n never became healthy; aborting")

    r = run(["podman", "cp", WORKFLOW_PATH, f"{CONTAINER_NAME}:/tmp/lead-intake.json"])
    check(r.returncode == 0, "copied workflow json into container")

    r = run(["podman", "exec", CONTAINER_NAME, "n8n", "import:workflow", "--input=/tmp/lead-intake.json"])
    check(r.returncode == 0, "n8n import:workflow succeeded")
    if r.returncode != 0:
        print(r.stdout, r.stderr)
        raise SystemExit("import:workflow failed; aborting")

    r = run(["podman", "exec", CONTAINER_NAME, "n8n", "list:workflow"])
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

    r = run(["podman", "exec", CONTAINER_NAME, "n8n", "update:workflow", f"--id={wf_id}", "--active=true"])
    check(r.returncode == 0, "n8n update:workflow --active=true succeeded")

    # Restarting registers the webhook route for the now-active workflow.
    # podman's rootless (pasta) networking can race the just-freed port on
    # restart; retry a few times.
    restarted = False
    for _ in range(6):
        r = run(["podman", "restart", CONTAINER_NAME])
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
    subprocess.run(["podman", "rm", "-f", CONTAINER_NAME], capture_output=True)
    for p in env.procs:
        p.terminate()
    for p in env.procs:
        try:
            p.wait(timeout=5)
        except Exception:
            p.kill()
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
        expected_accepted_events = 0  # each accepted send is one upsert event

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
            check(status == 200 and body.get("status") == "accepted", f"valid lead {lead['lead_id']} accepted (status={status}, body={body})")

        # --- 2. duplicate: resubmit L1's email under a new lead_id ---
        dup = {"lead_id": "L1-DUP", "name": "Ada L.", "email": "ada@example.com", "company": "Analytical Engines Ltd"}
        status, body = send_lead(env, dup)
        leads_in += 1
        expected_accepted_events += 1
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
        check(status == 200 and body.get("status") == "accepted", f"post-outage lead accepted (status={status}, body={body})")

        # --- reconciliation against CRM ground truth ---
        state = crm_state(env)
        contacts = state.get("contacts", {})
        rejected = state.get("rejected", [])
        dead_letter = state.get("dead_letter", [])

        actual_upsert_events = sum(c.get("upsert_count", 0) for c in contacts.values())
        check(actual_upsert_events == expected_accepted_events,
              f"CRM upsert events == accepted leads ({actual_upsert_events} == {expected_accepted_events})")
        check(len(rejected) == expected_rejected, f"rejected records == expected ({len(rejected)} == {expected_rejected})")
        check(len(dead_letter) == expected_dead_letter, f"dead-letter records == expected ({len(dead_letter)} == {expected_dead_letter})")

        total_out = actual_upsert_events + len(rejected) + len(dead_letter)
        check(total_out == leads_in, f"reconciliation: leads_in ({leads_in}) == upserts+rejected+dead_lettered ({total_out})")

        # duplicate-email lead must collapse into ONE contact with upsert_count==2
        ada = contacts.get("ada@example.com")
        check(ada is not None, "ada@example.com present in CRM contacts")
        check(ada is not None and ada.get("upsert_count") == 2, f"ada@example.com upsert_count == 2 (duplicate collapsed, not duplicated) (got {ada.get('upsert_count') if ada else None})")
        check(len(contacts) == 4, f"exactly 4 distinct CRM contacts (3 valid + post-outage, dup collapsed) (got {len(contacts)})")

        # rejected reasons spot-check
        rejected_by_id = {r.get("lead_id"): r for r in rejected}
        check("email_missing_or_invalid_type" in rejected_by_id.get("I1", {}).get("reasons", []), "I1 rejected for missing email")
        check("email_format_invalid" in rejected_by_id.get("I4", {}).get("reasons", []), "I4 rejected for bad email format")
        check("name_missing_or_invalid_type" in rejected_by_id.get("I5", {}).get("reasons", []), "I5 rejected: numeric name is not a valid string type")
        check("phone_invalid_type_or_format" in rejected_by_id.get("I6", {}).get("reasons", []), "I6 rejected: numeric phone is not a valid string type")
        check("phone_invalid_type_or_format" in rejected_by_id.get("I7", {}).get("reasons", []), "I7 rejected: malformed phone string")

        # dead-letter reason/stage spot-check
        dl_by_id = {d.get("lead_id"): d for d in dead_letter}
        check(dl_by_id.get("D1", {}).get("stage") == "enrichment", "D1 dead-lettered at enrichment stage")
        check(dl_by_id.get("D2", {}).get("stage") == "crm_upsert", "D2 dead-lettered at crm_upsert stage")
        check(dl_by_id.get("D1", {}).get("attempts") == 3, f"D1 exhausted 3 retry attempts (got {dl_by_id.get('D1', {}).get('attempts')})")

        # alerts: fired for accepted leads only (4), never for rejected/dead-lettered
        fired = alerts(env)
        alert_lead_ids = {a.get("lead_id") for a in fired}
        check(len(fired) == expected_accepted_events, f"alert fired once per accepted lead ({len(fired)} == {expected_accepted_events})")
        check(alert_lead_ids == {"L1", "L2", "L3", "L1-DUP", "L4"}, f"alerts fired for exactly the accepted lead_ids (got {alert_lead_ids})")

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
