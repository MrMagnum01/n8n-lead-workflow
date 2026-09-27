# n8n-lead-workflow

A lead-intake automation built in [n8n](https://n8n.io/): a webhook
receives a lead, validates it, enriches it, upserts it into a CRM, and
sends an alert -- with a proper error branch (retry + backoff, then a
dead-letter record) so a downstream outage never silently drops a lead.

**Role:** Synthetic portfolio demonstration, implemented with AI coding
agents and independently reviewed by a separate AI reviewer. No client
data or client work.

**The enrichment API and CRM are mocks.** Both are small local Python
HTTP servers written for this repo (`mocks/enrichment_server.py`,
`mocks/crm_server.py`), not a real enrichment vendor or a real CRM. There
is no real Slack or email either -- the "alert" step posts to a third
local mock (`mocks/alert_server.py`) that just logs what it received.

**Network scope, precisely:** every endpoint the *workload* calls at run
time -- the webhook, enrichment, CRM, and alert sink -- is `127.0.0.1`
only, and n8n's own diagnostics/telemetry/version-check/template flags
are explicitly disabled (`N8N_DIAGNOSTICS_ENABLED=false`,
`N8N_VERSION_NOTIFICATIONS_ENABLED=false`, `N8N_TEMPLATES_ENABLED=false`).
That is not the same as "nothing here ever touches the network": the
**container image** (`docker.io/n8nio/n8n:1.114.3`) is pulled from
Docker Hub the first time it isn't already present locally -- a one-time
registry pull, not a workload-time call -- and `--network=host` scopes
the *n8n process* to loopback, it is not host-level egress isolation.
No real enrichment/CRM/Slack/email vendor is integrated anywhere.

## Upwork job types this demonstrates

- "n8n workflow automation"
- "lead routing to CRM"
- "API integration automation"

## Licence note

n8n is licensed under the Sustainable Use License, not an OSI-approved
open-source licence. This repository runs it under a one-off, demo-specific
licence exception granted by the CEO
(`vault/20-decisions/2026-09-27-portfolio-demos-9-10-11.md`, decision ref
`bec3f3d`) -- see `LICENSES.md` for the full record. Every other
dependency here is the Python standard library; nothing else needed a
licence check.

## What the workflow does

`workflow/lead-intake.json` is the exported n8n workflow (import it via
n8n's UI, or via the CLI/REST as the test harness does -- see below).
Nodes, in order:

1. **Lead Webhook** -- `POST /webhook/lead-intake`, JSON body.
2. **Validate Lead** (Code node) -- strict validation, not a "coerce and
   continue": every required field must be present *and* the right type
   (`lead_id`, `name`, `email`, `company` are required strings; `email`
   must match an email shape; `phone` and `message` are optional but, if
   present, must be a string of the right shape/length). `phone`'s check
   is a documented character-whitelist shape, not phone-number-syntax
   validation: optional leading `+`, then 7-20 characters drawn from
   digits/spaces/parentheses/hyphens/dots, with **at least one digit**
   required somewhere in the string (a punctuation-only value like
   `-------` is rejected). `email`'s check is likewise a basic shape
   check, not address/mailbox verification. Nothing here silently drops
   or nulls out a bad value -- it becomes a rejection with a named
   reason.
3. **Valid?** -- an IF node. Invalid leads go to **Record Rejected**
   (`POST` to the mock CRM's `/rejected`). If that write is
   acknowledged, a `422` response with `status: "rejected"` follows --
   a recorded row. If the write itself fails (retried 3x, then not
   recovered), the response is a distinct, non-200
   `status: "rejection_not_recorded"` -- classification and confirmed
   storage are never conflated; a failed record write always surfaces
   as a failure, never as a false "rejected" success.
4. **Enrich And Upsert (retry+backoff)** (Code node) -- calls the mock
   enrichment API, then the mock CRM's `/contacts` upsert, each with up
   to 3 attempts and exponential backoff (200ms, 400ms) between retries.
   The CRM call carries a stable idempotency key derived from the lead's
   own `lead_id`; the mock CRM honours it, so a transport retry (request
   sent, acknowledgement lost) replays as the same logical write instead
   of a second one -- `leads_in == upserts + rejected + dead_lettered`
   holds even under an ambiguous retry. Both the enrichment and CRM `200`
   responses are validated for the fields/identity this pipeline actually
   depends on (`industry`/`company_size`/`fit_score`; `status: "ok"` plus
   `contact.email`); an empty or malformed `200` is never accepted as
   success -- it is classified as its own dead-letter stage
   (`enrichment_response_invalid` / `crm_response_invalid`) and is not
   blindly retried, since retrying a response-validation failure risks
   repeating a write that may already have committed. Any persistent
   transport failure is marked `dead_lettered` with the failing stage and
   the last error -- never just swallowed.
5. **Accepted?** -- routes a persistent failure to **Record Dead Letter**
   (`POST` to the mock CRM's `/dead-letter`). As with Record Rejected,
   an acknowledged write gets the normal `502`/`status: "dead_lettered"`
   response; a failed write gets a distinct, truthful
   `status: "dead_letter_not_recorded"` instead of falsely claiming the
   row exists. A success routes to **Send Alert** (`POST` to the mock
   alert sink); if the CRM upsert succeeded but the alert delivery fails
   after retries, the response is a non-200
   `status: "accepted_notification_failed"` (contact still saved,
   notification state explicit) -- Send Alert failure never continues
   silently on the success edge. Only a fully successful send gets the
   plain `200`/`status: "accepted"` response.

### Why an "outage" doesn't also break the dead-letter/rejection log

The mock CRM's outage toggle (used by the test harness to simulate a
downstream failure) affects only its `/contacts` upsert endpoint --
`/rejected` and `/dead-letter` stay reachable. A dead-letter queue that
goes down at exactly the moment the primary system does would turn every
failure into a second, silent failure, which defeats the point of having
one. A *separate* toggle (`/admin/recording-outage`, harness-only) takes
down `/rejected` and `/dead-letter` themselves, to prove the workflow
reports that truthfully (`*_not_recorded`) rather than claiming a row
that was never written -- see "What it does *not* claim" below for the
limit this implies. All three collections still live in one mock process
and one state file for this demo's simplicity, but that's a
synthetic-demo shortcut, not a claim that a dead-letter queue should
share fate with the system it's recording failures about.

### Retry/backoff, precisely

The retry logic is hand-written in the Code node (`this.helpers.httpRequest`
plus a manual retry loop), not n8n's built-in per-node "Retry On Fail"
setting -- so what's claimed here (3 attempts, exponential 200ms/400ms
backoff) is exactly what the code in `workflow/lead-intake.json` does,
not an assumption about an n8n feature's internal behaviour. The CRM
idempotency key is `lead-<lead_id>`; a repeat request under that key with
an unchanged payload returns the original cached result (no second
write), and a repeat under the same key with a *changed* payload is
rejected by the mock CRM as a conflict (`409`) rather than silently
applied -- so a transport retry and a genuine second submission are never
confused with each other.

## Mock services

| Service | File | Purpose |
|---|---|---|
| Enrichment | `mocks/enrichment_server.py` | Given `{email, company}`, returns a deterministic synthetic firmographic record (industry, size band, fit score) derived from a hash of the input. `POST /admin/outage {"on": true\|false}` simulates the vendor being down (`503`). `POST /admin/malformed-once` arms a one-shot `200 {}` malformed-success response, testing that the workflow validates the fields it depends on rather than trusting any `200`. |
| CRM | `mocks/crm_server.py` | Holds `contacts` (upserted by lowercased email -- a repeat email updates the existing record, tracked via `upsert_count`, rather than creating a duplicate), `rejected`, `dead_letter`, and an `idempotency` cache keyed by the request's `idempotency_key`, all in one atomically-written JSON state file. `GET /admin/state` returns all four for reconciliation. `POST /admin/outage` affects only `/contacts` (see above); `POST /admin/recording-outage` affects only `/rejected` and `/dead-letter`, for testing recording-path failure specifically. `POST /admin/drop-reply-once` commits a `/contacts` write normally but drops the reply on that one call, modelling a lost acknowledgement. `POST /admin/malformed-once` returns `200 {}` from `/contacts` without writing, modelling a malformed vendor success. |
| Alert sink | `mocks/alert_server.py` | Appends each alert to a JSON array file, same atomic-write pattern. `GET /alerts` reads it back. `POST /admin/outage {"on": true\|false}` simulates the sink being down (`503`), exercising Send Alert's retry and its distinct `accepted_notification_failed` response. |

## Running the test harness

Requires `python3` and `podman` (rootless), and that `docker.io/n8nio/n8n:1.114.3`
can be pulled once (`podman pull docker.io/n8nio/n8n:1.114.3` -- the
harness assumes the image is already present or pullable; it does not
pull it itself).

```
python3 tests/harness.py
```

This is a single self-contained script (standard library only -- no
`pip install` needed) that:

1. starts the three mock services on random free `127.0.0.1` ports;
2. starts n8n in a podman container, bound to `127.0.0.1` on a random
   free port, with a fresh temp data directory
   (`--network=host` plus `N8N_LISTEN_ADDRESS=127.0.0.1` and an explicit
   `N8N_PORT`, so the process itself -- not just a port-forward -- only
   listens on loopback);
3. imports `workflow/lead-intake.json` via `n8n import:workflow` and
   activates it via `n8n update:workflow --active=true` (the n8n CLI,
   inside the container -- no REST login or API key is used to get the
   workflow running);
4. fires a fixed set of synthetic leads at the webhook: 3 valid leads, a
   duplicate resubmission of one of them under a new `lead_id`, 10 invalid
   leads (one per validation-failure class, including a punctuation-only
   phone with no digit), a simulated enrichment outage, a simulated CRM
   outage, one more valid lead after recovery, a malformed (`200 {}`)
   enrichment response, a malformed (`200 {}`) CRM response, a CRM write
   whose acknowledgement is lost after it commits (idempotent retry), an
   alert-sink outage, and a rejection/dead-letter recording-path outage;
5. reads the mock CRM's `/admin/state` and the alert sink's `/alerts`,
   and asserts the result against known ground truth, including the
   reconciliation identity:

   ```
   leads_in == sum(contact.upsert_count for contact in CRM contacts)
               + len(rejected)
               + len(dead_letter)
   ```

   (upsert *events*, not distinct contacts -- the duplicate lead must
   increment an existing contact's `upsert_count`, not disappear from the
   count.)
6. tears everything down in a `finally` block -- container removed, mock
   processes killed, temp dirs deleted -- whether the run passed or
   failed.

Exit code `0` on success, `1` if any assertion failed (with a printed
list of which ones).

### What it does *not* claim

- It is not load-tested; the synthetic run is a couple dozen leads, one
  at a time, sequential, against mocks on localhost -- not a throughput
  or production-scale guarantee.
- The rejection/dead-letter recording path is only as durable as the
  mock CRM's single process and single state file; the demo tests that a
  *failed* recording write surfaces truthfully (never a false success),
  not that the recording store itself has independent, higher
  availability than the system it's recording failures about.
- The "backoff" is the workflow's own retry loop, run against the mock
  services on localhost; it is not evidence about behaviour against a
  real, rate-limited third-party API.
- `n8n update:workflow --active=true` requires a container restart to
  register the webhook route (n8n's own documented behaviour, not a
  limitation invented here); the harness handles this, including retrying
  the restart itself a few times, since rootless podman's `pasta`
  networking can briefly race the just-freed port on a fast restart.

## Screenshots

`docs/screenshots/` has 4 PNGs: the workflow canvas and an execution log,
each at desktop width and at 390px (mobile). These were captured locally
against a real running instance of this exact workflow (headless Chrome
driving n8n's editor UI over the Chrome DevTools Protocol) -- the
screenshot tooling itself is not part of this repository, only its
output images are.

- `workflow-canvas-desktop.png` / `workflow-canvas-mobile.png`
- `execution-log-desktop.png` / `execution-log-mobile.png` -- a real
  execution of a valid lead, every node green.

## Repository layout

```
workflow/lead-intake.json   the n8n workflow, exported
mocks/crm_server.py         mock CRM (contacts, rejected, dead-letter)
mocks/enrichment_server.py  mock enrichment API
mocks/alert_server.py       mock alert sink
tests/harness.py            end-to-end test harness (see above)
docs/screenshots/           canvas + execution log, desktop + mobile
LICENSES.md                 licence exception record + dependency audit
```
