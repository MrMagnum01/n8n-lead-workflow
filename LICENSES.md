# Third-party licences

## Licence exception for this demo (n8n)

This project runs [n8n](https://n8n.io/) in a container as the workflow
automation engine it demonstrates. n8n is licensed under the
**Sustainable Use License** (plus the n8n Enterprise License for
enterprise-only features, not used here) -- a source-available licence,
**not** an OSI-approved open-source licence.

The standing rule for this company's tooling is OSI-approved open source
only. The CEO granted a one-off licence exception to that rule **for this
demo specifically**, recorded at `vault/20-decisions/2026-09-27-portfolio-demos-9-10-11.md`
(decision ref `bec3f3d`): *"Demo 9, workflow automation: n8n allowed.
Licence exception to the open-source-only tools rule, granted for this
demo."* No other project should assume this exception carries over.

n8n itself is not modified, redistributed, or shipped in this repository
-- it is pulled at a pinned version from Docker Hub
(`docker.io/n8nio/n8n:1.114.3`) by the test harness and run locally in a
container. Only this repo's own workflow definition (a JSON document
n8n's editor produces and consumes) and this repo's own code are
committed here.

## Direct dependencies

None. There is no `requirements.txt` because nothing in this repository's
own code needs a third-party Python package.

## Standard library only

Everything else in this repo -- the three mock services
(`mocks/crm_server.py`, `mocks/enrichment_server.py`,
`mocks/alert_server.py`) and the test harness (`tests/harness.py`) -- uses
only the Python standard library (`http.server`, `socketserver`,
`urllib`, `json`, `hashlib`, `tempfile`, `threading`, `subprocess`,
`socket`). No third-party Python package is required to run this repo's
own code.

## Container base image

`docker.io/n8nio/n8n:1.114.3` -- pulled at a pinned tag, run unmodified.
Its own licence is n8n's Sustainable Use License, covered by the
exception above.

## Summary

Everything in this repository's own code is either self-written or uses
only the Python standard library. The one runtime dependency that is not
OSI-licensed -- n8n itself -- is covered by an explicit, logged,
demo-specific CEO exception, not by the company's default open-source-only
rule.
