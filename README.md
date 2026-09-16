# Agent Relay (PostgreSQL)

Agent Relay is a small FastAPI service for registering agents, delivering one
task at a time, and recording results. PostgreSQL persists the queue and
attempts, while workers execute tasks on their own machines. The included
worker deterministically returns `input.upper()`.

## Run it

```bash
docker compose up --build
```

Compose starts two services: `postgres` and the API. Open
<http://127.0.0.1:8001/> for the token-based dashboard. Inside the Compose
network the API reaches the database at the service hostname `postgres`, which
is what `RELAY_DATABASE_URL` in `compose.yaml` points at.

To run the API directly against a database you already have:

```bash
uv sync
RELAY_DATABASE_URL=postgresql+psycopg://relay:relay@127.0.0.1:55433/relay \
  uv run uvicorn main:app --reload
```

SQLite still works if you set `RELAY_DATABASE_URL=sqlite:///./agent-relay.db`,
which is how the test suite runs without a database server. It is a
development convenience, not the supported deployment.

`GET /health` is a liveness check and `GET /ready` verifies database
connectivity and schema (it queries the real tables, so a wiped volume
reports not-ready instead of passing with zero tables).

Register two identities and send a task:

```bash
alice=$(curl -sS -X POST http://127.0.0.1:8001/api/v1/agents \
  -H 'content-type: application/json' -d '{"name":"alice"}')
bob=$(curl -sS -X POST http://127.0.0.1:8001/api/v1/agents \
  -H 'content-type: application/json' -d '{"name":"uppercase"}')
```

The response contains each agent's secret `token` once. Keep it outside source
control. Use `Authorization: Bearer <token>` for all subsequent API calls;
registration is the only unauthenticated endpoint. For a shared installation,
set `RELAY_ENROLLMENT_SECRET` and send it as `X-Enrollment-Secret` when
registering.

## Run the deterministic worker

The worker can register itself and save credentials in a mode-0600 JSON file:

```bash
uv run python main.py worker \
  --base-url http://127.0.0.1:8001 \
  --name uppercase \
  --credentials ./uppercase-credentials.json \
  --worker-id laptop-1
```

For failure/redelivery demonstrations, make local execution intentionally slow
and stop the process after one completion:

```bash
uv run python main.py worker --credentials ./uppercase-credentials.json \
  --slow-seconds 75 --worker-id slow-laptop
```

The worker heartbeats during long work. Killing it leaves the claim leased;
after the 60-second lease expires, another worker can claim the task with a new
token and incremented attempt number. `RELAY_LEASE_SECONDS` and
`RELAY_MAX_ATTEMPTS` are configurable server settings.

An existing credential can also be supplied explicitly (the token is not
written to disk):

```bash
uv run python main.py worker --agent-id agent_123 --token agt_… --worker-id laptop-2
```

## Storage and delivery behavior

`database.py` contains SQLAlchemy models, engine setup, and the
`writer_transaction` helper that every claim, heartbeat, terminal submission,
and recovery pass opens. `storage.py` contains those operations; routes and
request models are kept in `main.py` and `schemas.py`.

Concurrency is where the two backends differ. On PostgreSQL a claim selects its
task with `FOR UPDATE SKIP LOCKED`, so simultaneous workers lock different rows
and none of them waits. SQLite has no such clause, so its transactions open with
`BEGIN IMMEDIATE` and serialize writers instead. Both satisfy the same rule —
one active lease per task — and the HTTP protocol and lifecycle in `SPEC.md` are
identical either way.

Claims are at-least-once and leased for 60 seconds by default. Heartbeats extend
an active lease. A completion or failure must include the recipient's bearer
token and claim token. Repeating the exact terminal request with that claim
token is idempotent; a stale token or different result receives `409`.

## Verify

The test suite covers the main protocol, sender/recipient access boundaries,
hashed claim-token behavior, idempotent terminal retries, concurrent claims,
lease expiry before and after recovery, pagination/error shape, and dashboard
asset serving:

```bash
uv run pytest -q
```

Tests default to a scratch database named `agent-relay-test.db` in the
platform temp directory (`/tmp` on Linux and macOS, `%TEMP%` on Windows) so
they don't reset your dev server's `./agent-relay.db`. The fixture drops and
recreates all tables on whatever `RELAY_DATABASE_URL` points at, so stop
the dev server first or set `RELAY_DATABASE_URL` to a scratch file before
running tests against another database.

This starter intentionally does not include Docker, Kubernetes, CI, external
brokers, an LLM, or a PostgreSQL implementation. Those are deployment and
student-port concerns rather than part of the local relay protocol.
