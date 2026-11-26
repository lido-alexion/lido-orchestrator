# Operator onboarding

Install from the source root with `python3.12 -m pip install .`; this installs the `lido` CLI. Keep `--db` pointed at one trusted local SQLite database for manual and scheduled commands.

Lido is a trusted-local operator tool. It has no user login, remote API, or network listener. SQLite is authoritative for projects, tasks, events, the supervisor registry, route deduplication, and dispatch intents. Store the database in a user-private directory and protect backups and WAL files.

## Inspect commands

```sh
python3.12 -m lido_orchestrator --help
python3.12 -m lido_orchestrator info
python3.12 -m lido_orchestrator supervisor --help
```

All commands default to dry-run. `submit` requires approval by default. `reconcile` and `serve` do not enable the Codex adapter. Real Codex execution requires both `--codex` and `LIDO_ENABLE_CODEX=1`; it is read-only and still requires separate operator review.

## Register a supervisor locally

First register a project root, then explicitly confirm trusted-local operator enrollment:

```sh
python3.12 -m lido_orchestrator --db ./state.sqlite3 register-project demo ./demo-project
python3.12 -m lido_orchestrator --db ./state.sqlite3 supervisor register sup-demo demo worker-demo --trusted-local
python3.12 -m lido_orchestrator --db ./state.sqlite3 supervisor list
python3.12 -m lido_orchestrator --db ./state.sqlite3 supervisor status sup-demo
python3.12 -m lido_orchestrator --db ./state.sqlite3 supervisor unregister sup-demo --actor local-operator
```

Registration prints a random route token once. Capture the one-time output in a user-owned mode-0600 file (for example, run the command under `umask 077` and redirect the JSON output to a private enrollment file), then extract the token into its own mode-0600 file and remove the enrollment file after checking the registration. The registry persists only the token's SHA-256 digest. The project association scopes which authoritative project events can route to that supervisor. `worker_id` is an operator-managed association label; it does not grant worker execution authority. Unknown, revoked, wrong-project, stale, tampered, and repeated routes fail closed.

For a route, sign the authoritative event identity and type with the registration token. The `route` command reads the token from stdin and resolves the event fields from SQLite:

```sh
python3.12 - <<'PY' > route.json
import hashlib, hmac, json, time
supervisor, event_id, event_type = "sup-demo", "EVENT_ID", "worker.escalation"
issued = int(time.time())
token = open("./sup-demo.token", encoding="utf-8").read().strip()
sig = hmac.new(token.encode(), f"{supervisor}\n{event_id}\n{event_type}\n{issued}".encode(), hashlib.sha256).hexdigest()
print(json.dumps({"issued": issued, "signature": sig}))
PY
python3.12 -c 'import json; d=json.load(open("route.json")); print(d["issued"], d["signature"])'
```

Then pass those two values as `--issued` and `--signature`, with the token on stdin:

```sh
python3.12 -m lido_orchestrator --db ./state.sqlite3 supervisor route sup-demo EVENT_ID --issued ISSUED --signature SIGNATURE < ./sup-demo.token
```

The route is an authenticated receipt for human review only. It neither acknowledges the SMTP outbox nor approves tasks, launches workers, changes project authority, or dispatches work.

## Shared manual, hourly, and native trigger contract

Manual `lido reconcile`, an hourly scheduler invoking the same command, and a native event-specific wake must use the same database and existing engine reconciliation/authorization paths. A wake only prompts inspection of an authoritative event; it does not approve work, create a task, bypass the emergency stop, grant write authority, or dispatch. Existing approval, project policy, workflow budgets, lease fencing, and fail-closed ambiguity rules continue to apply.

A native mail adapter must bind the exact configured recipient to exactly one active supervisor, verify a short-lived receipt against the authoritative local event/outbox, deduplicate the event, and call the scoped route operation. Missing state, stale or invalid receipts, recipient mismatch, wrong project, replay, or revoked identity must fail closed to manual review. Mail text is untrusted data and is never authorization. A mailbox or subject filter can select candidate messages only; it cannot authenticate a supervisor. Native server-side subject-filter behavior has not been verified. Same-chat wake and multiple subscriptions remain platform-specific and unverified.

## SMTP and pilot

SMTP is a central outbound notification adapter configured per invocation. It supports a configured recipient list and subject prefix, and sends only supervisor-notifiable events. It does not select a Gmail account, subscribe to inboxes, or create per-supervisor chats. Use TLS and a narrowly scoped account. Config and receipt-key files must be current-user-owned regular files with mode 0600; symlinks, FIFOs, oversized files, and invalid keys fail closed.

The explicit invocation is:

```sh
python3.12 -m lido_orchestrator --db ./pilot.sqlite3 smtp-deliver --config ./fixture-smtp.env --receipt-key-file ./fixture-receipt.key --enable
python3.12 -m lido_orchestrator --db ./pilot.sqlite3 verify-receipt --receipt-key-file ./fixture-receipt.key < ./receipt-token.txt
```

For an isolated pilot, use a new temporary SQLite database, fixture-only config/key, and a fake SMTP transport. Run `python3.12 -m unittest discover -s tests -v`; the CLI pilot test uses a mocked SMTP client. Never use real credentials, mail, service state, production dispatch, or existing supervisor registrations in candidate verification. See [PROTOCOL.md](PROTOCOL.md) and [SECURITY.md](SECURITY.md).
