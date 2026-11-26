# Operator onboarding

Mandatory operating guidance: [SUPERVISOR_PROTOCOL.md](SUPERVISOR_PROTOCOL.md). Prompt examples: [QUICK_GUIDE.md](QUICK_GUIDE.md). Prompt wording does not imply a supported CLI feature; inspect installed `lido supervisor --help` and `lido info` before relying on an operation.

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

Registration generation is 1. Authenticate a heartbeat with the current token on stdin; a valid heartbeat renews an active or stale lease only before the inactive threshold. The trusted-local actor is an audit label, not the chat identity. Run this from a protected shell and private token file:

```sh
python3.12 -m lido_orchestrator --db ./state.sqlite3 supervisor heartbeat sup-demo --generation 1 < ./sup-demo.token
python3.12 -m lido_orchestrator --db ./state.sqlite3 supervisor status sup-demo
```

The default lease thresholds are 3 hours stale, 6 hours inactive, and 24 hours orphaned. Configure them for any CLI command with `--supervisor-stale-seconds`, `--supervisor-inactive-seconds`, and `--supervisor-orphan-seconds`; values must be positive, ordered, and at most one year. They are based on the authenticated heartbeat, falling back to registration time. `reconcile` and `serve` run the reaper on their existing local cadence; that cadence does not renew any heartbeat. All authority checks use elapsed lease age even before a reaper updates the stored lifecycle. Missing heartbeat is not proof of deletion. The reaper preserves pending approvals/history, changes explicitly owned running and approved-ready tasks to `needs_review`, and disables recipient routing at inactive/orphaned. Takeover does not transfer old-generation work automatically: a trusted-local operator may explicitly adopt eligible never-dispatched pending tasks into the active generation, which requires fresh approval. Adoption is one-way; an active source owner blocks it, and the new active owner blocks later transfers. A deboarded ID stays revoked; register a replacement ID to recover eligible pending tasks. No attempted or ambiguous task is automatically replayed.

Lifecycle actions are trusted-local management operations. They preserve history and pending approvals; in-flight and approved-ready work becomes `needs_review` with ambiguous dispatch intent. Adoption requires zero attempts, no dispatch intent, same project, active target lease, and a resolved parent; it atomically transfers one task and restores explicit approval. `supervisor dispose TASK_ID --actor OPERATOR --reason REASON` cancels a pending task and its descendants only when the entire tree has zero attempts and no dispatch intents. Any attempted or ambiguous node rejects the entire operation. Suspension blocks event routing and scoped assignments; resume is for a suspended registration. Completion and deboarding block routing. Takeover rotates the route token and increments generation atomically, rejecting old tokens and old-generation tasks/events. Capture the new one-time token securely. Takeover does not restore an unbound recipient; explicitly bind it again if authorized. New assignments can be scoped with `submit PROJECT 'task' --supervisor sup-demo`; this requires a currently active lease. Existing/unscoped tasks are not assumed to belong to a supervisor.

```sh
python3.12 -m lido_orchestrator --db ./state.sqlite3 supervisor lifecycle sup-demo suspend --actor local-operator
python3.12 -m lido_orchestrator --db ./state.sqlite3 supervisor lifecycle sup-demo resume --actor local-operator
python3.12 -m lido_orchestrator --db ./state.sqlite3 supervisor lifecycle sup-demo takeover --actor local-operator
python3.12 -m lido_orchestrator --db ./state.sqlite3 supervisor reap --actor local-operator
```

Maintain an ownership ledger of supervisor/project IDs, generation, observed lease state, owned task IDs, approvals, in-flight disposition, and next action. `worker_id` remains a label; no worker authentication is implemented. Natural-language prompts in [QUICK_GUIDE.md](QUICK_GUIDE.md) are examples, not executable commands. Follow [SUPERVISOR_PROTOCOL.md](SUPERVISOR_PROTOCOL.md) for every supervisor run.

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

A native mail adapter must bind the exact configured recipient to exactly one active supervisor, verify a short-lived receipt against the authoritative local event/outbox, deduplicate the event, and call the scoped route operation. SMTP outbound claims are limited to supervisor-notifiable events owned by the exact active supervisor generation bound to the configured recipient; unscoped work and another supervisor's events are not sent. Missing state, stale or invalid receipts, recipient mismatch, wrong project, replay, or revoked identity must fail closed to manual review. Mail text is untrusted data and is never authorization. A mailbox or subject filter can select candidate messages only; it cannot authenticate a supervisor. Native provider integration is not implemented. Same-chat wake and provider filter behavior must be tested in an isolated target account; do not claim support based on a filter match.

## SMTP and pilot

SMTP is a central outbound notification adapter configured per invocation. It supports a configured recipient list and subject prefix, and sends only supervisor-notifiable events. It does not select a Gmail account, subscribe to inboxes, or create per-supervisor chats. Use TLS and a narrowly scoped account. Config and receipt-key files must be current-user-owned regular files with mode 0600; symlinks, FIFOs, oversized files, and invalid keys fail closed.

The explicit invocation is:

```sh
python3.12 -m lido_orchestrator --db ./pilot.sqlite3 smtp-deliver --config ./fixture-smtp.env --receipt-key-file ./fixture-receipt.key --enable
python3.12 -m lido_orchestrator --db ./pilot.sqlite3 verify-receipt --receipt-key-file ./fixture-receipt.key < ./receipt-token.txt
```

For an isolated pilot, use a new temporary SQLite database, fixture-only config/key, and a fake SMTP transport. Run `python3.12 -m unittest discover -s tests -v`; the CLI pilot test uses a mocked SMTP client. Never use real credentials, mail, service state, production dispatch, or existing supervisor registrations in candidate verification. See [PROTOCOL.md](PROTOCOL.md) and [SECURITY.md](SECURITY.md).
