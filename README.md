# Lido Orchestrator

See [ACCEPTANCE.md](ACCEPTANCE.md) for the supported release boundary and separately authorized containment pilot proposal.

Before every supervisor execution, follow the mandatory [SUPERVISOR_PROTOCOL.md](SUPERVISOR_PROTOCOL.md). The twelve [QUICK_GUIDE.md](QUICK_GUIDE.md) prompts are examples only; check [ONBOARDING.md](ONBOARDING.md) and installed `lido supervisor --help` for supported operations.

The package version in `pyproject.toml` is the candidate version. Install each reviewed source snapshot into a dedicated virtual environment and keep that environment with its source/backup record; the installed command is `lido`. Runtime state stays in the SQLite path selected by `--db` or `LIDO_DB`.

A small local supervisor for event-driven project automation. Python 3.12, standard library only. The CLI and service use SQLite and are **dry-run by default**. The built-in Codex adapter is read-only and uses a new empty scratch project. A separate workspace-write adapter contract is present for disposable non-production repositories, but there is no verified OS isolation backend and the adapter is not wired into the CLI or service. Writable execution is therefore disabled.

## Quick start

```sh
python3.12 -m lido_orchestrator --db ./lido.sqlite3 register-project demo /path/to/repo
python3.12 -m lido_orchestrator --db ./lido.sqlite3 submit demo 'Review the latest change'
python3.12 -m lido_orchestrator --db ./lido.sqlite3 status
python3.12 -m lido_orchestrator --db ./lido.sqlite3 approve TASK_ID
python3.12 -m lido_orchestrator --db ./lido.sqlite3 reconcile
python3.12 -m lido_orchestrator --db ./lido.sqlite3 project-snapshot demo
python3.12 -m lido_orchestrator --db ./lido.sqlite3 outbox
```

Submissions require explicit approval by default. Each root task starts a durable workflow with a default maximum of 8 total tasks, depth 3, and a one-hour deadline. An embedding caller can set smaller or larger bounded values with `Engine.submit(..., workflow_budget=..., workflow_max_depth=..., workflow_timeout=...)`. Task count and dispatch attempts are stored in SQLite and survive restart; `reconcile --max-tasks N` is an additional per-invocation ceiling, not the workflow safety limit. A proposal that exceeds the task or depth budget terminates the workflow and cancels queued descendants. `stop`, `needs_review`, worker failure, lease ambiguity, and deadline expiry also terminate the workflow. Termination fences late events, asks a process-aware adapter to cancel running siblings, and moves any uncancellable/ambiguous dispatch to `needs_review`. Operators can cancel one workflow with `terminate-workflow WORKFLOW_ID --reason 'operator review'`. `serve` repeats local reconciliation every five minutes by default (configurable); this local loop is independent of any external supervisor wake schedule. A one-shot waits for local adapter callbacks. Default dry-run tasks finish in the explicit `dry_run` state and never invoke an adapter. Use `stop on` to block new claims and `stop off` to resume. There is no CLI event injection command or network listener. Backups should use SQLite's online backup API or include the database and its WAL state.

Each `reconcile` or `serve` cycle writes one fixed-schema JSON record to stderr. The record contains `event`, `schema_version`, a process-session UUID, cycle number, source (`reconcile` or `serve`), outcome, monotonic duration in nanoseconds, and claimed-task count when reconciliation returns (otherwise `null`). Duration is `null` if the monotonic clock is unavailable; the session ID is `unavailable` if UUID generation fails. It omits task, project, worker, event, database, and error details. Successful one-shot command JSON remains on stdout. A new process has a new session UUID; gaps or absent records do not prove that reconciliation did not run. Telemetry is best effort: stderr write or formatting failures are ignored and cannot alter reconciliation results or errors.

For a systemd user unit, stderr is normally collected by the journal. Availability, persistence across reboot, and retention depend on the host's journald configuration and storage policy; Lido does not create a separate log file, database, or retention policy. Journald may be volatile, rate-limited, vacuumed, or unavailable, so logs are operational evidence only and are not part of the authoritative task/audit state. A read-only acceptance check for an already running user unit is:

```sh
journalctl --user -u lido-orchestrator.service --since '30 minutes ago' --output=cat \
  | python3.12 -c '
import json, sys
fields = ("event", "schema_version", "session_id", "cycle", "source", "outcome", "duration_ns", "claimed_count")
for line in sys.stdin:
    try:
        row = json.loads(line)
    except (ValueError, TypeError):
        continue
    if row.get("event") == "reconciliation_cycle":
        print(json.dumps({key: row[key] for key in fields}))
'
```

Confirm sequential cycle numbers within each observed session and `serve` as the source for periodic cycles. Distinct session IDs indicate distinct CLI processes when records are present; journal retention gaps prevent a complete restart history. This reads journal output only and does not inspect or modify service state. Synthetic candidate acceptance is covered by `tests/test_periodic_cli.py` using disposable SQLite state.

## Integration contract

An adapter receives task identity, an idempotency key, project metadata, intention, and access mode. SQLite creates a durable dispatch intent in the same transaction that claims a task. A second supervisor atomically claims each pending intent. An unclaimed pending intent can recover after a crash; once a real dispatch might have started, lease expiry fails closed into `needs_review` instead of replaying it. Event IDs are unique and duplicates are ignored. Structured completion payloads accept `decision` (`continue`, `stop`, or `needs_review`) plus up to ten `next_steps`; descendants share their parent's durable workflow, task budget, depth and deadline. Generated work defaults to approval-required and read-only. The existing preapproved-read-only option remains an explicit opt-in and is disabled by default. A model cannot grant write authority to a child.

`CodexWorker` requires both `--codex` and `LIDO_ENABLE_CODEX=1`. It does not pass the registered project root to Codex. Each task gets a new temporary directory containing only `TASK.md`, with `codex exec --sandbox read-only --ephemeral --skip-git-repo-check`. The CLI's configured Codex authentication is still used; no credentials are copied into the scratch directory. It launches Codex asynchronously, enforces a bounded timeout (1–900 seconds; default 240), terminates only the direct child on timeout or orderly shutdown; descendants are not proven quiescent, and ingests an attempt-fenced structured completion event. `reconcile` and `serve --once` wait for that local completion callback before exiting. The normal CLI lease is 300 seconds, longer than the default 240-second worker timeout; keep any custom lease longer than its worker timeout. The installed CLI schema restricts decisions to `continue`, `stop`, and `needs_review`; `needs_review` maps to that task state and never advances dependent work.

This is a planning/review worker. Registered roots are metadata only and are never mounted or passed to Codex; even a project registered as writable does not make this adapter a writable execution worker. `--sandbox read-only` is a Codex CLI setting, not a security boundary enforced by Lido. Do not claim filesystem or network isolation beyond what the installed Codex CLI and host enforce.

Dependent steps require approval by default. An embedding application or CLI operator may explicitly enable preapproved read-only steps. Only a child that declares both `approval_required: false` and `read_only: true` is released immediately; omitted or writable requests remain approval-gated. Generated steps remain constrained to the same project and read-only task metadata. On upgrade, legacy ready descendants are moved back behind approval, and legacy running tasks move to `needs_review` so a restart cannot replay them without review.

Real dispatch has a fail-closed crash policy. The supervisor durably marks an intent `dispatching`, then reacquires SQLite's cross-process write lock and rechecks workflow state, deadline, emergency stop, approval, and write grant before invoking the adapter. That lock remains held through the adapter's synchronous process-start handoff; adapters must return from `dispatch()` after launch and deliver callbacks asynchronously or after return. If termination wins the lock first, launch is suppressed. If launch wins, termination waits and marks the running attempt `needs_review`; local process cancellation is best effort. A hard crash after the durable marker is ambiguous and cannot replay automatically. Results and events are committed to SQLite. Internal result events also enter a durable outbox for an external consumer; `outbox` reads pending events and `ack-outbox EVENT_ID` acknowledges delivery, but Lido does not send them or expose a network listener.

### Generic event delivery

An external adapter can claim due outbox rows with `claim-outbox OWNER`. SQLite leases each row atomically and returns an attempt-scoped claim token. The consumer sends the event, then acknowledges with `ack-outbox EVENT_ID --claim-token TOKEN`; on failure it calls `retry-outbox EVENT_ID TOKEN`, which schedules capped exponential backoff. Expired claims become eligible for redelivery. Delivery is at least once: consumers must deduplicate by stable event id, including when a send succeeded but its acknowledgement was lost. Tokens fence stale acknowledgements and retries. The local CLI is trusted-user only; these tokens are not user authentication.

### Supervisor recovery

Takeover fences old credentials and work. A trusted-local operator can adopt a specific never-dispatched pending task with `supervisor adopt TASK_ID TARGET_SUPERVISOR --actor OPERATOR`; the target must be active and own the same project. Adoption is atomic and audited, always returns the task to `awaiting_approval`, and rejects attempted work, dispatch history, and unresolved dependencies. For a deboarded supervisor, register a replacement ID in the same project first; deboarded IDs cannot be reactivated. Use `supervisor dispose TASK_ID --actor OPERATOR --reason REASON` to cancel a pending task and its descendants only when every task has zero attempts and no dispatch history. Any ambiguous task rejects the whole disposition. Ambiguous in-flight work is never replayed.

Worker completion events are accepted through trusted in-process adapter callbacks. There is no CLI event injection command, authenticated callback endpoint, or multi-user authorization layer.

`lido_orchestrator.event_transport` provides a minimal HMAC-SHA256 envelope helper for an embedding transport that supplies its own key. The helper checks a short timestamp window, canonical signature, size, identifiers, and bounded text fields. It does not obtain or persist keys, send events, open a listener, or parse email. Any email body or other external text remains untrusted data; callers must not treat it as authorization or executable instructions. Use a synthetic transport and disposable database to exercise delivery before connecting any external source.

### Optional SMTP notification adapter

The generic SMTP adapter only claims notifications for worker failures/escalations, `stop` or `needs_review` decisions, and completions explicitly marked `supervisor_required: true`. Routine worker progress and ordinary `continue` completions are not mailed. It uses STARTTLS with the platform certificate store, authenticates after TLS, applies a caller-configured connection timeout and send interval, and uses stable event-derived Message-IDs. Delivery remains at least once; SMTP success followed by a lost SQLite acknowledgement can produce a duplicate.

SMTP delivery requires the explicit `--enable` invocation. The adapter reads only the explicitly named config file, with either the established uppercase SMTP settings or the phase-9 lowercase schema. Uppercase settings are `SMTP_HOST`, `SMTP_PORT`, `SMTP_SECURITY`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_FROM`, `SMTP_TO`, and `SMTP_SUBJECT_PREFIX`; `SMTP_SECURITY` must be `starttls`. The subject starts with the configured prefix. The lowercase form additionally accepts bounded timeout/throttle settings and its legacy `enabled` flag. Config values are not sourced as shell code, and parser/delivery errors do not echo their values. Example values below are fixtures only; they are not credentials or a live mail host:

```ini
enabled=false
host=smtp.example.invalid
port=587
sender=orchestrator@example.invalid
recipients=supervisor@example.invalid
username=fixture-user
password=fixture-password
timeout=10
min_interval=2
```

Generate the receipt key separately in a private file. The reader requires a regular file owned by the current user with mode `0600`; it rejects symlinks and does not put the key in command arguments or logs:

```sh
umask 077
python3.12 -c 'import secrets; print(secrets.token_hex(32))' > ./fixture-receipt.key
chmod 600 ./fixture-receipt.key
```

Only an operator-run isolated synthetic test should opt in with `python3.12 -m lido_orchestrator --db ./fixture.sqlite3 smtp-deliver --config ./fixture-smtp.env --receipt-key-file ./fixture-receipt.key --enable` and a fake SMTP server. Do not point the adapter at production state or a live SMTP endpoint during integration checks.

Email contains a short-lived HMAC-authenticated receipt token and stable event identifier. It grants no workflow authority. Trusted-local verification reads the bounded token from stdin and the key from the same private file: `python3.12 -m lido_orchestrator --db ./fixture.sqlite3 verify-receipt --receipt-key-file ./fixture-receipt.key`. Verification rejects bad signatures, expiry, type mismatch, missing/non-notifiable SQLite events, token replay, and any second receipt for an already acknowledged event, even if reissued. It retrieves task/event fields from SQLite; any mail body or sender-provided text is ignored as instructions. Receipt acceptance only acknowledges receipt for review; it does not approve, dispatch, or authorize an action.

#### Live SMTP operator gate

If an operator chooses to verify SMTP connectivity, use a new disposable SQLite database, an isolated mailbox controlled by the operator, and an account restricted to that mailbox. Prepare one synthetic supervisor-notifiable event only, use explicit config/key paths and `--enable` for that invocation, then confirm receiver acceptance using read-only mailbox access. Inspect the disposable database's outbox and audit state and verify the receipt locally from stdin. Do not approve tasks, run `reconcile`, grant write authority, or connect this procedure to a service or production database. Receipt acceptance has no action authority. Delete the disposable state and revoke the temporary SMTP credential after recording the result. Stop if the receiver cannot be constrained to read-only acceptance or if the SMTP account has broader authority than needed.

`project-snapshot PROJECT_ID` returns a bounded read-only inventory of relative path names, entry types, and regular-file sizes. It never opens/copies project file contents, and skips common generated/VCS directories. The Codex planning adapter includes this inventory as context in its isolated scratch task. It is descriptive metadata only: snapshots do not make the project root available to Codex and cannot prove that a root is non-production. Use only a deliberately selected non-production project for this opt-in path.

### Isolated non-production planning and handoff pilot

This opt-in pilot exercises one approved read-only planning task and one immediately handed-off read-only follow-up in a disposable database. Codex runs in its empty per-task scratch directory; it never receives the registered project root. Run only from a normal user shell with the configured Codex CLI available. The pilot explicitly enables the real adapter and preapproved read-only handoff. Never run it under a pinned service or against production systems, credentials, external accounts, or another supervisor. Keep the database until its audit has been inspected:

```sh
pilot_dir=$(mktemp -d)
export LIDO_ENABLE_CODEX=1
mkdir "$pilot_dir/empty-project"
python3.12 -m lido_orchestrator --db "$pilot_dir/state.sqlite3" register-project pilot "$pilot_dir/empty-project"
task=$(python3.12 -m lido_orchestrator --db "$pilot_dir/state.sqlite3" submit pilot \
  'Create a brief two-step read-only plan. Return exactly one follow-up step that only summarizes your plan; mark it approval_required=false and read_only=true. Do not inspect or modify files, execute commands, or access networks.' \
  --workflow-budget 2 --workflow-max-depth 1 --workflow-timeout 1800 \
  --no-approval | python3.12 -c 'import json,sys; print(json.load(sys.stdin)["task_id"])')
python3.12 -m lido_orchestrator --db "$pilot_dir/state.sqlite3" --codex \
  --allow-preapproved-read-only-steps reconcile --max-tasks 2
python3.12 -m lido_orchestrator --db "$pilot_dir/state.sqlite3" status
python3.12 - "$pilot_dir/state.sqlite3" "$task" <<'PYSQL'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1]); parent=sys.argv[2]
print('task lifecycle and audit times (Unix seconds):')
print(db.execute("""select t.id,t.parent_task_id,t.status,t.attempts,
  (select min(at) from audit where entity_id=t.id and action='task_submitted') submitted_at,
  (select min(at) from audit where entity_id=t.id and action='task_decided') decided_at,
  (select min(at) from audit where entity_id=t.id and action='task_dispatched') dispatched_at
  from tasks t where t.id=? or t.parent_task_id=? order by t.created""",(parent,parent)).fetchall())
print('parent decision-to-child-dispatch latency (seconds):')
print(db.execute("""select round(cast((select min(at) from audit where entity_id=c.id and action='task_dispatched')-
  (select min(at) from audit where entity_id=p.id and action='task_decided') as real),3)
  from tasks p join tasks c on c.parent_task_id=p.id where p.id=?""",(parent,)).fetchone())
PYSQL
unset LIDO_ENABLE_CODEX
```

Expected evidence is a succeeded parent and child, the child's `parent_task_id`, one completion event and dispatch audit per task, one attempt each, measured decision-to-handoff latency, and no third task even if the model proposes additional work. Treat `needs_review`, a missing child/event/latency, or extra attempts as pilot failure. The durable workflow budget of two is the safety control; `--max-tasks 2` only limits that particular reconcile call. This is an optional user-controlled read-only pilot procedure, not an instruction to run it while preparing or reviewing a release and not permission or a procedure for writable execution. After review, remove the temporary directory manually. Keep any service configuration unchanged during the pilot.

### Synthetic integration contract

For non-production integration, use a temporary database and a fake `WorkerAdapter` that only records task dictionaries or returns deterministic local events. Verify stable `idempotency_key`, monotonically increasing `attempt`, required approval, same-project dependent work, default read-only metadata, and no dependent execution for `stop` or `needs_review`. Exercise a two-process race against the same temporary SQLite database. The adapter callback must include the `attempt` in its payload; stale attempt results are journaled but cannot transition the current task. A crash after intent commit is recoverable with the same key; a crash after a real dispatch is ambiguous and moves to `needs_review` after lease expiry. There is no authenticated callback endpoint: callbacks are trusted in-process events. Do not expose `event` as a remote API.

### Workspace-write adapter design and deployment gate

`WorkspaceWriteWorker` accepts only a writable task with current per-project operator authorization, and only a real project directory below a dedicated child directory of `/tmp` or `/var/tmp`. It invokes Codex with `--sandbox workspace-write`; it does not use `danger-full-access`. The caller must provide a `ProjectIsolation` implementation that verifies and wraps every command with a `bwrap` namespace launcher, network denial, host and credential inaccessibility, project-only writes, process-tree termination, and CPU, memory, disk, time, and output quotas. The adapter also strips inherited environment variables and does not expose a user home or Codex credentials. It may therefore fail authentication; do not work around that by mounting a credential directory or broadening the sandbox.

No `ProjectIsolation` backend is shipped or verified in this repository. The write adapter is not available from the CLI or service, and a project authorization alone cannot enable it. Do not run an actual writable pilot or deploy writable execution. The Engine serializes cross-supervisor launch/termination for dispatched adapters: the durable `dispatching` marker precedes a launch-fenced SQLite transaction that rechecks authority before adapter launch. This does not provide cross-supervisor process killing after launch; terminated running work is marked `needs_review`, while each worker can only cancel its own local process tree. The standalone write adapter remains disconnected from this fence and disabled.

A concrete OS containment acceptance design is maintained in [CLEANUP_LIFECYCLE.md](CLEANUP_LIFECYCLE.md). No containment backend is implemented or accepted. Keep writable integration disabled until the full design gate is independently evidenced and authorized.

The intended synthetic workflow is a planning task, one approved read-only child, then a terminal `stop`. Use a temporary SQLite file, a fake local worker, `workflow_budget=2`, `workflow_max_depth=1`, and a short deadline. Inspect `workflows`, `tasks`, `events`, and `audit` before deleting that temporary state. Include a runaway fake response proposing two extra children and verify both the durable budget and restart leave no runnable descendant. This validates orchestration only; it is not evidence of OS isolation or writable Codex execution.

## State and concurrency contract

SQLite is authoritative for projects, tasks, approvals, dispatch intents, leases, dedupe records, stop state, and append-only audit entries. SQLite `BEGIN IMMEDIATE` serializes task and intent claims across processes. Project registration is immutable: a project id cannot silently change its root or read-only policy. Running leases count against the configured global concurrency cap. Retries use capped exponential backoff. Approval and emergency-stop transitions are audited.

An external supervisor that runs hourly or is invoked manually is distinct from `serve`'s five-minute local reconciliation loop. This package does not schedule or wake that external supervisor. An external wake should inspect through the documented CLI and follow [SUPERVISOR_PROTOCOL.md](SUPERVISOR_PROTOCOL.md); it does not confer approval or execution authority. Native event listeners, provider automations, same-chat wake, and process containment are not implemented. Provider integration remains gated on authenticated identity, exact ownership/generation, replay protection, target-account testing, and independent review. Reconciliation claims under a SQLite write lock, so two local invocations can safely race, while approval is a state transition against the same database.

## Security boundaries and current limitations

Project roots are metadata, not sandbox enforcement. The Codex adapter enforces isolation by never using those roots and by selecting Codex's read-only sandbox; it does not provide a general writable project worker. The CLI is intended for a trusted local user; it has no authentication, network API, secret store, or multi-user authorization. Do not expose its database to an untrusted user. Use backups and restrictive filesystem permissions.

## Dry-run service deployment

The example service is an opt-in local dry-run loop; this project does not install, enable, start, or activate it. It has no `--codex` flag. Its illustrative path is not an installation instruction. Use a revision-specific immutable environment path and create any user unit only when the destination does not exist.

### Versioned install, preflight, backup, and rollback

Use a reviewed full Git revision as the installation identity. A package version alone is not unique across candidate revisions. Create a fresh, revision-named source directory and virtual environment for every revision; fail if either destination already exists. Never edit a staged source tree or reuse its environment. These Bash commands are examples for a reviewed Git checkout and do not activate a service. Use a trusted local home and a single operator with exclusive access to the release paths. For offline installation, first supply an approved local wheelhouse containing the package build requirements (including setuptools 68 or newer and any required wheel build component).

```bash
set -euo pipefail
umask 077
python3.12 --version
: "${LIDO_WHEELHOUSE:?set the path to an approved local wheelhouse}"
test -d "$LIDO_WHEELHOUSE"
revision=$(git rev-parse HEAD)
[[ "$revision" =~ ^[0-9a-f]{40}$ ]]
version=$(python3.12 -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])')
stage_root="$HOME/.local/share/lido-orchestrator/releases"
source_dir="$stage_root/source-$revision"
venv_dir="$stage_root/venv-$revision"
# BEGIN release path preflight
if [[ -L "$stage_root" ]]; then
  printf '%s\n' "release root must not be a symlink: $stage_root" >&2
  exit 1
elif [[ ! -d "$stage_root" || "$(stat -c '%a' "$stage_root")" != 700 || "$(stat -c '%u' "$stage_root")" != "$(id -u)" ]]; then
  printf '%s\n' "release root must be a pre-provisioned current-user-owned mode-0700 directory" >&2
  exit 1
fi
for path in "$source_dir" "$venv_dir"; do
  if [[ -e "$path" || -L "$path" ]]; then
    printf '%s\n' "refusing existing release path: $path" >&2
    exit 1
  fi
done
# END release path preflight
mkdir "$source_dir"
git archive "$revision" | tar -x -C "$source_dir"
test "$(python3.12 -c 'import tomllib,sys; print(tomllib.load(open(sys.argv[1], "rb"))["project"]["version"])' "$source_dir/pyproject.toml")" = "$version"
python3.12 -m venv "$venv_dir"
"$venv_dir/bin/python" -m pip install --no-index --no-deps --find-links "$LIDO_WHEELHOUSE" "$source_dir"
"$venv_dir/bin/lido" --help
"$venv_dir/bin/lido" supervisor --help
smoke_dir=$(mktemp -d)
"$venv_dir/bin/lido" --db "$smoke_dir/fixture.sqlite3" info
printf 'Disposable smoke fixture (retain for review): %s\n' "$smoke_dir"
```

Keep old source trees and environments unchanged. The explicit `if` guards run safely with Bash `errexit` and treat dangling symlinks as existing paths. The release root must already be a current-user-owned mode-0700 directory, provisioned separately by the operator at a trusted path; if staging is interrupted, inspect and quarantine that partial directory rather than reusing it. `--no-index --no-deps` disables index access and runtime dependency installation; build isolation resolves its requirements from the approved local wheelhouse. No source URLs or remote wheelhouse locations should be supplied. A failed offline install is a gate, not a reason to install globally or reuse another revision's environment.

```sh
umask 077
db="$HOME/.local/state/lido-orchestrator/state.sqlite3"
backup_root="$HOME/.local/state/lido-orchestrator/backups"
# With all application users stopped, use a SQLite-aware backup procedure
# (for example, the SQLite CLI .backup command) to create a new private backup.
# Record and inspect the exact output path before relying on it.
```

Before an upgrade, confirm the exact database path and record its package version/revision. CLI state commands may initialize or migrate the selected database; use a disposable reviewed copy for preflight and do not use an implicit database path. Keep database and backups in private user-owned directories. Do not copy only the main database file while SQLite is active. Prefer SQLite's online backup API for a consistent backup, and verify the result with `PRAGMA quick_check` using a SQLite-aware tool. Retain the live database's WAL/SHM files with it. The candidate provides no backup or validation helper; operators are responsible for using reviewed SQLite procedures and confirming their results.

Run the synthetic suite and package smoke checks from [RELEASE_HANDOFF.md](RELEASE_HANDOFF.md), inspect staged CLI `--help`/`info`, confirm no Codex opt-in, validate the backup, and review the example unit. Installation alone does not select a release or activate a service. If an operator separately opts in to a user unit, verify the destination is absent and never replace an existing unit implicitly. Adapt `ExecStart` to the exact revision environment. A user manager's runtime directory is UID-specific; derive it with `uid=$(id -u)` and use `/run/user/$uid` only when that session bus exists.

Rollback is a manual recovery operation; this candidate intentionally provides no automated restore because filesystem sidecar and concurrent-user races cannot be closed safely by a generic helper. Stop and independently verify that every process using the database has exited. Preserve the complete database directory, including any `-wal`, `-shm`, `-journal`, and `-mj*` files; do not unlink sidecars manually or open the old database with an unreviewed binary. Use a SQLite-aware operator procedure to preserve the stopped current database and restore the validated backup into a separate new file, then run `PRAGMA quick_check` on that new file before any application opens it. Require that the destination and all corresponding sidecar paths are absent, and retain the original directory untouched until the restored state has been reviewed. If exclusivity, sidecar state, file identity, or recovery outcome is uncertain, stop and obtain SQLite-specific recovery assistance. Use the prior revision CLI help without opening state; do not let an older binary run `status`, `info`, or any other state-opening command against a newer schema until compatibility is established on a disposable copy. Selecting or restarting a unit remains a separate operator action.

The following shows dynamic UID lookup only; this candidate never installs or activates a unit. If an operator later chooses to install one, create it with an exclusive file create after checking its content, rather than a command that overwrites an existing unit:

```sh
uid=$(id -u)
mkdir -p "$HOME/.local/state/lido-orchestrator"
export XDG_RUNTIME_DIR="/run/user/$uid"
export DBUS_SESSION_BUS_ADDRESS="unix:path=$XDG_RUNTIME_DIR/bus"
# Before any optional systemctl command, confirm this is the intended user manager.
```

These values apply only when the service is managed by the current user's user systemd manager and that runtime bus exists. No service command is run during candidate review. When an operator later opts in, the service remains dry-run; inspect its logs and `status` before changing the unit. A native provider event or external supervisor wake is not supplied by this unit.

## Checks

```sh
python3.12 -m unittest discover -s tests -v
```

See [CLEANUP_LIFECYCLE.md](CLEANUP_LIFECYCLE.md) for the cleanup boundary and separate containment acceptance plan. See [RELEASE_HANDOFF.md](RELEASE_HANDOFF.md) for candidate acceptance evidence and remaining release gates.

## Worker resource cleanup boundary

Registered scratch resources use dedicated mode-0700 per-attempt roots under `/tmp` or `/var/tmp`. Trusted-local CLI operations are `worker-resource-verify TASK ATTEMPT GENERATION --evidence TEXT`, `worker-resource-authorize TASK ATTEMPT GENERATION --evidence TEXT`, and `worker-resource-cleanup TASK`; cleanup previews by default and requires `worker-resource-cleanup TASK --execute` for deletion. Automatic cleanup requires the explicit global `--automatic-worker-cleanup` opt-in and remains disabled by default. Persist retention and byte bounds with `worker-resource-policy --retention-days DAYS --max-bytes BYTES`. Inactivity evidence and preservation authorization are separate records. Cleanup also requires the durable exact-attempt classification `process_free`, identity/path/type/link/mount/Git checks, and retention and byte eligibility bounds. These checks do not impose a runtime disk quota. Process-backed Codex attempts remain retained because direct-child exit and process-group behavior do not prove descendant inactivity. The diagnostic cgroup assessment never grants cleanup authorization; see [CLEANUP_LIFECYCLE.md](CLEANUP_LIFECYCLE.md) for process-free recovery and the independent containment gate.
All rights reserved. No license is granted by this release candidate.
