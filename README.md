# Lido Orchestrator

Install the local package and CLI with `python3.12 -m pip install .`; the installed command is `lido`. Runtime state stays in the SQLite path selected by `--db` or `LIDO_DB`.

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

Submissions require explicit approval by default. Each root task starts a durable workflow with a default maximum of 8 total tasks, depth 3, and a one-hour deadline. An embedding caller can set smaller or larger bounded values with `Engine.submit(..., workflow_budget=..., workflow_max_depth=..., workflow_timeout=...)`. Task count and dispatch attempts are stored in SQLite and survive restart; `reconcile --max-tasks N` is an additional per-invocation ceiling, not the workflow safety limit. A proposal that exceeds the task or depth budget terminates the workflow and cancels queued descendants. `stop`, `needs_review`, worker failure, lease ambiguity, and deadline expiry also terminate the workflow. Termination fences late events, asks a process-aware adapter to cancel running siblings, and moves any uncancellable/ambiguous dispatch to `needs_review`. Operators can cancel one workflow with `terminate-workflow WORKFLOW_ID --reason 'operator review'`. `serve` repeats reconciliation every five minutes (configurable). A one-shot waits for local adapter callbacks. Default dry-run tasks finish in the explicit `dry_run` state and never invoke an adapter. Use `stop on` to block new claims and `stop off` to resume. `event EVENT_ID TASK_ID completed|failed` is a local CLI operation; there is no network listener. Backups should use SQLite's online backup API or include the database and its WAL state.

## Integration contract

An adapter receives task identity, an idempotency key, project metadata, intention, and access mode. SQLite creates a durable dispatch intent in the same transaction that claims a task. A second supervisor atomically claims each pending intent. An unclaimed pending intent can recover after a crash; once a real dispatch might have started, lease expiry fails closed into `needs_review` instead of replaying it. Event IDs are unique and duplicates are ignored. Structured completion payloads accept `decision` (`continue`, `stop`, or `needs_review`) plus up to ten `next_steps`; descendants share their parent's durable workflow, task budget, depth and deadline. Generated work defaults to approval-required and read-only. The existing preapproved-read-only option remains an explicit opt-in and is disabled by default. A model cannot grant write authority to a child.

`CodexWorker` requires both `--codex` and `LIDO_ENABLE_CODEX=1`. It does not pass the registered project root to Codex. Each task gets a new temporary directory containing only `TASK.md`, with `codex exec --sandbox read-only --ephemeral --skip-git-repo-check`. The CLI's configured Codex authentication is still used; no credentials are copied into the scratch directory. It launches Codex asynchronously, enforces a bounded timeout (1–900 seconds; default 240), kills the subprocess group on timeout or orderly shutdown, and ingests an attempt-fenced structured completion event. `reconcile` and `serve --once` wait for that local completion callback before exiting. The normal CLI lease is 300 seconds, longer than the default 240-second worker timeout; keep any custom lease longer than its worker timeout. The installed CLI schema restricts decisions to `continue`, `stop`, and `needs_review`; `needs_review` maps to that task state and never advances dependent work.

This is a planning/review worker. Registered roots are metadata only and are never mounted or passed to Codex; even a project registered as writable does not make this adapter a writable execution worker. `--sandbox read-only` is a Codex CLI setting, not a security boundary enforced by Lido. Do not claim filesystem or network isolation beyond what the installed Codex CLI and host enforce.

Dependent steps require approval by default. An embedding application or CLI operator may explicitly enable preapproved read-only steps. Only a child that declares both `approval_required: false` and `read_only: true` is released immediately; omitted or writable requests remain approval-gated. Generated steps remain constrained to the same project and read-only task metadata. On upgrade, legacy ready descendants are moved back behind approval, and legacy running tasks move to `needs_review` so a restart cannot replay them without review.

Real dispatch has a fail-closed crash policy. The supervisor durably marks an intent `dispatching`, then reacquires SQLite's cross-process write lock and rechecks workflow state, deadline, emergency stop, approval, and write grant before invoking the adapter. That lock remains held through the adapter's synchronous process-start handoff; adapters must return from `dispatch()` after launch and deliver callbacks asynchronously or after return. If termination wins the lock first, launch is suppressed. If launch wins, termination waits and marks the running attempt `needs_review`; local process cancellation is best effort. A hard crash after the durable marker is ambiguous and cannot replay automatically. Results and events are committed to SQLite. Internal result events also enter a durable outbox for an external consumer; `outbox` reads pending events and `ack-outbox EVENT_ID` acknowledges delivery, but Lido does not send them or expose a network listener.

### Generic event delivery

An external adapter can claim due outbox rows with `claim-outbox OWNER`. SQLite leases each row atomically and returns an attempt-scoped claim token. The consumer sends the event, then acknowledges with `ack-outbox EVENT_ID --claim-token TOKEN`; on failure it calls `retry-outbox EVENT_ID TOKEN`, which schedules capped exponential backoff. Expired claims become eligible for redelivery. Delivery is at least once: consumers must deduplicate by stable event id, including when a send succeeded but its acknowledgement was lost. Tokens fence stale acknowledgements and retries. The local CLI is trusted-user only; these tokens are not user authentication.

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

Live transport has not been exercised by this phase. If an operator later verifies SMTP connectivity, use a new disposable SQLite database, an isolated mailbox controlled by the operator, and an account restricted to that mailbox. Prepare one synthetic supervisor-notifiable event only, use explicit config/key paths and `--enable` for that invocation, then confirm receiver acceptance using read-only mailbox access. Inspect the disposable database's outbox and audit state and verify the receipt locally from stdin. Do not approve tasks, run `reconcile`, grant write authority, or connect this procedure to a service or production database. Receipt acceptance has no action authority. Delete the disposable state and revoke the temporary SMTP credential after recording the result. Stop if the receiver cannot be constrained to read-only acceptance or if the SMTP account has broader authority than needed.

`project-snapshot PROJECT_ID` returns a bounded read-only inventory of relative path names, entry types, and regular-file sizes. It never opens/copies project file contents, and skips common generated/VCS directories. The Codex planning adapter includes this inventory as context in its isolated scratch task. It is descriptive metadata only: snapshots do not make the project root available to Codex and cannot prove that a root is non-production. Use only a deliberately selected non-production project for this opt-in path.

### Isolated non-production planning and handoff pilot

This opt-in pilot exercises one approved read-only planning task and one immediately handed-off read-only follow-up in a disposable database. Codex runs in its empty per-task scratch directory; it never receives the registered project root. Run only from a normal user shell with the configured Codex CLI available. The pilot explicitly enables the real adapter and preapproved read-only handoff. Never run it under a service or against production data, credentials, external accounts, or another supervisor. Keep the database until its audit has been inspected:

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

Expected evidence is a succeeded parent and child, the child's `parent_task_id`, one completion event and dispatch audit per task, one attempt each, measured decision-to-handoff latency, and no third task even if the model proposes additional work. Treat `needs_review`, a missing child/event/latency, or extra attempts as pilot failure. The durable workflow budget of two is the safety control; `--max-tasks 2` only limits that particular reconcile call. This read-only Codex invocation is operator-run and was not executed while preparing this change. This procedure is not permission or a procedure for writable execution. After review, remove the temporary directory manually. Do not change the existing dry-run unit for this pilot.

### Synthetic integration contract

For non-production integration, use a temporary database and a fake `WorkerAdapter` that only records task dictionaries or returns deterministic local events. Verify stable `idempotency_key`, monotonically increasing `attempt`, required approval, same-project dependent work, default read-only metadata, and no dependent execution for `stop` or `needs_review`. Exercise a two-process race against the same temporary SQLite database. The adapter callback must include the `attempt` in its payload; stale attempt results are journaled but cannot transition the current task. A crash after intent commit is recoverable with the same key; a crash after a real dispatch is ambiguous and moves to `needs_review` after lease expiry. There is no authenticated callback endpoint: callbacks are trusted in-process events. Do not expose `event` as a remote API.

### Workspace-write adapter design and deployment gate

`WorkspaceWriteWorker` accepts only a writable task with current per-project operator authorization, and only a real project directory below a dedicated child directory of `/tmp` or `/var/tmp`. It invokes Codex with `--sandbox workspace-write`; it does not use `danger-full-access`. The caller must provide a `ProjectIsolation` implementation that verifies and wraps every command with a `bwrap` namespace launcher, network denial, host and credential inaccessibility, project-only writes, process-tree termination, and CPU, memory, disk, time, and output quotas. The adapter also strips inherited environment variables and does not expose a user home or Codex credentials. It may therefore fail authentication; do not work around that by mounting a credential directory or broadening the sandbox.

No `ProjectIsolation` backend is shipped or verified in this repository. The write adapter is not available from the CLI or service, and a project authorization alone cannot enable it. Do not run an actual writable pilot or deploy writable execution. Cross-supervisor launch/termination is now serialized for adapters dispatched through the Engine: the durable `dispatching` marker precedes the launch-fenced SQLite transaction, which rechecks authority before adapter launch. This does not provide cross-supervisor process killing after launch; terminated running work is marked `needs_review`, while each worker can only cancel its own local process tree. The standalone write adapter is still disconnected from this fence and remains disabled.

A future isolated writable pilot needs independently reviewed OS denial of host and credential reads and network, project-only writes, cleanup and resource quotas, a minimized mount profile, a narrowly scoped authentication design, a short actor/reason write grant, and a manual deployment gate. Keep writable integration disabled until every prerequisite is evidenced.

The intended synthetic workflow is a planning task, one approved read-only child, then a terminal `stop`. Use a temporary SQLite file, a fake local worker, `workflow_budget=2`, `workflow_max_depth=1`, and a short deadline. Inspect `workflows`, `tasks`, `events`, and `audit` before deleting that temporary state. Include a runaway fake response proposing two extra children and verify both the durable budget and restart leave no runnable descendant. This validates orchestration only; it is not evidence of OS isolation or writable Codex execution.

## State and concurrency contract

SQLite is authoritative for projects, tasks, approvals, dispatch intents, leases, dedupe records, stop state, and append-only audit entries. SQLite `BEGIN IMMEDIATE` serializes task and intent claims across processes. Project registration is immutable: a project id cannot silently change its root or read-only policy. Running leases count against the configured global concurrency cap. Retries use capped exponential backoff. Approval and emergency-stop transitions are audited.

For a ChatGPT hourly supervisor, use one shared local database and invoke read-only `status` for observation. Submit proposed intentions as `awaiting_approval`; an explicit local approval action is required before claim. Do not let the hourly process dispatch work or edit task/lease rows directly. Reconciliation claims under a SQLite write lock, so two supervisors can safely race to reconcile, while approval is a state transition against the same database. If unattended approved intentions are needed, use a separately authenticated narrow submit API that records the approving principal and intent; do not infer approval from a ChatGPT summary or status read.

## Security boundaries and current limitations

Project roots are metadata, not sandbox enforcement. The Codex adapter enforces isolation by never using those roots and by selecting Codex's read-only sandbox; it does not provide a general writable project worker. The CLI is intended for a trusted local user; it has no authentication, network API, secret store, or multi-user authorization. Do not expose its database to an untrusted user. Use backups and restrictive filesystem permissions.

## Service deployment

This source package does not install or activate a service. Review the deployment environment and keep any service dry-run until a separate operator review approves a concrete unit and state directory. The CLI remains available for local manual reconciliation.

## Checks

```sh
python3.12 -m unittest discover -s tests -v
```

See [PUBLIC_RELEASE_CHECKLIST.md](PUBLIC_RELEASE_CHECKLIST.md) for this candidate's exact allowlist, file hashes, and release gates.
