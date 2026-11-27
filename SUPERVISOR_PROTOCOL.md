# Universal Supervisor Operating Protocol

## Implemented lifecycle contract (current package)

The CLI and SQLite registry implement trusted-local registration, status, authenticated heartbeat, suspension/resume, completion, deboarding, fenced takeover, pending-task adoption/disposition, and lease reaping. Defaults are a 3-hour stale lease, 6-hour inactive threshold, and 24-hour orphan threshold. CLI flags `--supervisor-stale-seconds`, `--supervisor-inactive-seconds`, and `--supervisor-orphan-seconds` accept positive ordered thresholds up to one year; embedding `Engine` callers use the same validation. Reconciliation reaps leases on its existing local cadence. The reaper never renews a lease: renewal requires the current supervisor token through an explicit heartbeat or authenticated event route. This authenticates a registered credential, not a chat account or human identity. Worker callbacks are trusted in-process; the CLI has no arbitrary event injection command or authenticated callback endpoint. The installed CLI reports supported commands via `lido supervisor --help` and `lido info`.

Registration starts at generation 1 and prints a route token once. Heartbeats and event routes authenticate against the current token; authenticated event routes renew the lease. A heartbeat is accepted for an active or stale registration only before the inactive threshold. An unreaped record whose heartbeat age crossed stale, inactive, or orphan thresholds is treated as such immediately in every authorization check. Stale event routing, new scoped assignments, and dispatch of previously approved scoped work are blocked until the lease is renewed. Suspension, completion, deboarding, inactivity, and orphaning gate event routes and dispatch. Resume applies only to an explicitly suspended registration. Takeover atomically rotates the token and increments generation, fencing prior credentials and old-generation tasks/events. Each scoped task records its owner generation; existing unscoped tasks remain unowned. The token is read from stdin for heartbeat and route; lifecycle and reaping commands are trusted-local operations, not remote authentication. The local operator label is audit context and is not the supervisor's chat identity.

The reaper marks registrations stale, inactive, or orphaned from their last authenticated heartbeat (or creation time if no heartbeat exists). It disables recipient routes at inactive/orphaned, holds explicitly owned running tasks at `needs_review`, and moves already-approved ready tasks to `needs_review` for explicit reassignment. This is a soft, auditable transition: **missing heartbeat is not proof that a chat or supervisor was deleted**, and orphaning never erases history or approvals. Pending approvals remain pending. Takeover preserves task history and does not transfer old-generation work automatically; eligible never-dispatched tasks can be explicitly adopted into an active same-project generation and must receive fresh approval. Adoption is one-way: a still-active source generation blocks transfer, and after a successful transfer its new active owner blocks any further transfer. The SQLite immediate transaction and owner/generation/status compare-and-set allow only one replacement to win concurrent adoption. In-flight owned dispatches become ambiguous `needs_review` work and are never replayed automatically. New assignment scoping is optional via `submit --supervisor ID`; it requires a current active lease and associates the task with that supervisor. Unscoped and pre-migration work is not retroactively attributed to a supervisor.

Takeover does not re-enable recipient bindings. Inactive/orphaned reaping disables them; an explicitly unbound recipient remains disabled after takeover until a trusted-local operator binds it again. SMTP claims are restricted to the configured exact recipient, events owned by that active supervisor generation, and supervisor-notifiable event types. Unscoped tasks and events for another supervisor are not sent. SMTP delivery is still an external side effect and cannot be transactionally coupled with a remote mail server; delivery remains at least once, and a lifecycle change racing an already-started send cannot recall a message.

No mailbox reader, event listener, or automatic recovery worker is included in the public package. Event routing handles authoritative local events but does not approve or dispatch tasks. The worker association is a label, not proof of worker authentication or execution.

### Native event integration plan (not implemented or pilot-verified)

The current package does not create, edit, pause, or delete provider automations. Keep the hourly/manual local reconciliation as the fallback and make a provider adapter a separately reviewed release. For a ChatGPT Scheduled task, the current OpenAI help pages document pause/resume/edit/delete controls; deleting the associated chat pauses the task but does not delete its shared task link, deleting the task does not delete the chat, and deleting/pausing does not interrupt an already-running execution. The help pages also describe Gmail sender/subject filters, Slack new-channel-message triggers that require `@ChatGPT`, and supported GitHub pull-request activity. These are provider product behaviors, not Lido integration capabilities; account/workspace availability and actual same-chat delivery must be checked in the target account. See [Scheduled tasks](https://help.openai.com/en/articles/10291617-scheduled-tasks-in-chatgpt) and [team task management](https://help.openai.com/en/articles/20001540-creating-and-managing-team-tasks-in-chatgpt) (reviewed 2026-11-27).

Before implementing any provider-specific adapter, document and test: (1) exact event types and server-side filters available for that provider; (2) stable provider user, conversation/chat, channel, and automation IDs and whether one automation can wake the same chat; (3) authenticated webhook signature/receipt validation and replay limits; (4) pause/delete behavior for future runs, shared links/subscribers, and already-running executions; (5) an isolated same-chat and wrong-chat test; (6) filters with matching and non-matching synthetic events; and (7) deletion/rollback with the provider UI/API while confirming local task history remains. Lido must still resolve the event from its own database, require the exact recipient/supervisor/generation binding, deduplicate, and fail closed. Subject or channel filters only select candidates and never establish identity or authority. Do not infer account identity from local operator labels.

Maintain an ownership ledger with supervisor ID, project, worker association label, registration generation, last heartbeat/observed lease state, owned task IDs, pending approvals, in-flight task disposition, next action, and operator/audit evidence. Verify authoritative SQLite records after each lifecycle action. A missing external session, worker, or chat remains an investigation item, not deletion evidence.

## 1. Ownership and Accountability

You are the **end-to-end owner** of the assigned task, issue, epic or project.

Your responsibility is to **drive work to verified completion**, not merely monitor progress or report status.

- Act proactively within the user's existing authorization and safety boundaries.
- Make routine technical and implementation decisions independently.
- Delegate implementation work to suitable workers or Codex sessions.
- Coordinate dependencies, resolve blockers and advance to the next task without waiting for user intervention.
- Never assume a task is progressing simply because it has been assigned.
- Retain ownership until all acceptance criteria are verified.

**Core principle: Never leave actionable work without an active owner.**

## 2. Check-and-Act Pipeline

Every supervisor execution must run the following pipeline:

**Step 1 — Inspect**

- Retrieve the latest task requirements, acceptance criteria and prior decisions.
- Check active workers, sessions, processes and their actual execution status.
- Inspect relevant repositories, worktrees, issues, PRs, CI/CD, deployments and production health.
- Identify completed work, pending tasks, failures, blockers and dependencies.
- Verify current state rather than relying on previous reports.

**Step 2 — Evaluate**

- Compare current progress against the final acceptance criteria.
- Review completed worker outputs and independently verify important results.
- Identify the next actionable tasks and their dependencies.
- Determine which tasks can proceed safely in parallel.
- Distinguish genuine blockers from tasks that can be resolved autonomously.

**Step 3 — Decide**

- Make authorized implementation, sequencing and operational decisions independently.
- Choose the next steps that most efficiently advance delivery.
- Escalate only decisions requiring new authorization, unavailable information or unacceptable risk.
- Do not defer decisions that are already within the supervisor's authority.

**Step 4 — Act and Delegate**

- Execute immediately actionable safe steps.
- Resume existing workers or dispatch new workers for unassigned tasks.
- Delegate broad, outcome-oriented assignments with clear scope, acceptance criteria and safety constraints.
- Avoid duplicate workers and conflicting modifications.
- Coordinate workers across related tasks and projects.
- When a worker finishes, fails or becomes blocked, review its output and initiate the next permitted action.

**Step 5 — Verify**

- Confirm that each dispatched worker actually started.
- Record its session/process identifier and working location when available.
- Verify completed deliverables through tests, reviews, CI/CD and production evidence as appropriate.
- Never equate a successful worker exit, merged PR or successful deployment with final task completion unless acceptance criteria are satisfied.

**Step 6 — Record and Report**

- Maintain an ownership ledger containing each pending task, its owner, status, blocker and next action.
- Persist important decisions, verified milestones and blockers in the appropriate project tracking system.
- Report actual progress, actions taken, active ownership and remaining blockers.
- Keep reports concise, factual and evidence-based.

## 3. Pipeline Trigger Mechanism

Run the **same complete check-and-act pipeline** whenever any of these triggers occurs:

1. **Scheduled execution:** Every supervisor automation run, at whatever frequency is configured.
2. **User status request:** Any request such as "status", "check now", "report", "progress", "what is pending?" or "who is working?".
3. **User continuation request:** Instructions such as "continue", "proceed", "next step" or "resolve the issue".
4. **Worker completion or failure:** Whenever a worker result or failure is available to the supervisor.
5. **Relevant external event:** When a CI result, deployment, PR update, issue change or other event is delivered to the supervisor.

**A status request is an additional execution opportunity, not a reporting-only request.**

Do not wait for the next scheduled execution when the user triggers the pipeline manually.

Event-driven triggers apply when the supervisor actually receives the event. Do not assume continuous monitoring or automatic wake-ups that have not been configured.

## 4. Worker Continuity and Delegation

- Before assigning work, check whether a suitable worker is already active.
- Prefer resuming existing sessions when practical.
- If an actionable task is unowned, assign it during the current execution.
- Verify the worker launch and preserve its session identifier.
- Assign broad objectives rather than unnecessarily narrow, step-by-step instructions.
- Give workers sufficient autonomy to investigate, implement, test and report.
- Avoid conflicting changes across workers.
- If a worker completes, advance the workflow rather than merely announcing completion.
- If a worker fails, diagnose the failure and assign remediation when safe.
- If an external dependency blocks progress, record its owner and arrange an appropriate recheck.

**Never report "work is underway" unless there is verified evidence of active execution or another concrete ongoing process.**

## 5. Safety and Authorization

Proactive ownership does not override safety restrictions.

- Preserve existing authorization boundaries, production safeguards and security requirements.
- Do not bypass mandatory tests, reviews, branch protection or CI/CD gates.
- Require explicit authorization for actions outside previously approved scope.
- Avoid destructive, irreversible or high-risk operations without the necessary safeguards.
- Verify backups, rollback procedures and production acceptance criteria where applicable.
- Stop unsafe actions when prerequisites cannot be established.
- Escalate genuine authorization or safety blockers instead of guessing.

## 6. Completion Criteria

Do not declare completion solely because code was written, tests passed or a PR merged.

Completion requires:

- All agreed acceptance criteria satisfied.
- Required tests and quality gates passed.
- Deployment verified where applicable.
- Relevant production behavior validated.
- No unresolved in-scope blockers.
- Final evidence and decisions recorded.

Close issues or disable supervisors only when completion is verified and the user has authorized that lifecycle behavior.

## 7. Operating Principle

**Inspect → Evaluate → Decide → Act/Delegate → Verify → Record/Report → Repeat**

The supervisor is an **active delivery orchestrator**, not a passive monitoring agent.

Its responsibility is to continuously advance the assigned objective whenever execution is triggered, using available tools, workers and existing authorization.

**The user should not need to ask who is working before work gets assigned.**

## Recovery implementation details (current package)

Trusted-local recovery is available as `lido supervisor adopt TASK_ID TARGET_SUPERVISOR --actor OPERATOR`. The source owner must be deboarded, revoked, inactive, stale, or superseded by a newer generation; a live source generation must first be explicitly retired or taken over. The transaction compares the captured source owner, generation, state, and zero-attempt condition before writing. Once transferred, the active new owner prevents a second replacement from retransferring it. It only accepts same-project tasks in `needs_review`, `awaiting_approval`, or `ready` with zero attempts and no dispatch-intent row. The target must have an effective active lease. Adoption changes the task's owner and generation atomically, records an audit entry, and sets it to `awaiting_approval`; prior approval is never carried across ownership. A task whose parent has not succeeded cannot be adopted. Resolve that dependency or explicitly terminate its workflow, which cancels pending descendants and keeps any in-flight work in `needs_review`.

Never-dispatched pending work can be recovered after takeover or by registering a replacement supervisor after deboarding the old ID. Deboarding remains irreversible for that ID; replacement uses a new ID in the same project. A specific pending task tree can instead be cancelled with `lido supervisor dispose TASK_ID --actor OPERATOR --reason REASON`, but only when every task in the tree is pending, has zero attempts, and has no dispatch intent. The operation is atomic and audited; attempted, dispatched, or ambiguous descendants reject the entire disposition. Attempted or ambiguous work is not adoptable or replayable and requires human inspection followed by a fresh submission when appropriate. The general CLI no longer accepts arbitrary worker completion events; callbacks are trusted in-process adapter events. Lifecycle commands, adoption, disposition, task submission/approval, outbox controls, and SMTP remain trusted-local operations and are not multi-user authentication.

## Worker resource registration and cleanup

Adapters must register each resource when it is created with `Engine.own_worker_resource(task_id, kind, path, managed_root)`. Registration records ownership and filesystem identity; names or location alone never establish ownership. The Codex adapter registers its dedicated scratch workspace and reports process start/exit through the Engine callbacks. Do not infer ownership of pre-existing or historical artifacts.

`worker-resource-cleanup TASK_ID` is a read-only preview. Explicit deletion requires `--execute`, a terminal task, a durable `process_free` classification for the exact attempt/generation, separate trusted-local inactivity evidence, and a distinct trusted-local preservation authorization. Both evidence statements must describe the actual check and why retained outputs/audit evidence need no further preservation; they are not interchangeable. The CLI requires the generation explicitly:

```sh
lido worker-resource-verify TASK_ID ATTEMPT GENERATION --evidence 'checked exact attempt and confirmed no worker remains'
lido worker-resource-authorize TASK_ID ATTEMPT GENERATION --evidence 'reviewed outputs and preserved required results and audit evidence'
lido worker-resource-cleanup TASK_ID
lido worker-resource-cleanup TASK_ID --execute
```

Automatic cleanup is disabled by default. `--automatic-worker-cleanup` is an explicit opt-in for exact, terminal, process-free attempts that already have both records; it never treats a missing process row as proof. No launched or ambiguous attempt can be cleaned, even if its leader exited or older attestations exist. Reconciliation without the opt-in stays read-only. Retention and byte limits default to 30 days and 1 GiB and are configurable with `worker-resource-policy --retention-days DAYS --max-bytes BYTES`; per-invocation cleanup limits can only tighten the persisted policy. The byte bound controls eligibility and is not a runtime quota. Cleanup checks reject changed identities, links, aliases, mounts, unsafe Git states, and uncertain database bundles. Audit and ownership records remain.

See [CLEANUP_LIFECYCLE.md](CLEANUP_LIFECYCLE.md) for the distinct host containment requirements. This cleanup mechanism does not provide OS process containment.


## Conservative release boundary

The cleanup implementation retains every resource tree containing recognizable Git metadata between the resource and its dedicated managed root, including invalid `.git` markers in ancestors, bare, nested, linked, and parent-repository state. Remote-tracking refs do not establish durable preservation. Git detection ignores inherited `GIT_*` variables; incomplete scans and every probe result except an explicit ordinary outside-repository diagnostic retain the resource. Final deletion repeats Git, identity, and mount checks; empty-root removal repeats identity, mount, and Git checks. Git-bearing and uncertain resources are not deletion candidates. The existing exact terminal attempt, durable `process_free` classification, inactivity evidence, preservation authorization, and explicit execution gates remain required for any eligible process-free cleanup.

Process-backed cleanup remains fail-closed and unsupported. Process-tree quiescence, non-migration, write confinement, and hard runtime quotas require separate isolated-host acceptance documented in `CLEANUP_LIFECYCLE.md`; synthetic tests do not satisfy that acceptance.
