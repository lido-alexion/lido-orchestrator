# Cleanup lifecycle and Codex containment acceptance plan

## Supported cleanup boundary

Cleanup applies only to explicitly registered resources below a dedicated mode-0700 per-attempt root in `/tmp` or `/var/tmp`. Use one disposable root per attempt for generated scratch, temporary databases, logs, and reproducible workspaces. Do not place source-of-record data, credentials, or the only copy of results there. Preserve required outputs and audit evidence elsewhere before considering deletion.

Cleanup is process-free only. Before deletion, the durable execution ledger must classify the exact attempt, generation, and adapter dispatch as `process_free`; a missing process row is not proof. The task must be terminal, exact-attempt inactivity evidence and separate cleanup/preservation authorization must exist, and identity/path/type/link/mount/Git checks plus retention and byte eligibility limits must pass. Preview is the default. Automatic recovery is disabled by default and may remove only exactly authorized, explicitly process-free resources. Uncertain state retains the resource.

The mount table must be readable and parseable. A managed root that is itself a mountpoint, or any mountpoint anywhere beneath that root, is rejected even when a bind mount reports the same device. Resource kinds are checked against filesystem shape (directory for `workspace`, `worktree`, `venv`, and SQLite bundle `temporary_db`; regular file for `log`); a kind label cannot weaken checks. Register a SQLite bundle as its containing directory, never a standalone database or `-wal`, `-shm`, or `-journal` file. Every resource tree containing recognizable Git metadata is retained, regardless of resource kind; individual files inside a repository are checked against their containing directory as well. This includes ordinary repositories, bare repositories, nested repositories, linked worktrees, and repositories detected through a containing parent. No remote-tracking ref is treated as proof of durable preservation. Git probes remove inherited `GIT_*` environment variables; incomplete scans and uncertain Git state fail closed. Git-bearing or uncertain resources are not eligible for deletion, even when process-free and otherwise authorized. These checks are repeated at cleanup time.

Only registered, exact-attempt, explicitly process-free resources with no detected Git state between the resource and its dedicated managed root can reach the existing deletion gates. Invalid `.git` markers in any ancestor within that root are treated as Git state. A Git probe only means absence when it returns the normal outside-repository diagnostic; timeout, command failure, malformed diagnostics, and scan errors retain the resource. Deletion repeats Git, identity, and mount checks at the final mutation fence. Empty-root removal is a separate mutation and repeats root identity, emptiness, mount topology, and Git checks immediately before `rmdir`. The byte limit bounds cleanup eligibility; it is not a runtime quota. The current code does not restrict writes during execution. Mode-0700 scratch roots and process groups are not OS containment. Do not classify Codex or any process-backed adapter as process-free.

## Codex containment status

Process-backed Codex resources are not eligible for automatic or explicit cleanup. `assess_cgroup_containment()` is diagnostic only and always reports `authorized=false`. A transient cgroup permission observation, process-group membership, leader exit, timeout, or absent PID cannot prove non-migration, descendant quiescence, write confinement, or quota enforcement.

## Separate conservative OS containment path

This plan is a design and acceptance gate, not implemented functionality. It requires an isolated target host and explicit host authorization before any configuration or service change.

1. **Non-migration:** A privileged host administrator provisions one unique transient systemd service and dedicated delegated cgroup for each exact task/attempt/generation. The worker is launched inside the boundary before it can execute task code. Persist and verify the unit identity and canonical ControlGroup before launch is released. The worker UID must have no write access to ancestor or sibling `cgroup.procs` controls and no delegated ability to move itself or descendants elsewhere. Verify effective permissions from the worker identity; fail closed if delegation is broad, missing, or ambiguous.
2. **Quiescence:** The unit uses control-group shutdown semantics. Before cleanup, the supervisor revalidates the exact unit identity and recursively inspects the cgroup. Require the unit stopped and all descendant process membership empty across repeated checks, with no PID-only fallback. On supervisor restart, recover only by querying that exact unit and generation. Missing, reused, changed, or inaccessible unit/cgroup identity retains all resources.
3. **Write confinement:** A minimal mount and namespace profile exposes only the attempt workspace as writable. Source-of-record trees, user home, credentials, host sockets, and unrelated writable paths are absent. Deny network unless separately approved. Test both allowed writes inside the workspace and denied writes to representative host paths, credentials, sibling workspaces, and network endpoints. Do not solve authentication failures by mounting broad credential stores.
4. **Quotas:** Enforce and observe per-attempt CPU, memory, process-count, wall-time, output, and disk limits at the OS/filesystem layer. Disk limits must be hard runtime limits, not cleanup byte caps. Exceeding a limit must stop the whole unit and retain data for review.
5. **Authorization and review:** Obtain documented host-owner authorization for delegated systemd/cgroup setup, namespace/mount policy, quota policy, and the exact isolated acceptance host. Require independent security review and a rollback plan before changing host configuration. No privilege changes or host configuration changes are part of this release candidate.

## Required acceptance evidence before Codex cleanup

- Host authorization names the isolated host, approving owner, configuration scope, and rollback.
- An independently reviewed configuration record proves per-attempt unit identity, delegation boundaries, non-writable ancestor/sibling controls, and launch-before-execution ordering.
- A worker-identity test proves attempted cgroup migration fails for leader and descendant processes.
- Adversarial tests prove no writes outside the attempt workspace and no network access under the accepted policy.
- Hard quota tests exercise CPU, memory, process count, wall time, output, and disk exhaustion and confirm whole-unit termination.
- A deliberately escaped-descendant attempt, fast-exit race, supervisor restart, and unit identity mismatch all fail closed; unrelated sentinel processes remain untouched.
- After exact-unit shutdown, repeated recursive membership checks are empty, and an independent reviewer approves the evidence and cleanup state machine.

Until every item passes on the authorized isolated host, retain all launched or ambiguous Codex workspaces for explicit human recovery. Process-free cleanup support does not imply Codex containment support.

## Candidate test boundary

Run `python3.12 -m unittest discover -s tests -v` from this candidate directory. Cleanup and cgroup diagnostic tests use synthetic temporary fixtures only; they do not access host cgroup controls. No real Codex worker, service, registry, or host containment configuration is required for this suite.

## Mutation-path proof requirements

Every filesystem mutation follows the same gate: the current task is terminal; resource ownership is explicitly registered to the exact attempt; the durable execution classification for that exact attempt and generation is `process_free`; separate inactivity and preservation-authorization records match the same attempt and generation; and identity and policy checks pass. This applies to normal unlink/rmtree operations, recovery that marks an already-missing path removed, and removal of an empty managed root. A missing path or empty root is not evidence that cleanup was authorized. Database failures roll back the cleanup transaction/savepoint; if a file was already unlinked, a later retry can record removal only after the exact proof gates still pass.
