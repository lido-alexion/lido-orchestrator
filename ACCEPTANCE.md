# Release acceptance summary

| Area | Status | Boundary |
|---|---|---|
| Local orchestration, SQLite registry, approvals, durable dispatch fencing, lifecycle, and recovery | Usable | Trusted-local operator model; dry-run default; approval and ambiguity gates remain active. |
| Authenticated event envelope helper | Usable as an embedding component | No listener, mailbox reader, provider subscription, or automatic wake is included. |
| SMTP notifications | Optional and explicitly invoked | At-least-once delivery; requires caller-provided configuration and key. |
| Worker resource preview and process-free cleanup | Usable with explicit gates | Registered resources only; exact terminal attempt/generation, durable `process_free`, separate inactivity and preservation records, explicit execution, and identity/mount/Git checks are required. Uncertain resources are retained. |
| Process-backed cleanup or termination | Unsupported | No process-backed deletion, signaling, or inferred process absence. |
| OS containment, descendant quiescence, write confinement, and hard runtime quotas | Unsupported | Cleanup byte caps are eligibility limits only. Requires separate isolated-host authorization and acceptance. |
| Provider-native automation, mailbox ingestion, same-chat wake, and external publication/deployment | Unsupported by this package | No provider adapter or publication action is authorized by this candidate. |

## Reconciliation observability

Cycle telemetry is a fixed-schema JSON record on stderr for each CLI reconciliation cycle. It includes a process-session UUID and source label, monotonic duration, outcome, and claimed count; it excludes task and error details. It does not change successful stdout JSON. Emission is best effort and cannot stop reconciliation or replace its original exception. For systemd deployments, journal collection and retention are host-managed: records may be unavailable, volatile, rate-limited, or removed by journald policy, and are not authoritative application state. The read-only inspection procedure is documented in `README.md`; synthetic acceptance covers periodic cycles, process restart/source distinction, failure handling, and the stdout contract in `tests/test_periodic_cli.py`.

## Separate isolated-host containment pilot proposal

This is a proposal for a separately authorized pilot, not release functionality. A future pilot would require written approval from the host owner naming one disposable isolated host, the exact configuration scope, allowed test identities, and rollback procedure. Before touching that host, independent reviewers should approve a design that launches each exact attempt inside a unique non-migratable delegated boundary, proves recursive descendant quiescence, confines writes to disposable scratch, denies unapproved network access, and enforces hard CPU, memory, process, time, output, and disk limits.

The pilot should test migration attempts by both leaders and descendants, escaped descendants, restart recovery, identity mismatch, outside-workspace writes, network denial, each quota, and sentinel preservation. Any missing or ambiguous evidence must retain the resource. Host policy changes, privilege changes, and cleanup enablement require separate authorization after the pilot evidence receives independent security acceptance. No host policy is changed as part of this release candidate.
