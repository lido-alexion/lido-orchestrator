# Quick guide: supervisor prompts

Copy the relevant prompt into the supervisor conversation. For takeover or recovery, use a replacement conversation and substitute the actual supervisor ID. For listing or cleanup, use an authorized management conversation.

**Capability notice:** These are intended user-facing prompts, not a guarantee that every lifecycle operation is deployed. Consult [ONBOARDING.md](ONBOARDING.md) and the installed CLI for current support. Report unsupported steps explicitly; never invent successful registration, deboarding, recovery, or event delivery.

## Register

```text
Register yourself with Lido Orchestrator and enable event-driven supervision while retaining hourly fallback.
```

## Deboard

```text
Gracefully deboard yourself from Lido Orchestrator, preserving all task history and evidence.
```

## Take over

```text
Take over supervisor <supervisor-id> from Lido Orchestrator. Recover its context, pending tasks and active workers, and continue its work.
```

## Recover deleted chat

```text
Recover the orphaned supervisor <supervisor-id> from Lido Orchestrator and resume its assigned responsibilities.
```

## List supervisors

```text
List all registered Lido Orchestrator supervisors, their responsibilities, health and current status.
```

## Check progress

```text
Check current status.
```

## Continue

```text
Continue with the remaining work.
```

## Suspend

```text
Temporarily suspend yourself in Lido Orchestrator without losing task state or history.
```

## Resume

```text
Resume your Lido Orchestrator supervision and continue pending work.
```

## Clean up orphaned registrations

```text
Inspect all stale and orphaned Lido Orchestrator supervisors and safely deactivate abandoned registrations. Preserve pending work and history.
```

## Finish work

```text
Verify all acceptance criteria and complete your assigned work. Deboard when completion is verified and lifecycle authorization permits.
```

## Integration health

```text
Verify your Lido Orchestrator registration, event triggers, hourly fallback and worker associations.
```

## Rules

- Follow [SUPERVISOR_PROTOCOL.md](SUPERVISOR_PROTOCOL.md) for every execution, including manual status and continuation requests.
- Preserve existing project authorization, approval gates, task history and evidence.
- Takeover requires authorized ownership transfer, fencing or revoking the previous registration and checking live workers before assigning new work.
- Deboarding or suspension must safely hold unfinished work, retain results and disable only associated triggers where authorized.
- Missing heartbeats indicate possible abandonment, not proof a chat was deleted. Never permanently erase history merely because a lease expired.
- Verify each operation against authoritative state and report any remaining manual steps.
