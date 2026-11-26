# Security boundaries

- CLI and database are for one trusted local operator. There is no remote authentication, listener, or multi-user authorization layer.
- Dry-run is the default. Task approval is required by default. Codex dispatch requires two explicit opt-ins and runs read-only in scratch; no project-write adapter is exported or enabled.
- Project registration is metadata, not permission to read/write project contents. Snapshots expose bounded path metadata only.
- Supervisors require explicit `--trusted-local` enrollment. Tokens are shown once and stored only as SHA-256 digests. Route HMACs are scoped to an existing authoritative event and the supervisor's project; replay, mismatch, unknown identity, and revocation fail closed. Routing records receipt only and has no action authority.
- SMTP is opt-in for each invocation, STARTTLS-only, certificate-verified, and uses explicit files. SMTP passwords and receipt keys are excluded from config representations and diagnostics. Config and key files require regular-file type, current-user ownership, mode 0600, bounded reads, and no symlink following. FIFO input is rejected without blocking.
- Outbox delivery is at least once. Network delivery cannot be atomically committed with SQLite; timeout after remote acceptance can cause duplicate mail. Use stable event IDs/Message-IDs and operator review.
- Any native mail trigger must fail closed: missing/invalid signature, stale event, unknown/revoked supervisor, wrong project, replay, or unavailable authoritative DB means no route and no dispatch. Manual review is the fallback.
- No production dispatch, service change, external account, real credential, or live mail is part of the isolated pilot. No license is selected; rights remain reserved until a license is chosen.
