# Release handoff

## Candidate scope

This generic candidate contains the orchestrator package, public user and supervisor documentation, synthetic tests, and conservative process-free cleanup. It is allowlisted and contains no runtime database, credentials, private adapters, operational records, or historical snapshots. The public guide is byte-for-byte preserved from the prior public candidate. The public supervisor protocol is preserved verbatim as the prefix of this candidate protocol, followed by the current cleanup boundary.

## Cleanup capability

Cleanup preview is read-only. Deletion still requires explicit execution, a terminal task, exact attempt and generation ownership, durable `process_free` classification, separate inactivity evidence, and distinct preservation authorization. Any tree containing Git metadata or uncertain Git state within its dedicated managed root is retained, including invalid Git directory markers in ancestors, bare, nested, linked, and parent repositories. Only an explicit ordinary outside-repository probe result means Git metadata is absent. Git, identity, and mount checks repeat at the final deletion fence; empty managed-root removal repeats mount and identity checks. Process-backed cleanup is unsupported and remains fail-closed.

The byte cap is an eligibility limit, not a runtime quota. The package does not provide OS process containment, descendant-quiescence proof, write confinement, or hard resource quotas. See `CLEANUP_LIFECYCLE.md` for separate isolated-host acceptance requirements.

## CI and verification

The normal CI workflow runs the unittest suite and compile check on Python 3.12, then builds and installs the package into a disposable virtual environment and exercises lifecycle behavior against disposable SQLite state. Run the workflow from this candidate root. Local checks are:

```sh
env -u PYTHONPATH python3.12 -m unittest discover -s tests -q
python3.12 -m compileall -q lido_orchestrator tests
git diff --check
```

Synthetic tests use temporary fixtures only. Package installation must pass in the normal disposable CI job before release acceptance.

## Remaining acceptance

- Independent review of source, adversarial tests, generic documentation, and frozen inventory.
- Successful normal package CI, including disposable package installation.
- Separate authorized isolated-host acceptance before any process-backed cleanup is considered.

This handoff does not authorize commit, tag, publication, push, install outside CI, deployment, host policy changes, real workers, external provider actions, or production cleanup.
