"""Conservative, attempt-scoped worker resource ledger.

Cleanup is never authorized by a terminal callback alone. A trusted local
operator must record an explicit inactivity verification for the exact attempt.
Process identities are recorded for the Codex adapter on Linux. Unknown or
reused PIDs are never signalled; orphan recovery only proves inactivity.
"""
from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import time
from pathlib import Path

RESOURCE_KINDS = {"temporary_db", "workspace", "worktree", "venv", "log"}
TERMINAL = {"succeeded", "failed", "cancelled", "dry_run"}

def _mountpoints():
    """Read mount topology; unavailable or malformed data means refusal."""
    import re
    try:
        lines = Path("/proc/self/mountinfo").read_text().splitlines()
    except OSError as exc:
        raise ValueError("mount topology unavailable; refusing filesystem access") from exc
    result = set()
    for line in lines:
        fields = line.split(" - ", 1)[0].split()
        if len(fields) < 6:
            raise ValueError("mount topology malformed; refusing filesystem access")
        result.add(Path(re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), fields[4])))
    if not result:
        raise ValueError("mount topology empty; refusing filesystem access")
    return result

def _validate_kind_shape(kind, path, st):
    expected = {"temporary_db": stat.S_ISDIR(st.st_mode), "workspace": stat.S_ISDIR(st.st_mode),
                "worktree": stat.S_ISDIR(st.st_mode), "venv": stat.S_ISDIR(st.st_mode),
                "log": stat.S_ISREG(st.st_mode)}
    if kind not in expected or not expected[kind]:
        raise ValueError("resource kind does not match a supported filesystem shape")
    if path.name.lower().endswith(("-wal", "-shm", "-journal")):
        raise ValueError("SQLite sidecars cannot be registered independently")
    if stat.S_ISREG(st.st_mode):
        try:
            with path.open("rb") as handle:
                sqlite_header = handle.read(16) == b"SQLite format 3\x00"
        except OSError as exc:
            raise ValueError("resource type cannot be inspected") from exc
        if sqlite_header or path.name.lower().endswith((".sqlite", ".sqlite3", ".db")):
            raise ValueError("SQLite databases must be registered as a containing directory")

def assess_cgroup_containment(procs_paths):
    """Describe cgroup control-file visibility without granting cleanup authority.

    A same-UID writable ``cgroup.procs`` can move a process out of its current
    cgroup. This lightweight probe is diagnostic only: even an apparently
    restrictive result is not proof of descendant containment or a delegated
    cgroup boundary, so callers must keep process-backed resources retained.
    """
    paths = [Path(p) for p in procs_paths]
    if not paths:
        return {"available": False, "authorized": False,
                "reason": "no cgroup.procs controls supplied"}
    findings = []
    for path in paths:
        try:
            st = path.stat()
        except OSError as exc:
            return {"available": False, "authorized": False,
                    "reason": f"cannot inspect {path}: {exc.__class__.__name__}"}
        same_uid_writable = st.st_uid == os.getuid() and (
            bool(stat.S_IMODE(st.st_mode) & 0o222) or os.access(path, os.W_OK)
        )
        findings.append({"path": str(path), "owner_uid": st.st_uid,
                         "mode": stat.S_IMODE(st.st_mode),
                         "same_uid_writable": same_uid_writable})
    if any(item["same_uid_writable"] for item in findings):
        reason = "same UID can write a cgroup.procs control; process migration is possible"
    else:
        reason = "control-file permissions alone do not prove a delegated, non-escapable boundary"
    return {"available": True, "authorized": False, "reason": reason,
            "controls": findings}

def linux_process_identity(pid: int):
    """Return boot ID and /proc start ticks, or fail closed off Linux."""
    if not os.path.exists("/proc/self/stat"):
        raise OSError("Linux /proc process identity is unavailable")
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    raw = Path(f"/proc/{pid}/stat").read_text()
    # comm is parenthesized and may contain spaces or ')' characters.
    tail = raw[raw.rfind(")") + 2:].split()
    return boot, int(tail[19])  # field 22 (starttime), tail begins at field 3

def record_process(engine, task_id, attempt, generation, pid, pgid=None):
    boot, ticks = linux_process_identity(pid)
    engine.db.execute("UPDATE worker_attempt_execution SET classification='launched',updated=? WHERE task_id=? AND attempt=? AND generation=?", (engine.clock(),task_id,attempt,generation))
    engine.db.execute("INSERT INTO worker_process_attempts(task_id,attempt,generation,pid,pgid,boot_id,start_ticks,recorded) VALUES(?,?,?,?,?,?,?,?)",
                      (task_id, attempt, generation, pid, pgid, boot, ticks, engine.clock()))
    _audit(engine, None, task_id, "process_started", {"attempt":attempt,"generation":generation,"pid":pid,"pgid":pgid,"boot_id":boot,"start_ticks":ticks})

def record_process_exit(engine, task_id, attempt, generation, pid, exit_code):
    row = engine.db.execute("SELECT boot_id,start_ticks FROM worker_process_attempts WHERE task_id=? AND attempt=? AND generation=? AND pid=?", (task_id,attempt,generation,pid)).fetchone()
    if not row: return False
    try: current = linux_process_identity(pid)
    except (OSError, ValueError): current = None
    # wait() proves this exact child exited; identity is retained as durable evidence.
    engine.db.execute("UPDATE worker_process_attempts SET exited=?,exit_code=? WHERE task_id=? AND attempt=? AND generation=? AND pid=?", (engine.clock(),exit_code,task_id,attempt,generation,pid))
    _audit(engine, None, task_id, "process_exit_proven", {"attempt":attempt,"generation":generation,"pid":pid,"exit_code":exit_code,"identity_still_present":current == (row["boot_id"],row["start_ticks"])})
    return True


def _safe_path(path: Path, managed_root: Path):
    """Validate canonical private temp root and tree; reject links/mounts/aliases."""
    if not path.is_absolute() or not managed_root.is_absolute():
        raise ValueError("absolute paths required")
    lexical_root = Path(os.path.abspath(managed_root))
    root = managed_root.resolve(strict=True)
    if lexical_root != root:
        raise ValueError("managed root alias detected")
    if root.parent not in (Path("/tmp"), Path("/var/tmp")) or not root.name.startswith("lido-worker-"):
        raise ValueError("managed root must be a dedicated direct child of /tmp or /var/tmp")
    rst = root.lstat()
    if stat.S_ISLNK(rst.st_mode) or not stat.S_ISDIR(rst.st_mode) or rst.st_uid != os.getuid() or stat.S_IMODE(rst.st_mode) != 0o700:
        raise ValueError("managed root must be a mode-0700 directory owned by current user")
    mounts = _mountpoints()
    if root in mounts:
        raise ValueError("managed root is itself a mount or bind mount")
    # Walk lexical components without following symlinks, then compare to the
    # canonical path. This rejects aliases (including symlinked ancestors).
    lexical = Path(os.path.abspath(path))
    try:
        rel = lexical.relative_to(root)
    except ValueError as exc:
        raise ValueError("resource is outside managed root") from exc
    if not rel.parts:
        raise ValueError("resource must be a strict child of managed root")
    current = root
    for part in rel.parts:
        current = current / part
        st = current.lstat()
        if stat.S_ISLNK(st.st_mode):
            raise ValueError("symlink ownership is uncertain")
        if st.st_dev != rst.st_dev or st.st_uid != os.getuid():
            raise ValueError("resource ownership or filesystem boundary is uncertain")
        if not (stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode)):
            raise ValueError("special filesystem entries are unsupported")
        if stat.S_ISREG(st.st_mode) and st.st_nlink != 1:
            raise ValueError("hard-linked files are unsupported")
    resolved = lexical.resolve(strict=True)
    if resolved != lexical:
        raise ValueError("path alias detected")
    st = lexical.lstat()
    # Include same-device bind mounts, mounts at the root, ancestors of the
    # target, and every mountpoint beneath a directory target.
    if any(mp == root or root in mp.parents for mp in mounts):
        raise ValueError("managed root or resource tree contains a mount or bind mount")
    if stat.S_ISDIR(st.st_mode):
        def walk_error(exc):
            raise ValueError("resource tree scan incomplete; refusing filesystem access") from exc
        for directory, dirs, files in os.walk(lexical, followlinks=False, onerror=walk_error):
            for name in dirs + files:
                child = Path(directory) / name
                cst = child.lstat()
                if stat.S_ISLNK(cst.st_mode) or cst.st_dev != rst.st_dev or cst.st_uid != os.getuid():
                    raise ValueError("resource tree contains a link or foreign filesystem entry")
                if stat.S_ISREG(cst.st_mode) and cst.st_nlink != 1:
                    raise ValueError("resource tree contains a hard-linked file")
                if child.name.lower().endswith(("-wal", "-shm", "-journal")):
                    raise ValueError("SQLite WAL/SHM/journal companion detected; database quiescence is unproven")
                if not (stat.S_ISDIR(cst.st_mode) or stat.S_ISREG(cst.st_mode)):
                    raise ValueError("resource tree contains a special file")
    return st, lexical, rst


def _reject_git_repo():
    # No local ref, including a remote-tracking ref, proves durable preservation.
    # Keep the entire resource whenever it contains Git state.
    raise ValueError("Git-bearing resources are retained; durable preservation is unverified")


def _git_probe_env():
    """Prevent inherited Git variables from redirecting or masking repository state."""
    return {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}


def _has_git_metadata(path: Path, boundary: Path | None = None):
    """Return True for detected Git metadata; raise when detection is uncertain."""
    probe_path = path if path.is_dir() else path.parent
    def walk_error(exc):
        raise ValueError("Git metadata scan is incomplete; retaining resource") from exc

    # Git can discover a repository above the scanned resource. Check every
    # ancestor marker explicitly, including invalid gitdir files/directories.
    # Their invalidity is not evidence that the tree is safe to remove.
    ancestors = (probe_path, *probe_path.parents)
    if boundary is not None:
        boundary = Path(boundary).resolve(strict=True)
        if boundary not in ancestors:
            raise ValueError("Git scan boundary is not an ancestor; retaining resource")
        ancestors = ancestors[:ancestors.index(boundary) + 1]
    for ancestor in ancestors:
        try:
            if (ancestor / ".git").exists() or (ancestor / ".git").is_symlink():
                return True
        except OSError as exc:
            raise ValueError("Git metadata ancestor cannot be inspected; retaining resource") from exc

    for directory, dirs, files in os.walk(probe_path, followlinks=False, onerror=walk_error):
        names = set(dirs) | set(files)
        if ".git" in names:
            return True
        # A bare repository has no .git child. Its conventional core is
        # intentionally treated as Git-bearing even if the directory is partial.
        if {"HEAD", "objects", "refs"}.issubset(names) or {"HEAD", "objects"}.issubset(names):
            return True
        # Do not descend into likely repository object stores.
        if "objects" in dirs and "HEAD" in names:
            dirs[:] = [name for name in dirs if name != "objects"]
    try:
        probe = subprocess.run(["git", "-C", str(probe_path), "rev-parse", "--git-dir"], capture_output=True,
                               text=True, timeout=5, env=_git_probe_env())
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError("Git probe failed; retaining resource") from exc
    if probe.returncode == 0:
        return True
    # Only Git's ordinary, explicit outside-repository result means absence.
    # Unknown failures, malformed markers, config errors, and localization
    # differences all retain the resource.
    diagnostic = (probe.stderr or "").lower()
    if "not a git repository" in diagnostic:
        return False
    raise ValueError("Git probe result is uncertain; retaining resource")


def _preserve_git_work(path: Path, boundary: Path | None = None):
    """Retain any workspace containing Git state or whose status is uncertain."""
    if _has_git_metadata(path, boundary):
        _reject_git_repo()

def _reject_git_metadata(path: Path, boundary: Path | None = None):
    """Kinds other than workspace/worktree cannot claim a Git-bearing tree."""
    if _has_git_metadata(path, boundary):
        raise ValueError("Git-bearing or uncertain resources are retained")


def initialize(engine):
    engine.db.executescript("""
      CREATE TABLE IF NOT EXISTS worker_resources(
        id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL REFERENCES tasks(id),
        attempt INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 1,
        kind TEXT NOT NULL, path TEXT NOT NULL, managed_root TEXT NOT NULL,
        root_device INTEGER NOT NULL DEFAULT 0, root_inode INTEGER NOT NULL DEFAULT 0,
        device INTEGER NOT NULL, inode INTEGER NOT NULL, file_type INTEGER NOT NULL,
        state TEXT NOT NULL DEFAULT 'owned', created REAL NOT NULL, updated REAL NOT NULL,
        last_error TEXT, UNIQUE(task_id,attempt,path));
      CREATE INDEX IF NOT EXISTS worker_resources_task ON worker_resources(task_id,state);
      CREATE TABLE IF NOT EXISTS worker_resource_verifications(
        task_id TEXT NOT NULL, attempt INTEGER NOT NULL, generation INTEGER NOT NULL DEFAULT 1, verified REAL NOT NULL,
        evidence TEXT NOT NULL, PRIMARY KEY(task_id,attempt));
      CREATE TABLE IF NOT EXISTS worker_managed_roots(
        root TEXT PRIMARY KEY, task_id TEXT NOT NULL, attempt INTEGER NOT NULL,
        device INTEGER NOT NULL, inode INTEGER NOT NULL, created REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS worker_resource_audit(
        id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL, resource_id INTEGER,
        task_id TEXT NOT NULL, action TEXT NOT NULL, details TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS worker_resource_authorizations(
        task_id TEXT NOT NULL, attempt INTEGER NOT NULL, generation INTEGER NOT NULL,
        authorized REAL NOT NULL, evidence TEXT NOT NULL, PRIMARY KEY(task_id,attempt,generation));
      CREATE TABLE IF NOT EXISTS worker_resource_policy(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), retention_seconds INTEGER NOT NULL,
        max_bytes INTEGER NOT NULL, updated REAL NOT NULL);
      INSERT OR IGNORE INTO worker_resource_policy VALUES(1,2592000,1073741824,0);
      CREATE TABLE IF NOT EXISTS worker_process_attempts(
        task_id TEXT NOT NULL REFERENCES tasks(id), attempt INTEGER NOT NULL, generation INTEGER NOT NULL,
        pid INTEGER NOT NULL, pgid INTEGER, boot_id TEXT NOT NULL, start_ticks INTEGER NOT NULL,
        recorded REAL NOT NULL, exited REAL, exit_code INTEGER,
        PRIMARY KEY(task_id,attempt,generation,pid));
      CREATE TABLE IF NOT EXISTS worker_attempt_execution(
        task_id TEXT NOT NULL REFERENCES tasks(id), attempt INTEGER NOT NULL, generation INTEGER NOT NULL,
        classification TEXT NOT NULL, updated REAL NOT NULL, detail TEXT NOT NULL DEFAULT '',
        PRIMARY KEY(task_id,attempt,generation));
    """)
    cols = {r[1] for r in engine.db.execute("PRAGMA table_info(worker_resources)")}
    for name, decl in (("attempt", "INTEGER NOT NULL DEFAULT 0"), ("generation", "INTEGER NOT NULL DEFAULT 1"),
                       ("root_device", "INTEGER NOT NULL DEFAULT 0"), ("root_inode", "INTEGER NOT NULL DEFAULT 0")):
        if name not in cols:
            engine.db.execute(f"ALTER TABLE worker_resources ADD COLUMN {name} {decl}")
    vcols = {r[1] for r in engine.db.execute("PRAGMA table_info(worker_resource_verifications)")}
    if "generation" not in vcols:
        engine.db.execute("ALTER TABLE worker_resource_verifications ADD COLUMN generation INTEGER NOT NULL DEFAULT 1")


def register(engine, task_id, kind, path, managed_root):
    if kind == "process" or kind not in RESOURCE_KINDS:
        raise ValueError("process cleanup is unsupported; filesystem resource kind is invalid")
    task = engine.db.execute("SELECT status,attempts,COALESCE(supervisor_generation,1) AS generation FROM tasks WHERE id=?", (task_id,)).fetchone()
    if not task or task["status"] != "running":
        raise ValueError("resources may only be registered to the currently running attempt")
    st, resolved, rst = _safe_path(Path(path), Path(managed_root))
    _validate_kind_shape(kind, resolved, st)
    if kind == "worktree":
        _reject_git_repo()
    elif kind == "workspace":
        _preserve_git_work(resolved, Path(managed_root))
    else:
        _reject_git_metadata(resolved, Path(managed_root))
    # Serialize claims and reject any shared/overlapping roots or paths,
    # including claims belonging to another worker or attempt.
    nested = engine.db.in_transaction
    engine.db.execute("SAVEPOINT worker_resource_claim" if nested else "BEGIN IMMEDIATE")
    try:
        claims = engine.db.execute("SELECT task_id,attempt,path,managed_root FROM worker_resources WHERE state NOT IN ('removed','retained')").fetchall()
        for c in claims:
            old, oldroot = Path(c["path"]), Path(c["managed_root"])
            if resolved == old or resolved in old.parents or old in resolved.parents or rst.st_ino == 0:
                raise ValueError("resource overlaps an existing worker claim")
        root_claim = engine.db.execute("SELECT task_id,attempt,device,inode FROM worker_managed_roots WHERE root=?", (str(Path(managed_root).resolve(strict=True)),)).fetchone()
        if root_claim and (root_claim["task_id"], root_claim["attempt"], root_claim["device"], root_claim["inode"]) != (task_id, task["attempts"], rst.st_dev, rst.st_ino):
            raise ValueError("managed root is exclusively claimed by another task attempt")
        newroot = Path(managed_root).resolve(strict=True)
        for claim in engine.db.execute("SELECT root,task_id,attempt FROM worker_managed_roots").fetchall():
            prior = Path(claim["root"])
            if prior != newroot and (prior in newroot.parents or newroot in prior.parents):
                raise ValueError("managed root overlaps another exclusive root claim")
        now = engine.clock()
        root_path = str(Path(managed_root).resolve(strict=True))
        engine.db.execute("INSERT OR IGNORE INTO worker_managed_roots VALUES(?,?,?,?,?,?)", (root_path, task_id, task["attempts"], rst.st_dev, rst.st_ino, now))
        engine.db.execute("INSERT INTO worker_resources(task_id,attempt,generation,kind,path,managed_root,root_device,root_inode,device,inode,file_type,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?,'owned',?,?) ON CONFLICT(task_id,attempt,path) DO NOTHING",
                          (task_id, task["attempts"], task["generation"], kind, str(resolved), root_path, rst.st_dev, rst.st_ino, st.st_dev, st.st_ino, stat.S_IFMT(st.st_mode), now, now))
        row = engine.db.execute("SELECT id,kind,generation,managed_root,root_device,root_inode,device,inode,file_type FROM worker_resources WHERE task_id=? AND attempt=? AND path=?", (task_id, task["attempts"], str(resolved))).fetchone()
        if (row["kind"], row["generation"], row["managed_root"], row["root_device"], row["root_inode"], row["device"], row["inode"], row["file_type"]) != (kind, task["generation"], root_path, rst.st_dev, rst.st_ino, st.st_dev, st.st_ino, stat.S_IFMT(st.st_mode)):
            raise ValueError("existing attempt resource claim has a different identity")
        rid = row["id"]
        if engine.db.execute("SELECT changes()").fetchone()[0]:
            _audit(engine, rid, task_id, "registered", {"kind": kind, "attempt": task["attempts"], "path": str(resolved)})
        engine.db.execute("RELEASE SAVEPOINT worker_resource_claim" if nested else "COMMIT")
        return rid
    except Exception:
        if nested:
            engine.db.execute("ROLLBACK TO SAVEPOINT worker_resource_claim")
            engine.db.execute("RELEASE SAVEPOINT worker_resource_claim")
        else:
            engine.db.execute("ROLLBACK")
        raise


def verify_inactive(engine, task_id, attempt, evidence, generation=None):
    """Record operator's explicit exact-attempt inactivity evidence."""
    if not isinstance(evidence, str) or not evidence.strip() or len(evidence) > 2000:
        raise ValueError("non-empty bounded verification evidence is required")
    task = engine.db.execute("SELECT status,attempts,COALESCE(supervisor_generation,1) AS generation FROM tasks WHERE id=?", (task_id,)).fetchone()
    if generation is None and task:
        generation = task["generation"]
    if not task or task["status"] not in TERMINAL or task["attempts"] != attempt or generation != task["generation"]:
        raise ValueError("task must be terminal and attempt/generation must match exactly")
    execution = engine.db.execute("SELECT classification FROM worker_attempt_execution WHERE task_id=? AND attempt=? AND generation=?", (task_id,attempt,generation)).fetchone()
    if not execution or execution["classification"] not in ("process_free", "launched"):
        raise ValueError("durable process-free classification or recorded process identity is required; retain resources")
    if execution["classification"] == "launched" and not engine.db.execute("SELECT 1 FROM worker_process_attempts WHERE task_id=? AND attempt=? AND generation=?", (task_id,attempt,generation)).fetchone():
        raise ValueError("launched attempt lacks durable process identity; retain resources")
    procs = engine.db.execute("SELECT * FROM worker_process_attempts WHERE task_id=? AND attempt=? AND generation=?", (task_id,attempt,generation)).fetchall()
    for proc in procs:
        if proc["exited"] is None:
            try: ident = linux_process_identity(proc["pid"])
            except (FileNotFoundError, ProcessLookupError): ident = None
            except (OSError, ValueError): raise ValueError("process identity cannot be verified; retain resources")
            if ident == (proc["boot_id"],proc["start_ticks"]):
                raise ValueError("attempt process is still active; refusing inactivity proof")
            _audit(engine,None,task_id,"leader_inactive_descendants_unproven",{"attempt":attempt,"generation":generation,"pid":proc["pid"],"current_identity":ident})
        raise ValueError("descendant inactivity cannot be proven for a process-backed attempt; retain resources")
    engine.db.execute("INSERT OR REPLACE INTO worker_resource_verifications(task_id,attempt,generation,verified,evidence) VALUES(?,?,?,?,?)", (task_id, attempt, generation, engine.clock(), evidence.strip()))
    _audit(engine, None, task_id, "inactivity_verified", {"attempt": attempt, "generation": generation, "evidence": evidence.strip()})
    return {"task_id": task_id, "attempt": attempt, "generation": generation, "verified": True}


def authorize_cleanup(engine, task_id, attempt, evidence, generation=None):
    """Separate preservation/cleanup decision from worker inactivity evidence."""
    task = engine.db.execute("SELECT status,attempts,COALESCE(supervisor_generation,1) AS generation FROM tasks WHERE id=?", (task_id,)).fetchone()
    if not isinstance(evidence, str) or not evidence.strip() or len(evidence) > 2000:
        raise ValueError("non-empty bounded cleanup authorization evidence is required")
    if generation is None and task:
        generation = task["generation"]
    if not task or task["status"] not in TERMINAL or task["attempts"] != attempt or generation != task["generation"]:
        raise ValueError("task must be terminal and attempt/generation must match exactly")
    if not engine.db.execute("SELECT 1 FROM worker_resource_verifications WHERE task_id=? AND attempt=? AND generation=?", (task_id, attempt, generation)).fetchone():
        raise ValueError("exact-attempt inactivity evidence is required first")
    execution = engine.db.execute("SELECT classification FROM worker_attempt_execution WHERE task_id=? AND attempt=? AND generation=?", (task_id,attempt,generation)).fetchone()
    if not execution or execution["classification"] != "process_free":
        raise ValueError("durable process-free classification is required")
    engine.db.execute("INSERT OR REPLACE INTO worker_resource_authorizations VALUES(?,?,?,?,?)", (task_id, attempt, generation, engine.clock(), evidence.strip()))
    _audit(engine, None, task_id, "cleanup_authorized", {"attempt": attempt, "generation": generation, "evidence": evidence.strip()})
    return {"task_id": task_id, "attempt": attempt, "generation": generation, "authorized": True}


def set_policy(engine, *, retention_seconds, max_bytes):
    if retention_seconds < 0 or max_bytes < 1:
        raise ValueError("retention must be non-negative and max_bytes positive")
    engine.db.execute("UPDATE worker_resource_policy SET retention_seconds=?,max_bytes=?,updated=? WHERE singleton=1",
                      (retention_seconds, max_bytes, engine.clock()))
    return {"retention_seconds": retention_seconds, "max_bytes": max_bytes}


def _audit(engine, resource_id, task_id, action, data):
    engine.db.execute("INSERT INTO worker_resource_audit(at,resource_id,task_id,action,details) VALUES(?,?,?,?,?)",
                      (engine.clock(), resource_id, task_id, action, json.dumps(data, sort_keys=True)))


def _directory_is_empty(path: Path):
    with os.scandir(path) as entries:
        return next(entries, None) is None


def _cleanup_task(engine, task_id, *, execute=False, retry=False, retention_seconds=None, max_bytes=None):
    nested = engine.db.in_transaction
    engine.db.execute("SAVEPOINT worker_resource_cleanup" if nested else "BEGIN IMMEDIATE")
    task = engine.db.execute("SELECT status,attempts,COALESCE(supervisor_generation,1) AS generation FROM tasks WHERE id=?", (task_id,)).fetchone()
    if not task:
        if nested:
            engine.db.execute("ROLLBACK TO SAVEPOINT worker_resource_cleanup"); engine.db.execute("RELEASE SAVEPOINT worker_resource_cleanup")
        else: engine.db.execute("ROLLBACK")
        raise ValueError("unknown task")
    if task["status"] not in TERMINAL:
        if nested:
            engine.db.execute("ROLLBACK TO SAVEPOINT worker_resource_cleanup"); engine.db.execute("RELEASE SAVEPOINT worker_resource_cleanup")
        else: engine.db.execute("ROLLBACK")
        raise ValueError("task is active or ambiguous; cleanup refused")
    verified = engine.db.execute("SELECT 1 FROM worker_resource_verifications WHERE task_id=? AND attempt=? AND generation=?", (task_id, task["attempts"], task["generation"])).fetchone()
    execution = engine.db.execute("SELECT classification FROM worker_attempt_execution WHERE task_id=? AND attempt=? AND generation=?", (task_id,task["attempts"],task["generation"])).fetchone()
    process_free = bool(execution and execution["classification"] == "process_free")
    authorized = engine.db.execute("SELECT 1 FROM worker_resource_authorizations WHERE task_id=? AND attempt=? AND generation=?", (task_id, task["attempts"], task["generation"])).fetchone()
    policy = engine.db.execute("SELECT retention_seconds,max_bytes FROM worker_resource_policy WHERE singleton=1").fetchone()
    # Call-site limits can only tighten persisted policy, never broaden it.
    retention_seconds = policy["retention_seconds"] if retention_seconds is None else max(policy["retention_seconds"], retention_seconds)
    max_bytes = policy["max_bytes"] if max_bytes is None else min(policy["max_bytes"], max_bytes)
    rows = engine.db.execute("SELECT * FROM worker_resources WHERE task_id=? AND attempt=? AND state NOT IN ('removed','retained') ORDER BY id", (task_id, task["attempts"])).fetchall()
    results, total_bytes = [], 0
    for row in rows:
        item = {"id": row["id"], "kind": row["kind"], "path": row["path"]}
        reason = None
        try:
            st, path, rst = _safe_path(Path(row["path"]), Path(row["managed_root"]))
            _validate_kind_shape(row["kind"], path, st)
            if (rst.st_dev, rst.st_ino) != (row["root_device"], row["root_inode"]):
                raise ValueError("managed root identity changed")
            if (st.st_dev, st.st_ino, stat.S_IFMT(st.st_mode)) != (row["device"], row["inode"], row["file_type"]):
                raise ValueError("filesystem identity changed")
            if row["kind"] == "worktree":
                _reject_git_repo()
            elif row["kind"] == "workspace":
                _preserve_git_work(path, Path(row["managed_root"]))
            else:
                _reject_git_metadata(path, Path(row["managed_root"]))
            size = st.st_size
            if stat.S_ISDIR(st.st_mode):
                size = sum(p.lstat().st_size for p in path.rglob("*") if p.is_file() and not p.is_symlink())
            total_bytes += size
            if row["kind"] == "log" and engine.clock() - row["created"] < retention_seconds:
                item.update(action="retained", bytes=size, retention_seconds=retention_seconds)
                results.append(item)
                continue
            if not process_free:
                reason = "durable process-free classification required; launched or ambiguous attempts must be retained"
            elif row["generation"] != task["generation"]:
                reason = "resource generation does not match the current task generation"
            elif not verified:
                reason = "explicit inactivity verification required for this attempt"
            elif not authorized:
                reason = "separate cleanup and evidence-preservation authorization required"
            elif row["state"] == "blocked" and not retry:
                reason = "blocked resource requires --retry"
            elif total_bytes > max_bytes:
                reason = "cleanup byte limit exceeded"
            else:
                item.update(action="would_remove", bytes=size)
                if execute:
                    # Last identity recheck immediately before mutation. Dedicated
                    # mode-0700 attempt roots and no nested mounts limit race scope.
                    now, _, nowroot = _safe_path(path, Path(row["managed_root"]))
                    if (now.st_dev, now.st_ino, nowroot.st_ino) != (row["device"], row["inode"], row["root_inode"]):
                        raise ValueError("identity changed at deletion fence")
                    _validate_kind_shape(row["kind"], path, now)
                    if row["kind"] == "worktree":
                        _reject_git_repo()
                    elif row["kind"] == "workspace":
                        _preserve_git_work(path, Path(row["managed_root"]))
                    else:
                        _reject_git_metadata(path, Path(row["managed_root"]))
                    if stat.S_ISDIR(now.st_mode): shutil.rmtree(path)
                    else: path.unlink()
                    item["action"] = "removed"
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            # An authorized delete can outlive the SQLite state update. Missing
            # path recovery is safe only after both evidence records exist.
            if (isinstance(exc, FileNotFoundError) and execute and process_free
                    and row["generation"] == task["generation"]
                    and task["status"] in TERMINAL and verified and authorized):
                engine.db.execute("UPDATE worker_resources SET state='removed',updated=?,last_error=NULL WHERE id=?", (engine.clock(), row["id"]))
                item["action"] = "recovered_removed"
            else:
                reason = str(exc)
        if reason:
            item.update(action="blocked", reason=reason)
            if execute:
                engine.db.execute("UPDATE worker_resources SET state='blocked',updated=?,last_error=? WHERE id=?", (engine.clock(), reason[:1000], row["id"]))
        elif execute and item["action"] == "removed":
            engine.db.execute("UPDATE worker_resources SET state='removed',updated=?,last_error=NULL WHERE id=?", (engine.clock(), row["id"]))
        # Preview is strictly read-only: no ledger, audit, or task writes.
        if execute:
            _audit(engine, row["id"], task_id, item["action"], item)
        results.append(item)
    if execute:
        roots = engine.db.execute("SELECT * FROM worker_managed_roots WHERE task_id=? AND attempt=?", (task_id, task["attempts"])).fetchall()
        # A managed root is a mutation too. Require the same exact terminal
        # attempt proof gates used for its children, even when it is empty.
        root_cleanup_authorized = bool(
            process_free and verified and authorized
            and task["status"] in TERMINAL
            and engine.db.execute(
                "SELECT 1 FROM worker_attempt_execution WHERE task_id=? AND attempt=? AND generation=? AND classification='process_free'",
                (task_id, task["attempts"], task["generation"]),
            ).fetchone()
            and engine.db.execute(
                "SELECT 1 FROM worker_resource_verifications WHERE task_id=? AND attempt=? AND generation=?",
                (task_id, task["attempts"], task["generation"]),
            ).fetchone()
            and engine.db.execute(
                "SELECT 1 FROM worker_resource_authorizations WHERE task_id=? AND attempt=? AND generation=?",
                (task_id, task["attempts"], task["generation"]),
            ).fetchone()
        )
        for claim in roots:
            if not root_cleanup_authorized:
                continue
            stale_generation = engine.db.execute(
                "SELECT 1 FROM worker_resources WHERE task_id=? AND attempt=? AND managed_root=? AND generation!=? LIMIT 1",
                (task_id, task["attempts"], claim["root"], task["generation"]),
            ).fetchone()
            if stale_generation:
                continue
            pending = engine.db.execute("SELECT count(*) FROM worker_resources WHERE task_id=? AND attempt=? AND managed_root=? AND state!='removed'", (task_id, task["attempts"], claim["root"])).fetchone()[0]
            if pending:
                continue
            try:
                root = Path(claim["root"])
                rst = root.lstat()
                if (root.parent not in (Path("/tmp"), Path("/var/tmp")) or not root.name.startswith("lido-worker-")
                        or stat.S_ISLNK(rst.st_mode) or not stat.S_ISDIR(rst.st_mode)
                        or rst.st_uid != os.getuid() or stat.S_IMODE(rst.st_mode) != 0o700
                        or (rst.st_dev, rst.st_ino) != (claim["device"], claim["inode"])):
                    raise ValueError("managed root identity changed")
                # Root removal has its own final fence: re-read mount topology,
                # scan Git state/ancestors, then recheck identity and emptiness.
                mounts = _mountpoints()
                if any(mp == root or root in mp.parents for mp in mounts):
                    raise ValueError("managed root mount topology changed")
                if _has_git_metadata(root, root):
                    raise ValueError("managed root Git state is uncertain")
                if not _directory_is_empty(root):
                    continue  # unregistered leftovers are preserved for review
                final = root.lstat()
                if (final.st_dev, final.st_ino, stat.S_IFMT(final.st_mode)) != (claim["device"], claim["inode"], stat.S_IFDIR):
                    raise ValueError("managed root identity changed at removal fence")
                final_mounts = _mountpoints()
                if any(mp == root or root in mp.parents for mp in final_mounts):
                    raise ValueError("managed root mount topology changed at removal fence")
                if not _directory_is_empty(root):
                    continue
                if _has_git_metadata(root, root):
                    raise ValueError("managed root Git state changed at removal fence")
                os.rmdir(root)
                _audit(engine, None, task_id, "managed_root_removed", {"path": str(root), "attempt": task["attempts"]})
            except FileNotFoundError:
                pass
            except (OSError, ValueError) as exc:
                _audit(engine, None, task_id, "managed_root_retained", {"path": claim["root"], "reason": str(exc)[:500]})
    result = {"task_id": task_id, "attempt": task["attempts"], "generation": task["generation"], "execution_classification": execution["classification"] if execution else "missing_ambiguous", "dry_run": not execute, "verified_inactive": bool(verified), "cleanup_authorized": bool(authorized), "bytes_examined": total_bytes, "resources": results}
    engine.db.execute("RELEASE SAVEPOINT worker_resource_cleanup" if nested else "COMMIT")
    return result


def cleanup_task(engine, task_id, *, execute=False, retry=False, retention_seconds=None, max_bytes=None):
    """Run cleanup with transaction rollback on every database failure."""
    nested = engine.db.in_transaction
    try:
        return _cleanup_task(engine, task_id, execute=execute, retry=retry,
                             retention_seconds=retention_seconds, max_bytes=max_bytes)
    except Exception:
        if engine.db.in_transaction:
            if nested:
                try:
                    engine.db.execute("ROLLBACK TO SAVEPOINT worker_resource_cleanup")
                    engine.db.execute("RELEASE SAVEPOINT worker_resource_cleanup")
                except Exception:
                    # The inner path may already have released its savepoint.
                    pass
            else:
                engine.db.rollback()
        raise


def recover_verified(engine, *, automatic=False):
    """Preview by default; optional deletion is limited to proven process-free attempts."""
    rows = engine.db.execute("SELECT v.task_id FROM worker_resource_verifications v JOIN tasks t ON t.id=v.task_id AND t.attempts=v.attempt AND COALESCE(t.supervisor_generation,1)=v.generation WHERE t.status IN ('succeeded','failed','cancelled','dry_run') ORDER BY v.verified").fetchall()
    outcomes = []
    for row in rows:
        try:
            if automatic:
                task = engine.db.execute("SELECT attempts,COALESCE(supervisor_generation,1) AS generation FROM tasks WHERE id=?", (row["task_id"],)).fetchone()
                exact = engine.db.execute("SELECT 1 FROM worker_resource_verifications WHERE task_id=? AND attempt=? AND generation=?", (row["task_id"],task["attempts"],task["generation"])).fetchone()
                authorized = engine.db.execute("SELECT 1 FROM worker_resource_authorizations WHERE task_id=? AND attempt=? AND generation=?", (row["task_id"],task["attempts"],task["generation"])).fetchone()
                execution = engine.db.execute("SELECT classification FROM worker_attempt_execution WHERE task_id=? AND attempt=? AND generation=?", (row["task_id"],task["attempts"],task["generation"])).fetchone()
                # Only the explicit execution classification proves an
                # adapter declared itself process-free. PID row absence alone
                # is deliberately irrelevant.
                if exact and authorized and execution and execution["classification"] == "process_free":
                    outcomes.append(cleanup_task(engine, row["task_id"], execute=True, retry=True))
                else:
                    preview = cleanup_task(engine, row["task_id"], execute=False, retry=True)
                    for item in preview["resources"]:
                        if item["action"] == "would_remove":
                            item.update(action="blocked", reason="automatic cleanup requires explicit process-free proof and exact authorization")
                    outcomes.append(preview)
            else:
                outcomes.append(cleanup_task(engine, row["task_id"], execute=False, retry=True))
        except (OSError, ValueError, subprocess.SubprocessError):
            # Preserve task processing; the next reconciliation retries after
            # an operator resolves the block.
            continue
    return outcomes
