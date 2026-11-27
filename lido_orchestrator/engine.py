"""SQLite-backed workflow engine with a durable dispatch intent journal."""
from __future__ import annotations

import json
import sqlite3
import time
import threading
import uuid
from pathlib import Path
from typing import Any, Protocol
from . import resource_cleanup


class WorkerAdapter(Protocol):
    """Adapter dispatch returns synchronously after its launch handoff only.

    Engine holds its cross-process launch fence during this call. Result callbacks
    should be asynchronous (or delivered after dispatch returns), never inline.
    """
    def dispatch(self, task: dict[str, Any]) -> None: ...


class DryRunWorker:
    """Safe default adapter: no external action is performed."""
    process_free = True
    def dispatch(self, task: dict[str, Any]) -> None:
        return None


class Engine:
    MAX_WORKFLOW_TASKS = 100
    MAX_WORKFLOW_DEPTH = 10
    MAX_WORKFLOW_SECONDS = 86400
    MAX_INTENTION_CHARS = 4000
    MAX_EVENT_BYTES = 1_000_000

    def __init__(self, db_path: str | Path, *, max_concurrency: int = 2,
                 lease_seconds: int = 300, dry_run: bool = True,
                 worker: WorkerAdapter | None = None, clock=time.time,
                 fault_hook=None, allow_preapproved_read_only_steps: bool = False,
                 workflow_budget: int = 8, workflow_max_depth: int = 3,
                 workflow_timeout: int = 3600, supervisor_stale_seconds: int = 10800,
                 supervisor_inactive_seconds: int = 21600, supervisor_orphan_seconds: int = 86400,
                 automatic_worker_cleanup: bool = False):
        if not 1 <= max_concurrency <= 64 or not 1 <= lease_seconds <= 3600:
            raise ValueError("concurrency must be 1–64 and lease duration 1–3600 seconds")
        if not 1 <= workflow_budget <= self.MAX_WORKFLOW_TASKS or not 0 <= workflow_max_depth <= self.MAX_WORKFLOW_DEPTH or not 1 <= workflow_timeout <= self.MAX_WORKFLOW_SECONDS:
            raise ValueError("workflow limits exceed hard caps")
        self.db_path = str(db_path)
        self.max_concurrency, self.lease_seconds = max_concurrency, lease_seconds
        self.dry_run, self.worker, self.clock = dry_run, worker or DryRunWorker(), clock
        self.allow_preapproved_read_only_steps = allow_preapproved_read_only_steps
        self.workflow_budget, self.workflow_max_depth = workflow_budget, workflow_max_depth
        self.workflow_timeout = workflow_timeout
        self.automatic_worker_cleanup = bool(automatic_worker_cleanup)
        if not 1 <= supervisor_stale_seconds <= supervisor_inactive_seconds <= supervisor_orphan_seconds <= 31_536_000:
            raise ValueError("supervisor lifecycle thresholds must be positive, ordered, and at most one year")
        self.supervisor_stale_seconds = supervisor_stale_seconds
        self.supervisor_inactive_seconds = supervisor_inactive_seconds
        self.supervisor_orphan_seconds = supervisor_orphan_seconds
        self.max_dispatches: int | None = None
        self._dispatch_count = 0
        self.fault_hook = fault_hook
        self._lock = threading.RLock()
        self.db = sqlite3.connect(self.db_path, timeout=30, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=30000")
        self._schema()
        resource_cleanup.initialize(self)

    def close(self): self.db.close()

    def own_worker_resource(self, task_id: str, kind: str, path: str | Path, managed_root: str | Path):
        """Record a worker-created resource with a filesystem identity snapshot."""
        with self._lock:
            return resource_cleanup.register(self, task_id, kind, path, managed_root)

    def record_worker_process(self, task_id, attempt, generation, pid, pgid=None):
        with self._lock:
            return resource_cleanup.record_process(self, task_id, attempt, generation, pid, pgid)

    def record_worker_process_exit(self, task_id, attempt, generation, pid, exit_code):
        with self._lock:
            return resource_cleanup.record_process_exit(self, task_id, attempt, generation, pid, exit_code)

    def cleanup_worker_resources(self, task_id: str, *, execute: bool = False, retry: bool = False):
        with self._lock:
            return resource_cleanup.cleanup_task(self, task_id, execute=execute, retry=retry)

    def verify_worker_inactive(self, task_id: str, attempt: int, evidence: str, generation: int | None = None):
        with self._lock:
            return resource_cleanup.verify_inactive(self, task_id, attempt, evidence, generation)

    def authorize_worker_cleanup(self, task_id: str, attempt: int, evidence: str, generation: int | None = None):
        with self._lock:
            return resource_cleanup.authorize_cleanup(self, task_id, attempt, evidence, generation)

    def _schema(self):
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS projects(id TEXT PRIMARY KEY, root TEXT NOT NULL, policy TEXT NOT NULL, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id),
          intention TEXT NOT NULL, status TEXT NOT NULL, approval_required INTEGER NOT NULL,
          read_only INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 3,
          available_at REAL NOT NULL, lease_owner TEXT, lease_until REAL, created REAL NOT NULL, updated REAL NOT NULL,
          last_error TEXT, parent_task_id TEXT REFERENCES tasks(id), decision TEXT,
          workflow_id TEXT, depth INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS workflows(id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id),
          max_tasks INTEGER NOT NULL, max_depth INTEGER NOT NULL, deadline REAL NOT NULL,
          state TEXT NOT NULL, termination_reason TEXT, created REAL NOT NULL, updated REAL NOT NULL,
          dispatch_count INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS project_write_authorizations(project_id TEXT PRIMARY KEY REFERENCES projects(id),
          actor TEXT NOT NULL, reason TEXT NOT NULL, expires REAL NOT NULL, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS supervisors(id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id),
          worker_id TEXT NOT NULL, token_hash TEXT NOT NULL, created REAL NOT NULL, revoked REAL);
        CREATE TABLE IF NOT EXISTS supervisor_routes(supervisor_id TEXT NOT NULL REFERENCES supervisors(id),
          event_id TEXT NOT NULL REFERENCES events(id), routed REAL NOT NULL, PRIMARY KEY(supervisor_id,event_id));
        CREATE TABLE IF NOT EXISTS supervisor_recipients(recipient TEXT PRIMARY KEY,
          supervisor_id TEXT NOT NULL REFERENCES supervisors(id), enabled INTEGER NOT NULL DEFAULT 1,
          created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id),
          kind TEXT NOT NULL, payload TEXT NOT NULL, received REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY, event_type TEXT NOT NULL,
          payload TEXT NOT NULL, created REAL NOT NULL, delivered REAL,
          available_at REAL NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
          lease_token TEXT, lease_until REAL);
        CREATE TABLE IF NOT EXISTS dispatch_intents(task_id TEXT PRIMARY KEY REFERENCES tasks(id),
          intent_key TEXT NOT NULL UNIQUE, state TEXT NOT NULL, created REAL NOT NULL, delivered REAL, claimed REAL);
        CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL, action TEXT NOT NULL,
          entity_id TEXT, details TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS consumed_receipts(token_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, consumed REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS consumed_receipt_events(event_id TEXT PRIMARY KEY, consumed REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS smtp_delivery_slot(id INTEGER PRIMARY KEY CHECK(id=1), token TEXT, lease_until REAL, next_allowed REAL NOT NULL DEFAULT 0);
        INSERT OR IGNORE INTO smtp_delivery_slot(id,token,lease_until,next_allowed) VALUES(1,NULL,NULL,0);
        CREATE TABLE IF NOT EXISTS control(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT OR IGNORE INTO control(key,value) VALUES('emergency_stop','0');
        CREATE INDEX IF NOT EXISTS tasks_sched ON tasks(status,available_at,created);
        CREATE INDEX IF NOT EXISTS tasks_lease ON tasks(status,lease_until);
        """)
        supcols = {r[1] for r in self.db.execute("PRAGMA table_info(supervisors)")}
        for name, decl in (("generation", "INTEGER NOT NULL DEFAULT 1"), ("heartbeat", "REAL"),
                           ("lifecycle", "TEXT NOT NULL DEFAULT 'active'"), ("lifecycle_since", "REAL")):
            if name not in supcols: self.db.execute(f"ALTER TABLE supervisors ADD COLUMN {name} {decl}")
        self.db.execute("UPDATE supervisors SET heartbeat=created WHERE heartbeat IS NULL AND revoked IS NULL")
        columns = {r[1] for r in self.db.execute("PRAGMA table_info(dispatch_intents)")}
        if "claimed" not in columns: self.db.execute("ALTER TABLE dispatch_intents ADD COLUMN claimed REAL")
        outbox_columns = {r[1] for r in self.db.execute("PRAGMA table_info(outbox)")}
        for name, declaration in (("available_at", "REAL NOT NULL DEFAULT 0"),
                                  ("attempts", "INTEGER NOT NULL DEFAULT 0"),
                                  ("lease_token", "TEXT"), ("lease_until", "REAL")):
            if name not in outbox_columns:
                self.db.execute(f"ALTER TABLE outbox ADD COLUMN {name} {declaration}")
        task_columns = {r[1] for r in self.db.execute("PRAGMA table_info(tasks)")}
        if "supervisor_id" not in task_columns: self.db.execute("ALTER TABLE tasks ADD COLUMN supervisor_id TEXT REFERENCES supervisors(id)")
        if "supervisor_generation" not in task_columns: self.db.execute("ALTER TABLE tasks ADD COLUMN supervisor_generation INTEGER")
        self.db.execute("UPDATE tasks SET supervisor_generation=(SELECT generation FROM supervisors WHERE supervisors.id=tasks.supervisor_id) WHERE supervisor_id IS NOT NULL AND supervisor_generation IS NULL")
        if "parent_task_id" not in task_columns: self.db.execute("ALTER TABLE tasks ADD COLUMN parent_task_id TEXT REFERENCES tasks(id)")
        if "decision" not in task_columns: self.db.execute("ALTER TABLE tasks ADD COLUMN decision TEXT")
        if "workflow_id" not in task_columns: self.db.execute("ALTER TABLE tasks ADD COLUMN workflow_id TEXT")
        if "depth" not in task_columns: self.db.execute("ALTER TABLE tasks ADD COLUMN depth INTEGER NOT NULL DEFAULT 0")
        workflow_columns = {r[1] for r in self.db.execute("PRAGMA table_info(workflows)")}
        if "dispatch_count" not in workflow_columns: self.db.execute("ALTER TABLE workflows ADD COLUMN dispatch_count INTEGER NOT NULL DEFAULT 0")
        self.db.execute("INSERT OR IGNORE INTO consumed_receipt_events(event_id,consumed) SELECT event_id,MIN(consumed) FROM consumed_receipts GROUP BY event_id")
        # Existing databases are migrated conservatively: each legacy task gets
        # an independent one-task workflow, with no descendants authorized.
        legacy = self.db.execute("SELECT id,project_id,created,status,parent_task_id FROM tasks WHERE workflow_id IS NULL").fetchall()
        if legacy:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                for row in legacy:
                    self.db.execute("INSERT OR IGNORE INTO workflows(id,project_id,max_tasks,max_depth,deadline,state,termination_reason,created,updated) VALUES(?,?,?,?,?,?,?,?,?)",
                                    (row["id"], row["project_id"], 1, 0, row["created"] + 3600,
                                     "active", None, row["created"], row["created"]))
                    self.db.execute("UPDATE tasks SET workflow_id=? WHERE id=?", (row["id"], row["id"]))
                    if row["parent_task_id"] and row["status"] == "ready":
                        self.db.execute("UPDATE tasks SET status='awaiting_approval',approval_required=1,last_error='legacy descendant held for operator review' WHERE id=?", (row["id"],))
                        self.db.execute("INSERT INTO audit(at,action,entity_id,details) VALUES(?,?,?,?)",
                                        (self.clock(), "legacy_descendant_held", row["id"], json.dumps({"reason": "workflow authority migration"})))
                    elif row["status"] == "running":
                        self.db.execute("UPDATE tasks SET status='needs_review',lease_owner=NULL,lease_until=NULL,last_error='legacy running dispatch requires review' WHERE id=?", (row["id"],))
                        self.db.execute("UPDATE dispatch_intents SET state='ambiguous',claimed=NULL WHERE task_id=?", (row["id"],))
                        self.db.execute("INSERT INTO audit(at,action,entity_id,details) VALUES(?,?,?,?)",
                                        (self.clock(), "legacy_dispatch_held", row["id"], json.dumps({"reason": "workflow authority migration"})))
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK"); raise

    def _audit(self, action, entity=None, details=None):
        self.db.execute("INSERT INTO audit(at,action,entity_id,details) VALUES(?,?,?,?)",
                        (self.clock(), action, entity, json.dumps(details or {}, sort_keys=True)))

    def project_snapshot(self, project_id: str, *, max_entries: int = 200) -> dict[str, Any]:
        """Return bounded path metadata only; never opens or copies project files."""
        if not 1 <= max_entries <= 1000: raise ValueError("max_entries must be between 1 and 1000")
        row = self.db.execute("SELECT root FROM projects WHERE id=?", (project_id,)).fetchone()
        if not row: raise ValueError("unknown project")
        root = Path(row["root"]).resolve(strict=True)
        if not root.is_dir(): raise ValueError("project root is not a directory")
        entries = []
        for directory, dirs, files in __import__("os").walk(root, followlinks=False):
            dirs[:] = sorted(d for d in dirs if d not in {".git", ".hg", ".svn", "node_modules", ".venv", "venv"})
            for name in sorted(dirs + files):
                path = Path(directory) / name
                try: info = path.lstat()
                except OSError: continue
                rel = path.relative_to(root).as_posix()
                is_link = path.is_symlink()
                entries.append({"path": rel, "kind": "symlink" if is_link else "directory" if path.is_dir() else "file",
                                "size": info.st_size if not is_link and path.is_file() else None})
                if len(entries) >= max_entries:
                    return {"project_id": project_id, "root": str(root), "entries": entries,
                            "truncated": True, "content_included": False}
        return {"project_id": project_id, "root": str(root), "entries": entries,
                "truncated": False, "content_included": False}

    def pending_outbox(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000: raise ValueError("limit must be between 1 and 1000")
        return [dict(r) for r in self.db.execute("SELECT * FROM outbox WHERE delivered IS NULL ORDER BY created LIMIT ?", (limit,))]

    def acquire_smtp_delivery_slot(self, *, lease_seconds: int, min_interval: float) -> dict[str, Any] | None:
        """Serialize SMTP senders and reserve the next send time across processes."""
        if not 1 <= lease_seconds <= 5000 or not 0 <= min_interval <= 3600:
            raise ValueError("invalid SMTP delivery slot bounds")
        now = self.clock()
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                row = self.db.execute("SELECT token,lease_until,next_allowed FROM smtp_delivery_slot WHERE id=1").fetchone()
                if row["token"] is not None and row["lease_until"] is not None and row["lease_until"] > now:
                    self.db.execute("COMMIT"); return None
                token = uuid.uuid4().hex
                send_at = max(now, row["next_allowed"])
                self.db.execute("UPDATE smtp_delivery_slot SET token=?,lease_until=?,next_allowed=? WHERE id=1",
                                (token, now + lease_seconds, send_at + min_interval))
                self.db.execute("COMMIT")
                return {"token": token, "wait_seconds": max(0.0, send_at - now)}
            except Exception:
                if self.db.in_transaction: self.db.execute("ROLLBACK")
                raise

    def release_smtp_delivery_slot(self, token: str) -> bool:
        if not token: raise ValueError("SMTP slot token required")
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                changed = self.db.execute("UPDATE smtp_delivery_slot SET token=NULL,lease_until=NULL WHERE id=1 AND token=?", (token,)).rowcount
                self.db.execute("COMMIT"); return bool(changed)
            except Exception:
                if self.db.in_transaction: self.db.execute("ROLLBACK")
                raise

    def claim_outbox(self, *, owner: str, limit: int = 20, lease_seconds: int = 60,
                     event_types: tuple[str, ...] | None = None, recipient: str | None = None) -> list[dict[str, Any]]:
        """Lease due events atomically. Delivery is at least once; consumers dedupe by id."""
        if not owner.strip() or not 1 <= limit <= 100 or not 1 <= lease_seconds <= 5000:
            raise ValueError("owner, limit (1–100), and lease (1–5000s) are required")
        now = self.clock()
        if event_types is not None and (not event_types or any(not isinstance(t, str) or not t or len(t) > 100 for t in event_types)):
            raise ValueError("event type filter must contain valid types")
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                sql = "SELECT id FROM outbox WHERE delivered IS NULL AND available_at<=? AND (lease_until IS NULL OR lease_until<=?)"
                params: tuple[Any, ...] = (now, now)
                if event_types is not None:
                    sql += " AND event_type IN (" + ",".join("?" for _ in event_types) + ")"
                    params += tuple(event_types)
                if recipient is not None:
                    sql += " AND EXISTS (SELECT 1 FROM events e JOIN tasks t ON t.id=e.task_id JOIN supervisors s ON s.id=t.supervisor_id JOIN supervisor_recipients r ON r.supervisor_id=s.id WHERE e.id=substr(outbox.id,8) AND r.recipient=? AND r.enabled=1 AND t.supervisor_generation=s.generation AND s.revoked IS NULL AND s.lifecycle='active' AND COALESCE(s.heartbeat,s.created)+?>?)"
                    params += (recipient, self.supervisor_stale_seconds, now)
                rows = self.db.execute(sql + " ORDER BY created LIMIT ?", params + (limit,)).fetchall()
                result = []
                for row in rows:
                    token = uuid.uuid4().hex
                    self.db.execute("UPDATE outbox SET lease_token=?,lease_until=?,attempts=attempts+1 WHERE id=? AND delivered IS NULL", (token, now + lease_seconds, row["id"]))
                    item = self.db.execute("SELECT * FROM outbox WHERE id=?", (row["id"],)).fetchone()
                    result.append({**dict(item), "owner": owner})
                self._audit("outbox_claimed", owner, {"event_ids": [r["id"] for r in result]})
                self.db.execute("COMMIT")
                return result
            except Exception:
                self.db.execute("ROLLBACK"); raise

    def retry_outbox(self, event_id: str, *, claim_token: str, error: str = "delivery failed") -> float:
        """Release a claimed event with capped exponential retry delay."""
        if not claim_token: raise ValueError("claim token required")
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                row = self.db.execute("SELECT attempts,lease_until FROM outbox WHERE id=? AND delivered IS NULL AND lease_token=?", (event_id, claim_token)).fetchone()
                if not row or row["lease_until"] is None or row["lease_until"] <= self.clock(): raise ValueError("outbox claim is missing, expired, or no longer owned")
                now = self.clock(); delay = min(3600, 2 ** min(row["attempts"], 10))
                self.db.execute("UPDATE outbox SET available_at=?,lease_token=NULL,lease_until=NULL WHERE id=?", (now + delay, event_id))
                self._audit("outbox_retry_scheduled", event_id, {"attempt": row["attempts"], "delay_seconds": delay, "error": str(error)[:300]})
                self.db.execute("COMMIT"); return delay
            except Exception:
                self.db.execute("ROLLBACK"); raise

    def release_outbox_claim(self, event_id: str, *, claim_token: str) -> bool:
        """Release a live lease when the serialized delivery channel is busy."""
        if not claim_token: raise ValueError("outbox claim token required")
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                changed = self.db.execute("UPDATE outbox SET lease_token=NULL,lease_until=NULL WHERE id=? AND delivered IS NULL AND lease_token=?",
                                          (event_id, claim_token)).rowcount
                if changed: self._audit("outbox_claim_released", event_id, {"reason":"delivery channel busy"})
                self.db.execute("COMMIT"); return bool(changed)
            except Exception:
                if self.db.in_transaction: self.db.execute("ROLLBACK")
                raise

    def supervisor_outbox_eligible(self, event_id: str, recipient: str, event_type: str | None = None) -> bool:
        """Check current lifecycle, generation, task scope, and exact delivery binding."""
        now = self.clock()
        allowed = ("worker.escalation", "worker.supervisor_decision")
        if event_type is not None and event_type not in allowed: return False
        sql = "SELECT 1 FROM outbox o JOIN events e ON e.id=substr(o.id,8) JOIN tasks t ON t.id=e.task_id JOIN supervisors s ON s.id=t.supervisor_id JOIN supervisor_recipients r ON r.supervisor_id=s.id WHERE o.id=? AND o.event_type IN (?,?) AND r.recipient=? AND r.enabled=1 AND t.supervisor_generation=s.generation AND s.revoked IS NULL AND s.lifecycle='active' AND COALESCE(s.heartbeat,s.created)+?>?"
        params = (event_id, *allowed, recipient, self.supervisor_stale_seconds, now)
        if event_type is not None:
            sql += " AND o.event_type=?"; params += (event_type,)
        return self.db.execute(sql, params).fetchone() is not None

    def acknowledge_outbox(self, event_id: str, *, actor: str, claim_token: str | None = None):
        if not actor.strip(): raise ValueError("actor required")
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                row = self.db.execute("SELECT lease_token,lease_until FROM outbox WHERE id=? AND delivered IS NULL", (event_id,)).fetchone()
                if row and row["lease_token"] is not None and (row["lease_token"] != claim_token or row["lease_until"] is None or row["lease_until"] <= self.clock()):
                    raise ValueError("outbox event is leased by another delivery attempt")
                if claim_token is not None and (not row or row["lease_token"] != claim_token):
                    raise ValueError("outbox claim is missing, expired, or no longer owned")
                changed = self.db.execute("UPDATE outbox SET delivered=?,lease_token=NULL,lease_until=NULL WHERE id=? AND delivered IS NULL", (self.clock(), event_id)).rowcount
                if not changed: raise ValueError("outbox event is missing or already acknowledged")
                self._audit("outbox_acknowledged", event_id, {"actor": actor})
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK"); raise

    def register_project(self, project_id: str, root: str, *, read_only: bool = True):
        if not project_id.strip() or not root.strip(): raise ValueError("project id and root are required")
        resolved = str(Path(root).expanduser().resolve())
        policy = json.dumps({"read_only": bool(read_only)}, sort_keys=True)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute("SELECT root,policy FROM projects WHERE id=?", (project_id,)).fetchone()
            if existing and (existing["root"] != resolved or existing["policy"] != policy):
                raise ValueError("project is already registered with different root or policy")
            self.db.execute("INSERT OR IGNORE INTO projects VALUES(?,?,?,?)", (project_id, resolved, policy, self.clock()))
            self._audit("project_registered", project_id, {"read_only": bool(read_only)})
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK"); raise

    def register_supervisor(self, supervisor_id: str, project_id: str, worker_id: str, *, token_hash: str):
        if not supervisor_id.strip() or not worker_id.strip() or len(supervisor_id)>100 or len(worker_id)>100 or len(token_hash)!=64:
            raise ValueError("invalid supervisor registration")
        try: int(token_hash, 16)
        except (TypeError, ValueError): raise ValueError("invalid supervisor registration") from None
        if not self.db.execute("SELECT 1 FROM projects WHERE id=?",(project_id,)).fetchone(): raise ValueError("unknown project")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            now=self.clock()
            self.db.execute("INSERT INTO supervisors(id,project_id,worker_id,token_hash,created,revoked,generation,heartbeat,lifecycle,lifecycle_since) VALUES(?,?,?,?,?,NULL,1,?,'active',?)",
                            (supervisor_id,project_id,worker_id,token_hash,now,now,now))
            self._audit("supervisor_registered",supervisor_id,{"project_id":project_id,"worker_id":worker_id,"trusted_local":True})
            self.db.execute("COMMIT")
        except Exception: self.db.execute("ROLLBACK"); raise

    def list_supervisors(self):
        return [self._supervisor_view(r) for r in self.db.execute("SELECT * FROM supervisors ORDER BY id")]

    def _supervisor_view(self, row):
        result = dict(row)
        result["observed_lifecycle"] = result["lifecycle"]
        if result["revoked"] is not None: result["observed_lifecycle"] = "deboarded"
        elif result["lifecycle"] in ("active", "stale", "inactive"):
            age = self.clock() - (result["heartbeat"] if result["heartbeat"] is not None else result["created"])
            if age >= self.supervisor_orphan_seconds: result["observed_lifecycle"] = "orphaned"
            elif age >= self.supervisor_inactive_seconds: result["observed_lifecycle"] = "inactive"
            elif age >= self.supervisor_stale_seconds: result["observed_lifecycle"] = "stale"
        result.pop("token_hash", None)
        return result

    def _supervisor_is_live(self, row, now: float, *, allow_recovery: bool = False) -> bool:
        age = now - (row["heartbeat"] if row["heartbeat"] is not None else row["created"])
        allowed = ("active", "stale") if allow_recovery else ("active",)
        limit = self.supervisor_inactive_seconds if allow_recovery else self.supervisor_stale_seconds
        return row["revoked"] is None and row["lifecycle"] in allowed and age < limit

    def supervisor_status(self, supervisor_id: str):
        row=self.db.execute("SELECT * FROM supervisors WHERE id=?",(supervisor_id,)).fetchone()
        if not row: raise ValueError("unknown supervisor")
        result=self._supervisor_view(row); result["routed_events"]=self.db.execute("SELECT COUNT(*) FROM supervisor_routes WHERE supervisor_id=?",(supervisor_id,)).fetchone()[0]
        result["ownership_ledger"]=[{"task_id":r["id"],"status":r["status"],"owner_generation":r["supervisor_generation"],"pending_approval":r["status"]=="awaiting_approval",
            "in_flight":r["status"]=="running","last_updated":r["updated"],
            "next_action":"review old-generation work and explicitly reassign if authorized" if r["supervisor_generation"] != result["generation"] else "inspect ambiguous result" if r["status"]=="needs_review" else "review approval" if r["status"]=="awaiting_approval" else "verify completion" if r["status"] in ("succeeded","dry_run","failed","cancelled") else "continue authorized supervision"}
            for r in self.db.execute("SELECT id,status,updated,supervisor_generation FROM tasks WHERE supervisor_id=? ORDER BY created",(supervisor_id,))]
        return result

    def supervisor_heartbeat(self, supervisor_id: str, *, token: str, generation: int | None = None) -> dict:
        with self._lock:
            return self._supervisor_heartbeat(supervisor_id, token=token, generation=generation)

    def _supervisor_heartbeat(self, supervisor_id: str, *, token: str, generation: int | None = None) -> dict:
        import hashlib, hmac
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row=self.db.execute("SELECT * FROM supervisors WHERE id=?",(supervisor_id,)).fetchone()
            age = self.clock() - (row["heartbeat"] if row and row["heartbeat"] is not None else row["created"] if row else 0)
            if (not row or row["revoked"] is not None or row["lifecycle"] not in ("active", "stale") or age >= self.supervisor_inactive_seconds
                or (generation is not None and generation != row["generation"])
                or not isinstance(token,str) or not hmac.compare_digest(row["token_hash"],hashlib.sha256(token.encode()).hexdigest())):
                raise ValueError("supervisor heartbeat rejected")
            now=self.clock(); self.db.execute("UPDATE supervisors SET heartbeat=?,lifecycle='active',lifecycle_since=? WHERE id=? AND generation=?",(now,now,supervisor_id,row["generation"]))
            self._audit("supervisor_heartbeat",supervisor_id,{"generation":row["generation"]})
            self.db.execute("COMMIT")
            return {"supervisor_id":supervisor_id,"generation":row["generation"],"heartbeat":now,"lease_until":now+self.supervisor_stale_seconds}
        except Exception:
            if self.db.in_transaction: self.db.execute("ROLLBACK")
            raise

    def supervisor_lifecycle(self, supervisor_id: str, *, action: str, actor: str, token_hash: str | None = None) -> dict:
        with self._lock:
            return self._supervisor_lifecycle(supervisor_id, action=action, actor=actor, token_hash=token_hash)

    def _supervisor_lifecycle(self, supervisor_id: str, *, action: str, actor: str, token_hash: str | None = None) -> dict:
        if not actor.strip() or action not in {"suspend","resume","complete","deboard","takeover"}: raise ValueError("invalid lifecycle action")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row=self.db.execute("SELECT * FROM supervisors WHERE id=?",(supervisor_id,)).fetchone()
            if not row or row["revoked"] is not None: raise ValueError("unknown or deboarded supervisor")
            if action == "resume" and row["lifecycle"] != "suspended": raise ValueError("supervisor is not suspended")
            if action == "takeover":
                try:
                    if not isinstance(token_hash, str) or len(token_hash) != 64: raise ValueError
                    int(token_hash, 16)
                except (TypeError, ValueError): raise ValueError("takeover requires a rotated token digest") from None
            if action == "complete" and self.db.execute("SELECT 1 FROM tasks WHERE supervisor_id=? AND status NOT IN ('succeeded','dry_run','cancelled') LIMIT 1",(supervisor_id,)).fetchone():
                raise ValueError("supervisor has unfinished owned tasks or pending approvals")
            now=self.clock(); generation=row["generation"]
            if action == "takeover":
                generation += 1
                lifecycle="active"
                self.db.execute("UPDATE supervisors SET generation=?,token_hash=?,heartbeat=?,lifecycle=?,lifecycle_since=? WHERE id=?",(generation,token_hash,now,lifecycle,now,supervisor_id))
                # Running work is ambiguous across ownership transfer; hold it for inspection.
                self._hold_supervisor_tasks(supervisor_id, now, "supervisor takeover requires in-flight review")
                self._hold_ready_supervisor_tasks(supervisor_id, now, "supervisor generation changed; explicit reassignment required")
            elif action == "deboard":
                lifecycle="deboarded"
                self.db.execute("UPDATE supervisors SET revoked=?,lifecycle=?,lifecycle_since=? WHERE id=?",(now,lifecycle,now,supervisor_id))
                self.db.execute("UPDATE supervisor_recipients SET enabled=0 WHERE supervisor_id=?",(supervisor_id,))
            else:
                lifecycle={"suspend":"suspended","resume":"active","complete":"completed"}[action]
                self.db.execute("UPDATE supervisors SET lifecycle=?,lifecycle_since=?,heartbeat=CASE WHEN ?='active' THEN ? ELSE heartbeat END WHERE id=?",(lifecycle,now,lifecycle,now,supervisor_id))
            if action in ("suspend","complete","deboard"):
                self._hold_supervisor_tasks(supervisor_id, now, "supervisor lifecycle changed; inspect in-flight task")
                if action == "deboard":
                    self._hold_ready_supervisor_tasks(supervisor_id, now, "supervisor deboarded; explicit reassignment required")
            self._audit("supervisor_"+action,supervisor_id,{"actor":actor,"generation":generation})
            self.db.execute("COMMIT"); return self.supervisor_status(supervisor_id)
        except Exception:
            if self.db.in_transaction: self.db.execute("ROLLBACK")
            raise

    def reap_supervisors(self, *, actor: str) -> list[dict]:
        with self._lock:
            return self._reap_supervisors(actor=actor)

    def _reap_supervisors(self, *, actor: str) -> list[dict]:
        if not actor.strip(): raise ValueError("trusted-local actor required")
        now=self.clock(); changed=[]
        self.db.execute("BEGIN IMMEDIATE")
        try:
            rows=self.db.execute("SELECT * FROM supervisors WHERE revoked IS NULL AND lifecycle IN ('active','stale','inactive')").fetchall()
            for row in rows:
                age=now-(row["heartbeat"] if row["heartbeat"] is not None else row["created"])
                target="orphaned" if age>=self.supervisor_orphan_seconds else "inactive" if age>=self.supervisor_inactive_seconds else "stale" if age>=self.supervisor_stale_seconds else None
                if target and target != row["lifecycle"]:
                    self.db.execute("UPDATE supervisors SET lifecycle=?,lifecycle_since=? WHERE id=?",(target,now,row["id"]))
                    if target in ("inactive","orphaned"):
                        self.db.execute("UPDATE supervisor_recipients SET enabled=0 WHERE supervisor_id=?",(row["id"],))
                        self._hold_supervisor_tasks(row["id"], now, "supervisor lease expired; missing heartbeat is not proof of deletion")
                        self._hold_ready_supervisor_tasks(row["id"], now, "supervisor lease expired; explicit reassignment required")
                    self._audit("supervisor_lease_reaped",row["id"],{"actor":actor,"lifecycle":target,"age_seconds":age}); changed.append({"supervisor_id":row["id"],"lifecycle":target})
            self.db.execute("COMMIT"); return changed
        except Exception:
            if self.db.in_transaction: self.db.execute("ROLLBACK")
            raise

    def _hold_supervisor_tasks(self, supervisor_id: str, now: float, reason: str):
        running=self.db.execute("SELECT id FROM tasks WHERE supervisor_id=? AND status='running'",(supervisor_id,)).fetchall()
        for task in running:
            self.db.execute("UPDATE tasks SET status='needs_review',lease_owner=NULL,lease_until=NULL,last_error=?,updated=? WHERE id=?",(reason,now,task["id"]))
            self.db.execute("UPDATE dispatch_intents SET state='ambiguous',claimed=NULL WHERE task_id=?",(task["id"],))
            cancel=getattr(self.worker,"cancel",None)
            if cancel:
                try: cancel(task["id"])
                except Exception as exc: self._audit("supervisor_cancel_failed",task["id"],{"error":str(exc)[:500]})

    def _hold_ready_supervisor_tasks(self, supervisor_id: str, now: float, reason: str):
        rows=self.db.execute("SELECT id FROM tasks WHERE supervisor_id=? AND status='ready'",(supervisor_id,)).fetchall()
        for row in rows:
            self.db.execute("UPDATE tasks SET status='needs_review',last_error=?,updated=? WHERE id=? AND status='ready'",(reason,now,row["id"]))
            self._audit("supervisor_ready_task_held",row["id"],{"supervisor_id":supervisor_id,"reason":reason})

    def adopt_supervisor_task(self, task_id: str, supervisor_id: str, *, actor: str) -> dict:
        """Explicitly adopt a never-dispatched pending task into a live generation.

        This is intentionally narrower than retry/replay: any attempt, dispatch
        intent, unresolved dependency, or terminal task makes adoption ineligible.
        Previously approved work returns to the approval queue.
        """
        if not actor.strip(): raise ValueError("trusted-local actor required")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            task=self.db.execute("SELECT * FROM tasks WHERE id=?",(task_id,)).fetchone()
            target=self.db.execute("SELECT * FROM supervisors WHERE id=?",(supervisor_id,)).fetchone()
            now=self.clock()
            if not task or not target or target["revoked"] is not None or target["lifecycle"]!="active" or now-(target["heartbeat"] if target["heartbeat"] is not None else target["created"])>=self.supervisor_stale_seconds:
                raise ValueError("adoption requires an existing task and active target supervisor lease")
            if task["project_id"] != target["project_id"]: raise ValueError("adoption target must own the same project")
            if self.db.execute("SELECT 1 FROM dispatch_intents WHERE task_id=?",(task_id,)).fetchone():
                raise ValueError("task has dispatch history and cannot be adopted")
            if task["status"] not in ("needs_review","awaiting_approval","ready") or task["attempts"] != 0:
                raise ValueError("only never-dispatched pending work can be adopted")
            if task["parent_task_id"] and self.db.execute("SELECT 1 FROM tasks WHERE id=? AND status='succeeded'",(task["parent_task_id"],)).fetchone() is None:
                raise ValueError("task dependency is unresolved; resolve or dispose its workflow first")
            prior_owner=task["supervisor_id"]
            prior_generation=task["supervisor_generation"]
            if prior_owner == supervisor_id and task["supervisor_generation"] == target["generation"]:
                raise ValueError("task already belongs to the target generation")
            # A live prior owner must explicitly retire, lose its lease, or be
            # taken over before another supervisor may claim its work. This
            # prevents an operator typo from silently splitting ownership.
            if prior_owner is not None:
                prior=self.db.execute("SELECT * FROM supervisors WHERE id=?",(prior_owner,)).fetchone()
                if (prior and prior["generation"] == prior_generation and prior["revoked"] is None
                        and prior["lifecycle"] == "active"
                        and now-(prior["heartbeat"] if prior["heartbeat"] is not None else prior["created"]) < self.supervisor_stale_seconds):
                    raise ValueError("prior owner is active; retire or take over its generation before adoption")
            # Always require a fresh explicit approval after ownership changes.
            changed=self.db.execute("UPDATE tasks SET supervisor_id=?,supervisor_generation=?,status='awaiting_approval',approval_required=1,lease_owner=NULL,lease_until=NULL,updated=?,last_error=NULL WHERE id=? AND supervisor_id IS ? AND supervisor_generation IS ? AND status=? AND attempts=0 AND NOT EXISTS (SELECT 1 FROM dispatch_intents WHERE task_id=?)",
                            (supervisor_id,target["generation"],now,task_id,prior_owner,prior_generation,task["status"],task_id)).rowcount
            if not changed: raise ValueError("task ownership changed concurrently; reload and explicitly reassess")
            self._audit("supervisor_task_adopted",task_id,{"actor":actor,"from_supervisor":prior_owner,"from_generation":task["supervisor_generation"],"to_supervisor":supervisor_id,"to_generation":target["generation"],"approval_required":True})
            self.db.execute("COMMIT")
            return self.get_task(task_id)
        except Exception:
            if self.db.in_transaction: self.db.execute("ROLLBACK")
            raise

    def dispose_supervisor_task(self, task_id: str, *, actor: str, reason: str) -> list[str]:
        """Audited cancellation for pending work proven never dispatched."""
        if not actor.strip() or not reason.strip(): raise ValueError("trusted-local actor and disposition reason are required")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            rows=self.db.execute("""WITH RECURSIVE tree(id,parent_task_id,status,attempts) AS (
                SELECT id,parent_task_id,status,attempts FROM tasks WHERE id=?
                UNION SELECT t.id,t.parent_task_id,t.status,t.attempts FROM tasks t JOIN tree p ON t.parent_task_id=p.id)
                SELECT * FROM tree""",(task_id,)).fetchall()
            if not rows: raise ValueError("unknown task")
            for row in rows:
                if row["status"] not in ("needs_review","awaiting_approval","ready") or row["attempts"] != 0 or self.db.execute("SELECT 1 FROM dispatch_intents WHERE task_id=?",(row["id"],)).fetchone():
                    raise ValueError("task tree contains attempted or ambiguous work and cannot be disposed")
            now=self.clock()
            disposed=[]
            for row in rows:
                self.db.execute("UPDATE tasks SET status='cancelled',approval_required=0,lease_owner=NULL,lease_until=NULL,last_error=?,updated=? WHERE id=?",
                                ("trusted-local disposition: "+reason[:500],now,row["id"]))
                self._audit("supervisor_task_disposed",row["id"],{"actor":actor,"reason":reason[:500],"root_task_id":task_id})
                disposed.append(row["id"])
            self.db.execute("COMMIT")
            return disposed
        except Exception:
            if self.db.in_transaction: self.db.execute("ROLLBACK")
            raise

    def _supervisor_assignment_active(self, task_id: str, now: float) -> bool:
        row=self.db.execute("SELECT t.supervisor_id,t.supervisor_generation,s.generation,s.lifecycle,s.revoked,s.heartbeat,s.created FROM tasks t LEFT JOIN supervisors s ON s.id=t.supervisor_id WHERE t.id=?",(task_id,)).fetchone()
        return bool(row and (row["supervisor_id"] is None or (row["supervisor_generation"] == row["generation"] and row["revoked"] is None and row["lifecycle"]=="active"
            and now-(row["heartbeat"] if row["heartbeat"] is not None else row["created"]) < self.supervisor_stale_seconds)))

    def unregister_supervisor(self, supervisor_id: str, *, actor: str):
        self.supervisor_lifecycle(supervisor_id,action="deboard",actor=actor)

    def bind_supervisor_recipient(self, supervisor_id: str, recipient: str, *, actor: str):
        """Bind an exact inbound address to one active registered supervisor."""
        if not recipient or recipient != recipient.strip() or len(recipient) > 254 or "@" not in recipient:
            raise ValueError("invalid supervisor recipient")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            supervisor = self.db.execute("SELECT * FROM supervisors WHERE id=?", (supervisor_id,)).fetchone()
            if not supervisor or not self._supervisor_is_live(supervisor, self.clock()):
                raise ValueError("recipient binding requires an active supervisor lease")
            self.db.execute("INSERT INTO supervisor_recipients VALUES(?,?,1,?) ON CONFLICT(recipient) DO UPDATE SET supervisor_id=excluded.supervisor_id,enabled=1,created=excluded.created",
                            (recipient, supervisor_id, self.clock()))
            self._audit("supervisor_recipient_bound", supervisor_id, {"recipient": recipient, "actor": actor})
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK"); raise

    def unbind_supervisor_recipient(self, recipient: str, *, actor: str):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute("UPDATE supervisor_recipients SET enabled=0 WHERE recipient=? AND enabled=1", (recipient,)).rowcount
            if not changed: raise ValueError("unknown or disabled recipient")
            self._audit("supervisor_recipient_unbound", recipient, {"actor": actor})
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK"); raise

    def route_supervisor_event(self, supervisor_id: str, event_id: str, *, issued: int, signature: str, token: str):
        """Authenticate a supervisor, then resolve only matching authoritative event data."""
        import hashlib, hmac
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                sup=self.db.execute("SELECT * FROM supervisors WHERE id=? AND revoked IS NULL",(supervisor_id,)).fetchone()
                if not sup or not isinstance(token,str) or not 32 <= len(token) <= 1024 or not hmac.compare_digest(sup["token_hash"],hashlib.sha256(token.encode()).hexdigest()): raise ValueError("unknown or revoked supervisor")
                if not self._supervisor_is_live(sup, self.clock()): raise ValueError("supervisor lease expired or inactive")
                if not isinstance(issued,int) or abs(self.clock()-issued)>300: raise ValueError("stale supervisor receipt")
                row=self.db.execute("SELECT e.id,e.task_id,e.kind,e.payload,e.received,o.event_type,t.project_id,t.supervisor_id,t.supervisor_generation FROM events e JOIN outbox o ON o.id='worker:'||e.id JOIN tasks t ON t.id=e.task_id WHERE e.id=?",(event_id,)).fetchone()
                if not row or row["project_id"]!=sup["project_id"] or row["supervisor_id"] != supervisor_id or row["supervisor_generation"] != sup["generation"]: raise ValueError("event is not authoritative for this supervisor generation")
                body=f"{supervisor_id}\n{event_id}\n{row['event_type']}\n{issued}".encode()
                expected=hmac.new(token.encode(),body,hashlib.sha256).hexdigest()
                if not isinstance(signature,str) or not hmac.compare_digest(expected,signature): raise ValueError("supervisor receipt authentication failed")
                self.db.execute("UPDATE supervisors SET heartbeat=? WHERE id=? AND generation=?",(self.clock(),supervisor_id,sup["generation"]))
                self.db.execute("INSERT INTO supervisor_routes VALUES(?,?,?)",(supervisor_id,event_id,self.clock()))
                self._audit("supervisor_event_routed",supervisor_id,{"event_id":event_id,"event_type":row["event_type"]})
                self.db.execute("COMMIT")
                result=dict(row); result["payload"]=json.loads(result["payload"]); result.pop("project_id",None); return result
            except sqlite3.IntegrityError as exc:
                self.db.execute("ROLLBACK"); raise ValueError("supervisor receipt replay rejected") from exc
            except Exception:
                if self.db.in_transaction: self.db.execute("ROLLBACK")
                raise

    def route_verified_supervisor_receipt(self, supervisor_id: str, event_id: str, *, recipient: str, receipt_token: str,
                                          receipt_key: bytes, issued: int, signature: str, token: str):
        """Atomically consume an authenticated SMTP receipt and route its event."""
        import hashlib, hmac
        from .smtp_delivery import verify_receipt
        receipt = verify_receipt(receipt_token, receipt_key)
        if receipt.get("event_id") != event_id or receipt.get("recipient") != recipient:
            raise ValueError("receipt target or event mismatch")
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                sup = self.db.execute("SELECT * FROM supervisors WHERE id=? AND revoked IS NULL", (supervisor_id,)).fetchone()
                if not sup or not isinstance(token, str) or not 32 <= len(token) <= 1024 or not hmac.compare_digest(sup["token_hash"], hashlib.sha256(token.encode()).hexdigest()):
                    raise ValueError("unknown or revoked supervisor")
                if not self._supervisor_is_live(sup, self.clock()):
                    raise ValueError("supervisor lease expired or inactive")
                binding = self.db.execute("SELECT 1 FROM supervisor_recipients WHERE recipient=? AND supervisor_id=? AND enabled=1", (recipient, supervisor_id)).fetchone()
                if not binding:
                    raise ValueError("recipient is not bound to this supervisor")
                if not isinstance(issued, int) or abs(self.clock() - issued) > 300:
                    raise ValueError("stale supervisor receipt")
                row = self.db.execute("SELECT e.id,e.task_id,e.kind,e.payload,e.received,o.event_type,t.project_id,t.supervisor_id,t.supervisor_generation FROM events e JOIN outbox o ON o.id='worker:'||e.id JOIN tasks t ON t.id=e.task_id WHERE e.id=?", (event_id,)).fetchone()
                if not row or row["project_id"] != sup["project_id"] or row["supervisor_id"] != supervisor_id or row["supervisor_generation"] != sup["generation"]:
                    raise ValueError("event is not authoritative for this supervisor")
                if receipt.get("event_type") != row["event_type"]:
                    raise ValueError("receipt type mismatch")
                body = f"{supervisor_id}\n{event_id}\n{row['event_type']}\n{issued}".encode()
                expected = hmac.new(token.encode(), body, hashlib.sha256).hexdigest()
                if not isinstance(signature, str) or not hmac.compare_digest(expected, signature):
                    raise ValueError("supervisor receipt authentication failed")
                # Both replay fences commit together, so a wrong-project wake
                # cannot consume a valid mail receipt and races have one winner.
                self.db.execute("INSERT INTO consumed_receipt_events(event_id,consumed) VALUES(?,?)", (event_id, self.clock()))
                self.db.execute("INSERT INTO consumed_receipts(token_id,event_id,consumed) VALUES(?,?,?)", (receipt["token_id"], event_id, self.clock()))
                self.db.execute("UPDATE supervisors SET heartbeat=? WHERE id=? AND generation=?", (self.clock(), supervisor_id, sup["generation"]))
                self.db.execute("INSERT INTO supervisor_routes VALUES(?,?,?)", (supervisor_id, event_id, self.clock()))
                self._audit("receipt_consumed", event_id, {"actor": "trusted-local-native-bridge", "event_type": row["event_type"]})
                self._audit("supervisor_event_routed", supervisor_id, {"event_id": event_id, "event_type": row["event_type"]})
                self.db.execute("COMMIT")
                result = dict(row); result["payload"] = json.loads(result["payload"]); result.pop("project_id", None)
                return result
            except sqlite3.IntegrityError as exc:
                self.db.execute("ROLLBACK")
                raise ValueError("supervisor receipt replay rejected") from exc
            except Exception:
                if self.db.in_transaction: self.db.execute("ROLLBACK")
                raise

    def authorize_project_write(self, project_id: str, *, actor: str, reason: str, expires_in: int = 86400):
        """Grant time-limited write intent; this is not an isolation guarantee."""
        if not actor.strip() or not reason.strip() or not 1 <= expires_in <= 86400:
            raise ValueError("write authorization requires actor, reason, and expiry within 24 hours")
        project = self.db.execute("SELECT policy FROM projects WHERE id=?", (project_id,)).fetchone()
        if not project: raise ValueError("unknown project")
        if json.loads(project["policy"])["read_only"]:
            raise ValueError("project must be registered writable before authorizing writes")
        now = self.clock()
        self.db.execute("INSERT INTO project_write_authorizations VALUES(?,?,?,?,?) ON CONFLICT(project_id) DO UPDATE SET actor=excluded.actor,reason=excluded.reason,expires=excluded.expires,created=excluded.created",
                        (project_id, actor, reason, now + expires_in, now))
        self._audit("project_write_authorized", project_id, {"actor": actor, "reason": reason, "expires": now + expires_in})

    def revoke_project_write(self, project_id: str, *, actor: str):
        self.db.execute("DELETE FROM project_write_authorizations WHERE project_id=?", (project_id,))
        now = self.clock()
        self.db.execute("UPDATE tasks SET status='cancelled',updated=?,last_error='project write authorization revoked' WHERE project_id=? AND read_only=0 AND status IN ('ready','awaiting_approval')",
                        (now, project_id))
        running = self.db.execute("SELECT id,workflow_id FROM tasks WHERE project_id=? AND read_only=0 AND status='running'", (project_id,)).fetchall()
        for task in running:
            self.db.execute("UPDATE tasks SET status='needs_review',lease_owner=NULL,lease_until=NULL,last_error='project write authorization revoked during dispatch',updated=? WHERE id=?",
                            (now, task["id"]))
            self.db.execute("UPDATE dispatch_intents SET state='ambiguous',claimed=NULL WHERE task_id=?", (task["id"],))
            cancel = getattr(self.worker, "cancel", None)
            if cancel:
                try: cancel(task["id"])
                except Exception as exc: self._audit("workflow_cancel_failed", task["id"], {"error": str(exc)[:500]})
            self._terminate_workflow(task["workflow_id"], "write_authorization_revoked", now)
        self._audit("project_write_revoked", project_id, {"actor": actor})

    def submit(self, project_id: str, intention: str, *, approval_required: bool = True,
               read_only: bool | None = None, max_attempts: int = 3,
               parent_task_id: str | None = None,
               supervisor_id: str | None = None,
               workflow_budget: int | None = None, workflow_max_depth: int | None = None,
               workflow_timeout: int | None = None) -> str:
        if not intention.strip() or len(intention) > self.MAX_INTENTION_CHARS: raise ValueError("intention must contain 1–4000 characters")
        if not 1 <= max_attempts <= 10: raise ValueError("max_attempts must be between 1 and 10")
        own_tx = not self.db.in_transaction
        if own_tx: self.db.execute("BEGIN IMMEDIATE")
        try:
            p = self.db.execute("SELECT policy FROM projects WHERE id=?", (project_id,)).fetchone()
            if p is None: raise ValueError(f"unknown project: {project_id}")
            if supervisor_id is not None:
                supervisor = self.db.execute("SELECT * FROM supervisors WHERE id=? AND project_id=?", (supervisor_id, project_id)).fetchone()
                if (not supervisor or supervisor["revoked"] is not None or supervisor["lifecycle"] != "active"
                        or self.clock()-(supervisor["heartbeat"] if supervisor["heartbeat"] is not None else supervisor["created"]) >= self.supervisor_stale_seconds):
                    raise ValueError("new assignment requires an active supervisor for this project")
            depth, workflow_id = 0, None
            if parent_task_id:
                parent = self.db.execute("SELECT project_id,status,workflow_id,depth,supervisor_id,supervisor_generation FROM tasks WHERE id=?", (parent_task_id,)).fetchone()
                if not parent or parent["project_id"] != project_id or parent["status"] != "succeeded":
                    raise ValueError("dependent task requires a succeeded parent in the same project")
                if supervisor_id is not None and supervisor_id != parent["supervisor_id"]:
                    raise ValueError("dependent task must retain its parent's supervisor ownership")
                if parent["supervisor_id"] is not None:
                    owner=self.db.execute("SELECT * FROM supervisors WHERE id=?",(parent["supervisor_id"],)).fetchone()
                    if (not owner or parent["supervisor_generation"] != owner["generation"] or owner["revoked"] is not None or owner["lifecycle"] != "active"
                        or self.clock()-(owner["heartbeat"] if owner["heartbeat"] is not None else owner["created"]) >= self.supervisor_stale_seconds):
                        raise ValueError("dependent assignment requires an active supervisor lease")
                workflow_id, depth = parent["workflow_id"], parent["depth"] + 1
                workflow = self.db.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
                count = self.db.execute("SELECT count(*) FROM tasks WHERE workflow_id=?", (workflow_id,)).fetchone()[0]
                if not workflow or workflow["project_id"] != project_id or workflow["state"] != "active":
                    raise ValueError("parent workflow is missing or terminated")
                if depth > workflow["max_depth"]: raise ValueError("workflow depth limit reached")
                if self.clock() >= workflow["deadline"]: raise ValueError("workflow deadline passed")
                if count >= workflow["max_tasks"]: raise ValueError("workflow task budget exhausted")
            project_ro = json.loads(p["policy"])["read_only"]
            task_ro = project_ro if read_only is None else bool(read_only)
            if project_ro and not task_ro: raise ValueError("read-only project cannot accept a writable task")
            if not task_ro:
                grant = self.db.execute("SELECT expires FROM project_write_authorizations WHERE project_id=?", (project_id,)).fetchone()
                if not grant or grant["expires"] <= self.clock():
                    raise ValueError("writable task requires current explicit project authorization")
            tid, now = str(uuid.uuid4()), self.clock()
            if workflow_id is None:
                workflow_id = tid
                budget = self.workflow_budget if workflow_budget is None else workflow_budget
                depth_limit = self.workflow_max_depth if workflow_max_depth is None else workflow_max_depth
                timeout = self.workflow_timeout if workflow_timeout is None else workflow_timeout
                if not 1 <= budget <= self.MAX_WORKFLOW_TASKS or not 0 <= depth_limit <= self.MAX_WORKFLOW_DEPTH or not 1 <= timeout <= self.MAX_WORKFLOW_SECONDS:
                    raise ValueError("workflow limits exceed hard caps")
                self.db.execute("INSERT INTO workflows(id,project_id,max_tasks,max_depth,deadline,state,termination_reason,created,updated) VALUES(?,?,?,?,?,?,?,?,?)",
                                (workflow_id, project_id, budget, depth_limit, now + timeout, "active", None, now, now))
            status = "awaiting_approval" if approval_required else "ready"
            if parent_task_id:
                owner_id, owner_generation = parent["supervisor_id"], parent["supervisor_generation"]
            elif supervisor_id is not None:
                owner_id, owner_generation = supervisor_id, supervisor["generation"]
            else:
                owner_id, owner_generation = None, None
            self.db.execute("INSERT INTO tasks(id,project_id,intention,status,approval_required,read_only,max_attempts,available_at,created,updated,parent_task_id,workflow_id,depth,supervisor_id,supervisor_generation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (tid, project_id, intention, status, int(approval_required), int(task_ro), max_attempts, now, now, now, parent_task_id, workflow_id, depth, owner_id, owner_generation))
            self._audit("task_submitted", tid, {"project_id": project_id, "approval_required": approval_required,
                        "read_only": task_ro, "parent_task_id": parent_task_id})
            if own_tx: self.db.execute("COMMIT")
            return tid
        except Exception:
            if own_tx and self.db.in_transaction: self.db.execute("ROLLBACK")
            raise

    def _terminate_workflow(self, workflow_id: str, reason: str, now: float):
        self.db.execute("UPDATE workflows SET state='terminated',termination_reason=?,updated=? WHERE id=? AND state='active'",
                        (reason, now, workflow_id))
        self.db.execute("UPDATE tasks SET status='cancelled',updated=?,last_error=? WHERE workflow_id=? AND status IN ('ready','awaiting_approval')",
                        (now, "workflow terminated: " + reason, workflow_id))
        running = self.db.execute("SELECT id FROM tasks WHERE workflow_id=? AND status='running'", (workflow_id,)).fetchall()
        for task in running:
            self.db.execute("UPDATE tasks SET status='needs_review',lease_owner=NULL,lease_until=NULL,last_error=?,updated=? WHERE id=?",
                            ("workflow terminated during dispatch: " + reason, now, task["id"]))
            self.db.execute("UPDATE dispatch_intents SET state='ambiguous',claimed=NULL WHERE task_id=?", (task["id"],))
            cancel = getattr(self.worker, "cancel", None)
            if cancel:
                try: cancel(task["id"])
                except Exception as exc:
                    self._audit("workflow_cancel_failed", task["id"], {"error": str(exc)[:500]})
        self._audit("workflow_terminated", workflow_id, {"reason": reason})

    def approve(self, task_id: str, *, actor: str = "local-operator"):
        now=self.clock()
        cur = self.db.execute("UPDATE tasks SET status='ready',approval_required=0,updated=? WHERE id=? AND status='awaiting_approval' AND (supervisor_id IS NULL OR EXISTS (SELECT 1 FROM supervisors s WHERE s.id=tasks.supervisor_id AND s.generation=tasks.supervisor_generation AND s.revoked IS NULL AND s.lifecycle='active' AND COALESCE(s.heartbeat,s.created)+?>?))",
                              (now, task_id, self.supervisor_stale_seconds, now))
        if not cur.rowcount: raise ValueError("task is not awaiting approval")
        self._audit("task_approved", task_id, {"actor": actor})

    def stop(self, enabled: bool = True):
        self.db.execute("UPDATE control SET value=? WHERE key='emergency_stop'", ("1" if enabled else "0",))
        self._audit("emergency_stop_" + ("enabled" if enabled else "disabled"))

    def terminate_workflow(self, workflow_id: str, *, actor: str, reason: str):
        if not actor.strip() or not reason.strip(): raise ValueError("actor and termination reason are required")
        own_tx = not self.db.in_transaction
        if own_tx: self.db.execute("BEGIN IMMEDIATE")
        try:
            workflow = self.db.execute("SELECT state FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            if not workflow: raise ValueError("unknown workflow")
            if workflow["state"] == "active": self._terminate_workflow(workflow_id, "operator: " + reason[:200], self.clock())
            self._audit("workflow_terminated_by_operator", workflow_id, {"actor": actor, "reason": reason[:200]})
            if own_tx: self.db.execute("COMMIT")
        except Exception:
            if own_tx and self.db.in_transaction: self.db.execute("ROLLBACK")
            raise

    def ingest_event(self, event_id: str, task_id: str, kind: str, payload: dict[str, Any] | None = None) -> bool:
        with self._lock:
            return self._ingest_event(event_id, task_id, kind, payload)

    def _ingest_event(self, event_id: str, task_id: str, kind: str, payload: dict[str, Any] | None = None) -> bool:
        if kind not in ("completed", "failed"): raise ValueError("event kind must be completed or failed")
        if not event_id: raise ValueError("event id is required")
        payload = payload or {}
        if not isinstance(payload, dict): raise ValueError("event payload must be an object")
        if len(json.dumps(payload, sort_keys=True).encode("utf-8")) > self.MAX_EVENT_BYTES:
            raise ValueError("event payload exceeds the 1 MB limit")
        now = self.clock()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            accepted = False
            task = self.db.execute("SELECT status,attempts,max_attempts,lease_until,workflow_id,supervisor_id,supervisor_generation FROM tasks WHERE id=?", (task_id,)).fetchone()
            if not task: raise ValueError("unknown task")
            try:
                self.db.execute("INSERT INTO events VALUES(?,?,?,?,?)", (event_id, task_id, kind, json.dumps(payload, sort_keys=True), now))
            except sqlite3.IntegrityError:
                self._audit("duplicate_event_ignored", task_id, {"event_id": event_id})
                self.db.execute("COMMIT"); return False
            # Adapter callbacks are fenced to the attempt that produced them. A
            # late result from an earlier retry must never finish the new lease.
            event_attempt = payload.get("attempt")
            owner_current = True
            if task["supervisor_id"] is not None:
                owner = self.db.execute("SELECT * FROM supervisors WHERE id=?", (task["supervisor_id"],)).fetchone()
                owner_current = bool(owner and task["supervisor_generation"] == owner["generation"]
                                     and self._supervisor_is_live(owner, now, allow_recovery=True))
            if (task["status"] == "running" and task["lease_until"] is not None and task["lease_until"] > now and owner_current
                    and (event_attempt is None or event_attempt == task["attempts"])):
                accepted = True
                if kind == "completed":
                    decision = payload.get("decision")
                    if decision not in (None, "continue", "stop", "needs_review"):
                        raise ValueError("invalid completion decision")
                    resolved_decision = decision or "continue"
                    resolved_status = "needs_review" if resolved_decision == "needs_review" else "succeeded"
                    self.db.execute("UPDATE tasks SET status=?,decision=?,lease_owner=NULL,lease_until=NULL,updated=? WHERE id=?",
                                    (resolved_status, resolved_decision, now, task_id))
                    # A structured decision may define bounded dependent work.
                    steps = payload.get("next_steps", []) if resolved_decision == "continue" else []
                    if not isinstance(steps, list) or len(steps) > 10:
                        raise ValueError("next_steps must be a list of at most 10 items")
                    workflow = self.db.execute("SELECT * FROM workflows WHERE id=?", (task["workflow_id"],)).fetchone()
                    task_count = self.db.execute("SELECT count(*) FROM tasks WHERE workflow_id=?", (task["workflow_id"],)).fetchone()[0]
                    if self.clock() >= workflow["deadline"]:
                        self._terminate_workflow(task["workflow_id"], "deadline_exceeded", now)
                    elif resolved_decision == "stop":
                        self._terminate_workflow(task["workflow_id"], "worker_stop", now)
                    elif resolved_decision == "needs_review":
                        self._terminate_workflow(task["workflow_id"], "needs_review", now)
                    elif workflow["state"] != "active":
                        self._audit("workflow_descendants_rejected", task["workflow_id"], {"reason": workflow["termination_reason"]})
                    elif self.db.execute("SELECT depth FROM tasks WHERE id=?", (task_id,)).fetchone()[0] >= workflow["max_depth"] and steps:
                        self._terminate_workflow(task["workflow_id"], "depth_limit", now)
                    elif task_count + len(steps) > workflow["max_tasks"]:
                        self._terminate_workflow(task["workflow_id"], "task_budget_exhausted", now)
                    owner_id=self.db.execute("SELECT supervisor_id FROM tasks WHERE id=?",(task_id,)).fetchone()[0]
                    owner_live=True
                    if owner_id:
                        owner=self.db.execute("SELECT * FROM supervisors WHERE id=?",(owner_id,)).fetchone()
                        owner_live=bool(owner and self.db.execute("SELECT supervisor_generation FROM tasks WHERE id=?",(task_id,)).fetchone()[0] == owner["generation"] and owner["revoked"] is None and owner["lifecycle"]=="active"
                            and self.clock()-(owner["heartbeat"] if owner["heartbeat"] is not None else owner["created"]) < self.supervisor_stale_seconds)
                        if not owner_live and steps:
                            self._audit("supervisor_descendants_held",owner_id,{"parent_task_id":task_id,"reason":"supervisor lease inactive"})
                    active_workflow=self.db.execute("SELECT state FROM workflows WHERE id=?", (task["workflow_id"],)).fetchone()[0] == "active"
                    for step in steps if active_workflow and owner_live else []:
                        if not isinstance(step, dict) or set(step) - {"intention", "approval_required", "read_only"} or not isinstance(step.get("intention"), str) or not step["intention"].strip() or len(step["intention"]) > 2000:
                            raise ValueError("each next step needs an intention and supported options")
                        preapproved = (self.allow_preapproved_read_only_steps and
                                       step.get("approval_required") is False and
                                       step.get("read_only", True) is True)
                        self.submit(self.db.execute("SELECT project_id FROM tasks WHERE id=?", (task_id,)).fetchone()[0],
                                    step["intention"], approval_required=not preapproved,
                                    read_only=True, parent_task_id=task_id)
                    self._audit("task_decided", task_id, {"decision": resolved_decision, "status": resolved_status, "scheduled_steps": len(steps)})
                else:
                    # A launched adapter may have made partial or unobservable
                    # progress. Retrying a failure callback is unsafe without a
                    # proof that dispatch never started.
                    self.db.execute("UPDATE tasks SET status='needs_review',lease_owner=NULL,lease_until=NULL,last_error=?,updated=? WHERE id=?",
                                    (str(payload.get("error", "worker reported failure"))[:1000], now, task_id))
                    self.db.execute("UPDATE dispatch_intents SET state='ambiguous',claimed=NULL WHERE task_id=?", (task_id,))
                    self._audit("dispatch_ambiguous", task_id, {"reason": "worker_failure_event"})
                    self._terminate_workflow(task["workflow_id"], "worker_failure", now)
            else:
                self._audit("worker_event_stale", task_id, {"event_id": event_id, "kind": kind,
                            "attempt": event_attempt, "current_attempt": task["attempts"]})
            if accepted:
                decision = payload.get("decision")
                # Routine worker progress stays internal. Only escalations,
                # explicit stop decisions, or explicitly supervisor-required
                # completions are eligible for external notification.
                notify = (kind == "failed" or decision in ("stop", "needs_review")
                          or payload.get("supervisor_required") is True)
                if notify:
                    event_type = "worker.escalation" if kind == "failed" or decision == "needs_review" else "worker.supervisor_decision"
                    self.db.execute("INSERT OR IGNORE INTO outbox(id,event_type,payload,created) VALUES(?,?,?,?)",
                                    ("worker:" + event_id, event_type,
                                     json.dumps({"event_id": event_id, "task_id": task_id, "payload": payload}, sort_keys=True), now))
            self._audit("worker_event_ingested", task_id, {"event_id": event_id, "kind": kind})
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK"); raise
        self.reconcile()
        # Terminal callbacks never authorize deletion. The operator must record
        # exact-attempt inactivity evidence, then request cleanup explicitly.
        return True

    def consume_receipt(self, event_id: str, token_id: str, *, expected_type: str, actor: str = "local-receipt") -> dict[str, Any]:
        """Resolve an authenticated receipt against authoritative local event data, once."""
        if not isinstance(event_id, str) or not event_id or len(event_id) > 200 or not isinstance(token_id, str) or not token_id or len(token_id) > 128:
            raise ValueError("invalid receipt identity")
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                row = self.db.execute("SELECT e.id,e.task_id,e.kind,e.payload,e.received,o.event_type FROM events e JOIN outbox o ON o.id='worker:'||e.id WHERE e.id=?", (event_id,)).fetchone()
                if not row: raise ValueError("receipt event is missing or not supervisor-notifiable")
                if row["event_type"] != expected_type: raise ValueError("receipt event type mismatch")
                # Deduplicate by authoritative event identity, not just token.
                # Reissuing a valid receipt for the same event cannot reset ack state.
                self.db.execute("INSERT INTO consumed_receipt_events(event_id,consumed) VALUES(?,?)", (event_id, self.clock()))
                self.db.execute("INSERT INTO consumed_receipts(token_id,event_id,consumed) VALUES(?,?,?)", (token_id, event_id, self.clock()))
                self._audit("receipt_consumed", event_id, {"actor": actor, "event_type": row["event_type"]})
                self.db.execute("COMMIT")
                result = dict(row); result["payload"] = json.loads(result["payload"])
                return result
            except sqlite3.IntegrityError as exc:
                self.db.execute("ROLLBACK")
                raise ValueError("receipt event already acknowledged or token replay rejected") from exc
            except Exception:
                if self.db.in_transaction: self.db.execute("ROLLBACK")
                raise

    def _retry(self, task_id, attempts, maximum, message):
        now = self.clock()
        status, available = ("failed", now) if attempts >= maximum else ("ready", now + min(3600, 2 ** max(0, attempts - 1)))
        self.db.execute("UPDATE tasks SET status=?,available_at=?,lease_owner=NULL,lease_until=NULL,last_error=?,updated=? WHERE id=?",
                        (status, available, message[:1000], now, task_id))
        self.db.execute("UPDATE dispatch_intents SET state='retry',delivered=NULL WHERE task_id=?", (task_id,))
        self.db.execute("UPDATE dispatch_intents SET claimed=NULL WHERE task_id=?", (task_id,))

    def reconcile(self, *, owner: str | None = None) -> list[str]:
        """Create durable intents transactionally, then deliver pending intents."""
        with self._lock:
            return self._reconcile(owner=owner)

    def _reconcile(self, *, owner: str | None = None) -> list[str]:
        owner = owner or f"local-{uuid.uuid4()}"
        # Reaping runs on the existing local reconciliation cadence. It does
        # not renew leases; only the authenticated supervisor path can do so.
        self._reap_supervisors(actor="local-reconciliation-cadence")
        now = self.clock()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.db.execute("SELECT value FROM control WHERE key='emergency_stop'").fetchone()[0] == "1":
                self.db.execute("COMMIT"); return []
            unauthorized = self.db.execute("SELECT DISTINCT t.workflow_id FROM tasks t LEFT JOIN project_write_authorizations a ON a.project_id=t.project_id WHERE t.read_only=0 AND t.status IN ('ready','awaiting_approval','running') AND (a.expires IS NULL OR a.expires<=?)", (now,)).fetchall()
            for workflow in unauthorized:
                self._terminate_workflow(workflow["workflow_id"], "write_authorization_expired", now)
            expired_workflows = self.db.execute("SELECT id FROM workflows WHERE state='active' AND deadline<=?", (now,)).fetchall()
            for workflow in expired_workflows:
                self._terminate_workflow(workflow["id"], "deadline_exceeded", now)
            expired = self.db.execute("SELECT id,attempts,max_attempts,workflow_id FROM tasks WHERE status='running' AND lease_until<=?", (now,)).fetchall()
            for r in expired:
                if not self.dry_run:
                    # Once real dispatch may have started, replay cannot be proven safe.
                    self.db.execute("UPDATE tasks SET status='needs_review',lease_owner=NULL,lease_until=NULL,last_error='real dispatch outcome is ambiguous',updated=? WHERE id=?", (now,r["id"]))
                    self.db.execute("UPDATE dispatch_intents SET state='ambiguous',claimed=NULL WHERE task_id=?", (r["id"],))
                    self._audit("dispatch_ambiguous",r["id"],{})
                    self._terminate_workflow(r["workflow_id"], "task_lease_expired", now)
                else: self._retry(r["id"], r["attempts"], r["max_attempts"], "worker lease expired")
            if self.dry_run:
                self.db.execute("UPDATE dispatch_intents SET state='pending',claimed=NULL WHERE state='dispatching' AND claimed<=?", (now-self.lease_seconds,))
            # Recover dispatch intents left pending by a process crash, preserving their stable idempotency key.
            active = self.db.execute("SELECT count(*) FROM tasks WHERE status='running'").fetchone()[0]
            remaining = (self.max_dispatches - self._dispatch_count) if self.max_dispatches is not None else self.max_concurrency
            slots = max(0, min(self.max_concurrency-active, remaining))
            rows = self.db.execute("SELECT t.*,p.root,CASE WHEN a.expires>? THEN 1 ELSE 0 END AS write_authorized FROM tasks t JOIN projects p ON p.id=t.project_id JOIN workflows w ON w.id=t.workflow_id LEFT JOIN project_write_authorizations a ON a.project_id=p.id LEFT JOIN supervisors s ON s.id=t.supervisor_id WHERE t.status='ready' AND t.available_at<=? AND w.state='active' AND w.deadline>? AND (t.supervisor_id IS NULL OR (t.supervisor_generation=s.generation AND s.revoked IS NULL AND s.lifecycle='active' AND COALESCE(s.heartbeat,s.created)+?>?)) ORDER BY t.created LIMIT ?", (now, now, now, self.supervisor_stale_seconds, now, slots)).fetchall()
            for r in rows:
                claimed_task = self.db.execute("UPDATE tasks SET status='running',attempts=attempts+1,lease_owner=?,lease_until=?,updated=? WHERE id=? AND status='ready' AND EXISTS (SELECT 1 FROM workflows w WHERE w.id=tasks.workflow_id AND w.state='active' AND w.deadline>?)",
                                (owner, now+self.lease_seconds, now, r["id"], now)).rowcount
                if not claimed_task: continue
                self.db.execute("INSERT INTO dispatch_intents(task_id,intent_key,state,created) VALUES(?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET state='pending',created=excluded.created",
                                (r["id"], r["id"], "pending", now))
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK"); raise
        if self.fault_hook: self.fault_hook("after_intent_commit")
        out = self.db.execute("SELECT t.*,p.root,d.intent_key,CASE WHEN a.expires>? THEN 1 ELSE 0 END AS write_authorized FROM dispatch_intents d JOIN tasks t ON t.id=d.task_id JOIN projects p ON p.id=t.project_id LEFT JOIN project_write_authorizations a ON a.project_id=p.id WHERE d.state='pending' AND t.status='running' ORDER BY d.created", (self.clock(),)).fetchall()
        dispatched=[]
        for r in out:
            # Keep this transaction open through the adapter's synchronous launch
            # handoff. Termination either commits first (and suppresses launch), or
            # waits until launch is recorded and then fences it as ambiguous.
            self.db.execute("BEGIN IMMEDIATE")
            claimed = self.db.execute("UPDATE dispatch_intents SET state='dispatching',claimed=? WHERE task_id=? AND state='pending'", (self.clock(),r["id"])).rowcount
            if not claimed:
                self.db.execute("COMMIT")
                continue
            try:
                now = self.clock()
                workflow = self.db.execute("SELECT w.* FROM workflows w JOIN tasks t ON t.workflow_id=w.id WHERE t.id=?", (r["id"],)).fetchone()
                task_state = self.db.execute("SELECT status,approval_required,read_only FROM tasks WHERE id=?", (r["id"],)).fetchone()
                stopped = self.db.execute("SELECT value FROM control WHERE key='emergency_stop'").fetchone()[0] == "1"
                auth = self.db.execute("SELECT expires FROM project_write_authorizations WHERE project_id=?", (r["project_id"],)).fetchone() if not r["read_only"] else True
                supervisor_live=self._supervisor_assignment_active(r["id"],now)
                permitted = (workflow and workflow["state"] == "active" and workflow["deadline"] > now
                             and workflow["dispatch_count"] < workflow["max_tasks"] and task_state
                             and task_state["status"] == "running" and not task_state["approval_required"]
                             and not stopped and supervisor_live and (r["read_only"] or (auth and auth["expires"] > now)))
                if not permitted:
                    self.db.execute("UPDATE dispatch_intents SET state='suppressed',claimed=NULL WHERE task_id=?", (r["id"],))
                    self.db.execute("UPDATE tasks SET status='cancelled',lease_owner=NULL,lease_until=NULL,last_error='dispatch suppressed by final safety fence',updated=? WHERE id=? AND status='running'", (now, r["id"]))
                    if workflow and workflow["state"] == "active":
                        reason = "emergency_stop" if stopped else "write_authorization_expired" if not r["read_only"] and not (auth and auth["expires"] > now) else "dispatch_fence_rejected"
                        self._terminate_workflow(workflow["id"], reason, now)
                    self.db.execute("COMMIT")
                    continue
                self.db.execute("UPDATE workflows SET dispatch_count=dispatch_count+1,updated=? WHERE id=?", (now, workflow["id"]))
                self._dispatch_count += 1
                # Commit attempt classification with the durable dispatch
                # ambiguity marker before opening the final launch fence.
                classification = "dry_run" if self.dry_run else ("process_free" if getattr(self.worker, "process_free", False) is True else "intent")
                self.db.execute("INSERT INTO worker_attempt_execution(task_id,attempt,generation,classification,updated,detail) VALUES(?,?,?,?,?,?) ON CONFLICT(task_id,attempt,generation) DO UPDATE SET classification=excluded.classification,updated=excluded.updated,detail=excluded.detail",
                                (r["id"], r["attempts"], r["supervisor_generation"] or 1, classification, now, "explicit adapter process_free declaration" if classification == "process_free" else "durable pre-dispatch intent"))
                # Persist ambiguity before crossing the process boundary. A hard
                # supervisor crash after this point must never make the intent
                # replayable, even if the launch-fence transaction rolls back.
                self.db.execute("COMMIT")
                self.db.execute("BEGIN IMMEDIATE")
                workflow = self.db.execute("SELECT * FROM workflows WHERE id=?", (workflow["id"],)).fetchone()
                task_state = self.db.execute("SELECT status,approval_required FROM tasks WHERE id=?", (r["id"],)).fetchone()
                now = self.clock()
                stopped = self.db.execute("SELECT value FROM control WHERE key='emergency_stop'").fetchone()[0] == "1"
                auth = self.db.execute("SELECT expires FROM project_write_authorizations WHERE project_id=?", (r["project_id"],)).fetchone() if not r["read_only"] else True
                supervisor_live=self._supervisor_assignment_active(r["id"],now)
                if (not workflow or workflow["state"] != "active" or workflow["deadline"] <= now
                        or not task_state or task_state["status"] != "running" or task_state["approval_required"]
                        or stopped or not supervisor_live or (not r["read_only"] and not (auth and auth["expires"] > now))):
                    if workflow and workflow["state"] == "active" and task_state and task_state["status"] == "running":
                        self.db.execute("UPDATE dispatch_intents SET state='suppressed',claimed=NULL WHERE task_id=? AND state='dispatching'", (r["id"],))
                        self.db.execute("UPDATE tasks SET status='cancelled',lease_owner=NULL,lease_until=NULL,last_error='launch fence rejected dispatch',updated=? WHERE id=? AND status='running'", (now, r["id"]))
                        if stopped or (not r["read_only"] and not (auth and auth["expires"] > now)):
                            reason = "emergency_stop" if stopped else "write_authorization_expired"
                            self._terminate_workflow(workflow["id"], reason, now)
                    self.db.execute("COMMIT")
                    continue
                task = {"id":r["id"], "attempt":r["attempts"], "generation":r["supervisor_generation"] or 1, "idempotency_key":r["intent_key"], "project_id":r["project_id"], "project_root":r["root"],
                        "intention":r["intention"], "read_only":bool(r["read_only"]), "write_authorized":bool(r["write_authorized"]), "dry_run":self.dry_run,
                        "project_snapshot":self.project_snapshot(r["project_id"]) }
                # The classification above committed before opening this
                # final launch fence; no missing PID row is treated as proof.
                if not self.dry_run: self.worker.dispatch(task)
                if self.fault_hook: self.fault_hook("after_external_dispatch")
                self.db.execute("UPDATE dispatch_intents SET state='delivered',delivered=?,claimed=NULL WHERE task_id=? AND state='dispatching'", (self.clock(),r["id"]))
                if self.dry_run:
                    self.db.execute("UPDATE tasks SET status='dry_run',lease_owner=NULL,lease_until=NULL,updated=? WHERE id=?", (self.clock(),r["id"]))
                self._audit("task_dispatched" if not self.dry_run else "task_dry_run", r["id"], {"owner":owner,"dry_run":self.dry_run})
                self.db.execute("COMMIT")
                dispatched.append(r["id"])
            except Exception as e:
                # Once claimed for delivery, even a thrown exception may follow
                # a partial external side effect. Do not automatically replay.
                self.db.execute("UPDATE tasks SET status='needs_review',lease_owner=NULL,lease_until=NULL,last_error=?,updated=? WHERE id=?",
                                (str(e)[:1000], self.clock(), r["id"]))
                self.db.execute("UPDATE dispatch_intents SET state='ambiguous',claimed=NULL WHERE task_id=?", (r["id"],))
                self._audit("dispatch_ambiguous",r["id"],{"error":str(e)[:1000]})
                self._terminate_workflow(r["workflow_id"], "dispatch_exception", self.clock())
                self.db.execute("COMMIT")
            except BaseException:
                # Test hooks and orderly interpreter exits model a hard stop; keep
                # the already committed dispatching marker and release the lock.
                if self.db.in_transaction: self.db.execute("ROLLBACK")
                raise
        # Periodically retry only terminal attempts with durable explicit
        # inactivity evidence. Ambiguous/orphaned tasks remain untouched.
        with self._lock:
            resource_cleanup.recover_verified(self, automatic=self.automatic_worker_cleanup)
        return dispatched

    def get_task(self, task_id):
        row=self.db.execute("SELECT * FROM tasks WHERE id=?",(task_id,)).fetchone()
        return dict(row) if row else None

    def snapshot(self):
        return {"emergency_stop":self.db.execute("SELECT value FROM control WHERE key='emergency_stop'").fetchone()[0]=="1",
                "projects":[dict(r) for r in self.db.execute("SELECT * FROM projects ORDER BY id")],
                "tasks":[dict(r) for r in self.db.execute("SELECT * FROM tasks ORDER BY created")],
                "events":[dict(r) for r in self.db.execute("SELECT * FROM events ORDER BY received")],
                "dispatch_intents":[dict(r) for r in self.db.execute("SELECT * FROM dispatch_intents ORDER BY created")],
                "outbox":[dict(r) for r in self.db.execute("SELECT * FROM outbox ORDER BY created")],
                "workflows":[dict(r) for r in self.db.execute("SELECT * FROM workflows ORDER BY created")],
                "write_authorizations":[dict(r) for r in self.db.execute("SELECT * FROM project_write_authorizations ORDER BY created")],
                "audit":[dict(r) for r in self.db.execute("SELECT * FROM audit ORDER BY id")], "dry_run":self.dry_run}
