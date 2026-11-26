"""SQLite-backed workflow engine with a durable dispatch intent journal."""
from __future__ import annotations

import json
import sqlite3
import time
import threading
import uuid
from pathlib import Path
from typing import Any, Protocol


class WorkerAdapter(Protocol):
    """Adapter dispatch returns synchronously after its launch handoff only.

    Engine holds its cross-process launch fence during this call. Result callbacks
    should be asynchronous (or delivered after dispatch returns), never inline.
    """
    def dispatch(self, task: dict[str, Any]) -> None: ...


class DryRunWorker:
    """Safe default adapter: no external action is performed."""
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
                 workflow_timeout: int = 3600):
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

    def close(self): self.db.close()

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
        columns = {r[1] for r in self.db.execute("PRAGMA table_info(dispatch_intents)")}
        if "claimed" not in columns: self.db.execute("ALTER TABLE dispatch_intents ADD COLUMN claimed REAL")
        outbox_columns = {r[1] for r in self.db.execute("PRAGMA table_info(outbox)")}
        for name, declaration in (("available_at", "REAL NOT NULL DEFAULT 0"),
                                  ("attempts", "INTEGER NOT NULL DEFAULT 0"),
                                  ("lease_token", "TEXT"), ("lease_until", "REAL")):
            if name not in outbox_columns:
                self.db.execute(f"ALTER TABLE outbox ADD COLUMN {name} {declaration}")
        task_columns = {r[1] for r in self.db.execute("PRAGMA table_info(tasks)")}
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
                     event_types: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
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
        if not self.db.execute("SELECT 1 FROM projects WHERE id=?",(project_id,)).fetchone(): raise ValueError("unknown project")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute("INSERT INTO supervisors VALUES(?,?,?,?,?,NULL)",(supervisor_id,project_id,worker_id,token_hash,self.clock()))
            self._audit("supervisor_registered",supervisor_id,{"project_id":project_id,"worker_id":worker_id,"trusted_local":True})
            self.db.execute("COMMIT")
        except Exception: self.db.execute("ROLLBACK"); raise

    def list_supervisors(self):
        return [dict(r) for r in self.db.execute("SELECT id,project_id,worker_id,created,revoked FROM supervisors ORDER BY id")]

    def supervisor_status(self, supervisor_id: str):
        row=self.db.execute("SELECT id,project_id,worker_id,created,revoked FROM supervisors WHERE id=?",(supervisor_id,)).fetchone()
        if not row: raise ValueError("unknown supervisor")
        result=dict(row); result["routed_events"]=self.db.execute("SELECT COUNT(*) FROM supervisor_routes WHERE supervisor_id=?",(supervisor_id,)).fetchone()[0]
        return result

    def unregister_supervisor(self, supervisor_id: str, *, actor: str):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed=self.db.execute("UPDATE supervisors SET revoked=? WHERE id=? AND revoked IS NULL",(self.clock(),supervisor_id)).rowcount
            if not changed: raise ValueError("unknown or revoked supervisor")
            self._audit("supervisor_revoked",supervisor_id,{"actor":actor}); self.db.execute("COMMIT")
        except Exception: self.db.execute("ROLLBACK"); raise

    def route_supervisor_event(self, supervisor_id: str, event_id: str, *, issued: int, signature: str, token: str):
        """Authenticate a supervisor, then resolve only matching authoritative event data."""
        import hashlib, hmac
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                sup=self.db.execute("SELECT * FROM supervisors WHERE id=? AND revoked IS NULL",(supervisor_id,)).fetchone()
                if not sup or not isinstance(token,str) or not 32 <= len(token) <= 1024 or not hmac.compare_digest(sup["token_hash"],hashlib.sha256(token.encode()).hexdigest()): raise ValueError("unknown or revoked supervisor")
                if not isinstance(issued,int) or abs(self.clock()-issued)>300: raise ValueError("stale supervisor receipt")
                row=self.db.execute("SELECT e.id,e.task_id,e.kind,e.payload,e.received,o.event_type,t.project_id FROM events e JOIN outbox o ON o.id='worker:'||e.id JOIN tasks t ON t.id=e.task_id WHERE e.id=?",(event_id,)).fetchone()
                if not row or row["project_id"]!=sup["project_id"]: raise ValueError("event is not authoritative for this supervisor")
                body=f"{supervisor_id}\n{event_id}\n{row['event_type']}\n{issued}".encode()
                expected=hmac.new(token.encode(),body,hashlib.sha256).hexdigest()
                if not isinstance(signature,str) or not hmac.compare_digest(expected,signature): raise ValueError("supervisor receipt authentication failed")
                self.db.execute("INSERT INTO supervisor_routes VALUES(?,?,?)",(supervisor_id,event_id,self.clock()))
                self._audit("supervisor_event_routed",supervisor_id,{"event_id":event_id,"event_type":row["event_type"]})
                self.db.execute("COMMIT")
                result=dict(row); result["payload"]=json.loads(result["payload"]); result.pop("project_id",None); return result
            except sqlite3.IntegrityError as exc:
                self.db.execute("ROLLBACK"); raise ValueError("supervisor receipt replay rejected") from exc
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
               workflow_budget: int | None = None, workflow_max_depth: int | None = None,
               workflow_timeout: int | None = None) -> str:
        if not intention.strip() or len(intention) > self.MAX_INTENTION_CHARS: raise ValueError("intention must contain 1–4000 characters")
        if not 1 <= max_attempts <= 10: raise ValueError("max_attempts must be between 1 and 10")
        own_tx = not self.db.in_transaction
        if own_tx: self.db.execute("BEGIN IMMEDIATE")
        try:
            p = self.db.execute("SELECT policy FROM projects WHERE id=?", (project_id,)).fetchone()
            if p is None: raise ValueError(f"unknown project: {project_id}")
            depth, workflow_id = 0, None
            if parent_task_id:
                parent = self.db.execute("SELECT project_id,status,workflow_id,depth FROM tasks WHERE id=?", (parent_task_id,)).fetchone()
                if not parent or parent["project_id"] != project_id or parent["status"] != "succeeded":
                    raise ValueError("dependent task requires a succeeded parent in the same project")
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
            self.db.execute("INSERT INTO tasks(id,project_id,intention,status,approval_required,read_only,max_attempts,available_at,created,updated,parent_task_id,workflow_id,depth) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (tid, project_id, intention, status, int(approval_required), int(task_ro), max_attempts, now, now, now, parent_task_id, workflow_id, depth))
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
        cur = self.db.execute("UPDATE tasks SET status='ready',approval_required=0,updated=? WHERE id=? AND status='awaiting_approval'",
                              (self.clock(), task_id))
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
            task = self.db.execute("SELECT status,attempts,max_attempts,lease_until,workflow_id FROM tasks WHERE id=?", (task_id,)).fetchone()
            if not task: raise ValueError("unknown task")
            try:
                self.db.execute("INSERT INTO events VALUES(?,?,?,?,?)", (event_id, task_id, kind, json.dumps(payload, sort_keys=True), now))
            except sqlite3.IntegrityError:
                self._audit("duplicate_event_ignored", task_id, {"event_id": event_id})
                self.db.execute("COMMIT"); return False
            # Adapter callbacks are fenced to the attempt that produced them. A
            # late result from an earlier retry must never finish the new lease.
            event_attempt = payload.get("attempt")
            if (task["status"] == "running" and task["lease_until"] is not None and task["lease_until"] > now
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
                    for step in steps if self.db.execute("SELECT state FROM workflows WHERE id=?", (task["workflow_id"],)).fetchone()[0] == "active" else []:
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
            rows = self.db.execute("SELECT t.*,p.root,CASE WHEN a.expires>? THEN 1 ELSE 0 END AS write_authorized FROM tasks t JOIN projects p ON p.id=t.project_id JOIN workflows w ON w.id=t.workflow_id LEFT JOIN project_write_authorizations a ON a.project_id=p.id WHERE t.status='ready' AND t.available_at<=? AND w.state='active' AND w.deadline>? ORDER BY t.created LIMIT ?", (now, now, now, slots)).fetchall()
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
                permitted = (workflow and workflow["state"] == "active" and workflow["deadline"] > now
                             and workflow["dispatch_count"] < workflow["max_tasks"] and task_state
                             and task_state["status"] == "running" and not task_state["approval_required"]
                             and not stopped and (r["read_only"] or (auth and auth["expires"] > now)))
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
                if (not workflow or workflow["state"] != "active" or workflow["deadline"] <= now
                        or not task_state or task_state["status"] != "running" or task_state["approval_required"]
                        or stopped or (not r["read_only"] and not (auth and auth["expires"] > now))):
                    if workflow and workflow["state"] == "active" and task_state and task_state["status"] == "running":
                        self.db.execute("UPDATE dispatch_intents SET state='suppressed',claimed=NULL WHERE task_id=? AND state='dispatching'", (r["id"],))
                        self.db.execute("UPDATE tasks SET status='cancelled',lease_owner=NULL,lease_until=NULL,last_error='launch fence rejected dispatch',updated=? WHERE id=? AND status='running'", (now, r["id"]))
                        if stopped or (not r["read_only"] and not (auth and auth["expires"] > now)):
                            reason = "emergency_stop" if stopped else "write_authorization_expired"
                            self._terminate_workflow(workflow["id"], reason, now)
                    self.db.execute("COMMIT")
                    continue
                task = {"id":r["id"], "attempt":r["attempts"], "idempotency_key":r["intent_key"], "project_id":r["project_id"], "project_root":r["root"],
                        "intention":r["intention"], "read_only":bool(r["read_only"]), "write_authorized":bool(r["write_authorized"]), "dry_run":self.dry_run,
                        "project_snapshot":self.project_snapshot(r["project_id"]) }
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
