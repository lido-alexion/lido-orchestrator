import hashlib
import hmac
import contextlib
import io
import sqlite3
import threading
import tempfile
import unittest
from pathlib import Path

from lido_orchestrator.engine import Engine
from lido_orchestrator.cli import main


class SupervisorLifecycleTests(unittest.TestCase):
    def test_cli_does_not_offer_unauthenticated_event_injection(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised: main(["event","event-id","task-id","completed"])
        self.assertEqual(2,raised.exception.code)
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised: main(["bridge-wakeup"])
        self.assertEqual(2,raised.exception.code)

    def test_explicit_pending_adoption_requires_fresh_approval_and_can_complete(self):
        class Worker:
            def dispatch(self,task): pass
        with tempfile.TemporaryDirectory() as tmp:
            now=[1000.0]
            path=Path(tmp)/"state.sqlite"
            engine=Engine(path,clock=lambda:now[0],dry_run=False,worker=Worker(),supervisor_stale_seconds=10,supervisor_inactive_seconds=20,supervisor_orphan_seconds=40)
            try:
                engine.register_project("project",tmp)
                old="a"*48
                engine.register_supervisor("old","project","old-worker",token_hash=hashlib.sha256(old.encode()).hexdigest())
                pending=engine.submit("project","pending work",supervisor_id="old")
                approved=engine.submit("project","formerly approved work",approval_required=False,supervisor_id="old")
                engine.supervisor_lifecycle("old",action="takeover",actor="trusted-local",token_hash=hashlib.sha256(("b"*48).encode()).hexdigest())
                self.assertEqual("needs_review",engine.get_task(approved)["status"])
                adopted_pending=engine.adopt_supervisor_task(pending,"old",actor="trusted-local")
                adopted_approved=engine.adopt_supervisor_task(approved,"old",actor="trusted-local")
                self.assertEqual("awaiting_approval",adopted_pending["status"])
                self.assertEqual("awaiting_approval",adopted_approved["status"])
                self.assertEqual(1,adopted_approved["approval_required"])
                with self.assertRaises(ValueError): engine.supervisor_heartbeat("old",token=old,generation=1)
                self.assertTrue(engine.ingest_event("old-callback",approved,"completed",{"attempt":1,"decision":"stop"}))
                self.assertEqual("awaiting_approval",engine.get_task(approved)["status"])
                engine.approve(pending,actor="reviewer")
                engine.approve(approved,actor="reviewer")
                engine.reconcile()
                for task in (pending,approved):
                    engine.ingest_event("new-"+task,task,"completed",{"attempt":1,"decision":"stop"})
                    self.assertEqual("succeeded",engine.get_task(task)["status"])
                engine.supervisor_lifecycle("old",action="complete",actor="trusted-local")
            finally: engine.close()

    def test_deboarded_id_is_replaced_and_adoption_races_across_engines(self):
        class Worker:
            def dispatch(self,task): pass
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/"state.sqlite"
            first=Engine(path,dry_run=False,worker=Worker(),supervisor_stale_seconds=10,supervisor_inactive_seconds=20,supervisor_orphan_seconds=40)
            second=None
            try:
                first.register_project("project",tmp)
                first.register_supervisor("retired","project","old",token_hash=hashlib.sha256(("a"*48).encode()).hexdigest())
                task=first.submit("project","replacement task",supervisor_id="retired")
                first.supervisor_lifecycle("retired",action="deboard",actor="trusted-local")
                first.register_supervisor("replacement","project","new",token_hash=hashlib.sha256(("b"*48).encode()).hexdigest())
                second=Engine(path,dry_run=False,worker=Worker(),supervisor_stale_seconds=10,supervisor_inactive_seconds=20,supervisor_orphan_seconds=40)
                outcomes=[]
                def adopt(engine):
                    try: outcomes.append(engine.adopt_supervisor_task(task,"replacement",actor="trusted-local")["status"])
                    except ValueError: outcomes.append("lost-race")
                threads=[threading.Thread(target=adopt,args=(engine,)) for engine in (first,second)]
                for thread in threads: thread.start()
                for thread in threads: thread.join(5)
                self.assertEqual(["awaiting_approval","lost-race"],sorted(outcomes))
                row=first.get_task(task)
                self.assertEqual("replacement",row["supervisor_id"])
                self.assertEqual(1,row["supervisor_generation"])
                with self.assertRaisesRegex(ValueError,"unknown or deboarded"):
                    first.supervisor_lifecycle("retired",action="takeover",actor="trusted-local",token_hash=hashlib.sha256(("c"*48).encode()).hexdigest())
                first.approve(task,actor="reviewer")
                first.reconcile()
                first.ingest_event("replacement-done",task,"completed",{"attempt":1,"decision":"stop"})
                self.assertEqual("succeeded",first.get_task(task)["status"])
            finally:
                if second: second.close()
                first.close()

    def test_active_prior_owner_blocks_adoption_and_two_replacements_cannot_retransfer(self):
        class Worker:
            def __init__(self): self.tasks=[]
            def dispatch(self,task): self.tasks.append(task["id"])
        with tempfile.TemporaryDirectory() as tmp:
            now=[1000.0]; path=Path(tmp)/"state.sqlite"
            first=Engine(path,clock=lambda:now[0],dry_run=False,worker=Worker(),supervisor_stale_seconds=10,supervisor_inactive_seconds=20,supervisor_orphan_seconds=40)
            second=third=None
            try:
                first.register_project("project",tmp)
                first.register_supervisor("old","project","old",token_hash=hashlib.sha256(("a"*48).encode()).hexdigest())
                task=first.submit("project","handoff candidate",supervisor_id="old")
                for sid,key in (("replacement-a","b"),("replacement-b","c")):
                    first.register_supervisor(sid,"project",sid,token_hash=hashlib.sha256((key*48).encode()).hexdigest())
                with self.assertRaisesRegex(ValueError,"prior owner is active"):
                    first.adopt_supervisor_task(task,"replacement-a",actor="operator")
                first.supervisor_lifecycle("old",action="deboard",actor="operator")
                second=Engine(path,clock=lambda:now[0],dry_run=False,worker=Worker(),supervisor_stale_seconds=10,supervisor_inactive_seconds=20,supervisor_orphan_seconds=40)
                third=Engine(path,clock=lambda:now[0],dry_run=False,worker=Worker(),supervisor_stale_seconds=10,supervisor_inactive_seconds=20,supervisor_orphan_seconds=40)
                barrier=threading.Barrier(2); outcomes=[]
                def adopt(engine,sid):
                    barrier.wait()
                    try: outcomes.append((sid,engine.adopt_supervisor_task(task,sid,actor="operator")["status"]))
                    except ValueError as exc: outcomes.append((sid,str(exc)))
                threads=[threading.Thread(target=adopt,args=(second,"replacement-a")),threading.Thread(target=adopt,args=(third,"replacement-b"))]
                for thread in threads: thread.start()
                for thread in threads: thread.join(5)
                self.assertEqual(1,sum(result[1]=="awaiting_approval" for result in outcomes),outcomes)
                self.assertEqual(1,sum("prior owner is active" in result[1] for result in outcomes),outcomes)
                row=first.get_task(task)
                self.assertIn(row["supervisor_id"],("replacement-a","replacement-b"))
                self.assertEqual("awaiting_approval",row["status"])
                self.assertEqual(1,row["approval_required"])
                self.assertEqual(0,first.db.execute("SELECT COUNT(*) FROM dispatch_intents WHERE task_id=?",(task,)).fetchone()[0])
                first.approve(task,actor="reviewer")
                first.reconcile()
                owner=first.db.execute("SELECT supervisor_id FROM tasks WHERE id=?",(task,)).fetchone()[0]
                first.ingest_event("handoff-complete",task,"completed",{"attempt":1,"decision":"stop"})
                self.assertEqual("succeeded",first.get_task(task)["status"])
                self.assertEqual(1,first.db.execute("SELECT COUNT(*) FROM events WHERE task_id=?",(task,)).fetchone()[0])
            finally:
                if third: third.close()
                if second: second.close()
                first.close()

    def test_adoption_rejects_ambiguous_dispatch_and_unresolved_dependencies(self):
        class Worker:
            def dispatch(self,task): pass
        with tempfile.TemporaryDirectory() as tmp:
            engine=Engine(Path(tmp)/"state.sqlite",dry_run=False,worker=Worker())
            try:
                engine.register_project("project",tmp)
                engine.register_supervisor("old","project","old",token_hash=hashlib.sha256(("a"*48).encode()).hexdigest())
                engine.register_supervisor("new","project","new",token_hash=hashlib.sha256(("b"*48).encode()).hexdigest())
                parent=engine.submit("project","ambiguous",approval_required=False,supervisor_id="old")
                child=engine.submit("project","dependent")
                # Model a legacy/inconsistent pending dependency. The API will
                # never create this shape because it requires a succeeded parent.
                engine.db.execute("UPDATE tasks SET parent_task_id=? WHERE id=?",(parent,child))
                engine.reconcile()
                engine.supervisor_lifecycle("old",action="takeover",actor="trusted-local",token_hash=hashlib.sha256(("c"*48).encode()).hexdigest())
                with self.assertRaisesRegex(ValueError,"dispatch history"):
                    engine.adopt_supervisor_task(parent,"new",actor="trusted-local")
                with self.assertRaisesRegex(ValueError,"dependency"):
                    engine.adopt_supervisor_task(child,"new",actor="trusted-local")
                self.assertEqual("ambiguous",engine.db.execute("SELECT state FROM dispatch_intents WHERE task_id=?",(parent,)).fetchone()[0])
            finally: engine.close()

    def test_disposition_cancels_only_proven_pending_dependency_tree(self):
        class Worker:
            def dispatch(self,task): pass
        with tempfile.TemporaryDirectory() as tmp:
            engine=Engine(Path(tmp)/"state.sqlite",dry_run=False,worker=Worker())
            try:
                engine.register_project("project",tmp)
                engine.register_supervisor("old","project","old",token_hash=hashlib.sha256(("a"*48).encode()).hexdigest())
                root=engine.submit("project","pending root",supervisor_id="old")
                child=engine.submit("project","synthetic dependent")
                engine.db.execute("UPDATE tasks SET parent_task_id=? WHERE id=?",(root,child))
                self.assertEqual({root,child},set(engine.dispose_supervisor_task(root,actor="operator",reason="obsolete request")))
                self.assertEqual("cancelled",engine.get_task(root)["status"])
                self.assertEqual("cancelled",engine.get_task(child)["status"])
                blocked=engine.submit("project","pending root",supervisor_id="old")
                ambiguous=engine.submit("project","synthetic in-flight",approval_required=False,supervisor_id="old")
                engine.db.execute("UPDATE tasks SET parent_task_id=? WHERE id=?",(blocked,ambiguous))
                engine.reconcile()
                with self.assertRaisesRegex(ValueError,"attempted or ambiguous"):
                    engine.dispose_supervisor_task(blocked,actor="operator",reason="unsafe tree")
                self.assertEqual("awaiting_approval",engine.get_task(blocked)["status"])
                self.assertEqual("running",engine.get_task(ambiguous)["status"])
            finally: engine.close()

    def test_two_supervisors_same_project_effective_expiry_and_takeover_fence(self):
        class Worker:
            def __init__(self): self.tasks=[]
            def dispatch(self, task): self.tasks.append(task["id"])
        with tempfile.TemporaryDirectory() as tmp:
            now=[1000.0]
            worker=Worker()
            engine=Engine(Path(tmp)/"state.sqlite",clock=lambda:now[0],dry_run=False,worker=worker,max_concurrency=3,
                supervisor_stale_seconds=10,supervisor_inactive_seconds=20,supervisor_orphan_seconds=40)
            try:
                engine.register_project("shared",tmp)
                tokens={"a":"a"*48,"b":"b"*48}
                recipients={"a":"a@example.test","b":"b@example.test"}
                tasks={}
                for key in ("a","b"):
                    sid="sup-"+key
                    engine.register_supervisor(sid,"shared","fake-"+key,token_hash=hashlib.sha256(tokens[key].encode()).hexdigest())
                    engine.bind_supervisor_recipient(sid,recipients[key],actor="fixture trusted-local")
                    tasks[key]=engine.submit("shared","fake task "+key,approval_required=False,supervisor_id=sid)
                in_flight=engine.submit("shared","fake in-flight",approval_required=False,supervisor_id="sup-a")
                engine.reconcile()
                self.assertEqual(3,len(worker.tasks))
                held_ready=engine.submit("shared","approved old-generation ready task",approval_required=False,supervisor_id="sup-a")
                pending=engine.submit("shared","old-generation pending approval",supervisor_id="sup-a")
                engine.stop(True)
                for key in ("a","b"):
                    engine.ingest_event("event-"+key,tasks[key],"failed",{"attempt":1,"error":"fixture"})
                for key in ("a","b"):
                    rows=engine.claim_outbox(owner="smtp-test",limit=10,lease_seconds=60,
                        event_types=("worker.escalation","worker.supervisor_decision"),recipient=recipients[key])
                    self.assertEqual(["worker:event-"+key],[row["id"] for row in rows])
                signature=hmac.new(tokens["b"].encode(),f"sup-b\nevent-a\nworker.escalation\n{int(now[0])}".encode(),hashlib.sha256).hexdigest()
                with self.assertRaisesRegex(ValueError,"not authoritative"):
                    engine.route_supervisor_event("sup-b","event-a",issued=int(now[0]),signature=signature,token=tokens["b"])
                now[0]+=41  # No reaper has run; stored state still says active.
                self.assertEqual("orphaned",engine.supervisor_status("sup-a")["observed_lifecycle"])
                self.assertEqual([],engine.claim_outbox(owner="smtp-test",limit=10,lease_seconds=60,
                    event_types=("worker.escalation","worker.supervisor_decision"),recipient=recipients["a"]))
                engine.unbind_supervisor_recipient(recipients["a"],actor="fixture trusted-local")
                with self.assertRaises(ValueError): engine.supervisor_heartbeat("sup-a",token=tokens["a"],generation=1)
                engine.ingest_event("late-event",in_flight,"completed",{"attempt":1,"decision":"stop"})
                self.assertEqual("needs_review",engine.get_task(in_flight)["status"])
                engine.reconcile()
                self.assertEqual("needs_review",engine.get_task(in_flight)["status"])
                self.assertEqual("needs_review",engine.get_task(held_ready)["status"])
                self.assertEqual("awaiting_approval",engine.get_task(pending)["status"])
                replacement="c"*48
                engine.supervisor_lifecycle("sup-a",action="takeover",actor="fixture trusted-local",
                    token_hash=hashlib.sha256(replacement.encode()).hexdigest())
                self.assertEqual(2,engine.supervisor_status("sup-a")["generation"])
                self.assertEqual(0,engine.db.execute("SELECT enabled FROM supervisor_recipients WHERE recipient=?",(recipients["a"],)).fetchone()[0])
                with self.assertRaisesRegex(ValueError,"not awaiting approval"):
                    engine.approve(pending)
                with self.assertRaises(ValueError): engine.route_supervisor_event("sup-a","event-a",issued=int(now[0]),signature="0"*64,token=replacement)
                self.assertEqual(["sup-a","sup-b"],sorted({row["supervisor_id"] for row in engine.db.execute("SELECT supervisor_id FROM tasks WHERE supervisor_id IS NOT NULL").fetchall()}))
            finally:
                engine.close()

    def test_deboard_holds_inflight_work_and_requests_cancellation(self):
        class Worker:
            def __init__(self): self.cancelled = []
            def dispatch(self, task): pass
            def cancel(self, task_id): self.cancelled.append(task_id)
        with tempfile.TemporaryDirectory() as tmp:
            worker = Worker()
            engine = Engine(Path(tmp) / "state.sqlite", dry_run=False, worker=worker)
            try:
                engine.register_project("project", tmp)
                engine.register_supervisor("sup", "project", "worker-label",
                    token_hash=hashlib.sha256(("a" * 48).encode()).hexdigest())
                task = engine.submit("project", "in flight", approval_required=False, supervisor_id="sup")
                engine.reconcile()
                engine.supervisor_lifecycle("sup", action="deboard", actor="test")
                self.assertEqual("needs_review", engine.get_task(task)["status"])
                self.assertEqual("ambiguous", engine.db.execute("SELECT state FROM dispatch_intents WHERE task_id=?", (task,)).fetchone()[0])
                self.assertEqual([task], worker.cancelled)
            finally:
                engine.close()

    def test_suspension_and_completion_preserve_pending_approval(self):
        with tempfile.TemporaryDirectory() as tmp:
            engine = Engine(Path(tmp) / "state.sqlite")
            try:
                engine.register_project("project", tmp)
                token = "a" * 48
                engine.register_supervisor("sup", "project", "worker-label",
                    token_hash=hashlib.sha256(token.encode()).hexdigest())
                task = engine.submit("project", "pending approval", supervisor_id="sup")
                ready = engine.submit("project", "approved but held", approval_required=False, supervisor_id="sup")
                engine.supervisor_lifecycle("sup", action="suspend", actor="test")
                engine.reconcile()
                with self.assertRaises(ValueError):
                    engine.supervisor_lifecycle("sup", action="complete", actor="test")
                self.assertEqual("awaiting_approval", engine.get_task(task)["status"])
                self.assertEqual("ready", engine.get_task(ready)["status"])
                engine.supervisor_lifecycle("sup", action="resume", actor="test")
                self.assertEqual("awaiting_approval", engine.get_task(task)["status"])
            finally:
                engine.close()

    def test_legacy_registry_migrates_with_generation_and_creation_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy.sqlite"
            conn = sqlite3.connect(path)
            conn.executescript("CREATE TABLE projects(id TEXT PRIMARY KEY,root TEXT NOT NULL,policy TEXT NOT NULL,created REAL NOT NULL); CREATE TABLE supervisors(id TEXT PRIMARY KEY,project_id TEXT NOT NULL,worker_id TEXT NOT NULL,token_hash TEXT NOT NULL,created REAL NOT NULL,revoked REAL); CREATE TABLE tasks(id TEXT PRIMARY KEY,project_id TEXT NOT NULL REFERENCES projects(id),intention TEXT NOT NULL,status TEXT NOT NULL,approval_required INTEGER NOT NULL,read_only INTEGER NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,max_attempts INTEGER NOT NULL DEFAULT 3,available_at REAL NOT NULL,lease_owner TEXT,lease_until REAL,created REAL NOT NULL,updated REAL NOT NULL,last_error TEXT,parent_task_id TEXT REFERENCES tasks(id),decision TEXT,workflow_id TEXT,depth INTEGER NOT NULL DEFAULT 0);")
            conn.execute("INSERT INTO projects VALUES('project',?,?,?)", (tmp, "{}", 50.0))
            conn.execute("INSERT INTO supervisors VALUES('sup','project','label',?,50,NULL)", (hashlib.sha256(("a" * 48).encode()).hexdigest(),))
            conn.commit(); conn.close()
            engine = Engine(path, clock=lambda: 60.0)
            try:
                status = engine.supervisor_status("sup")
                self.assertEqual(1, status["generation"])
                self.assertEqual(50, status["heartbeat"])
                self.assertEqual("active", status["lifecycle"])
                self.assertIn("supervisor_id", {row[1] for row in engine.db.execute("PRAGMA table_info(tasks)")})
            finally:
                engine.close()

    def test_thresholds_heartbeat_takeover_and_assignment_fence(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = [1000.0]
            engine = Engine(Path(tmp) / "state.sqlite", clock=lambda: now[0],
                            supervisor_stale_seconds=10, supervisor_inactive_seconds=20,
                            supervisor_orphan_seconds=40)
            try:
                engine.register_project("project", tmp)
                old, new = "a" * 48, "b" * 48
                engine.register_supervisor("sup", "project", "worker-label",
                    token_hash=hashlib.sha256(old.encode()).hexdigest())
                task = engine.submit("project", "owned", supervisor_id="sup")
                self.assertEqual("sup", engine.db.execute("SELECT supervisor_id FROM tasks WHERE id=?", (task,)).fetchone()[0])
                now[0] += 11
                self.assertEqual("stale", engine.supervisor_status("sup")["observed_lifecycle"])
                with self.assertRaises(ValueError): engine.submit("project", "blocked", supervisor_id="sup")
                engine.supervisor_heartbeat("sup", token=old, generation=1)
                now[0] += 41
                engine.reap_supervisors(actor="test")
                self.assertEqual("orphaned", engine.supervisor_status("sup")["lifecycle"])
                engine.supervisor_lifecycle("sup", action="takeover", actor="test",
                    token_hash=hashlib.sha256(new.encode()).hexdigest())
                self.assertEqual(2, engine.supervisor_status("sup")["generation"])
                with self.assertRaises(ValueError): engine.supervisor_heartbeat("sup", token=old, generation=1)
                self.assertEqual("active", engine.supervisor_status("sup")["lifecycle"])
            finally:
                engine.close()


if __name__ == "__main__":
    unittest.main()
