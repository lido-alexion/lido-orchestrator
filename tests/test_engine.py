import json
import multiprocessing
import os
import sqlite3
import threading
import tempfile
import unittest
import time
import subprocess
import sys
from unittest.mock import patch
from pathlib import Path

from lido_orchestrator.engine import Engine
from lido_orchestrator.codex_worker import CodexWorker


class Clock:
    now=1000.0
    def __call__(self): return self.now


class SyntheticWorker:
    def __init__(self): self.calls=[]
    def dispatch(self, task): self.calls.append(task)


class BlockingWorker(SyntheticWorker):
    def __init__(self, entered, release): super().__init__(); self.entered, self.release = entered, release
    def dispatch(self, task):
        self.entered.set()
        if not self.release.wait(5): raise TimeoutError("test launch release timed out")
        super().dispatch(task)


def _race_claim(db, barrier, output):
    engine=Engine(db,dry_run=False,worker=SyntheticWorker(),max_concurrency=1)
    barrier.wait()
    output.put(len(engine.reconcile()))
    engine.close()


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.db=Path(self.tmp.name)/"state.sqlite"; self.clock=Clock()
        self.worker=SyntheticWorker()
        self.e=Engine(self.db,clock=self.clock,max_concurrency=1,lease_seconds=10,dry_run=False,worker=self.worker)
        self.e.register_project("demo",self.tmp.name)
    def tearDown(self): self.e.close(); self.tmp.cleanup()
    def test_approval_gate_event_dedup_and_structured_dependent_step(self):
        tid=self.e.submit("demo","inspect",approval_required=True)
        self.assertEqual([],self.e.reconcile()); self.assertEqual("awaiting_approval",self.e.get_task(tid)["status"])
        self.e.approve(tid); self.assertEqual([tid],self.e.reconcile())
        payload={"decision":"continue","summary":"done","next_steps":[{"intention":"summarize","approval_required":True}]}
        self.assertTrue(self.e.ingest_event("event-1",tid,"completed",payload))
        self.assertFalse(self.e.ingest_event("event-1",tid,"completed",payload))
        child=[t for t in self.e.snapshot()["tasks"] if t["parent_task_id"]==tid][0]
        self.assertEqual("awaiting_approval",child["status"])
        self.assertEqual("succeeded",self.e.get_task(tid)["status"])
    def test_dependent_steps_cannot_remove_approval_or_request_writes(self):
        tid=self.e.submit("demo","inspect",approval_required=False)
        self.e.reconcile()
        self.e.ingest_event("step-event",tid,"completed",{"decision":"continue","next_steps":[
            {"intention":"follow up","approval_required":False,"read_only":False}]})
        child=[t for t in self.e.snapshot()["tasks"] if t["parent_task_id"]==tid][0]
        self.assertEqual("awaiting_approval",child["status"])
        self.assertEqual(1,child["read_only"])
    def test_restart_lease_recovery_and_backoff(self):
        tid=self.e.submit("demo","inspect",approval_required=False)
        self.e.reconcile(); self.e.close()
        self.e=Engine(self.db,clock=self.clock,max_concurrency=1,lease_seconds=10,dry_run=False,worker=SyntheticWorker())
        self.clock.now+=11; self.assertEqual([],self.e.reconcile())
        self.assertEqual("needs_review",self.e.get_task(tid)["status"])
        self.clock.now+=1; self.assertEqual([],self.e.reconcile())
    def test_emergency_stop_and_read_only_boundary(self):
        with self.assertRaises(ValueError): self.e.submit("demo","write",approval_required=False,read_only=False)
        tid=self.e.submit("demo","inspect",approval_required=False); self.e.stop()
        self.assertEqual([],self.e.reconcile()); self.e.stop(False); self.assertEqual([tid],self.e.reconcile())
    def test_immutable_project_registration(self):
        with self.assertRaises(ValueError): self.e.register_project("demo", "/tmp")
    def test_crash_after_intent_commit_recovers_same_key(self):
        tid=self.e.submit("demo","inspect",approval_required=False)
        def crash(point):
            if point=="after_intent_commit": raise SystemExit("simulated process death")
        self.e.fault_hook=crash
        with self.assertRaises(SystemExit): self.e.reconcile()
        intent=self.e.snapshot()["dispatch_intents"][0]
        self.e.fault_hook=None
        self.assertEqual([tid],self.e.reconcile())
        self.assertEqual(intent["intent_key"],self.worker.calls[0]["idempotency_key"])
    def test_crash_after_external_dispatch_is_not_replayed(self):
        tid=self.e.submit("demo","inspect",approval_required=False)
        def crash(point):
            if point=="after_external_dispatch": raise SystemExit("simulated death after side effect")
        self.e.fault_hook=crash
        with self.assertRaises(SystemExit): self.e.reconcile()
        self.e.fault_hook=None
        self.clock.now += 11
        self.assertEqual([],self.e.reconcile())
        self.assertEqual("needs_review",self.e.get_task(tid)["status"])
        self.assertEqual(1,len(self.worker.calls))

    def test_cross_supervisor_termination_waits_for_launch_fence(self):
        entered, release = threading.Event(), threading.Event()
        self.e.worker = BlockingWorker(entered, release)
        tid = self.e.submit("demo", "bounded", approval_required=False)
        workflow = self.e.get_task(tid)["workflow_id"]
        second = Engine(self.db, dry_run=False, worker=SyntheticWorker(), clock=self.clock)
        dispatch_thread = threading.Thread(target=self.e.reconcile)
        dispatch_thread.start(); self.assertTrue(entered.wait(2))
        done = threading.Event()
        terminate_thread = threading.Thread(target=lambda: (second.terminate_workflow(workflow, actor="reviewer", reason="stop"), done.set()))
        terminate_thread.start()
        self.assertFalse(done.wait(.1), "termination must serialize behind in-flight launch handoff")
        release.set(); dispatch_thread.join(3); terminate_thread.join(3)
        self.assertTrue(done.is_set())
        self.assertEqual(1, len(self.e.worker.calls))
        self.assertEqual("terminated", second.snapshot()["workflows"][0]["state"])
        self.assertEqual("needs_review", second.get_task(tid)["status"])
        second.close()

    def test_cross_supervisor_termination_before_launch_suppresses_adapter(self):
        entered, release = threading.Event(), threading.Event()
        tid = self.e.submit("demo", "bounded", approval_required=False)
        workflow = self.e.get_task(tid)["workflow_id"]
        second = Engine(self.db, dry_run=False, worker=SyntheticWorker(), clock=self.clock)
        def after_claim(point):
            if point == "after_intent_commit":
                entered.set()
                if not release.wait(5): raise TimeoutError("test termination release timed out")
        self.e.fault_hook = after_claim
        dispatch_thread = threading.Thread(target=self.e.reconcile)
        dispatch_thread.start(); self.assertTrue(entered.wait(2))
        second.terminate_workflow(workflow, actor="reviewer", reason="stop before launch")
        release.set(); dispatch_thread.join(3)
        self.assertFalse(dispatch_thread.is_alive())
        self.assertEqual([], self.worker.calls)
        self.assertEqual("needs_review", second.get_task(tid)["status"])
        self.assertEqual("ambiguous", second.snapshot()["dispatch_intents"][0]["state"])
        second.close()

    def test_project_snapshot_is_bounded_path_metadata_and_completion_outbox(self):
        root = Path(self.tmp.name) / "fixture"; root.mkdir()
        (root / "source.py").write_text("secret-looking contents", encoding="utf-8")
        (root / ".env").write_text("TOKEN=do-not-copy", encoding="utf-8")
        (root / "source-link").symlink_to(root / "source.py")
        (root / ".git").mkdir()
        (root / ".git" / "ignored").write_text("excluded", encoding="utf-8")
        self.e.register_project("fixture", str(root))
        snapshot = self.e.project_snapshot("fixture", max_entries=3)
        self.assertFalse(snapshot["content_included"])
        paths = {item["path"] for item in snapshot["entries"]}
        self.assertIn("source.py", paths); self.assertNotIn(".git/ignored", paths)
        symlink = next(item for item in snapshot["entries"] if item["path"] == "source-link")
        self.assertEqual("symlink", symlink["kind"]); self.assertIsNone(symlink["size"])
        self.assertNotIn("secret-looking contents", json.dumps(snapshot))
        tid = self.e.submit("fixture", "summarize metadata", approval_required=False)
        self.e.reconcile()
        self.assertEqual([], self.e.pending_outbox())
        self.e.ingest_event("outbox-event", tid, "completed", {"attempt": 1, "decision": "stop"})
        pending = self.e.pending_outbox()
        self.assertEqual("worker:outbox-event", pending[0]["id"])
        self.e.acknowledge_outbox(pending[0]["id"], actor="operator")
        self.assertEqual([], self.e.pending_outbox())

    def test_supervisor_registry_route_scope_replay_and_revocation(self):
        import hashlib, hmac
        self.e.register_project("other",self.tmp.name)
        token_a=b"fixture-token-a-0123456789abcdef"; token_b=b"fixture-token-b-0123456789abcdef"
        self.e.register_supervisor("sup-a","demo","worker-a",token_hash=hashlib.sha256(token_a).hexdigest())
        self.e.register_supervisor("sup-b","other","worker-b",token_hash=hashlib.sha256(token_b).hexdigest())
        tid=self.e.submit("demo","synthetic",approval_required=False); self.e.reconcile(); self.e.ingest_event("sup-event",tid,"failed",{"note":"authoritative"})
        issued=int(self.clock.now); kind="worker.escalation"
        sig=hmac.new(token_a,f"sup-a\nsup-event\n{kind}\n{issued}".encode(),hashlib.sha256).hexdigest()
        routed=self.e.route_supervisor_event("sup-a","sup-event",issued=issued,signature=sig,token=token_a.decode())
        self.assertEqual(tid,routed["task_id"])
        with self.assertRaisesRegex(ValueError,"replay"):
            self.e.route_supervisor_event("sup-a","sup-event",issued=issued,signature=sig,token=token_a.decode())
        sig_b=hmac.new(token_b,f"sup-b\nsup-event\n{kind}\n{issued}".encode(),hashlib.sha256).hexdigest()
        with self.assertRaisesRegex(ValueError,"not authoritative"):
            self.e.route_supervisor_event("sup-b","sup-event",issued=issued,signature=sig_b,token=token_b.decode())
        with self.assertRaises(ValueError): self.e.route_supervisor_event("sup-a","sup-event",issued=issued,signature=sig,token="wrong")
        self.e.unregister_supervisor("sup-a",actor="fixture-operator")
        with self.assertRaisesRegex(ValueError,"revoked"):
            self.e.route_supervisor_event("sup-a","sup-event",issued=issued,signature=sig,token=token_a.decode())
        with self.assertRaisesRegex(ValueError,"unknown"): self.e.supervisor_status("missing")

    def test_supervisor_cli_trusted_registration_and_signed_route(self):
        from lido_orchestrator.cli import main
        import contextlib, io, time, hmac, hashlib
        db=Path(self.tmp.name)/"cli.sqlite"; root=Path(self.tmp.name)/"project"
        root.mkdir()
        def invoke(args, stdin=None):
            output=io.StringIO()
            with contextlib.redirect_stdout(output):
                if stdin is None: main(["--db",str(db),*args])
                else:
                    from unittest.mock import patch
                    with patch("sys.stdin",io.StringIO(stdin)): main(["--db",str(db),*args])
            return json.loads(output.getvalue())
        invoke(["register-project","cli-demo",str(root)])
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stderr(io.StringIO()): invoke(["supervisor","register","cli-sup","cli-demo","worker-cli"])
        enrolled=invoke(["supervisor","register","cli-sup","cli-demo","worker-cli","--trusted-local"])
        token=enrolled["route_token_once"]
        engine=Engine(db,dry_run=False)
        try:
            task=engine.submit("cli-demo","fixture",approval_required=False); engine.reconcile(); engine.ingest_event("cli-route-event",task,"failed",{"fixture":True})
        finally: engine.close()
        issued=int(time.time()); kind="worker.escalation"
        signature=hmac.new(token.encode(),f"cli-sup\ncli-route-event\n{kind}\n{issued}".encode(),hashlib.sha256).hexdigest()
        routed=invoke(["supervisor","route","cli-sup","cli-route-event","--issued",str(issued),"--signature",signature],token+"\n")
        self.assertEqual("cli-route-event",routed["id"])
        self.assertEqual("cli-sup",invoke(["supervisor","status","cli-sup"])["id"])
        self.assertEqual(["cli-sup"],[x["id"] for x in invoke(["supervisor","list"])["supervisors"]])
        invoke(["supervisor","unregister","cli-sup","--actor","fixture-operator"])

    def test_multi_process_claim_respects_global_concurrency(self):
        self.e.clock = time.time
        self.e.submit("demo","one",approval_required=False)
        self.e.submit("demo","two",approval_required=False)
        barrier=multiprocessing.Barrier(2); output=multiprocessing.Queue()
        workers=[multiprocessing.Process(target=_race_claim,args=(str(self.db),barrier,output)) for _ in range(2)]
        for proc in workers: proc.start()
        for proc in workers: proc.join(15)
        self.assertTrue(all(proc.exitcode==0 for proc in workers))
        self.assertEqual(1,output.get(timeout=2)+output.get(timeout=2))

    def test_manual_hourly_and_native_trigger_paths_share_durable_fence(self):
        second=Engine(self.db,dry_run=False,worker=SyntheticWorker(),max_concurrency=1,clock=self.clock)
        try:
            self.e.worker=SyntheticWorker()
            self.e.submit("demo","one shared trigger event",approval_required=False)
            barrier=threading.Barrier(4); outcomes=[]
            def trigger(engine):
                barrier.wait(); outcomes.append(len(engine.reconcile()))
            threads=[threading.Thread(target=trigger,args=(engine,)) for engine in (self.e,second,second)]
            for thread in threads: thread.start()
            barrier.wait()
            for thread in threads: thread.join(5)
            self.assertEqual(1,sum(outcomes))
            self.assertEqual(1,len(self.e.snapshot()["dispatch_intents"]))
        finally: second.close()

    def test_durable_workflow_budget_cancels_excess_model_steps_across_restart(self):
        root=self.e.submit("demo","inspect",approval_required=False,workflow_budget=2,workflow_max_depth=2)
        self.e.reconcile()
        self.e.ingest_event("budget-parent",root,"completed",{"decision":"continue","next_steps":[
            {"intention":"one bounded child","approval_required":False,"read_only":True}]})
        child=[t for t in self.e.snapshot()["tasks"] if t["parent_task_id"]==root][0]
        self.assertEqual("awaiting_approval",child["status"])
        self.e.approve(child["id"]); self.e.reconcile()
        self.e.ingest_event("budget-child",child["id"],"completed",{"decision":"continue","next_steps":[
            {"intention":"extra one","approval_required":False,"read_only":True},
            {"intention":"extra two","approval_required":False,"read_only":True}]})
        tasks=self.e.snapshot()["tasks"]
        self.assertEqual(2,len(tasks))
        self.assertEqual("terminated",self.e.snapshot()["workflows"][0]["state"])
        self.assertEqual("task_budget_exhausted",self.e.snapshot()["workflows"][0]["termination_reason"])
        self.e.close()
        self.e=Engine(self.db,clock=self.clock,max_concurrency=1,lease_seconds=10,dry_run=False,worker=self.worker)
        self.assertEqual([],self.e.reconcile())
        self.assertEqual(2,len(self.e.snapshot()["tasks"]))

    def test_concurrent_submissions_cannot_overfill_workflow_budget(self):
        root=self.e.submit("demo","bounded root",approval_required=False,workflow_budget=2)
        self.e.reconcile()
        self.e.ingest_event("bounded-root-done",root,"completed",{"decision":"continue"})
        barrier=threading.Barrier(2); outcomes=[]; lock=threading.Lock()
        def submit_child(label):
            engine=Engine(self.db,clock=self.clock,dry_run=False,worker=SyntheticWorker())
            barrier.wait()
            try:
                engine.submit("demo",label,parent_task_id=root)
                value="submitted"
            except ValueError:
                value="rejected"
            finally:
                engine.close()
            with lock: outcomes.append(value)
        threads=[threading.Thread(target=submit_child,args=(str(i),)) for i in range(2)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(5)
        self.assertEqual(["rejected","submitted"],sorted(outcomes))
        self.assertEqual(2,len([t for t in self.e.snapshot()["tasks"] if t["workflow_id"]==root]))

    def test_workflow_depth_stop_and_review_terminate_descendants(self):
        root=self.e.submit("demo","bounded",approval_required=False,workflow_budget=4,workflow_max_depth=0)
        self.e.reconcile()
        self.e.ingest_event("depth-parent",root,"completed",{"decision":"continue","next_steps":[
            {"intention":"too deep","approval_required":True,"read_only":True}]})
        snapshot=self.e.snapshot()
        self.assertEqual(1,len(snapshot["tasks"]))
        self.assertEqual("depth_limit",snapshot["workflows"][0]["termination_reason"])
        self.assertEqual([],self.e.reconcile())

    def test_workflow_deadline_cancels_awaiting_approval_on_restart(self):
        root=self.e.submit("demo","plan",approval_required=False,workflow_budget=3,workflow_timeout=2)
        self.e.reconcile()
        self.e.ingest_event("deadline-child",root,"completed",{"decision":"continue","next_steps":[
            {"intention":"review later","approval_required":True,"read_only":True}]})
        child=[t for t in self.e.snapshot()["tasks"] if t["parent_task_id"]==root][0]
        self.assertEqual("awaiting_approval",child["status"])
        self.clock.now += 3
        self.assertEqual([],self.e.reconcile())
        self.assertEqual("cancelled",self.e.get_task(child["id"])["status"])

    def test_write_authorization_and_adapter_boundaries_fail_closed(self):
        self.e.register_project("writable",self.tmp.name,read_only=False)
        with self.assertRaises(ValueError): self.e.submit("writable","edit",read_only=False)
        self.e.authorize_project_write("writable",actor="operator",reason="disposable test")
        task=self.e.submit("writable","edit one file",read_only=False,approval_required=True)
        self.assertEqual("awaiting_approval",self.e.get_task(task)["status"])
        self.e.revoke_project_write("writable",actor="operator")
        self.assertEqual("cancelled",self.e.get_task(task)["status"])

    def test_expired_write_authorization_cannot_survive_until_dispatch(self):
        self.e.register_project("writable",self.tmp.name,read_only=False)
        self.e.authorize_project_write("writable",actor="operator",reason="disposable test",expires_in=1)
        task=self.e.submit("writable","edit",read_only=False,approval_required=False)
        self.clock.now += 2
        self.assertEqual([],self.e.reconcile())
        self.assertEqual("cancelled",self.e.get_task(task)["status"])
        self.assertEqual("write_authorization_expired",self.e.snapshot()["workflows"][0]["termination_reason"])

    def test_hard_workflow_task_and_payload_limits(self):
        with self.assertRaises(ValueError): self.e.submit("demo","x",workflow_budget=101)
        with self.assertRaises(ValueError): self.e.submit("demo","x",workflow_timeout=86401)
        with self.assertRaises(ValueError): self.e.submit("demo","x"*4001)
        task=self.e.submit("demo","bounded",approval_required=False)
        self.e.reconcile()
        with self.assertRaises(ValueError): self.e.ingest_event("oversized",task,"completed",{"summary":"x"*1_000_001})
        self.assertEqual("running",self.e.get_task(task)["status"])

    def test_legacy_ready_descendants_migrate_to_approval_gate(self):
        legacy=Path(self.tmp.name)/"legacy.sqlite"
        db=sqlite3.connect(legacy)
        db.executescript("""
          CREATE TABLE projects(id TEXT PRIMARY KEY,root TEXT NOT NULL,policy TEXT NOT NULL,created REAL NOT NULL);
          CREATE TABLE tasks(id TEXT PRIMARY KEY,project_id TEXT NOT NULL,intention TEXT NOT NULL,status TEXT NOT NULL,
            approval_required INTEGER NOT NULL,read_only INTEGER NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,available_at REAL NOT NULL,lease_owner TEXT,lease_until REAL,
            created REAL NOT NULL,updated REAL NOT NULL,last_error TEXT,parent_task_id TEXT,decision TEXT);
          CREATE TABLE events(id TEXT PRIMARY KEY,task_id TEXT NOT NULL,kind TEXT NOT NULL,payload TEXT NOT NULL,received REAL NOT NULL);
          CREATE TABLE dispatch_intents(task_id TEXT PRIMARY KEY,intent_key TEXT NOT NULL UNIQUE,state TEXT NOT NULL,
            created REAL NOT NULL,delivered REAL,claimed REAL);
          CREATE TABLE audit(id INTEGER PRIMARY KEY AUTOINCREMENT,at REAL NOT NULL,action TEXT NOT NULL,entity_id TEXT,details TEXT NOT NULL);
          CREATE TABLE control(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          INSERT INTO projects VALUES('demo','/tmp','{\"read_only\":true}',1000);
          INSERT INTO tasks(id,project_id,intention,status,approval_required,read_only,attempts,max_attempts,available_at,created,updated)
            VALUES('root','demo','parent','succeeded',0,1,1,3,1000,1000,1000);
          INSERT INTO tasks(id,project_id,intention,status,approval_required,read_only,attempts,max_attempts,available_at,created,updated,parent_task_id)
            VALUES('child','demo','legacy generated task','ready',0,1,0,3,1001,1001,1001,'root');
        """)
        db.close()
        migrated=Engine(legacy,clock=self.clock,dry_run=False,worker=SyntheticWorker())
        try:
            self.assertEqual("awaiting_approval",migrated.get_task("child")["status"])
            self.assertEqual(1,migrated.get_task("child")["approval_required"])
            self.assertEqual([],migrated.reconcile())
            self.assertEqual(2,migrated.db.execute("SELECT count(*) FROM workflows").fetchone()[0])
        finally: migrated.close()

    def test_workflow_stop_cancels_approved_children(self):
        root=self.e.submit("demo","plan",approval_required=False)
        self.e.reconcile()
        self.e.ingest_event("stop-parent",root,"completed",{"decision":"continue","next_steps":[
            {"intention":"review","approval_required":True,"read_only":True},
            {"intention":"another review","approval_required":True,"read_only":True}]})
        children=[t for t in self.e.snapshot()["tasks"] if t["parent_task_id"]==root]
        child=children[0]
        self.e.approve(child["id"]); self.e.reconcile()
        self.e.ingest_event("stop-child",child["id"],"completed",{"decision":"stop"})
        self.assertEqual("cancelled",self.e.get_task(children[1]["id"])["status"])
        self.assertEqual("worker_stop",self.e.snapshot()["workflows"][0]["termination_reason"])

    def test_operator_can_terminate_specific_workflow(self):
        root=self.e.submit("demo","operator gated",approval_required=True)
        workflow=self.e.snapshot()["workflows"][0]["id"]
        self.e.terminate_workflow(workflow,actor="operator",reason="review requested")
        self.assertEqual("cancelled",self.e.get_task(root)["status"])
        self.assertEqual("terminated",self.e.snapshot()["workflows"][0]["state"])
    def test_codex_adapter_requires_two_part_opt_in(self):
        old=os.environ.pop("LIDO_ENABLE_CODEX",None)
        try:
            with self.assertRaises(RuntimeError): CodexWorker(enabled=True)
        finally:
            if old is not None: os.environ["LIDO_ENABLE_CODEX"]=old
    def test_default_dry_run_is_explicit_and_terminal(self):
        dry=Engine(self.db,clock=self.clock)
        try:
            tid=dry.submit("demo","preview",approval_required=False)
            self.assertEqual([tid],dry.reconcile())
            self.assertEqual("dry_run",dry.get_task(tid)["status"])
        finally: dry.close()

    def test_real_expired_dispatch_is_fail_closed(self):
        tid=self.e.submit("demo","inspect",approval_required=False)
        self.e.reconcile(); self.clock.now += 11
        self.e.reconcile()
        self.assertEqual("needs_review",self.e.get_task(tid)["status"])
        self.assertEqual("ambiguous",self.e.snapshot()["dispatch_intents"][0]["state"])
        self.assertEqual(1,len(self.worker.calls))

    def test_late_attempt_event_cannot_complete_retry(self):
        tid=self.e.submit("demo","inspect",approval_required=False)
        self.e.reconcile()
        self.e._retry(tid,1,3,"test known pre-dispatch reset")
        self.clock.now += 3
        self.assertEqual([tid],self.e.reconcile())
        self.assertEqual(2,self.e.get_task(tid)["attempts"])
        self.assertTrue(self.e.ingest_event("late-attempt-1",tid,"completed",{"attempt":1,"decision":"stop"}))
        self.assertEqual("running",self.e.get_task(tid)["status"])
        self.assertTrue(self.e.ingest_event("attempt-2",tid,"completed",{"attempt":2,"decision":"stop"}))
        self.assertEqual("succeeded",self.e.get_task(tid)["status"])

    def test_worker_failure_event_is_ambiguous_and_never_retried(self):
        tid=self.e.submit("demo","may have partial effect",approval_required=False)
        self.e.reconcile()
        self.e.ingest_event("failure",tid,"failed",{"attempt":1,"error":"cancelled"})
        self.assertEqual("needs_review",self.e.get_task(tid)["status"])
        self.assertEqual("ambiguous",self.e.snapshot()["dispatch_intents"][0]["state"])
        self.clock.now += 100
        self.e.reconcile()
        self.assertEqual(1,len(self.worker.calls))

    def test_exception_after_claim_is_ambiguous_without_relaunch(self):
        tid=self.e.submit("demo","may have partial effect",approval_required=False)
        def fault(point):
            if point=="after_external_dispatch": raise RuntimeError("collector handoff failed")
        self.e.fault_hook=fault
        self.e.reconcile()
        self.assertEqual("needs_review",self.e.get_task(tid)["status"])
        self.clock.now += 100
        self.e.fault_hook=None
        self.e.reconcile()
        self.assertEqual(1,len(self.worker.calls))

    def test_needs_review_decision_is_not_success_and_never_advances(self):
        tid=self.e.submit("demo","inspect",approval_required=False)
        self.e.reconcile()
        self.e.ingest_event("review-result",tid,"completed",{"attempt":1,"decision":"needs_review",
            "next_steps":[{"intention":"must not run","approval_required":False,"read_only":True}]})
        self.assertEqual("needs_review",self.e.get_task(tid)["status"])
        self.assertEqual(1,len(self.e.snapshot()["tasks"]))

    def test_event_after_lease_expiry_cannot_resolve_ambiguous_dispatch(self):
        tid=self.e.submit("demo","inspect",approval_required=False)
        self.e.reconcile()
        self.clock.now += 11
        self.assertTrue(self.e.ingest_event("after-lease",tid,"completed",{"attempt":1,"decision":"stop"}))
        self.assertEqual("needs_review",self.e.get_task(tid)["status"])

    def test_explicit_preapproval_is_bounded_by_safe_policy(self):
        tid=self.e.submit("demo","inspect",approval_required=False)
        self.e.reconcile()
        payload={"decision":"continue","next_steps":[{"intention":"follow up","approval_required":False,"read_only":True}]}
        self.e.ingest_event("default-policy",tid,"completed",payload)
        child=[t for t in self.e.snapshot()["tasks"] if t["parent_task_id"]==tid][0]
        self.assertEqual("awaiting_approval",child["status"])
        immediate=Engine(Path(self.tmp.name)/"preapproved.sqlite",clock=self.clock,dry_run=False,
                         worker=self.worker,allow_preapproved_read_only_steps=True)
        try:
            immediate.register_project("demo",self.tmp.name)
            tid2=immediate.submit("demo","inspect again",approval_required=False)
            immediate.reconcile()
            immediate.ingest_event("enabled-policy",tid2,"completed",payload)
            child2=[t for t in immediate.snapshot()["tasks"] if t["parent_task_id"]==tid2][0]
            self.assertIn(child2["status"],("ready","running"))
        finally: immediate.close()

    def test_callback_completion_can_release_preapproved_next_step_immediately(self):
        db=Path(self.tmp.name)/"immediate.sqlite"
        immediate=Engine(db,clock=self.clock,max_concurrency=1,lease_seconds=10,dry_run=False,
                         worker=self.worker,allow_preapproved_read_only_steps=True)
        try:
            immediate.register_project("demo",self.tmp.name)
            parent=immediate.submit("demo","plan",approval_required=False)
            immediate.reconcile()
            before=time.monotonic()
            immediate.ingest_event("parent-done",parent,"completed",{"decision":"continue","next_steps":[
                {"intention":"read-only follow-up","approval_required":False,"read_only":True}]})
            elapsed=time.monotonic()-before
            child=[t for t in immediate.snapshot()["tasks"] if t["parent_task_id"]==parent][0]
            self.assertEqual("running",child["status"])
            self.assertLess(elapsed,1.0)
        finally: immediate.close()

    def test_callback_chain_drains_all_collectors_without_second_reconcile(self):
        db=Path(self.tmp.name)/"chain.sqlite"
        immediate=Engine(db,clock=self.clock,max_concurrency=1,lease_seconds=10,dry_run=False,
                         worker=self.worker,allow_preapproved_read_only_steps=True)
        try:
            immediate.register_project("demo",self.tmp.name)
            parent=immediate.submit("demo","plan",approval_required=False)
            immediate.reconcile()
            immediate.ingest_event("chain-parent",parent,"completed",{"decision":"continue","next_steps":[
                {"intention":"handoff", "approval_required":False,"read_only":True}]})
            child=[t for t in immediate.snapshot()["tasks"] if t["parent_task_id"]==parent][0]
            self.assertEqual("running",child["status"])
            immediate.ingest_event("chain-child",child["id"],"completed",{"attempt":1,"decision":"stop"})
            self.assertEqual("succeeded",immediate.get_task(child["id"])["status"])
            self.assertEqual(2,len(self.worker.calls))
        finally: immediate.close()

    @unittest.skipUnless(os.name == "posix", "process smoke uses a POSIX executable")
    def test_codex_worker_process_smoke_in_isolated_scratch(self):
        with tempfile.TemporaryDirectory() as td:
            executable=Path(td)/"mock-codex"
            executable.write_text("#!/usr/bin/env python3\nimport json,sys,pathlib\na=sys.argv\nout=pathlib.Path(a[a.index('--output-last-message')+1])\nout.write_text(json.dumps({'decision':'continue','summary':'ok','next_steps':[]}))\nprint('ok')\n")
            executable.chmod(0o755)
            old=os.environ.get("LIDO_ENABLE_CODEX"); os.environ["LIDO_ENABLE_CODEX"]="1"
            events=[]
            try:
                worker=CodexWorker(enabled=True,executable=str(executable),timeout=5,scratch_root=td,
                                   event_sink=lambda *event: events.append(event))
                worker.dispatch({"id":"task","attempt":1,"idempotency_key":"stable","intention":"harmless"})
                worker.wait(timeout=5)
                for _ in range(100):
                    if events: break
                    time.sleep(.02)
                self.assertEqual("stable:attempt-1:result",events[0][0])
                self.assertEqual("completed",events[0][2])
                self.assertEqual("continue",events[0][3]["decision"])
                self.assertEqual([],list(Path(td).glob("lido-codex-*")))
            finally:
                if old is None: os.environ.pop("LIDO_ENABLE_CODEX",None)
                else: os.environ["LIDO_ENABLE_CODEX"]=old

    def test_codex_worker_mocked_cli_failure_does_not_emit_completion(self):
        old=os.environ.get("LIDO_ENABLE_CODEX"); os.environ["LIDO_ENABLE_CODEX"]="1"
        try:
            events=[]
            with patch("lido_orchestrator.codex_worker.shutil.which",return_value="/fake/codex"), \
                 patch("lido_orchestrator.codex_worker.subprocess.Popen",side_effect=OSError("no exec")):
                worker=CodexWorker(enabled=True,event_sink=lambda *event:events.append(event))
                with self.assertRaises(RuntimeError): worker.dispatch({"id":"t","attempt":1,"idempotency_key":"k","intention":"x"})
            self.assertEqual([],events)
        finally:
            if old is None: os.environ.pop("LIDO_ENABLE_CODEX",None)
            else: os.environ["LIDO_ENABLE_CODEX"]=old

    @unittest.skipUnless(os.name == "posix", "process-tree smoke uses POSIX process groups")
    def test_codex_worker_timeout_kills_process_tree_and_records_failure(self):
        with tempfile.TemporaryDirectory() as td:
            executable=Path(td)/"mock-codex"
            pidfile=Path(td)/"child.pid"
            executable.write_text("#!/usr/bin/env python3\nimport subprocess,sys,pathlib,time,os\np=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'])\npathlib.Path(os.environ['CHILD_PID_PATH']).write_text(str(p.pid))\ntime.sleep(30)\n")
            executable.chmod(0o755)
            old=os.environ.get("LIDO_ENABLE_CODEX"); os.environ["LIDO_ENABLE_CODEX"]="1"
            old_pid=os.environ.get("CHILD_PID_PATH"); os.environ["CHILD_PID_PATH"]=str(pidfile)
            try:
                events=[]
                worker=CodexWorker(enabled=True,executable=str(executable),timeout=1,scratch_root=td,
                    event_sink=lambda *event: events.append(event))
                worker.dispatch({"id":"task","attempt":1,"idempotency_key":"stable","intention":"harmless"})
                worker.wait(timeout=5)
                self.assertEqual("failed",events[0][2])
                child_pid=int(pidfile.read_text())
                for _ in range(100):
                    try: os.kill(child_pid,0)
                    except ProcessLookupError: break
                    time.sleep(.02)
                else: self.fail("child process survived timeout cleanup")
                self.assertEqual(set(),worker._threads)
            finally:
                if old is None: os.environ.pop("LIDO_ENABLE_CODEX",None)
                else: os.environ["LIDO_ENABLE_CODEX"]=old
                if old_pid is None: os.environ.pop("CHILD_PID_PATH",None)
                else: os.environ["CHILD_PID_PATH"]=old_pid

    def test_codex_worker_cancellation_before_launch_prevents_process_start(self):
        old=os.environ.get("LIDO_ENABLE_CODEX"); os.environ["LIDO_ENABLE_CODEX"]="1"
        try:
            with patch("lido_orchestrator.codex_worker.shutil.which",return_value="/fake/codex"), \
                 patch("lido_orchestrator.codex_worker.subprocess.Popen") as popen:
                worker=CodexWorker(enabled=True)
                worker.cancel("cancelled-task")
                with self.assertRaisesRegex(RuntimeError,"cancelled before Codex launch"):
                    worker.dispatch({"id":"cancelled-task","attempt":1,"idempotency_key":"key","intention":"x"})
                popen.assert_not_called()
        finally:
            if old is None: os.environ.pop("LIDO_ENABLE_CODEX",None)
            else: os.environ["LIDO_ENABLE_CODEX"]=old

    @unittest.skipUnless(os.name == "posix", "CLI process smoke uses POSIX executables")
    def test_one_shot_cli_drains_immediate_handoff_without_killing_second_batch(self):
        with tempfile.TemporaryDirectory() as td:
            bindir=Path(td)/"bin"; bindir.mkdir()
            fake=bindir/"codex"
            fake.write_text("#!/usr/bin/env python3\nimport json,pathlib,sys\na=sys.argv\nout=pathlib.Path(a[a.index('--output-last-message')+1])\ntask=pathlib.Path('TASK.md').read_text()\nif 'Create a brief two-step plan' in task:\n r={'decision':'continue','summary':'planned','next_steps':[{'intention':'Summarize the plan','approval_required':False,'read_only':True}]}\nelse:\n r={'decision':'stop','summary':'handoff complete','next_steps':[]}\nout.write_text(json.dumps(r))\n")
            fake.chmod(0o755)
            env={**os.environ,"LIDO_ENABLE_CODEX":"1","PATH":str(bindir)+os.pathsep+os.environ.get("PATH","")}
            for mode in (("reconcile","--max-tasks","2"),("serve","--once","--max-tasks","2")):
                current_db=Path(td)/(mode[0]+".sqlite3")
                subprocess.run([sys.executable,"-m","lido_orchestrator","--db",str(current_db),"register-project","pilot",td],
                               check=True,env=env,capture_output=True,text=True)
                parent_out=subprocess.run([sys.executable,"-m","lido_orchestrator","--db",str(current_db),"submit","pilot",
                    "Create a brief two-step plan","--no-approval"],check=True,env=env,capture_output=True,text=True)
                parent_id=__import__("json").loads(parent_out.stdout)["task_id"]
                result=subprocess.run([sys.executable,"-m","lido_orchestrator","--db",str(current_db),"--codex",
                    "--allow-preapproved-read-only-steps",*mode],check=True,env=env,capture_output=True,text=True,timeout=10)
                self.assertTrue(result.stdout)
                dbcon=__import__("sqlite3").connect(current_db)
                tasks=dbcon.execute("select id,parent_task_id,status,attempts from tasks order by created").fetchall()
                self.assertEqual(2,len(tasks))
                self.assertEqual("succeeded",tasks[0][2]); self.assertEqual("succeeded",tasks[1][2])
                self.assertEqual(parent_id,tasks[1][1])
                self.assertEqual([1,1],[tasks[0][3],tasks[1][3]])
                self.assertEqual(2,dbcon.execute("select count(*) from events where kind='completed'").fetchone()[0])
                self.assertEqual(2,dbcon.execute("select count(*) from audit where action='task_dispatched'").fetchone()[0])
                dbcon.close()


if __name__=="__main__": unittest.main()
