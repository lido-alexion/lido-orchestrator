from __future__ import annotations
import argparse, json, os, time, sys, uuid
import hashlib, hmac, secrets
from dataclasses import replace
from .engine import Engine
from .codex_worker import CodexWorker
from .smtp_delivery import SMTPOutboxAdapter, load_config, load_receipt_key, verify_receipt
from . import resource_cleanup


def _new_session_id() -> str:
    try:
        return str(uuid.uuid4())
    except BaseException:
        return "unavailable"


def _reconcile_cycle(engine, *, source: str, session_id: str, cycle: int) -> list[str]:
    """Run one cycle and emit bounded operational telemetry without affecting it."""
    try:
        started = time.monotonic_ns()
    except BaseException:
        started = None
    outcome = "success"
    claimed_count = None
    try:
        claimed = engine.reconcile()
        claimed_count = len(claimed)
        return claimed
    except BaseException:
        outcome = "error"
        raise
    finally:
        try:
            record = {
                "event": "reconciliation_cycle",
                "schema_version": 1,
                "session_id": session_id,
                "cycle": cycle,
                "source": source,
                "outcome": outcome,
                "duration_ns": max(0, time.monotonic_ns() - started) if started is not None else None,
                "claimed_count": claimed_count,
            }
            sys.stderr.write(json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n")
            sys.stderr.flush()
        except BaseException:
            # Telemetry is best effort; it must not change reconciliation behavior.
            pass

def main(argv=None):
    p=argparse.ArgumentParser(prog="lido",description="Local durable Codex supervisor (dry-run by default)",allow_abbrev=False)
    p.add_argument("--db",default=os.environ.get("LIDO_DB","lido.sqlite3"))
    p.add_argument("--supervisor-stale-seconds",type=int,default=10800,help="supervisor lease stale threshold (default 10800)")
    p.add_argument("--supervisor-inactive-seconds",type=int,default=21600,help="supervisor lease inactive threshold (default 21600)")
    p.add_argument("--supervisor-orphan-seconds",type=int,default=86400,help="supervisor lease orphan threshold (default 86400)")
    p.add_argument("--codex",action="store_true",help="enable the real Codex adapter (also requires LIDO_ENABLE_CODEX=1)")
    p.add_argument("--allow-preapproved-read-only-steps",action="store_true",
                   help="release explicitly preapproved read-only dependent steps immediately")
    p.add_argument("--automatic-worker-cleanup",action="store_true",
                   help="opt in to process-free cleanup after exact inactivity and preservation authorization")
    sub=p.add_subparsers(dest="cmd",required=True)
    sub.add_parser("info",help="show command and safety summary")
    sup=sub.add_parser("supervisor"); ss=sup.add_subparsers(dest="supervisor_cmd",required=True)
    a=ss.add_parser("register"); a.add_argument("supervisor_id"); a.add_argument("project_id"); a.add_argument("worker_id"); a.add_argument("--trusted-local",action="store_true",required=True,help="confirm registration by the trusted local operator")
    ss.add_parser("list")
    a=ss.add_parser("status"); a.add_argument("supervisor_id")
    a=ss.add_parser("unregister"); a.add_argument("supervisor_id"); a.add_argument("--actor",required=True)
    a=ss.add_parser("heartbeat"); a.add_argument("supervisor_id"); a.add_argument("--generation",type=int)
    a=ss.add_parser("lifecycle"); a.add_argument("supervisor_id"); a.add_argument("action",choices=["suspend","resume","complete","deboard","takeover"]); a.add_argument("--actor",required=True)
    a=ss.add_parser("adopt"); a.add_argument("task_id"); a.add_argument("supervisor_id"); a.add_argument("--actor",required=True,help="trusted local operator making an audited recovery decision")
    a=ss.add_parser("dispose"); a.add_argument("task_id"); a.add_argument("--actor",required=True); a.add_argument("--reason",required=True)
    a=ss.add_parser("reap"); a.add_argument("--actor",required=True,help="trusted local management identity")
    a=ss.add_parser("route"); a.add_argument("supervisor_id"); a.add_argument("event_id"); a.add_argument("--issued",type=int,required=True); a.add_argument("--signature",required=True,help="hex HMAC over supervisor id, event id, authoritative type and issued time")
    a=ss.add_parser("bind-recipient"); a.add_argument("supervisor_id"); a.add_argument("recipient"); a.add_argument("--actor",required=True)
    a=ss.add_parser("unbind-recipient"); a.add_argument("recipient"); a.add_argument("--actor",required=True)
    a=sub.add_parser("register-project"); a.add_argument("project_id"); a.add_argument("root"); a.add_argument("--writable",action="store_true")
    a=sub.add_parser("submit"); a.add_argument("project_id"); a.add_argument("intention"); a.add_argument("--no-approval",action="store_true"); a.add_argument("--read-only",action="store_true",default=None); a.add_argument("--max-attempts",type=int,default=3); a.add_argument("--supervisor",help="scope this new assignment to an active registered supervisor")
    a.add_argument("--workflow-budget",type=int,default=None,help="durable total task/dispatch budget for this workflow")
    a.add_argument("--workflow-max-depth",type=int,default=None); a.add_argument("--workflow-timeout",type=int,default=None,help="workflow lifetime in seconds")
    a=sub.add_parser("approve"); a.add_argument("task_id"); a.add_argument("--actor",default="local-operator")
    a=sub.add_parser("status")
    a=sub.add_parser("project-snapshot"); a.add_argument("project_id"); a.add_argument("--max-entries",type=int,default=200)
    sub.add_parser("outbox")
    a=sub.add_parser("claim-outbox"); a.add_argument("owner"); a.add_argument("--limit",type=int,default=20); a.add_argument("--lease-seconds",type=int,default=60)
    a=sub.add_parser("retry-outbox"); a.add_argument("event_id"); a.add_argument("claim_token"); a.add_argument("--error",default="delivery failed")
    a=sub.add_parser("ack-outbox"); a.add_argument("event_id"); a.add_argument("--actor",default="local-operator"); a.add_argument("--claim-token")
    a=sub.add_parser("smtp-deliver"); a.add_argument("--config",required=True); a.add_argument("--receipt-key-file",required=True); a.add_argument("--enable",action="store_true",help="explicitly enable SMTP delivery for this invocation")
    a=sub.add_parser("verify-receipt"); a.add_argument("--receipt-key-file",required=True)
    a=sub.add_parser("reconcile"); a.add_argument("--max-tasks",type=int,default=20)
    a=sub.add_parser("serve"); a.add_argument("--interval",type=int,default=300); a.add_argument("--once",action="store_true"); a.add_argument("--max-tasks",type=int,default=20)
    a=sub.add_parser("stop"); a.add_argument("state",choices=["on","off"])
    a=sub.add_parser("terminate-workflow"); a.add_argument("workflow_id"); a.add_argument("--actor",default="local-operator"); a.add_argument("--reason",required=True)
    a=sub.add_parser("worker-resource-verify",help="record trusted-local exact-attempt inactivity evidence"); a.add_argument("task_id"); a.add_argument("attempt",type=int); a.add_argument("generation",type=int); a.add_argument("--evidence",required=True)
    a=sub.add_parser("worker-resource-authorize",help="separately authorize disposal after preservation review"); a.add_argument("task_id"); a.add_argument("attempt",type=int); a.add_argument("generation",type=int); a.add_argument("--evidence",required=True)
    a=sub.add_parser("worker-resource-cleanup",help="inspect or clean resources explicitly owned by a task"); a.add_argument("task_id"); a.add_argument("--execute",action="store_true",help="perform cleanup; default is dry-run"); a.add_argument("--retry",action="store_true"); a.add_argument("--retention-days",type=int); a.add_argument("--max-bytes",type=int)
    a=sub.add_parser("worker-resource-policy",help="persist cleanup retention and byte limits"); a.add_argument("--retention-days",type=int,required=True); a.add_argument("--max-bytes",type=int,required=True)
    args=p.parse_args(argv)
    if not 1 <= args.supervisor_stale_seconds <= args.supervisor_inactive_seconds <= args.supervisor_orphan_seconds <= 31536000:
        p.error("supervisor lease thresholds must be positive, ordered, and at most one year")
    if args.codex and os.environ.get("LIDO_ENABLE_CODEX") != "1":
        p.error("--codex also requires LIDO_ENABLE_CODEX=1")
    worker=CodexWorker(enabled=True,
        event_sink=lambda eid,tid,kind,payload: engine.ingest_event(eid,tid,kind,payload),
        resource_register=lambda tid,kind,path,root: engine.own_worker_resource(tid,kind,path,root),
        process_started=lambda tid,attempt,generation,pid,pgid: engine.record_worker_process(tid,attempt,generation,pid,pgid),
        process_exited=lambda tid,attempt,generation,pid,code: engine.record_worker_process_exit(tid,attempt,generation,pid,code)) if args.codex else None
    engine=Engine(args.db,dry_run=not args.codex,worker=worker,
                  allow_preapproved_read_only_steps=args.allow_preapproved_read_only_steps,
                  supervisor_stale_seconds=args.supervisor_stale_seconds,
                  supervisor_inactive_seconds=args.supervisor_inactive_seconds,
                  supervisor_orphan_seconds=args.supervisor_orphan_seconds,
                  automatic_worker_cleanup=args.automatic_worker_cleanup)
    try:
        if args.cmd=="register-project": engine.register_project(args.project_id,args.root,read_only=not args.writable); out={"registered":args.project_id}
        elif args.cmd=="info": out={"commands":["supervisor register|list|status|heartbeat|lifecycle|adopt|dispose|reap|unregister|route|bind-recipient|unbind-recipient","register-project","submit","approve","status","reconcile","serve","smtp-deliver","verify-receipt","worker-resource-verify","worker-resource-authorize","worker-resource-cleanup","worker-resource-policy"],"defaults":{"dispatch":"disabled unless explicitly opted in","approval":"required","network_listener":False,"worker_resource_cleanup":"dry-run; --execute required for explicit deletion","automatic_worker_cleanup":"disabled unless --automatic-worker-cleanup is set; process_free attempts only","supervisor_lease_stale_seconds":args.supervisor_stale_seconds,"supervisor_inactive_seconds":args.supervisor_inactive_seconds,"supervisor_orphan_seconds":args.supervisor_orphan_seconds}}
        elif args.cmd=="supervisor":
            if args.supervisor_cmd=="register":
                token=secrets.token_urlsafe(32)
                engine.register_supervisor(args.supervisor_id,args.project_id,args.worker_id,token_hash=hashlib.sha256(token.encode()).hexdigest())
                out={"registered":args.supervisor_id,"project_id":args.project_id,"worker_id":args.worker_id,"generation":1,"trusted_local":True,"route_token_once":token}
            elif args.supervisor_cmd=="list": out={"supervisors":engine.list_supervisors()}
            elif args.supervisor_cmd=="status": out=engine.supervisor_status(args.supervisor_id)
            elif args.supervisor_cmd=="unregister": engine.unregister_supervisor(args.supervisor_id,actor=args.actor); out={"revoked":args.supervisor_id}
            elif args.supervisor_cmd=="heartbeat":
                token=sys.stdin.readline(1026).strip(); out=engine.supervisor_heartbeat(args.supervisor_id,token=token,generation=args.generation)
            elif args.supervisor_cmd=="reap": out={"changed":engine.reap_supervisors(actor=args.actor)}
            elif args.supervisor_cmd=="lifecycle":
                new_token=secrets.token_urlsafe(32) if args.action=="takeover" else None
                result=engine.supervisor_lifecycle(args.supervisor_id,action=args.action,actor=args.actor,
                    token_hash=hashlib.sha256(new_token.encode()).hexdigest() if new_token else None)
                out={"supervisor":result}
                if new_token: out["route_token_once"]=new_token
            elif args.supervisor_cmd=="adopt": out={"task":engine.adopt_supervisor_task(args.task_id,args.supervisor_id,actor=args.actor)}
            elif args.supervisor_cmd=="dispose": out={"cancelled":engine.dispose_supervisor_task(args.task_id,actor=args.actor,reason=args.reason)}
            elif args.supervisor_cmd=="bind-recipient": engine.bind_supervisor_recipient(args.supervisor_id,args.recipient,actor=args.actor); out={"bound":args.recipient,"supervisor_id":args.supervisor_id}
            elif args.supervisor_cmd=="unbind-recipient": engine.unbind_supervisor_recipient(args.recipient,actor=args.actor); out={"unbound":args.recipient}
            else:
                token=sys.stdin.readline(1026).strip()
                if len(token)>1024: p.error("supervisor route rejected")
                out=engine.route_supervisor_event(args.supervisor_id,args.event_id,issued=args.issued,signature=args.signature,token=token)
        elif args.cmd=="submit": out={"task_id":engine.submit(args.project_id,args.intention,approval_required=not args.no_approval,read_only=args.read_only,max_attempts=args.max_attempts,
            supervisor_id=args.supervisor,workflow_budget=args.workflow_budget,workflow_max_depth=args.workflow_max_depth,workflow_timeout=args.workflow_timeout)}
        elif args.cmd=="approve": engine.approve(args.task_id,actor=args.actor); out={"approved":args.task_id}
        elif args.cmd=="status": out=engine.snapshot()
        elif args.cmd=="project-snapshot": out=engine.project_snapshot(args.project_id,max_entries=args.max_entries)
        elif args.cmd=="outbox": out={"pending":engine.pending_outbox()}
        elif args.cmd=="claim-outbox": out={"claimed":engine.claim_outbox(owner=args.owner,limit=args.limit,lease_seconds=args.lease_seconds)}
        elif args.cmd=="retry-outbox": out={"retry_in_seconds":engine.retry_outbox(args.event_id,claim_token=args.claim_token,error=args.error)}
        elif args.cmd=="ack-outbox": engine.acknowledge_outbox(args.event_id,actor=args.actor,claim_token=args.claim_token); out={"acknowledged":args.event_id}
        elif args.cmd=="smtp-deliver":
            if not args.enable: p.error("SMTP delivery is disabled; pass --enable for an explicit invocation")
            config=load_config(args.config)
            config=replace(config, receipt_key=load_receipt_key(args.receipt_key_file))
            out={"sent":SMTPOutboxAdapter(engine,config).deliver_once()}
        elif args.cmd=="verify-receipt":
            try:
                token=sys.stdin.readline(1026)
                if len(token)>1024: raise ValueError
                token=token.strip()
                key=load_receipt_key(args.receipt_key_file)
                receipt=verify_receipt(token,key)
                event=engine.consume_receipt(receipt["event_id"],receipt["token_id"],expected_type=receipt["event_type"])
                out={"accepted":True,"event_id":event["id"],"event_type":event["event_type"],"task_id":event["task_id"]}
            except Exception:
                p.error("receipt rejected")
        elif args.cmd=="reconcile":
            if not 1<=args.max_tasks<=Engine.MAX_WORKFLOW_TASKS: p.error("--max-tasks must be between 1 and 100")
            engine.max_dispatches=args.max_tasks
            claimed=_reconcile_cycle(engine, source="reconcile", session_id=_new_session_id(), cycle=1)
            if worker is not None:
                worker.wait()
            out={"claimed":claimed}
        elif args.cmd=="stop": engine.stop(args.state=="on"); out={"emergency_stop":args.state=="on"}
        elif args.cmd=="terminate-workflow": engine.terminate_workflow(args.workflow_id,actor=args.actor,reason=args.reason); out={"terminated":args.workflow_id}
        elif args.cmd=="worker-resource-verify": out={"verified":engine.verify_worker_inactive(args.task_id,args.attempt,args.evidence,args.generation)}
        elif args.cmd=="worker-resource-authorize": out={"authorized":engine.authorize_worker_cleanup(args.task_id,args.attempt,args.evidence,args.generation)}
        elif args.cmd=="worker-resource-cleanup":
            if (args.retention_days is not None and args.retention_days < 0) or (args.max_bytes is not None and args.max_bytes < 1): p.error("retention and byte limits must be non-negative/positive")
            out=resource_cleanup.cleanup_task(engine,args.task_id,execute=args.execute,retry=args.retry,
                retention_seconds=args.retention_days*86400 if args.retention_days is not None else None,max_bytes=args.max_bytes)
        elif args.cmd=="worker-resource-policy":
            if args.retention_days < 0 or args.max_bytes < 1: p.error("retention and byte limits must be non-negative/positive")
            out=resource_cleanup.set_policy(engine,retention_seconds=args.retention_days*86400,max_bytes=args.max_bytes)
        else:
            if args.interval<1: p.error("interval must be positive")
            if args.once:
                if not 1<=args.max_tasks<=Engine.MAX_WORKFLOW_TASKS: p.error("--max-tasks must be between 1 and 100")
                engine.max_dispatches=args.max_tasks
            session_id = _new_session_id()
            cycle = 0
            while True:
                cycle += 1
                _reconcile_cycle(engine, source="serve", session_id=session_id, cycle=cycle)
                if args.once:
                    # The callback itself reconciles immediate dependent work.
                    # wait() drains that callback chain without starting a fresh
                    # unrelated batch after the one-shot boundary.
                    if worker is not None: worker.wait()
                    break
                time.sleep(args.interval)
            out={"serving":not args.once,"interval_seconds":args.interval}
        print(json.dumps(out,indent=2,sort_keys=True))
    finally:
        if worker is not None: worker.close()
        engine.close()

if __name__=="__main__": main()
