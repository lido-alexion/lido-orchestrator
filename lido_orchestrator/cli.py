from __future__ import annotations
import argparse, json, os, time, sys
import hashlib, hmac, secrets
from dataclasses import replace
from .engine import Engine
from .codex_worker import CodexWorker
from .smtp_delivery import SMTPOutboxAdapter, load_config, load_receipt_key, verify_receipt

def main(argv=None):
    p=argparse.ArgumentParser(prog="lido",description="Local durable Codex supervisor (dry-run by default)")
    p.add_argument("--db",default=os.environ.get("LIDO_DB","lido.sqlite3"))
    p.add_argument("--codex",action="store_true",help="enable the real Codex adapter (also requires LIDO_ENABLE_CODEX=1)")
    p.add_argument("--allow-preapproved-read-only-steps",action="store_true",
                   help="release explicitly preapproved read-only dependent steps immediately")
    sub=p.add_subparsers(dest="cmd",required=True)
    sub.add_parser("info",help="show command and safety summary")
    sup=sub.add_parser("supervisor"); ss=sup.add_subparsers(dest="supervisor_cmd",required=True)
    a=ss.add_parser("register"); a.add_argument("supervisor_id"); a.add_argument("project_id"); a.add_argument("worker_id"); a.add_argument("--trusted-local",action="store_true",required=True,help="confirm registration by the trusted local operator")
    ss.add_parser("list")
    a=ss.add_parser("status"); a.add_argument("supervisor_id")
    a=ss.add_parser("unregister"); a.add_argument("supervisor_id"); a.add_argument("--actor",required=True)
    a=ss.add_parser("route"); a.add_argument("supervisor_id"); a.add_argument("event_id"); a.add_argument("--issued",type=int,required=True); a.add_argument("--signature",required=True,help="hex HMAC over supervisor id, event id, authoritative type and issued time")
    a=sub.add_parser("register-project"); a.add_argument("project_id"); a.add_argument("root"); a.add_argument("--writable",action="store_true")
    a=sub.add_parser("submit"); a.add_argument("project_id"); a.add_argument("intention"); a.add_argument("--no-approval",action="store_true"); a.add_argument("--read-only",action="store_true",default=None); a.add_argument("--max-attempts",type=int,default=3)
    a.add_argument("--workflow-budget",type=int,default=None,help="durable total task/dispatch budget for this workflow")
    a.add_argument("--workflow-max-depth",type=int,default=None); a.add_argument("--workflow-timeout",type=int,default=None,help="workflow lifetime in seconds")
    a=sub.add_parser("approve"); a.add_argument("task_id"); a.add_argument("--actor",default="local-operator")
    a=sub.add_parser("event"); a.add_argument("event_id"); a.add_argument("task_id"); a.add_argument("kind",choices=["completed","failed"]); a.add_argument("--payload",default="{}")
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
    args=p.parse_args(argv)
    if args.codex and os.environ.get("LIDO_ENABLE_CODEX") != "1":
        p.error("--codex also requires LIDO_ENABLE_CODEX=1")
    worker=CodexWorker(enabled=True,event_sink=lambda eid,tid,kind,payload: engine.ingest_event(eid,tid,kind,payload)) if args.codex else None
    engine=Engine(args.db,dry_run=not args.codex,worker=worker,
                  allow_preapproved_read_only_steps=args.allow_preapproved_read_only_steps)
    try:
        if args.cmd=="register-project": engine.register_project(args.project_id,args.root,read_only=not args.writable); out={"registered":args.project_id}
        elif args.cmd=="info": out={"commands":["supervisor register|list|status|unregister|route","register-project","submit","approve","status","reconcile","serve","smtp-deliver","verify-receipt"],"defaults":{"dispatch":"disabled unless explicitly opted in","approval":"required","network_listener":False}}
        elif args.cmd=="supervisor":
            if args.supervisor_cmd=="register":
                token=secrets.token_urlsafe(32)
                engine.register_supervisor(args.supervisor_id,args.project_id,args.worker_id,token_hash=hashlib.sha256(token.encode()).hexdigest())
                out={"registered":args.supervisor_id,"project_id":args.project_id,"worker_id":args.worker_id,"trusted_local":True,"route_token_once":token}
            elif args.supervisor_cmd=="list": out={"supervisors":engine.list_supervisors()}
            elif args.supervisor_cmd=="status": out=engine.supervisor_status(args.supervisor_id)
            elif args.supervisor_cmd=="unregister": engine.unregister_supervisor(args.supervisor_id,actor=args.actor); out={"revoked":args.supervisor_id}
            else:
                token=sys.stdin.readline(1026).strip()
                if len(token)>1024: p.error("supervisor route rejected")
                out=engine.route_supervisor_event(args.supervisor_id,args.event_id,issued=args.issued,signature=args.signature,token=token)
        elif args.cmd=="submit": out={"task_id":engine.submit(args.project_id,args.intention,approval_required=not args.no_approval,read_only=args.read_only,max_attempts=args.max_attempts,
            workflow_budget=args.workflow_budget,workflow_max_depth=args.workflow_max_depth,workflow_timeout=args.workflow_timeout)}
        elif args.cmd=="approve": engine.approve(args.task_id,actor=args.actor); out={"approved":args.task_id}
        elif args.cmd=="event": out={"accepted":engine.ingest_event(args.event_id,args.task_id,args.kind,json.loads(args.payload))}
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
            claimed=engine.reconcile()
            if worker is not None:
                worker.wait()
            out={"claimed":claimed}
        elif args.cmd=="stop": engine.stop(args.state=="on"); out={"emergency_stop":args.state=="on"}
        elif args.cmd=="terminate-workflow": engine.terminate_workflow(args.workflow_id,actor=args.actor,reason=args.reason); out={"terminated":args.workflow_id}
        else:
            if args.interval<1: p.error("interval must be positive")
            if args.once:
                if not 1<=args.max_tasks<=Engine.MAX_WORKFLOW_TASKS: p.error("--max-tasks must be between 1 and 100")
                engine.max_dispatches=args.max_tasks
            while True:
                engine.reconcile()
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
