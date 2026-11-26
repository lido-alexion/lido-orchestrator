import tempfile
import os
import unittest
import io, json, contextlib, hashlib, threading
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch

from lido_orchestrator.engine import Engine
from lido_orchestrator.smtp_delivery import SMTPOutboxAdapter, make_receipt, parse_config, verify_receipt, load_receipt_key, load_config


KEY = "11" * 32
CONFIG = """enabled=true
host=mail.synthetic.invalid
port=587
sender=orchestrator@example.invalid
recipients=supervisor@example.invalid
username=fixture-user
password=fixture-password
timeout=5
min_interval=0
"""


class FakeSMTP:
    instances = []
    def __init__(self, host, port, timeout):
        self.host, self.port, self.timeout = host, port, timeout
        self.sent = []
        self.instances.append(self)
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def ehlo(self): pass
    def starttls(self, *, context):
        assert context.check_hostname and context.verify_mode
    def login(self, username, password): self.credentials = (username, password)
    def send_message(self, msg, **kwargs): self.sent.append((msg, kwargs))


class SMTPDeliveryTests(unittest.TestCase):
    def test_strict_parser_and_secret_safe_errors(self):
        cfg = parse_config(CONFIG)
        self.assertTrue(cfg.enabled)
        for malformed in (CONFIG + "\nextra=value", CONFIG.replace("enabled=true", "enabled=TRUE"),
                          CONFIG.replace("password=fixture-password", "password=fixture-password\npassword=oops"),
                          CONFIG + "receipt_key=" + KEY,
                          CONFIG.replace("timeout=5", "timeout=999")):
            with self.assertRaisesRegex(ValueError, "invalid SMTP configuration") as err:
                parse_config(malformed)
            self.assertNotIn("fixture-password", str(err.exception))

    def test_synthetic_tls_delivery_and_stable_message_id(self):
        FakeSMTP.instances.clear()
        config = replace(parse_config(CONFIG), receipt_key=bytes.fromhex(KEY))
        item = {"id": "worker:event-123", "event_type": "worker.escalation", "lease_token": "claim"}
        class EngineStub:
            def acquire_smtp_delivery_slot(self, **kwargs): return {"token":"slot", "wait_seconds":0}
            def release_smtp_delivery_slot(self, token): pass
            def claim_outbox(self, **kwargs): return [item]
            def acknowledge_outbox(self, *args, **kwargs): self.acked = args[0]
            def retry_outbox(self, *args, **kwargs): self.failed = True
        engine = EngineStub()
        with patch("lido_orchestrator.smtp_delivery.smtplib.SMTP", FakeSMTP):
            adapter = SMTPOutboxAdapter(engine, config, clock=lambda: 1000, smtp_factory=FakeSMTP)
            self.assertEqual(1, adapter.deliver_once())
        msg = FakeSMTP.instances[0].sent[0][0]
        self.assertEqual("<" + __import__("hashlib").sha256(item["id"].encode()).hexdigest() + "@lido.local>", msg["Message-ID"])
        self.assertEqual("worker:event-123", engine.acked)
        self.assertIn("Receipt:", msg.get_content())

    def test_receipt_authoritative_match_expiry_and_replay(self):
        now = [1000]
        key = bytes.fromhex(KEY)
        token = make_receipt("event-1", "worker.escalation", key, now=now[0], lifetime=30)
        with tempfile.TemporaryDirectory() as temp:
            engine = Engine(Path(temp) / "db.sqlite", dry_run=False, clock=lambda: now[0])
            try:
                engine.register_project("demo", temp)
                task = engine.submit("demo", "synthetic", approval_required=False)
                engine.reconcile()
                engine.ingest_event("event-1", task, "failed", {"attempt": 1})
                receipt = verify_receipt(token, key, now=now[0])
                authoritative = engine.consume_receipt(receipt["event_id"], receipt["token_id"], expected_type=receipt["event_type"])
                self.assertEqual("worker.escalation", authoritative["event_type"])
                with self.assertRaisesRegex(ValueError, "replay"):
                    engine.consume_receipt(receipt["event_id"], receipt["token_id"], expected_type=receipt["event_type"])
                renewed = verify_receipt(make_receipt("event-1", "worker.escalation", key, now=now[0]), key, now=now[0])
                with self.assertRaisesRegex(ValueError, "already acknowledged"):
                    engine.consume_receipt(renewed["event_id"], renewed["token_id"], expected_type=renewed["event_type"])
                mismatch = make_receipt("event-1", "worker.normal", key, now=now[0])
                bad = verify_receipt(mismatch, key, now=now[0])
                with self.assertRaisesRegex(ValueError, "mismatch"):
                    engine.consume_receipt(bad["event_id"], bad["token_id"], expected_type=bad["event_type"])
                with self.assertRaisesRegex(ValueError, "expired"):
                    verify_receipt(token, key, now=1031)
            finally:
                engine.close()

    def test_uppercase_existing_settings_and_private_key_file(self):
        cfg = parse_config("""SMTP_HOST=mail.synthetic.invalid
SMTP_PORT=587
SMTP_SECURITY=starttls
SMTP_USERNAME=fixture-user
SMTP_PASSWORD=fixture-password
SMTP_FROM=orchestrator@example.invalid
SMTP_TO=supervisor@example.invalid
SMTP_SUBJECT_PREFIX=[LIDO-WAKE]
""")
        self.assertEqual("[LIDO-WAKE]", cfg.subject_prefix)
        self.assertEqual("starttls", cfg.security)
        self.assertNotIn("fixture-password", repr(cfg))
        self.assertEqual('[LIDO-WAKE]',parse_config('''SMTP_HOST=mail.synthetic.invalid\nSMTP_PORT=587\nSMTP_SECURITY=starttls\nSMTP_USERNAME=fixture-user\nSMTP_PASSWORD=fixture-password\nSMTP_FROM=orchestrator@example.invalid\nSMTP_TO=supervisor@example.invalid\nSMTP_SUBJECT_PREFIX="[LIDO-WAKE]"\n''').subject_prefix)
        with tempfile.TemporaryDirectory() as temp:
            key_path = Path(temp) / "receipt.key"
            key_path.write_text(KEY + "\n")
            key_path.chmod(0o600)
            self.assertEqual(bytes.fromhex(KEY), load_receipt_key(key_path))
            key_link=Path(temp)/"receipt-link"; key_link.symlink_to(key_path)
            with self.assertRaisesRegex(ValueError,"unable to load receipt key"): load_receipt_key(key_link)
            key_fifo=Path(temp)/"receipt-fifo"; os.mkfifo(key_fifo)
            with self.assertRaisesRegex(ValueError,"unable to load receipt key"): load_receipt_key(key_fifo)
            key_path.chmod(0o640)
            with self.assertRaisesRegex(ValueError, "unable to load receipt key"):
                load_receipt_key(key_path)
            config_path=Path(temp)/"smtp.env"
            config_path.write_text(CONFIG); config_path.chmod(0o600)
            self.assertNotIn("fixture-password",repr(load_config(config_path)))
            config_link=Path(temp)/"smtp-link"; config_link.symlink_to(config_path)
            with self.assertRaisesRegex(ValueError,"unable to load SMTP configuration"): load_config(config_link)
            fifo=Path(temp)/"smtp-fifo"; os.mkfifo(fifo)
            with self.assertRaisesRegex(ValueError,"unable to load SMTP configuration"): load_config(fifo)

    def test_refusal_retries_and_one_event_lease_covers_throttle(self):
        config = replace(parse_config(CONFIG.replace("min_interval=0", "min_interval=120")), receipt_key=bytes.fromhex(KEY))
        item = {"id":"worker:e1", "event_type":"worker.escalation", "lease_token":"claim"}
        class EngineStub:
            def acquire_smtp_delivery_slot(self, **kwargs): return {"token":"slot", "wait_seconds":120}
            def release_smtp_delivery_slot(self, token): self.released=token
            def claim_outbox(self, **kwargs):
                self.kwargs = kwargs
                return [item]
            def acknowledge_outbox(self, *args, **kwargs): raise AssertionError("refused mail must not acknowledge")
            def retry_outbox(self, *args, **kwargs): self.retried = True
        class RefusingSMTP(FakeSMTP):
            def send_message(self, msg, **kwargs): return {"supervisor@example.invalid": (550, b"rejected")}
        engine=EngineStub()
        with patch("lido_orchestrator.smtp_delivery.time.sleep") as sleep:
            self.assertEqual(0, SMTPOutboxAdapter(engine,config,smtp_factory=RefusingSMTP).deliver_once())
        sleep.assert_called_once_with(120)
        self.assertTrue(engine.retried)
        self.assertEqual("slot",engine.released)
        self.assertEqual(1, engine.kwargs["limit"])
        self.assertGreaterEqual(engine.kwargs["lease_seconds"], config.min_interval + config.timeout * 8)

    def test_cli_isolated_fixture_pilot_and_stdin_receipt(self):
        from lido_orchestrator.cli import main
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/"pilot.sqlite3"
            conf=Path(temp)/"smtp.env"
            keyfile=Path(temp)/"receipt.key"
            conf.write_text("""SMTP_HOST=mail.synthetic.invalid
SMTP_PORT=587
SMTP_SECURITY=starttls
SMTP_USERNAME=fixture-user
SMTP_PASSWORD=fixture-password
SMTP_FROM=orchestrator@example.invalid
SMTP_TO=supervisor@example.invalid
SMTP_SUBJECT_PREFIX=[LIDO-WAKE]
""")
            conf.chmod(0o600)
            keyfile.write_text(KEY+"\n"); keyfile.chmod(0o600)
            engine=Engine(db,dry_run=False)
            try:
                engine.register_project("demo",temp)
                task=engine.submit("demo","fixture event",approval_required=False)
                engine.reconcile()
                engine.ingest_event("pilot-event",task,"failed",{"attempt":1})
            finally: engine.close()
            FakeSMTP.instances.clear()
            output=io.StringIO()
            with patch("lido_orchestrator.smtp_delivery.smtplib.SMTP",FakeSMTP), contextlib.redirect_stdout(output):
                main(["--db",str(db),"smtp-deliver","--config",str(conf),"--receipt-key-file",str(keyfile),"--enable"])
            sent=json.loads(output.getvalue())
            self.assertEqual(1,sent["sent"])
            msg=FakeSMTP.instances[0].sent[0][0]
            self.assertTrue(msg["Subject"].startswith("[LIDO-WAKE] "))
            self.assertEqual("<"+hashlib.sha256(b"worker:pilot-event").hexdigest()+"@lido.local>",msg["Message-ID"])
            token=next(line.split(": ",1)[1] for line in msg.get_content().splitlines() if line.startswith("Receipt:"))
            out=io.StringIO()
            with patch("sys.stdin",io.StringIO(token+"\n")), contextlib.redirect_stdout(out):
                main(["--db",str(db),"verify-receipt","--receipt-key-file",str(keyfile)])
            self.assertEqual("pilot-event",json.loads(out.getvalue())["event_id"])
            with self.assertRaises(SystemExit):
                with patch("sys.stdin",io.StringIO(token+"\n")), contextlib.redirect_stderr(io.StringIO()):
                    main(["--db",str(db),"verify-receipt","--receipt-key-file",str(keyfile)])

    def test_concurrent_adapters_claim_distinct_single_event_leases(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/"db.sqlite"
            engine=Engine(db,dry_run=False)
            second=Engine(db,dry_run=False)
            try:
                engine.register_project("demo",temp)
                task=engine.submit("demo","synthetic",approval_required=False); engine.reconcile()
                engine.ingest_event("parallel-1",task,"failed",{"attempt":1})
                task2=engine.submit("demo","synthetic2",approval_required=False); engine.reconcile()
                engine.ingest_event("parallel-2",task2,"failed",{"attempt":1})
                claims=[]; barrier=threading.Barrier(3)
                def claim(target):
                    barrier.wait()
                    claims.extend(target.claim_outbox(owner="parallel",limit=1,lease_seconds=90,
                        event_types=("worker.escalation","worker.supervisor_decision")))
                workers=[threading.Thread(target=claim,args=(target,)) for target in (engine,second)]
                for worker in workers: worker.start()
                barrier.wait()
                for worker in workers: worker.join()
                self.assertEqual(2,len(claims))
                self.assertEqual(2,len({row["id"] for row in claims}))
                slot_barrier=threading.Barrier(3); slots=[]
                def get_slot(target):
                    slot_barrier.wait()
                    slots.append(target.acquire_smtp_delivery_slot(lease_seconds=90,min_interval=2))
                workers=[threading.Thread(target=get_slot,args=(target,)) for target in (engine,second)]
                for worker in workers: worker.start()
                slot_barrier.wait()
                for worker in workers: worker.join()
                acquired=[slot for slot in slots if slot is not None]
                self.assertEqual(1,len(acquired))
                engine.release_smtp_delivery_slot(acquired[0]["token"])
                next_slot=engine.acquire_smtp_delivery_slot(lease_seconds=90,min_interval=2)
                self.assertGreater(next_slot["wait_seconds"],0)
                engine.release_smtp_delivery_slot(next_slot["token"])
            finally:
                engine.close(); second.close()

    def test_routine_completion_is_not_emailed(self):
        with tempfile.TemporaryDirectory() as temp:
            engine = Engine(Path(temp) / "db.sqlite", dry_run=False)
            try:
                engine.register_project("demo", temp)
                task = engine.submit("demo", "routine", approval_required=False)
                engine.reconcile()
                engine.ingest_event("routine-1", task, "completed", {"attempt": 1, "decision": "continue"})
                self.assertEqual([], engine.pending_outbox())
            finally:
                engine.close()

    def test_empty_outbox_does_not_consume_throttle_slot(self):
        with tempfile.TemporaryDirectory() as temp:
            engine=Engine(Path(temp)/"db.sqlite",dry_run=False)
            try:
                config=replace(parse_config(CONFIG.replace("min_interval=0","min_interval=60")),receipt_key=bytes.fromhex(KEY))
                self.assertEqual(0,SMTPOutboxAdapter(engine,config).deliver_once())
                self.assertEqual(0,engine.db.execute("SELECT next_allowed FROM smtp_delivery_slot WHERE id=1").fetchone()[0])
            finally: engine.close()


if __name__ == "__main__": unittest.main()
