from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from lido_orchestrator.cli import _reconcile_cycle


ROOT = Path(__file__).resolve().parents[1]


class PeriodicCliReconciliationAcceptanceTests(unittest.TestCase):
    def test_cycle_telemetry_is_fixed_schema_and_failure_is_non_interfering(self):
        class Engine:
            def reconcile(self):
                return ["task detail must not be logged"]

        class BrokenStderr:
            def write(self, value):
                raise OSError("fixture output failure")

            def flush(self):
                raise OSError("fixture flush failure")

        with patch("lido_orchestrator.cli.sys.stderr", BrokenStderr()):
            self.assertEqual(_reconcile_cycle(Engine(), source="reconcile", session_id="fixture", cycle=1),
                             ["task detail must not be logged"])

        class FailingEngine:
            def reconcile(self):
                raise RuntimeError("original reconciliation failure")

        with patch("lido_orchestrator.cli.sys.stderr", BrokenStderr()):
            with self.assertRaisesRegex(RuntimeError, "original reconciliation failure"):
                _reconcile_cycle(FailingEngine(), source="serve", session_id="fixture", cycle=3)

        records = []
        class CaptureStderr:
            def write(self, value):
                records.append(value)

            def flush(self):
                pass

        with patch("lido_orchestrator.cli.sys.stderr", CaptureStderr()):
            with self.assertRaisesRegex(RuntimeError, "original reconciliation failure"):
                _reconcile_cycle(FailingEngine(), source="serve", session_id="fixture", cycle=2)
        import json
        record = json.loads(records[0])
        self.assertEqual(set(record), {"event", "schema_version", "session_id", "cycle", "source",
                                       "outcome", "duration_ns", "claimed_count"})
        self.assertEqual((record["source"], record["outcome"], record["claimed_count"]), ("serve", "error", None))
        self.assertGreaterEqual(record["duration_ns"], 0)
        self.assertNotIn("task detail", records[0])
        self.assertNotIn("original reconciliation failure", records[0])

    def test_one_shot_stdout_contract_and_restart_source_labels(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "fixture.sqlite3"
            env = {**os.environ}
            env.pop("PYTHONPATH", None)
            def run(command):
                return subprocess.run([sys.executable, "-m", "lido_orchestrator", "--db", str(db), command],
                                      cwd=ROOT, env=env, check=True, capture_output=True, text=True, timeout=10)
            first, second = run("reconcile"), run("reconcile")
            self.assertEqual(set(__import__("json").loads(first.stdout)), {"claimed"})
            a, b = (__import__("json").loads(result.stderr) for result in (first, second))
            self.assertEqual((a["source"], b["source"]), ("reconcile", "reconcile"))
            self.assertNotEqual(a["session_id"], b["session_id"])

    def test_serve_process_runs_reaper_on_actual_periodic_cadence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "fixture.sqlite3"
            project = root / "project"
            project.mkdir()
            env = dict(os.environ)
            env.pop("PYTHONPATH", None)

            def cli(*args: str) -> None:
                subprocess.run([sys.executable, "-m", "lido_orchestrator", "--db", str(db), *args],
                               cwd=ROOT, env=env, check=True, capture_output=True, text=True, timeout=10)

            cli("register-project", "fixture-project", str(project))
            cli("supervisor", "register", "fixture-supervisor", "fixture-project", "fixture-worker", "--trusted-local")
            proc = subprocess.Popen([
                sys.executable, "-m", "lido_orchestrator", "--db", str(db),
                "--supervisor-stale-seconds", "1", "--supervisor-inactive-seconds", "2",
                "--supervisor-orphan-seconds", "3", "serve", "--interval", "1",
            ], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                time.sleep(4.2)
            finally:
                proc.terminate()
                stdout, stderr = proc.communicate(timeout=5)
            with sqlite3.connect(db) as check:
                rows = check.execute("SELECT details FROM audit WHERE action='supervisor_lease_reaped'").fetchall()
            self.assertTrue(rows, "periodic serve loop did not persist a lease reaping transition")
            self.assertTrue(any("orphaned" in row[0] for row in rows))
            import json
            cycles = [json.loads(line) for line in stderr.splitlines()]
            self.assertGreaterEqual(len(cycles), 3)
            self.assertEqual([entry["cycle"] for entry in cycles], list(range(1, len(cycles) + 1)))
            self.assertEqual({entry["source"] for entry in cycles}, {"serve"})
            self.assertEqual({entry["session_id"] for entry in cycles}, {cycles[0]["session_id"]})
            self.assertEqual(stdout, "")


if __name__ == "__main__":
    unittest.main()
