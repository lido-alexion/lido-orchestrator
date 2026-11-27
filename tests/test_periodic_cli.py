from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PeriodicCliReconciliationAcceptanceTests(unittest.TestCase):
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
                proc.communicate(timeout=5)
            with sqlite3.connect(db) as check:
                rows = check.execute("SELECT details FROM audit WHERE action='supervisor_lease_reaped'").fetchall()
            self.assertTrue(rows, "periodic serve loop did not persist a lease reaping transition")
            self.assertTrue(any("orphaned" in row[0] for row in rows))


if __name__ == "__main__":
    unittest.main()
