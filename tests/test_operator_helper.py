import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "tools" / "supervisor-operator.py"


class OperatorHelperTests(unittest.TestCase):
    def test_enroll_heartbeat_deboard_and_symlink_collision_preservation(self):
        with tempfile.TemporaryDirectory(prefix="lido-operator-test-") as td:
            base = Path(td)
            project = base / "project"
            project.mkdir()
            state = base / "secrets"
            db = base / "state.sqlite3"
            cli = [sys.executable, "-m", "lido_orchestrator", "--db", str(db)]
            subprocess.run(cli + ["register-project", "fixture", str(project)], cwd=ROOT, check=True, capture_output=True)
            command = [sys.executable, str(HELPER), "--db", str(db), "--state-dir", str(state)]
            enrolled = subprocess.run(command + ["enroll", "fixture-1", "fixture", "worker"], cwd=ROOT,
                                      check=True, capture_output=True, text=True)
            token = state / "fixture-1.token"
            self.assertNotIn(token.read_text().strip(), enrolled.stdout)
            self.assertEqual(0o700, state.stat().st_mode & 0o777)
            self.assertEqual(0o600, token.stat().st_mode & 0o777)
            beat = subprocess.run(command + ["heartbeat", "fixture-1", "--generation", "1"], cwd=ROOT,
                                  check=True, capture_output=True, text=True)
            self.assertIn('"generation": 1', beat.stdout)
            done = subprocess.run(command + ["deboard", "fixture-1"], cwd=ROOT,
                                  check=True, capture_output=True, text=True)
            self.assertIn('"deboarded"', done.stdout)
            self.assertFalse(token.exists())

            sentinel = base / "sentinel"
            sentinel.write_text("keep")
            token.symlink_to(sentinel)
            failed = subprocess.run(command + ["enroll", "fixture-1", "fixture", "worker"], cwd=ROOT,
                                    capture_output=True, text=True)
            self.assertNotEqual(0, failed.returncode)
            self.assertTrue(token.is_symlink())
            self.assertEqual("keep", sentinel.read_text())

    def test_state_directory_symlink_is_rejected_without_chmod_target(self):
        with tempfile.TemporaryDirectory(prefix="lido-operator-test-") as td:
            base = Path(td)
            private = base / "private"
            private.mkdir(mode=0o755)
            private.chmod(0o755)
            alias = base / "alias"
            alias.symlink_to(private, target_is_directory=True)
            run = subprocess.run([sys.executable, str(HELPER), "--db", str(base / "x.db"),
                                  "--state-dir", str(alias), "status", "fake"], cwd=ROOT,
                                 capture_output=True, text=True)
            self.assertNotEqual(0, run.returncode)
            self.assertEqual(0o755, private.stat().st_mode & 0o777)


if __name__ == "__main__":
    unittest.main()
