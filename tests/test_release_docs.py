from __future__ import annotations

import re
import os
import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ReleaseDocumentationAcceptanceTests(unittest.TestCase):
    def test_local_markdown_links_resolve(self):
        for markdown in ROOT.glob("*.md"):
            for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", markdown.read_text()):
                if "://" in target or target.startswith("#"):
                    continue
                path = target.split("#", 1)[0]
                self.assertTrue((markdown.parent / path).exists(), f"{markdown.name}: {target}")

    def test_example_user_service_stays_dry_run(self):
        unit = (ROOT / "examples/lido-orchestrator.user.service").read_text()
        exec_start = next(line for line in unit.splitlines() if line.startswith("ExecStart="))
        self.assertIn("/venv-FULL_GIT_REVISION/bin/lido", exec_start)
        self.assertIn(" serve --interval 300", exec_start)
        self.assertIn("--db %h/.local/state/lido-orchestrator/state.sqlite3", exec_start)
        self.assertNotIn("--codex", exec_start)
        self.assertNotIn("LIDO_ENABLE_CODEX", unit)

    def test_install_and_rollback_guidance_is_revision_pinned_and_guarded(self):
        readme = (ROOT / "README.md").read_text()
        self.assertIn("git rev-parse HEAD", readme)
        self.assertIn("source-$revision", readme)
        self.assertIn("[[ -e \"$path\" || -L \"$path\" ]]", readme)
        self.assertIn("BEGIN release path preflight", readme)
        self.assertIn("dangling symlinks", readme)
        self.assertIn("--no-index --no-deps", readme)
        self.assertIn("online backup API", readme)
        self.assertIn("no backup or validation helper", readme)
        self.assertIn("no automated restore", readme)
        self.assertIn("set -eu", readme)
        self.assertIn('--db "$smoke_dir/fixture.sqlite3" info', readme)
        self.assertIn('--find-links "$LIDO_WHEELHOUSE"', readme)
        self.assertIn("uid=$(id -u)", readme)
        self.assertNotIn("/run/user/1000", readme)

    def test_release_path_preflight_is_executable_and_fails_before_writes(self):
        readme = (ROOT / "README.md").read_text()
        block = readme.split("# BEGIN release path preflight\n", 1)[1].split("# END release path preflight", 1)[0]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stage = root / "releases"
            stage.mkdir(mode=0o700)
            stage.chmod(0o700)
            source, venv = stage / "source-rev", stage / "venv-rev"

            def run() -> subprocess.CompletedProcess[str]:
                return subprocess.run(["bash", "-c", block + '\nprintf preflight-passed\n'],
                                      env={**os.environ, "stage_root": str(stage),
                                           "source_dir": str(source), "venv_dir": str(venv)},
                                      capture_output=True, text=True, check=False)

            self.assertEqual(run().returncode, 0)
            source.mkdir()
            result = run()
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("preflight-passed", result.stdout)
            source.rmdir()
            source.symlink_to(root / "dangling-target")
            result = run()
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("preflight-passed", result.stdout)
            source.unlink()
            stage.chmod(0o755)
            result = run()
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("preflight-passed", result.stdout)
            self.assertFalse(source.exists())
            self.assertFalse(venv.exists())

    def test_docs_distinguish_local_loop_from_external_and_native_wakes(self):
        readme = (ROOT / "README.md").read_text()
        onboarding = (ROOT / "ONBOARDING.md").read_text()
        self.assertIn("five minutes", readme)
        self.assertIn("does not schedule or wake that external supervisor", readme)
        self.assertIn("Native event listeners", readme)
        self.assertIn("not implemented", onboarding)

    def test_candidate_version_matches_packaging_metadata(self):
        project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
        self.assertEqual(project["name"], "lido-orchestrator")
        self.assertRegex(project["version"], r"^\d+\.\d+\.\d+")


if __name__ == "__main__":
    unittest.main()
