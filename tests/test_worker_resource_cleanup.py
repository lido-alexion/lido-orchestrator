import os
import tempfile
import unittest
from pathlib import Path

from lido_orchestrator import resource_cleanup
from lido_orchestrator.engine import Engine


class WorkerResourceCleanupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="lido-cleanup-test-")
        self.root = Path(tempfile.mkdtemp(prefix="lido-worker-", dir="/tmp"))
        self.db = Path(self.tmp.name) / "state.sqlite3"
        self.engine = Engine(self.db)
        self.engine.register_project("fixture", self.tmp.name)
        self.task = self.engine.submit("fixture", "fixture work")
        self.engine.db.execute("UPDATE tasks SET status='running',attempts=1 WHERE id=?", (self.task,))
        # Most fixture cases model an explicitly process-free adapter attempt.
        self.engine.db.execute("INSERT INTO worker_attempt_execution VALUES(?,1,1,'process_free',?,?)",
                               (self.task, self.engine.clock(), "synthetic process-free fixture"))
        self.engine.db.execute("UPDATE worker_resource_policy SET retention_seconds=0 WHERE singleton=1")

    def tearDown(self):
        self.engine.close()
        self.tmp.cleanup()
        import shutil
        shutil.rmtree(self.root, ignore_errors=True)

    def test_cgroup_probe_never_authorizes_process_cleanup(self):
        from lido_orchestrator.resource_cleanup import assess_cgroup_containment
        writable = self.root / "own.cgroup.procs"
        parent = self.root / "parent.cgroup.procs"
        writable.write_text("")
        parent.write_text("")
        writable.chmod(0o600)
        parent.chmod(0o600)
        result = assess_cgroup_containment([writable, parent])
        self.assertFalse(result["authorized"])
        self.assertTrue(all(item["same_uid_writable"] for item in result["controls"]))
        self.assertIn("migration is possible", result["reason"])
        missing = assess_cgroup_containment([self.root / "missing.cgroup.procs"])
        self.assertFalse(missing["available"])
        self.assertFalse(missing["authorized"])
        self.assertFalse(assess_cgroup_containment([])["authorized"])

    def test_dry_run_execute_idempotency_and_retained_log_evidence(self):
        self.engine.db.execute("UPDATE worker_resource_policy SET retention_seconds=2592000 WHERE singleton=1")
        workspace = self.root / "workspace"
        workspace.mkdir()
        (workspace / "temporary.sqlite3").write_bytes(b"fixture")
        log = self.root / "worker.log"
        log.write_text("fixture evidence")
        self.engine.own_worker_resource(self.task, "workspace", workspace, self.root)
        self.engine.own_worker_resource(self.task, "log", log, self.root)

        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        before = self.engine.db.execute("SELECT count(*) FROM worker_resource_audit").fetchone()[0]
        preview = self.engine.cleanup_worker_resources(self.task)
        self.assertEqual("blocked", preview["resources"][0]["action"])
        self.assertEqual("retained", preview["resources"][1]["action"])
        self.assertTrue(workspace.exists())
        self.assertEqual(before, self.engine.db.execute("SELECT count(*) FROM worker_resource_audit").fetchone()[0])
        self.engine.verify_worker_inactive(self.task, 1, "fixture process exited and wait status recorded")
        self.engine.authorize_worker_cleanup(self.task, 1, "fixture data inspected; preservation no longer required")
        preview = self.engine.cleanup_worker_resources(self.task)
        self.assertEqual("would_remove", preview["resources"][0]["action"])
        result = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("removed", result["resources"][0]["action"])
        self.assertTrue(log.exists())
        again = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual(["retained"], [r["action"] for r in again["resources"]])
        self.assertGreater(self.engine.db.execute("SELECT count(*) FROM worker_resource_audit").fetchone()[0], 0)

    def test_unowned_symlink_and_active_task_fail_closed(self):
        target = self.root / "target"
        target.mkdir()
        link = self.root / "link"
        link.symlink_to(target, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.engine.own_worker_resource(self.task, "workspace", link, self.root)
        self.engine.db.execute("UPDATE tasks SET status='running' WHERE id=?", (self.task,))
        with self.assertRaisesRegex(ValueError, "active or ambiguous"):
            self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertTrue(target.exists())

    def test_mount_topology_fails_closed_for_root_descendant_and_missing_data(self):
        from unittest.mock import patch
        from lido_orchestrator import resource_cleanup
        item = self.root / "mounted-tree"
        item.mkdir()
        nested = item / "nested"
        nested.mkdir()
        with patch.object(resource_cleanup, "_mountpoints", return_value={self.root}):
            with self.assertRaisesRegex(ValueError, "mount"):
                resource_cleanup._safe_path(item, self.root)
        with patch.object(resource_cleanup, "_mountpoints", return_value={nested}):
            with self.assertRaisesRegex(ValueError, "mount"):
                resource_cleanup._safe_path(item, self.root)
        with patch.object(resource_cleanup, "_mountpoints", side_effect=ValueError("unavailable")):
            with self.assertRaisesRegex(ValueError, "unavailable"):
                resource_cleanup._safe_path(item, self.root)

    def test_resource_kind_cannot_bypass_shape_or_sqlite_sidecar_guards(self):
        database = self.root / "state.sqlite3"
        database.write_bytes(b"fixture")
        with self.assertRaisesRegex(ValueError, "filesystem shape"):
            self.engine.own_worker_resource(self.task, "workspace", database, self.root)
        database.write_bytes(b"SQLite format 3\x00" + b"fixture")
        with self.assertRaisesRegex(ValueError, "SQLite databases"):
            self.engine.own_worker_resource(self.task, "log", database, self.root)
        sidecar = self.root / "state.sqlite3-wal"
        sidecar.write_bytes(b"wal")
        with self.assertRaisesRegex(ValueError, "sidecars"):
            self.engine.own_worker_resource(self.task, "log", sidecar, self.root)
        directory = self.root / "log-directory"
        directory.mkdir()
        with self.assertRaisesRegex(ValueError, "filesystem shape"):
            self.engine.own_worker_resource(self.task, "log", directory, self.root)
        git_tree = self.root / "venv-with-repo"
        (git_tree / ".git").mkdir(parents=True)
        with self.assertRaisesRegex(ValueError, "Git-bearing"):
            self.engine.own_worker_resource(self.task, "venv", git_tree, self.root)

    def test_workspace_refuses_ignored_files_nested_repositories_and_unique_refs(self):
        import subprocess
        base = self.root / "repository"
        base.mkdir()
        subprocess.run(["git", "init", "-q", str(base)], check=True)
        subprocess.run(["git", "-C", str(base), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "commit", "--allow-empty", "-m", "base"], check=True)
        subprocess.run(["git", "-C", str(base), "branch", "--set-upstream-to=origin/main"], capture_output=True)
        # The test's preservation check rejects ignored data even when ordinary
        # status would report a clean tree.
        (base / ".gitignore").write_text("ignored.bin\\n")
        subprocess.run(["git", "-C", str(base), "add", ".gitignore"], check=True)
        subprocess.run(["git", "-C", str(base), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "commit", "-m", "ignore rule"], check=True)
        (base / "ignored.bin").write_bytes(b"hidden")
        with self.assertRaisesRegex(ValueError, "Git-bearing"):
            self.engine.own_worker_resource(self.task, "workspace", base, self.root)
        (base / "ignored.bin").unlink()
        nested = base / "nested"
        nested.mkdir()
        (nested / ".git").mkdir()
        with self.assertRaisesRegex(ValueError, "Git-bearing"):
            self.engine.own_worker_resource(self.task, "workspace", base, self.root)
        import shutil
        shutil.rmtree(nested)
        subprocess.run(["git", "-C", str(base), "branch", "local-only"], check=True)
        # A local ref with commits beyond the checked-out HEAD must be noticed.
        with self.assertRaisesRegex(ValueError, "Git-bearing"):
            self.engine.own_worker_resource(self.task, "workspace", base, self.root)

    def test_bare_and_nested_bare_repositories_are_retained(self):
        import subprocess
        bare = self.root / "bare.git"
        subprocess.run(["git", "init", "--bare", "-q", str(bare)], check=True)
        with self.assertRaisesRegex(ValueError, "Git-bearing"):
            self.engine.own_worker_resource(self.task, "workspace", bare, self.root)
        outer = self.root / "outer"
        outer.mkdir()
        nested = outer / "nested.git"
        subprocess.run(["git", "init", "--bare", "-q", str(nested)], check=True)
        with self.assertRaisesRegex(ValueError, "Git-bearing"):
            self.engine.own_worker_resource(self.task, "venv", outer, self.root)

    def test_remote_tracking_ref_does_not_prove_durable_preservation(self):
        import subprocess
        repo = self.root / "remote-tracking-only"
        repo.mkdir()
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        subprocess.run(["git", "-C", str(repo), "-c", "user.name=Fixture", "-c",
                        "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "fixture"], check=True)
        subprocess.run(["git", "-C", str(repo), "update-ref", "refs/remotes/origin/main", "HEAD"], check=True)
        with self.assertRaisesRegex(ValueError, "Git-bearing"):
            self.engine.own_worker_resource(self.task, "workspace", repo, self.root)

    def test_log_file_inside_repository_is_retained(self):
        import subprocess
        repo = self.root / "source"
        repo.mkdir()
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        log = repo / "run.log"
        log.write_text("fixture output")
        with self.assertRaisesRegex(ValueError, "Git-bearing"):
            self.engine.own_worker_resource(self.task, "log", log, self.root)

    def test_git_state_added_after_registration_blocks_deletion(self):
        import subprocess
        resource = self.root / "late-repository"
        resource.mkdir()
        self.engine.own_worker_resource(self.task, "venv", resource, self.root)
        subprocess.run(["git", "init", "-q", str(resource)], check=True)
        subprocess.run(["git", "-C", str(resource), "-c", "user.name=Fixture", "-c",
                        "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "preserve me"], check=True)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "synthetic attempt exited")
        self.engine.authorize_worker_cleanup(self.task, 1, "reviewed fixture output")
        result = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("blocked", result["resources"][0]["action"])
        self.assertTrue((resource / ".git").is_dir())

    def test_incomplete_filesystem_walk_fails_closed(self):
        from unittest.mock import patch
        item = self.root / "directory"
        item.mkdir()
        def incomplete_walk(*args, **kwargs):
            kwargs["onerror"](PermissionError("fixture scan failure"))
            return iter(())
        with patch.object(resource_cleanup, "_mountpoints", return_value=set()), \
             patch.object(resource_cleanup.os, "walk", side_effect=incomplete_walk):
            with self.assertRaisesRegex(ValueError, "scan incomplete"):
                resource_cleanup._safe_path(item, self.root)

    def test_inherited_git_environment_cannot_mask_repository_detection(self):
        import subprocess
        from unittest.mock import patch
        repo = self.root / "env-masked.git"
        subprocess.run(["git", "init", "--bare", "-q", str(repo)], check=True)
        with patch.dict(os.environ, {"GIT_DIR": str(self.root / "does-not-exist"),
                                    "GIT_WORK_TREE": str(self.root / "elsewhere")}):
            with self.assertRaisesRegex(ValueError, "Git-bearing"):
                resource_cleanup._preserve_git_work(repo)
            with self.assertRaisesRegex(ValueError, "Git-bearing"):
                resource_cleanup._reject_git_metadata(repo)

    def test_invalid_gitdir_marker_in_ancestor_is_git_bearing(self):
        ancestor = self.root / "ancestor"
        resource = ancestor / "resource"
        resource.mkdir(parents=True)
        (ancestor / ".git").write_text("gitdir: /missing/fixture-target\n")
        self.assertTrue(resource_cleanup._has_git_metadata(resource))
        with self.assertRaisesRegex(ValueError, "Git-bearing"):
            self.engine.own_worker_resource(self.task, "venv", resource, self.root)

    def test_uncertain_git_probe_failures_retain_but_normal_absence_is_known(self):
        from unittest.mock import patch
        from types import SimpleNamespace
        clean = self.root / "no-repository"
        clean.mkdir()
        with patch.object(resource_cleanup.subprocess, "run",
                          return_value=SimpleNamespace(returncode=128, stderr="fatal: not a git repository")):
            self.assertFalse(resource_cleanup._has_git_metadata(clean, self.root))
        for result in (
            SimpleNamespace(returncode=2, stderr="fatal: bad config file"),
            SimpleNamespace(returncode=128, stderr=""),
        ):
            with patch.object(resource_cleanup.subprocess, "run", return_value=result):
                with self.assertRaisesRegex(ValueError, "uncertain"):
                    resource_cleanup._has_git_metadata(clean, self.root)
        with patch.object(resource_cleanup.subprocess, "run", side_effect=TimeoutError("fixture timeout")):
            with self.assertRaisesRegex(ValueError, "probe failed"):
                resource_cleanup._has_git_metadata(clean, self.root)

    def test_final_deletion_fence_repeats_git_detection(self):
        from unittest.mock import patch
        item = self.root / "final-fence"
        item.write_text("retain")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "synthetic process-free completion")
        self.engine.authorize_worker_cleanup(self.task, 1, "synthetic preservation review")
        original = resource_cleanup._has_git_metadata
        calls = []
        def flip(path, boundary=None):
            calls.append(Path(path))
            if len(calls) >= 2:
                raise ValueError("Git probe result is uncertain; retaining resource")
            return original(path, boundary)
        with patch.object(resource_cleanup, "_has_git_metadata", side_effect=flip):
            result = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("blocked", result["resources"][0]["action"])
        self.assertGreaterEqual(len(calls), 2)
        self.assertTrue(item.exists())

    def test_root_removal_rechecks_mount_topology_and_identity(self):
        from unittest.mock import patch
        item = self.root / "removed-before-root"
        item.write_text("fixture")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        item.unlink()
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "synthetic process-free completion")
        self.engine.authorize_worker_cleanup(self.task, 1, "synthetic preservation review")
        real_mountpoints = resource_cleanup._mountpoints
        count = []
        def mount_flip():
            count.append(1)
            return {self.root} if len(count) >= 2 else real_mountpoints()
        with patch.object(resource_cleanup, "_mountpoints", side_effect=mount_flip):
            result = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertTrue(self.root.exists())
        self.assertGreaterEqual(len(count), 2)
        self.assertEqual(0, self.engine.db.execute(
            "SELECT count(*) FROM worker_resource_audit WHERE action='managed_root_removed'").fetchone()[0])

    def test_final_resource_identity_swap_is_retained(self):
        from unittest.mock import patch
        item = self.root / "identity-fence.log"
        item.write_text("original")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "synthetic process-free completion")
        self.engine.authorize_worker_cleanup(self.task, 1, "synthetic preservation review")
        original = resource_cleanup._safe_path
        calls = []
        def swap(path, root):
            calls.append(1)
            if len(calls) == 2:
                replacement = Path(path).with_name("replacement.tmp")
                replacement.write_text("replacement")
                replacement.replace(path)
            return original(path, root)
        with patch.object(resource_cleanup, "_safe_path", side_effect=swap):
            outcome = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("blocked", outcome["resources"][0]["action"])
        self.assertEqual("replacement", item.read_text())

    def test_managed_root_identity_swap_at_rmdir_fence_is_retained(self):
        from unittest.mock import patch
        item = self.root / "already-absent"
        item.write_text("fixture")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        item.unlink()
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "synthetic process-free completion")
        self.engine.authorize_worker_cleanup(self.task, 1, "synthetic preservation review")
        real_scandir = os.scandir
        swapped = []
        def replace_root(path):
            if Path(path) == self.root and not swapped:
                swapped.append(True)
                self.root.rmdir()
                self.root.mkdir(mode=0o700)
                (self.root / "sentinel").write_text("foreign replacement")
            return real_scandir(path)
        with patch.object(resource_cleanup, "_mountpoints", return_value=set()), \
             patch.object(resource_cleanup.os, "scandir", side_effect=replace_root):
            self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertTrue((self.root / "sentinel").exists())
        self.assertEqual(0, self.engine.db.execute(
            "SELECT count(*) FROM worker_resource_audit WHERE action='managed_root_removed'").fetchone()[0])

    def test_managed_root_git_state_is_rechecked_at_rmdir_fence(self):
        from unittest.mock import patch
        item = self.root / "already-absent"
        item.write_text("fixture")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        item.unlink()
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "synthetic process-free completion")
        self.engine.authorize_worker_cleanup(self.task, 1, "synthetic preservation review")
        original = resource_cleanup._has_git_metadata
        calls = []
        def add_git_at_fence(path, boundary=None):
            calls.append(1)
            if len(calls) >= 2:
                raise ValueError("managed root Git state changed at removal fence")
            return original(path, boundary)
        with patch.object(resource_cleanup, "_mountpoints", return_value=set()), \
             patch.object(resource_cleanup, "_has_git_metadata", side_effect=add_git_at_fence):
            self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertTrue(self.root.exists())
        self.assertGreaterEqual(len(calls), 2)

    def test_changed_inode_is_blocked_and_audited(self):
        item = self.root / "changed-database-like-resource"
        item.write_text("first")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "fixture exited")
        self.engine.authorize_worker_cleanup(self.task, 1, "disposable fixture")
        item.unlink()
        item.mkdir()
        outcome = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("blocked", outcome["resources"][0]["action"])
        self.assertTrue(item.exists())
        retry = self.engine.cleanup_worker_resources(self.task, execute=True, retry=True)
        self.assertEqual("blocked", retry["resources"][0]["action"])

    def test_uncommitted_worktree_cannot_be_owned(self):
        import subprocess
        tree = self.root / "worktree"
        tree.mkdir()
        subprocess.run(["git", "init", "-q", str(tree)], check=True)
        subprocess.run(["git", "-C", str(tree), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "commit", "--allow-empty", "-m", "fixture"], check=True)
        (tree / "dirty.txt").write_text("uncommitted")
        with self.assertRaisesRegex(ValueError, "Git-bearing"):
            self.engine.own_worker_resource(self.task, "worktree", tree, self.root)

    def test_sqlite_sidecar_bundle_is_refused_and_venv_tree_is_eligible(self):
        bundle = self.root / "database-bundle"
        bundle.mkdir()
        (bundle / "state.sqlite3").write_bytes(b"fixture")
        (bundle / "state.sqlite3-wal").write_bytes(b"uncheckpointed")
        with self.assertRaisesRegex(ValueError, "WAL/SHM"):
            self.engine.own_worker_resource(self.task, "temporary_db", bundle, self.root)
        (bundle / "state.sqlite3-wal").unlink()
        venv = self.root / "venv"
        (venv / "bin").mkdir(parents=True)
        (venv / "bin" / "python").write_bytes(b"fixture interpreter")
        self.engine.own_worker_resource(self.task, "venv", venv, self.root)

    def test_terminal_callback_alone_does_not_authorize_cleanup(self):
        item = self.root / "kept"
        item.write_text("evidence")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        result = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("blocked", result["resources"][0]["action"])
        self.assertTrue(item.exists())
        with self.assertRaisesRegex(ValueError, "attempt/generation"):
            self.engine.verify_worker_inactive(self.task, 1, "wrong generation", generation=2)

    def test_missing_process_row_and_old_attestations_do_not_prove_process_free(self):
        item = self.root / "ambiguous"
        item.write_text("keep")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.db.execute("UPDATE worker_attempt_execution SET classification='intent' WHERE task_id=?", (self.task,))
        self.engine.db.execute("INSERT INTO worker_resource_verifications VALUES(?,?,?,?,?)",
                               (self.task, 1, 1, self.engine.clock(), "old assertion"))
        self.engine.db.execute("INSERT INTO worker_resource_authorizations VALUES(?,?,?,?,?)",
                               (self.task, 1, 1, self.engine.clock(), "old authorization"))
        result = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("intent", result["execution_classification"])
        self.assertEqual("blocked", result["resources"][0]["action"])
        with self.assertRaisesRegex(ValueError, "durable process-free"):
            self.engine.verify_worker_inactive(self.task, 1, "new assertion")
        self.assertTrue(item.exists())

    def test_ambiguous_classification_survives_engine_restart(self):
        item = self.root / "restart-ambiguous"
        item.write_text("retain across restart")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.db.execute("UPDATE worker_attempt_execution SET classification='intent' WHERE task_id=?", (self.task,))
        self.engine.db.execute("INSERT INTO worker_resource_verifications VALUES(?,?,?,?,?)",
                               (self.task, 1, 1, self.engine.clock(), "old operator record"))
        self.engine.close()
        self.engine = Engine(self.db)
        result = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("intent", result["execution_classification"])
        self.assertEqual("blocked", result["resources"][0]["action"])
        self.assertTrue(item.exists())

    def test_inactivity_is_not_cleanup_authority_and_reconcile_recovery_is_read_only(self):
        from lido_orchestrator import resource_cleanup
        item = self.root / "preserve-me"
        item.write_text("evidence")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "wait status observed")
        preview = self.engine.cleanup_worker_resources(self.task)
        self.assertEqual("blocked", preview["resources"][0]["action"])
        self.assertIn("preservation authorization", preview["resources"][0]["reason"])
        resource_cleanup.recover_verified(self.engine)
        self.assertTrue(item.exists())
        self.engine.authorize_worker_cleanup(self.task, 1, "reviewed disposable data")
        self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertFalse(item.exists())

    def test_opt_in_automatic_recovery_requires_exact_proof_and_no_process_record(self):
        from lido_orchestrator import resource_cleanup
        item = self.root / "automatic"
        item.write_text("fixture")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "isolated process-free fixture")
        self.engine.authorize_worker_cleanup(self.task, 1, "fixture preservation review complete")
        resource_cleanup.recover_verified(self.engine)
        self.assertTrue(item.exists(), "default recovery remains read-only")
        outcomes = resource_cleanup.recover_verified(self.engine, automatic=True)
        self.assertEqual("removed", outcomes[0]["resources"][0]["action"])
        self.assertFalse(item.exists())

    def test_automatic_recovery_refuses_any_recorded_process(self):
        from lido_orchestrator import resource_cleanup
        item = self.root / "process-backed"
        item.write_text("fixture")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        # Fixture identity is recorded without launching or signalling a process.
        self.engine.db.execute("INSERT INTO worker_process_attempts(task_id,attempt,generation,pid,pgid,boot_id,start_ticks,recorded) VALUES(?,1,1,99999999,99999999,'fixture',1,?)",
                               (self.task, self.engine.clock()))
        self.engine.db.execute("UPDATE worker_attempt_execution SET classification='launched' WHERE task_id=?", (self.task,))
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        # Model durable records from an older/manual review; automatic recovery
        # still rejects the process-backed attempt on descendant proof limits.
        self.engine.db.execute("INSERT INTO worker_resource_verifications VALUES(?,?,?,?,?)",
                               (self.task, 1, 1, self.engine.clock(), "fixture legacy evidence"))
        with self.assertRaisesRegex(ValueError, "classification"):
            self.engine.authorize_worker_cleanup(self.task, 1, "preservation review complete")
        outcomes = resource_cleanup.recover_verified(self.engine, automatic=True)
        self.assertEqual("blocked", outcomes[0]["resources"][0]["action"])
        self.assertTrue(item.exists())

    def test_crash_after_delete_is_reconciled_only_with_both_evidence_records(self):
        item = self.root / "already-deleted"
        item.write_text("fixture")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "exit independently observed")
        self.engine.authorize_worker_cleanup(self.task, 1, "preservation review complete")
        item.unlink()  # Model process death after unlink and before SQLite state commit.
        outcome = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("recovered_removed", outcome["resources"][0]["action"])
        self.assertEqual("removed", self.engine.db.execute("SELECT state FROM worker_resources").fetchone()[0])

    def test_hardlink_and_cross_worker_claims_are_rejected(self):
        item = self.root / "first"
        item.write_text("fixture")
        os.link(item, self.root / "alias")
        with self.assertRaisesRegex(ValueError, "hard-linked"):
            self.engine.own_worker_resource(self.task, "log", item, self.root)
        (self.root / "alias").unlink()
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        other = self.engine.submit("fixture", "other")
        self.engine.db.execute("UPDATE tasks SET status='running',attempts=1 WHERE id=?", (other,))
        with self.assertRaisesRegex(ValueError, "overlaps"):
            self.engine.own_worker_resource(other, "log", item, self.root)

    def test_retention_expiry_and_byte_cap(self):
        self.engine.db.execute("UPDATE worker_resource_policy SET retention_seconds=2592000 WHERE singleton=1")
        log = self.root / "aged.log"
        log.write_bytes(b"fixture-log")
        self.engine.own_worker_resource(self.task, "log", log, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.db.execute("UPDATE worker_resources SET created=? WHERE path=?", (self.engine.clock() - 40 * 86400, str(log)))
        self.engine.verify_worker_inactive(self.task, 1, "fixture process exit independently checked")
        self.engine.authorize_worker_cleanup(self.task, 1, "fixture log inspected")
        capped = __import__("lido_orchestrator.resource_cleanup", fromlist=["cleanup_task"]).cleanup_task(
            self.engine, self.task, execute=True, max_bytes=1)
        self.assertEqual("blocked", capped["resources"][0]["action"])
        self.assertEqual(len(b"fixture-log"), capped["bytes_examined"])
        self.assertTrue(log.exists())
        removed = __import__("lido_orchestrator.resource_cleanup", fromlist=["cleanup_task"]).cleanup_task(
            self.engine, self.task, execute=True, retry=True, max_bytes=1024)
        self.assertEqual("removed", removed["resources"][0]["action"])
        self.assertFalse(log.exists())
        self.assertFalse(self.root.exists())

    @unittest.skipUnless(Path("/proc/sys/kernel/random/boot_id").exists(), "Linux process identity required")
    def test_process_identity_live_exit_and_pid_reuse_are_fail_closed(self):
        import subprocess
        child = subprocess.Popen(["python3", "-c", "import time; time.sleep(30)"])
        try:
            self.engine.record_worker_process(self.task, 1, 1, child.pid, child.pid)
            self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
            with self.assertRaisesRegex(ValueError, "still active"):
                self.engine.verify_worker_inactive(self.task, 1, "fixture", 1)
        finally:
            child.terminate()
            code = child.wait(timeout=5)
        self.engine.record_worker_process_exit(self.task, 1, 1, child.pid, code)
        with self.assertRaisesRegex(ValueError, "descendant inactivity cannot be proven"):
            self.engine.verify_worker_inactive(self.task, 1, "isolated fixture child wait status", 1)
        row = self.engine.db.execute("SELECT exit_code,exited,boot_id,start_ticks FROM worker_process_attempts").fetchone()
        self.assertEqual(code, row["exit_code"])
        self.assertIsNotNone(row["exited"])
        self.assertTrue(row["boot_id"])
        self.assertGreater(row["start_ticks"], 0)

    def test_empty_managed_root_requires_both_exact_proof_gates(self):
        item = self.root / "already-unlinked"
        item.write_text("fixture")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        item.unlink()
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertTrue(self.root.exists())
        self.assertEqual(0, self.engine.db.execute(
            "SELECT count(*) FROM worker_resource_audit WHERE action='managed_root_removed'").fetchone()[0])
        self.engine.verify_worker_inactive(self.task, 1, "isolated process-free fixture")
        self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertTrue(self.root.exists())
        self.engine.authorize_worker_cleanup(self.task, 1, "preservation reviewed")
        self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertFalse(self.root.exists())

    def test_missing_path_recovery_requires_process_free_classification(self):
        item = self.root / "missing-ambiguous"
        item.write_text("retain evidence")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.db.execute("UPDATE worker_attempt_execution SET classification='intent' WHERE task_id=?", (self.task,))
        self.engine.db.execute("INSERT INTO worker_resource_verifications VALUES(?,?,?,?,?)",
                               (self.task, 1, 1, self.engine.clock(), "legacy verification"))
        self.engine.db.execute("INSERT INTO worker_resource_authorizations VALUES(?,?,?,?,?)",
                               (self.task, 1, 1, self.engine.clock(), "legacy authorization"))
        item.unlink()
        result = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("blocked", result["resources"][0]["action"])
        self.assertEqual("blocked", self.engine.db.execute(
            "SELECT state FROM worker_resources WHERE task_id=?", (self.task,)).fetchone()[0])

    def test_resource_registration_remains_bound_to_original_generation(self):
        item = self.root / "generation-bound"
        item.write_text("fixture")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.assertEqual(1, self.engine.db.execute(
            "SELECT generation FROM worker_resources WHERE task_id=?", (self.task,)).fetchone()[0])
        self.engine.db.execute("UPDATE tasks SET supervisor_generation=2 WHERE id=?", (self.task,))
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        with self.assertRaisesRegex(ValueError, "attempt/generation"):
            self.engine.verify_worker_inactive(self.task, 1, "old generation", generation=1)
        with self.assertRaisesRegex(ValueError, "attempt/generation"):
            self.engine.authorize_worker_cleanup(self.task, 1, "old generation", generation=1)
        # Even newly created generation-2 proof cannot authorize deletion of a
        # resource registered to generation 1.
        self.engine.db.execute("INSERT INTO worker_attempt_execution VALUES(?,1,2,'process_free',?,?)",
                               (self.task, self.engine.clock(), "synthetic new generation"))
        self.engine.verify_worker_inactive(self.task, 1, "new generation process-free check", generation=2)
        self.engine.authorize_worker_cleanup(self.task, 1, "new generation preservation review", generation=2)
        result = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("blocked", result["resources"][0]["action"])
        self.assertIn("generation", result["resources"][0]["reason"])
        self.assertTrue(item.exists())

    def test_state_write_failure_after_unlink_rolls_back_for_gated_recovery(self):
        item = self.root / "state-write-failure"
        item.write_text("fixture")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "process-free fixture")
        self.engine.authorize_worker_cleanup(self.task, 1, "preservation reviewed")
        self.engine.db.execute("CREATE TRIGGER fail_resource_state BEFORE UPDATE OF state ON worker_resources "
                               "BEGIN SELECT RAISE(ABORT, 'synthetic state failure'); END")
        with self.assertRaisesRegex(Exception, "synthetic state failure"):
            self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertFalse(item.exists())
        self.assertFalse(self.engine.db.in_transaction)
        self.engine.db.execute("DROP TRIGGER fail_resource_state")
        result = self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertEqual("recovered_removed", result["resources"][0]["action"])

    def test_database_failure_rolls_back_only_cleanup_savepoint(self):
        item = self.root / "nested-transaction"
        item.write_text("fixture")
        self.engine.own_worker_resource(self.task, "log", item, self.root)
        self.engine.db.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (self.task,))
        self.engine.verify_worker_inactive(self.task, 1, "process-free fixture")
        self.engine.authorize_worker_cleanup(self.task, 1, "preservation reviewed")
        self.engine.db.execute("BEGIN")
        self.engine.db.execute("CREATE TEMP TRIGGER fail_nested_state BEFORE UPDATE OF state ON worker_resources "
                               "BEGIN SELECT RAISE(ABORT, 'synthetic nested failure'); END")
        with self.assertRaisesRegex(Exception, "synthetic nested failure"):
            self.engine.cleanup_worker_resources(self.task, execute=True)
        self.assertTrue(self.engine.db.in_transaction, "caller transaction remains active")
        self.assertFalse(item.exists())
        self.engine.db.rollback()
        self.assertFalse(self.engine.db.in_transaction)


if __name__ == "__main__":
    unittest.main()
