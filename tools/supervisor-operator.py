#!/usr/bin/env python3.12
"""Small local operator wrapper for supervisor enrollment and lease renewal."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="authoritative local SQLite database")
    parser.add_argument("--state-dir", required=True, help="private directory for route tokens")
    parser.add_argument("--python", default=sys.executable, help="Python used to run lido_orchestrator")
    sub = parser.add_subparsers(dest="action", required=True)
    enroll = sub.add_parser("enroll")
    enroll.add_argument("supervisor_id")
    enroll.add_argument("project_id")
    enroll.add_argument("worker_id")
    beat = sub.add_parser("heartbeat")
    beat.add_argument("supervisor_id")
    beat.add_argument("--generation", type=int, required=True)
    sub.add_parser("status").add_argument("supervisor_id")
    deboard = sub.add_parser("deboard")
    deboard.add_argument("supervisor_id")
    deboard.add_argument("--actor", default="local-operator")
    args = parser.parse_args()

    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", args.supervisor_id):
        parser.error("supervisor ID must contain only letters, digits, dot, underscore, or hyphen")
    state = Path(args.state_dir).expanduser().absolute()
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        info = state.lstat()
        if not os.path.samestat(info, state.stat()) or not state.is_dir() or info.st_uid != os.geteuid():
            raise OSError("unsafe state directory")
        if info.st_mode & 0o077 or info.st_uid != os.geteuid():
            raise OSError("state directory permissions are unsafe")
        dirfd = os.open(state, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(dirfd)
        if not os.path.samestat(info, opened):
            os.close(dirfd)
            raise OSError("state directory changed")
    except OSError:
        parser.error("state directory must be a stable private current-user-owned directory")
    token_name = args.supervisor_id + ".token"
    token_file = state / token_name
    base = [args.python, "-m", "lido_orchestrator", "--db", args.db]

    if args.action == "enroll":
        try:
            os.stat(token_name, dir_fd=dirfd, follow_symlinks=False)
            parser.error("token file already exists; inspect status or choose a new supervisor ID")
        except FileNotFoundError:
            pass
        cmd = base + ["supervisor", "register", args.supervisor_id, args.project_id,
                      args.worker_id, "--trusted-local"]
        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True)
            token = json.loads(result.stdout)["route_token_once"]
            if not isinstance(token, str) or not re.fullmatch(r"[A-Za-z0-9_-]{32,128}", token):
                raise ValueError("registry returned an invalid one-time token")
            fd = os.open(token_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600, dir_fd=dirfd)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(token + "\n")
            print(json.dumps({"enrolled": args.supervisor_id, "token_file": str(token_file),
                              "permissions": "0600", "generation": 1}))
        except Exception:
            # Registry registration may already have committed. Never unlink an
            # unexpected path on failure; inspect the registry before recovery.
            print("enrollment failed; inspect local registry before retrying", file=sys.stderr)
            raise SystemExit(1)
    elif args.action == "heartbeat":
        try:
            fd = os.open(token_name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=dirfd)
            info = os.fstat(fd)
            if info.st_uid != os.geteuid() or not (info.st_mode & 0o170000) == 0o100000 or info.st_nlink != 1 or info.st_mode & 0o077:
                os.close(fd)
                raise OSError("unsafe token file")
            with os.fdopen(fd, encoding="utf-8") as stream:
                content = stream.read(4097)
            token = content[:-1] if content.endswith("\n") else content
            if len(content) > 4096 or not re.fullmatch(r"[A-Za-z0-9_-]{32,128}", token):
                raise OSError("invalid token file")
        except OSError:
            parser.error("protected token file is unavailable")
        result = subprocess.run(base + ["supervisor", "heartbeat", args.supervisor_id,
                                        "--generation", str(args.generation)],
                                input=token + "\n", text=True, capture_output=True)
        if result.returncode:
            print("heartbeat rejected; inspect supervisor status", file=sys.stderr)
            raise SystemExit(result.returncode)
        sys.stdout.write(result.stdout)
    elif args.action == "status":
        result = subprocess.run(base + ["supervisor", "status", args.supervisor_id], check=True)
        raise SystemExit(result.returncode)
    else:
        subprocess.run(base + ["supervisor", "lifecycle", args.supervisor_id, "deboard",
                               "--actor", args.actor], check=True, stdout=subprocess.DEVNULL)
        try:
            info = os.stat(token_name, dir_fd=dirfd, follow_symlinks=False)
            if info.st_uid == os.geteuid() and info.st_nlink == 1 and (info.st_mode & 0o170000) == 0o100000:
                os.unlink(token_name, dir_fd=dirfd)
            else:
                raise OSError("token file changed; refusing unlink")
        except FileNotFoundError:
            pass
        print(json.dumps({"deboarded": args.supervisor_id, "local_token_removed": True,
                          "history_preserved": True}))


if __name__ == "__main__":
    main()
