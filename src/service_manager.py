"""Local background service management for QuestBoard watch mode."""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
from typing import Any

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STATE_FILE = os.path.join(ROOT_DIR, ".questboard_service.json")
LOG_FILE = os.path.join(ROOT_DIR, ".questboard_service.log")


def _load_state() -> dict[str, Any] | None:
    if not os.path.exists(STATE_FILE):
        return None
    with open(STATE_FILE, encoding="utf-8") as handle:
        return json.load(handle)


def _save_state(state: dict[str, Any]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)


def _remove_state() -> None:
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)


def _is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def get_service_status() -> dict[str, Any]:
    state = _load_state() or {}
    pid = int(state.get("pid", 0) or 0)
    running = _is_pid_running(pid)
    return {
        "running": running,
        "pid": pid if pid else None,
        "interval_seconds": int(state.get("interval_seconds", 0) or 0) or None,
        "started_at": state.get("started_at", ""),
        "log_path": LOG_FILE,
    }


def start_service(*, interval_seconds: int = 15, force_restart: bool = False) -> dict[str, Any]:
    status = get_service_status()
    if status.get("running") and not force_restart:
        return status
    if status.get("running") and force_restart:
        stop_service()

    command = [
        sys.executable,
        "-m",
        "src.cli",
        "watch",
        "--interval",
        str(interval_seconds),
        "--iterations",
        "0",
    ]

    with open(LOG_FILE, "a", encoding="utf-8") as log_handle:
        kwargs: dict[str, Any] = {
            "cwd": ROOT_DIR,
            "stdin": subprocess.DEVNULL,
            "stdout": log_handle,
            "stderr": log_handle,
        }
        if sys.platform == "win32":
            creationflags = 0
            for flag_name in ("DETACHED_PROCESS", "CREATE_NEW_PROCESS_GROUP", "CREATE_NO_WINDOW"):
                creationflags |= int(getattr(subprocess, flag_name, 0))
            kwargs["creationflags"] = creationflags
        else:
            kwargs["start_new_session"] = True
        proc = subprocess.Popen(command, **kwargs)

    state = {
        "pid": proc.pid,
        "interval_seconds": interval_seconds,
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "command": command,
    }
    _save_state(state)
    return get_service_status()


def stop_service() -> dict[str, Any]:
    status = get_service_status()
    pid = status.get("pid")
    if not pid:
        _remove_state()
        return {"stopped": False, **status}

    if status.get("running"):
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                cwd=ROOT_DIR,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        else:
            import signal

            os.kill(pid, signal.SIGTERM)

    _remove_state()
    return {"stopped": True, **status}
