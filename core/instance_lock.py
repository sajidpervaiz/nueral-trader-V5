"""Instance lock — prevents two bot instances trading the same account.

Uses a PID file with advisory lock (fcntl.flock on Linux/Mac).
Falls back to PID-in-file check on platforms without flock.
"""
from __future__ import annotations

import atexit
import os
import sys
from pathlib import Path

from loguru import logger


class InstanceLock:
    """Ensures only one trading bot instance runs per account/profile."""

    def __init__(self, lock_file: str | Path = "data/.neural_trader.lock") -> None:
        self._path = Path(lock_file)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd: int | None = None
        self._locked = False

    def acquire(self) -> bool:
        """Try to acquire exclusive lock.  Returns True if successful."""
        try:
            import fcntl
            self._fd = os.open(str(self._path), os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.write(self._fd, f"{os.getpid()}\n".encode())
            os.fsync(self._fd)
            self._locked = True
            atexit.register(self.release)
            logger.info("Instance lock acquired (pid={})", os.getpid())
            return True
        except (BlockingIOError, OSError):
            # Another instance holds the lock
            existing_pid = self._read_pid()
            logger.error(
                "FATAL: Another instance is running (pid={}). "
                "Remove {} manually if stale.",
                existing_pid, self._path,
            )
            return False
        except ImportError:
            # No fcntl (Windows) — fallback to PID check
            return self._pid_fallback()

    def _pid_fallback(self) -> bool:
        """Fallback for systems without flock: check if PID in file is alive."""
        if self._path.exists():
            existing_pid = self._read_pid()
            if existing_pid and self._pid_alive(existing_pid):
                logger.error(
                    "FATAL: Another instance is running (pid={})", existing_pid,
                )
                return False
            else:
                logger.warning("Stale lock file found (pid={}), removing", existing_pid)
        self._path.write_text(f"{os.getpid()}\n")
        self._locked = True
        atexit.register(self.release)
        logger.info("Instance lock acquired via PID file (pid={})", os.getpid())
        return True

    def _read_pid(self) -> int | None:
        try:
            content = self._path.read_text().strip()
            return int(content) if content else None
        except (ValueError, OSError):
            return None

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False

    def release(self) -> None:
        """Release the instance lock."""
        if not self._locked:
            return
        try:
            if self._fd is not None:
                import fcntl
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
                self._fd = None
        except (ImportError, OSError):
            pass
        try:
            self._path.unlink(missing_ok=True)
        except OSError:
            pass
        self._locked = False
        logger.info("Instance lock released")

    @property
    def locked(self) -> bool:
        return self._locked

    def __enter__(self) -> "InstanceLock":
        if not self.acquire():
            sys.exit(1)
        return self

    def __exit__(self, *args: object) -> None:
        self.release()
