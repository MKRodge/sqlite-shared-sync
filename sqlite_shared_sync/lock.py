"""
sqlite_shared_sync.lock — file-based shared-drive lock.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_LOCK_FILENAME = ".sync.lock"


class LockFile:
    """Simple file-based lock for shared-drive coordination.

    Creates a ``{shared_dir}/.sync.lock`` JSON file containing the holder's
    hostname, PID, and acquisition time.  Stale locks (older than *timeout*
    seconds) are silently overridden.

    Usage::

        lock = LockFile(shared_dir, timeout=30, retries=3, retry_delay=2)
        if lock.acquire():
            try:
                ...
            finally:
                lock.release()
    """

    def __init__(self, shared_dir: Path, *,
                 timeout: int = 30, retries: int = 3, retry_delay: int = 2):
        self.lock_path = Path(shared_dir) / _LOCK_FILENAME
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.acquired = False

    def acquire(self) -> bool:
        """Attempt to acquire the lock. Returns True on success."""
        for attempt in range(self.retries):
            if self._try_acquire():
                self.acquired = True
                return True
            if attempt < self.retries - 1:
                logger.info(
                    f"Lock busy, retrying in {self.retry_delay}s "
                    f"(attempt {attempt + 2}/{self.retries})"
                )
                time.sleep(self.retry_delay)
        return False

    def _try_acquire(self) -> bool:
        if self.lock_path.exists():
            try:
                content = json.loads(self.lock_path.read_text())
                lock_time = datetime.fromisoformat(content.get("time", ""))
                age = (datetime.now() - lock_time).total_seconds()
                if age < self.timeout:
                    holder = content.get("host", "unknown")
                    logger.debug(f"Lock held by {holder} ({age:.0f}s old)")
                    return False
                logger.warning(f"Stale lock ({age:.0f}s old), overriding")
            except (json.JSONDecodeError, ValueError, OSError):
                logger.warning("Corrupt lock file, overriding")

        try:
            self.lock_path.write_text(json.dumps({
                "host": socket.gethostname(),
                "time": datetime.now().isoformat(),
                "pid": os.getpid(),
            }))
            return True
        except OSError as e:
            logger.error(f"Failed to write lock file: {e}")
            return False

    def release(self):
        """Release the lock."""
        if self.acquired:
            try:
                self.lock_path.unlink(missing_ok=True)
            except OSError:
                pass
            self.acquired = False

    def __enter__(self):
        if not self.acquire():
            raise TimeoutError(
                f"Could not acquire sync lock at {self.lock_path} "
                f"after {self.retries} attempts"
            )
        return self

    def __exit__(self, *_):
        self.release()
