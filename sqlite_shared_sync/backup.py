"""
sqlite_shared_sync.backup — timestamped file backup utility.
"""

from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def backup_file(file_path: Path, max_backups: int = 5) -> bool:
    """Create a timestamped backup of *file_path* and prune old copies.

    Backup files are named ``{stem}.bak.{YYYYMMDD_HHMMSS}{suffix}``.
    After creation, only the newest *max_backups* copies are kept.

    Returns True if the backup was created successfully.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        return False

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = file_path.with_suffix(f".bak.{ts}")
    try:
        shutil.copy2(file_path, backup_path)
        logger.debug(f"Backup created: {backup_path.name}")
    except OSError as e:
        logger.warning(f"Failed to create backup of {file_path.name}: {e}")
        return False

    # Prune old backups
    pattern = f"{file_path.stem}.bak.*"
    backups = sorted(file_path.parent.glob(pattern), key=lambda p: p.stat().st_mtime)
    while len(backups) > max_backups:
        old = backups.pop(0)
        try:
            old.unlink()
            logger.debug(f"Removed old backup: {old.name}")
        except OSError:
            pass

    return True
