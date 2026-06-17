"""
sqlite_shared_sync.meta — sync metadata persistence.

The metadata JSON file stores the last successful sync timestamp outside the
database, because the database file may be overwritten during a pull operation.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def read_meta(meta_path: Path) -> dict:
    """Read sync metadata from *meta_path*. Returns {} if absent or corrupt."""
    meta_path = Path(meta_path)
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def write_meta(meta_path: Path, data: dict) -> None:
    """Write *data* to *meta_path*."""
    try:
        Path(meta_path).write_text(json.dumps(data, indent=2))
    except OSError as e:
        logger.warning(f"Failed to write sync metadata to {meta_path}: {e}")


def get_last_sync_time(meta_path: Path) -> Optional[datetime]:
    """Return the last successful sync timestamp, or None if not recorded."""
    ts = read_meta(meta_path).get("last_sync_time")
    if ts:
        try:
            return datetime.fromisoformat(ts)
        except ValueError:
            pass
    return None


def record_sync_time(meta_path: Path) -> datetime:
    """Record the current time as the last successful sync and return it."""
    now = datetime.now()
    meta = read_meta(meta_path)
    meta["last_sync_time"] = now.isoformat()
    write_meta(meta_path, meta)
    return now
