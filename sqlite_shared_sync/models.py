"""
sqlite_shared_sync.models — public data classes.

Import these to configure the sync manager::

    from sqlite_shared_sync import SyncTableDef, SyncConfig, SyncResult
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Tuple


@dataclass
class SyncTableDef:
    """Describes one table to be synchronised.

    Args:
        name: Table name (must exist in both databases).
        pk: Primary-key column name. Used to match rows across databases.
        ts: Timestamp column used to decide which version is newer.
            Must be a DATETIME column; SQLite space-separated format is assumed.
        push_only: When True, rows flow local → shared only. Pull is skipped.
    """
    name: str
    pk: str
    ts: str
    push_only: bool = False


@dataclass
class SyncConfig:
    """Complete configuration for a SyncManager instance.

    Args:
        local_db_path: Path to the local SQLite database file.
        shared_db_path: Path to the shared SQLite database file (on a network drive
            or shared volume). Resolve this to an actual path before constructing
            SyncConfig — the manager does not read it from application settings.
        tables: Ordered list of SyncTableDef entries to synchronise. Order matters
            when tables have foreign-key dependencies; list parent tables first.
        meta_path: Where to store the sync metadata JSON file (last_sync_time, etc.).
            Defaults to ``{local_db_path.parent}/sync_meta.json``. This file must
            survive database overwrites (hence stored outside the database).
        lock_timeout: Seconds before a stale lock is overridden. Default 30.
        lock_retries: Number of attempts to acquire the lock. Default 3.
        lock_retry_delay: Seconds between lock-acquire retries. Default 2.
        max_backups: Timestamped backup copies to retain. Older ones are pruned.
        on_post_sync: Optional callback invoked after the main sync loop completes
            but before the connection is committed. Signature::

                def hook(conn: sqlite3.Connection, result: SyncResult) -> None: ...

            Use this for app-specific post-processing: push-only tables without
            timestamps, cross-table reconciliation, compound-PK junction tables, etc.
    """
    local_db_path: Path
    shared_db_path: Path
    tables: List[SyncTableDef]
    meta_path: Optional[Path] = None
    lock_timeout: int = 30
    lock_retries: int = 3
    lock_retry_delay: int = 2
    max_backups: int = 5
    on_post_sync: Optional[Callable[[sqlite3.Connection, "SyncResult"], None]] = None

    def __post_init__(self):
        self.local_db_path = Path(self.local_db_path)
        self.shared_db_path = Path(self.shared_db_path)
        if self.meta_path is None:
            self.meta_path = self.local_db_path.parent / "sync_meta.json"
        else:
            self.meta_path = Path(self.meta_path)


@dataclass
class SyncResult:
    """Outcome of a single sync call.

    Attributes:
        success: True when the sync completed without fatal errors.
        pushed: Total rows pushed local → shared.
        pulled: Total rows pulled shared → local.
        skipped: Tables skipped (e.g. heartbeat found no changes).
        error: Human-readable error message, or None.
        offline: True when the shared database was unreachable.
        details: Per-table detail strings, e.g. ``["customers: pushed 2", "rfqs: pulled 1"]``.
    """
    success: bool = False
    pushed: int = 0
    pulled: int = 0
    skipped: int = 0
    error: Optional[str] = None
    offline: bool = False
    details: List[str] = field(default_factory=list)
