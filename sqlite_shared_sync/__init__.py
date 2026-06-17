"""
sqlite_shared_sync — bidirectional SQLite sync over a shared drive.

Uses SQLite's ``ATTACH DATABASE`` to compare and reconcile two databases
without loading data into Python. Pure stdlib, zero runtime dependencies.

Quick start::

    from pathlib import Path
    from sqlite_shared_sync import SyncManager, SyncConfig, SyncTableDef

    config = SyncConfig(
        local_db_path=Path("local.db"),
        shared_db_path=Path("/mnt/share/shared.db"),
        tables=[
            SyncTableDef("customers", pk="customer_id", ts="updated_at"),
            SyncTableDef("orders",    pk="order_id",    ts="updated_at"),
            SyncTableDef("audit_log", pk="id",          ts="created_at",
                         push_only=True),
        ],
    )
    manager = SyncManager(config)

    # First machine: seed the shared DB from local
    manager.initial_push()

    # Every subsequent run: bidirectional sync
    result = manager.sync()
    print(f"Pushed {result.pushed}, pulled {result.pulled}")
"""

from sqlite_shared_sync.models import SyncConfig, SyncResult, SyncTableDef  # noqa: F401
from sqlite_shared_sync.manager import SyncManager, ensure_indexes, _to_sqlite_ts  # noqa: F401
from sqlite_shared_sync.lock import LockFile  # noqa: F401
from sqlite_shared_sync.backup import backup_file  # noqa: F401
from sqlite_shared_sync.meta import (  # noqa: F401
    get_last_sync_time,
    record_sync_time,
    read_meta,
    write_meta,
)

__version__ = "0.1.0"
__author__ = "Mark Rodgers"
