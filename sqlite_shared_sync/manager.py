"""
sqlite_shared_sync.manager — ATTACH-based bidirectional SQLite sync.

The core algorithm is a single-pass, schema-safe bidirectional diff:

1. Attach the shared DB as ``shared``.
2. For each table, fetch ``(pk, ts)`` from both sides — two lightweight scans,
   no cross-DB JOIN (SQLite cannot optimise across ATTACH boundaries).
3. Compute which PKs need pushing (local newer) or pulling (shared newer).
4. Apply both directions with ``INSERT OR REPLACE … SELECT … WHERE pk IN (…)``.
5. Call the optional ``on_post_sync`` hook for app-specific post-processing.
6. Commit and detach.

Only columns present in both schemas are transferred (PRAGMA table_info comparison),
so a column added to one side via ALTER TABLE doesn't break the sync — it just won't
be synced until both sides have the column.
"""

from __future__ import annotations

import logging
import shutil
import socket
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from .backup import backup_file
from .lock import LockFile
from .meta import get_last_sync_time, record_sync_time
from .models import SyncConfig, SyncResult, SyncTableDef

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Timestamp conversion
# ---------------------------------------------------------------------------

def _to_sqlite_ts(dt) -> str:
    """Convert a datetime (or ISO string) to SQLite's space-separated format.

    SQLite stores DateTime columns as ``'YYYY-MM-DD HH:MM:SS.ffffff'`` (space).
    Python's ``datetime.isoformat()`` produces the T-separated form.  Using T
    in WHERE clauses causes all rows to compare as older than the cutoff because
    ASCII ``' '`` (32) < ``'T'`` (84), silently skipping every table.
    """
    s = dt.isoformat() if hasattr(dt, 'isoformat') else str(dt)
    return s.replace('T', ' ')


# ---------------------------------------------------------------------------
# Index helpers
# ---------------------------------------------------------------------------

IndexDef = Tuple[str, str, str]   # (table_name, ts_col, pk_col)


def ensure_indexes(db_path: Path, index_defs: List[IndexDef]) -> bool:
    """Create covering sync indexes on *db_path* if they don't already exist.

    Each entry in *index_defs* creates::

        CREATE INDEX IF NOT EXISTS idx_{table}_sync ON {table}({ts_col}, {pk_col})

    Safe to call repeatedly — ``IF NOT EXISTS`` makes it a no-op when indexes
    are already present (< 1ms).

    Returns True on success, False if the database is unreachable or locked.
    """
    try:
        conn = sqlite3.connect(str(db_path), timeout=60)
        for tname, ts_col, pk_col in index_defs:
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{tname}_sync "
                f"ON {tname}({ts_col}, {pk_col})"
            )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"ensure_indexes: {e}")
        return False


# ---------------------------------------------------------------------------
# Per-table sync
# ---------------------------------------------------------------------------

def _sync_table(conn: sqlite3.Connection, table: SyncTableDef,
                last_sync: Optional[datetime]) -> Tuple[int, int, List, List]:
    """Sync one table bidirectionally via ATTACH.

    Args:
        conn: Local connection with shared DB attached as ``shared``.
        table: Table definition (name, pk, ts, push_only).
        last_sync: Last successful sync time — used for heartbeat skip.

    Returns:
        ``(pushed, pulled, push_ids, pull_ids)``
    """
    t0 = time.perf_counter()
    tname, pk, ts = table.name, table.pk, table.ts

    # Heartbeat skip: if neither side has changed since last_sync, skip the
    # table entirely.  Relies on indexed (ts, pk) covering index for speed.
    if last_sync is not None:
        ls_iso = _to_sqlite_ts(last_sync)
        try:
            shared_new = conn.execute(
                f"SELECT 1 FROM shared.{tname} WHERE {ts} > ? LIMIT 1", (ls_iso,)
            ).fetchone()
            local_new = conn.execute(
                f"SELECT 1 FROM {tname} WHERE {ts} > ? LIMIT 1", (ls_iso,)
            ).fetchone()
            if not shared_new and not local_new:
                logger.debug(f"  [SYNC] {tname}: skip (no changes since {ls_iso[:19]})")
                return 0, 0, [], []
        except Exception as hb_err:
            logger.debug(f"  [SYNC] {tname}: heartbeat probe failed ({hb_err}), full scan")

    # Fetch (pk, ts) from both sides — avoids reading BLOB overflow pages
    # and eliminates the cross-DB JOIN SQLite cannot optimise.
    try:
        local_rows  = conn.execute(f"SELECT {pk}, {ts} FROM {tname}").fetchall()
        shared_rows = conn.execute(f"SELECT {pk}, {ts} FROM shared.{tname}").fetchall()
    except Exception as e:
        logger.warning(f"  [SYNC] {tname}: fetch failed — {e}")
        return 0, 0, [], []

    local_map  = {r[0]: r[1] for r in local_rows}
    shared_map = {r[0]: r[1] for r in shared_rows}

    push_ids: List = []
    pull_ids: List = []

    for pk_val, local_ts in local_map.items():
        if pk_val not in shared_map:
            push_ids.append(pk_val)  # missing from shared — always push
        elif local_ts and shared_map[pk_val] and local_ts > shared_map[pk_val]:
            push_ids.append(pk_val)  # local is newer — push

    if not table.push_only:
        for pk_val, shared_ts in shared_map.items():
            if pk_val not in local_map:
                pull_ids.append(pk_val)  # missing from local — always pull
            else:
                local_ts = local_map[pk_val]
                if shared_ts and local_ts and shared_ts > local_ts:
                    pull_ids.append(pk_val)  # shared is newer — pull

    logger.debug(
        f"  [SYNC] {tname}: diff {(time.perf_counter()-t0)*1000:.1f}ms "
        f"↑{len(push_ids)} ↓{len(pull_ids)} "
        f"(local={len(local_map)} shared={len(shared_map)})"
    )

    # Resolve column lists explicitly — positional INSERT is unsafe when ALTER TABLE
    # has added columns at different times on each machine (column order may differ).
    try:
        local_cols  = [r[1] for r in conn.execute(f"PRAGMA table_info({tname})").fetchall()]
        shared_cols = {r[1] for r in conn.execute(f"PRAGMA shared.table_info({tname})").fetchall()}
    except Exception as e:
        logger.warning(f"  [SYNC] {tname}: PRAGMA failed — {e}")
        return 0, 0, [], []

    common_cols = [c for c in local_cols if c in shared_cols]
    if not common_cols:
        logger.warning(f"  [SYNC] {tname}: no common columns — skip")
        return 0, 0, [], []
    cols_sql = ', '.join(common_cols)

    pushed = 0
    pulled = 0

    if push_ids:
        ph = ','.join('?' * len(push_ids))
        try:
            ta0 = time.perf_counter()
            conn.execute(
                f"INSERT OR REPLACE INTO shared.{tname} ({cols_sql}) "
                f"SELECT {cols_sql} FROM {tname} WHERE {pk} IN ({ph})",
                push_ids
            )
            pushed = len(push_ids)
            logger.info(
                f"  [SYNC] {tname}: pushed {pushed} rows "
                f"in {(time.perf_counter()-ta0)*1000:.1f}ms"
            )
        except Exception as e:
            logger.warning(f"  [SYNC] {tname}: push failed — {e}")

    if pull_ids:
        ph = ','.join('?' * len(pull_ids))
        try:
            tb0 = time.perf_counter()
            conn.execute(
                f"INSERT OR REPLACE INTO {tname} ({cols_sql}) "
                f"SELECT {cols_sql} FROM shared.{tname} WHERE {pk} IN ({ph})",
                pull_ids
            )
            pulled = len(pull_ids)
            logger.info(
                f"  [SYNC] {tname}: pulled {pulled} rows "
                f"in {(time.perf_counter()-tb0)*1000:.1f}ms"
            )
        except Exception as e:
            logger.warning(f"  [SYNC] {tname}: pull failed — {e}")

    logger.debug(f"  [SYNC] {tname}: total {(time.perf_counter()-t0)*1000:.1f}ms")
    return pushed, pulled, push_ids, pull_ids


# ---------------------------------------------------------------------------
# SyncManager
# ---------------------------------------------------------------------------

class SyncManager:
    """Orchestrates ATTACH-based bidirectional sync between local and shared databases.

    Example::

        from pathlib import Path
        from sqlite_shared_sync import SyncManager, SyncConfig, SyncTableDef

        config = SyncConfig(
            local_db_path=Path("local.db"),
            shared_db_path=Path("L:/share/shared.db"),
            tables=[
                SyncTableDef("customers", pk="customer_id", ts="updated_at"),
                SyncTableDef("rfqs",      pk="rfq_id",      ts="updated_at"),
                SyncTableDef("audit_log", pk="id",          ts="created_at", push_only=True),
            ],
        )
        manager = SyncManager(config)
        result = manager.sync()
    """

    def __init__(self, config: SyncConfig):
        self.config = config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_shared_available(self) -> bool:
        """Return True if the shared database file exists and is reachable."""
        p = self.config.shared_db_path
        return p.parent.exists() and p.exists()

    def get_last_sync_time(self) -> Optional[datetime]:
        """Return the timestamp of the last successful sync, or None."""
        return get_last_sync_time(self.config.meta_path)

    def sync(self, table_names: Optional[List[str]] = None,
             progress_fn: Optional[callable] = None) -> SyncResult:
        """Run sync for the given tables (or all tables if *table_names* is None).

        Args:
            table_names: Subset of table names to sync. Must be a subset of the
                names in ``config.tables``. None means all tables.
            progress_fn: Optional ``callable(str)`` for progress feedback.

        Returns:
            A SyncResult describing what happened.
        """
        result = SyncResult()
        cfg = self.config

        tables = cfg.tables if table_names is None else [
            t for t in cfg.tables if t.name in table_names
        ]
        if not tables:
            result.success = True
            return result

        if not cfg.shared_db_path.parent.exists():
            result.offline = True
            result.error = f"Shared drive not available: {cfg.shared_db_path.parent}"
            logger.info(f"Sync skipped: {result.error}")
            return result

        lock = LockFile(
            cfg.shared_db_path.parent,
            timeout=cfg.lock_timeout,
            retries=cfg.lock_retries,
            retry_delay=cfg.lock_retry_delay,
        )
        if not lock.acquire():
            result.error = "Could not acquire sync lock (another process may be syncing)"
            logger.warning(result.error)
            return result

        try:
            return self._do_sync(tables, result, progress_fn)
        except Exception as e:
            result.error = str(e)
            logger.error(f"Sync failed: {e}", exc_info=True)
            return result
        finally:
            lock.release()

    def initial_push(self) -> SyncResult:
        """First-time setup: copy local DB to the shared location.

        Creates the parent directory if it doesn't exist, then copies the full
        database file.  Subsequent syncs use the ATTACH-based algorithm.
        """
        result = SyncResult()
        cfg = self.config

        try:
            cfg.shared_db_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            result.error = f"Cannot create shared directory: {e}"
            return result

        try:
            shutil.copy2(cfg.local_db_path, cfg.shared_db_path)
            record_sync_time(cfg.meta_path)
            result.success = True
            result.pushed = 1
            logger.info(f"Initial push: copied {cfg.local_db_path} → {cfg.shared_db_path}")
        except OSError as e:
            result.error = f"Failed to copy database: {e}"
            logger.error(result.error)

        return result

    def backup_databases(self) -> dict:
        """Manually back up both local and (if reachable) shared databases."""
        cfg = self.config
        out: dict = {
            "local_backed_up": False,
            "shared_backed_up": False,
            "local_path": str(cfg.local_db_path),
            "shared_path": None,
            "error": None,
        }
        try:
            backup_file(cfg.local_db_path, cfg.max_backups)
            out["local_backed_up"] = True
        except Exception as e:
            out["error"] = f"Local backup failed: {e}"
            logger.error(out["error"])
            return out

        if cfg.shared_db_path.exists():
            try:
                backup_file(cfg.shared_db_path, cfg.max_backups)
                out["shared_backed_up"] = True
                out["shared_path"] = str(cfg.shared_db_path)
            except Exception as e:
                out["error"] = f"Shared backup failed: {e}"
                logger.error(out["error"])

        return out

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _do_sync(self, tables: List[SyncTableDef], result: SyncResult,
                 progress_fn: Optional[callable]) -> SyncResult:
        cfg = self.config

        if not cfg.shared_db_path.exists():
            logger.info("Shared DB does not exist — skipping (run initial_push first)")
            result.success = True
            return result

        last_sync = get_last_sync_time(cfg.meta_path)

        t_start = time.perf_counter()
        logger.info(f"[SYNC] ═══ Starting sync on {socket.gethostname()} ═══")
        logger.info(f"[SYNC]   local  : {cfg.local_db_path}")
        logger.info(f"[SYNC]   shared : {cfg.shared_db_path}")
        logger.info(f"[SYNC]   last   : {last_sync.isoformat() if last_sync else 'Never'}")
        logger.info(f"[SYNC]   tables : {[t.name for t in tables]}")

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(str(cfg.local_db_path), timeout=30)
            conn.execute(f"ATTACH DATABASE '{cfg.shared_db_path}' AS shared")
            conn.execute("PRAGMA foreign_keys = OFF")

            total_pushed = 0
            total_pulled = 0

            for i, tbl in enumerate(tables, 1):
                if progress_fn:
                    progress_fn(f"Syncing {tbl.name}… ({i}/{len(tables)})")
                pushed, pulled, _, _ = _sync_table(conn, tbl, last_sync)
                total_pushed += pushed
                total_pulled += pulled
                if pushed:
                    result.details.append(f"{tbl.name}: pushed {pushed}")
                if pulled:
                    result.details.append(f"{tbl.name}: pulled {pulled}")

            if cfg.on_post_sync:
                if progress_fn:
                    progress_fn("Post-sync hook…")
                try:
                    cfg.on_post_sync(conn, result)
                except Exception as e:
                    logger.error(f"on_post_sync hook failed: {e}", exc_info=True)
                    result.details.append(f"post_sync: error — {e}")

            conn.execute("PRAGMA foreign_keys = ON")
            conn.commit()

            elapsed = (time.perf_counter() - t_start) * 1000
            logger.info(
                f"[SYNC] ═══ Complete: {elapsed:.0f}ms  "
                f"pushed={total_pushed} pulled={total_pulled} ═══"
            )

        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            result.error = f"Sync failed: {e}"
            logger.error(result.error, exc_info=True)
            return result
        finally:
            if conn:
                try:
                    conn.execute("DETACH DATABASE shared")
                    conn.close()
                except Exception:
                    pass

        record_sync_time(cfg.meta_path)
        result.pushed = total_pushed
        result.pulled = total_pulled
        result.success = True
        return result
