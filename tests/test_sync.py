"""
Tests for sqlite_shared_sync.

Uses two real SQLite files — one "local", one "shared" — to verify the sync
algorithm without mocking. All tests run in a fresh tmp_path, so they are
fully isolated from each other.
"""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from sqlite_shared_sync import (
    SyncConfig, SyncManager, SyncResult, SyncTableDef,
    LockFile, backup_file, get_last_sync_time, record_sync_time,
)
from sqlite_shared_sync.manager import _to_sqlite_ts, _sync_table, ensure_indexes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(path: Path) -> sqlite3.Connection:
    """Create a minimal test schema and return an open connection."""
    conn = sqlite3.connect(str(path))
    conn.execute("""
        CREATE TABLE customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT,
            is_deleted INTEGER DEFAULT 0,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE audit_log (
            id TEXT PRIMARY KEY,
            message TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    return conn


def _ts(offset_seconds: int = 0) -> str:
    """Return a SQLite-formatted timestamp offset from now."""
    return _to_sqlite_ts(datetime.now() + timedelta(seconds=offset_seconds))


def _config(tmp_path: Path, tables=None) -> SyncConfig:
    local = tmp_path / "local.db"
    shared = tmp_path / "shared.db"
    _make_db(local).close()
    _make_db(shared).close()

    if tables is None:
        tables = [SyncTableDef("customers", pk="customer_id", ts="updated_at")]

    return SyncConfig(
        local_db_path=local,
        shared_db_path=shared,
        tables=tables,
        meta_path=tmp_path / "sync_meta.json",
    )


# ---------------------------------------------------------------------------
# _to_sqlite_ts
# ---------------------------------------------------------------------------

def test_to_sqlite_ts_replaces_T():
    result = _to_sqlite_ts(datetime(2026, 6, 15, 14, 30, 0))
    assert 'T' not in result
    assert '2026-06-15 14:30:00' in result


def test_to_sqlite_ts_string_input():
    result = _to_sqlite_ts("2026-06-15T14:30:00")
    assert result == "2026-06-15 14:30:00"


# ---------------------------------------------------------------------------
# LockFile
# ---------------------------------------------------------------------------

def test_lock_acquire_release(tmp_path):
    lock = LockFile(tmp_path, timeout=30, retries=1)
    assert lock.acquire()
    assert (tmp_path / ".sync.lock").exists()
    lock.release()
    assert not (tmp_path / ".sync.lock").exists()


def test_lock_context_manager(tmp_path):
    lock = LockFile(tmp_path, timeout=30, retries=1)
    with lock:
        assert (tmp_path / ".sync.lock").exists()
    assert not (tmp_path / ".sync.lock").exists()


def test_lock_blocks_second_acquire(tmp_path):
    lock1 = LockFile(tmp_path, timeout=30, retries=1)
    lock2 = LockFile(tmp_path, timeout=30, retries=1)
    assert lock1.acquire()
    assert not lock2.acquire()  # blocked
    lock1.release()


def test_lock_stale_override(tmp_path):
    import json, time
    lock_path = tmp_path / ".sync.lock"
    # Write a lock that's 60s old (stale for timeout=30)
    lock_path.write_text(json.dumps({
        "host": "old_host",
        "time": (datetime.now() - timedelta(seconds=60)).isoformat(),
        "pid": 99999,
    }))
    lock = LockFile(tmp_path, timeout=30, retries=1)
    assert lock.acquire()  # stale lock overridden
    lock.release()


# ---------------------------------------------------------------------------
# backup_file
# ---------------------------------------------------------------------------

def test_backup_creates_file(tmp_path):
    db = tmp_path / "test.db"
    db.write_bytes(b"sqlite")
    result = backup_file(db, max_backups=5)
    assert result
    backups = list(tmp_path.glob("test.bak.*"))
    assert len(backups) == 1


def test_backup_prunes_old(tmp_path):
    db = tmp_path / "test.db"
    db.write_bytes(b"sqlite")
    for _ in range(7):
        backup_file(db, max_backups=3)
    backups = list(tmp_path.glob("test.bak.*"))
    assert len(backups) <= 3


def test_backup_missing_file(tmp_path):
    result = backup_file(tmp_path / "nonexistent.db", max_backups=5)
    assert not result


# ---------------------------------------------------------------------------
# Meta persistence
# ---------------------------------------------------------------------------

def test_record_and_get_last_sync(tmp_path):
    meta = tmp_path / "sync.json"
    assert get_last_sync_time(meta) is None
    before = datetime.now()
    record_sync_time(meta)
    after = datetime.now()
    ts = get_last_sync_time(meta)
    assert ts is not None
    assert before <= ts <= after


# ---------------------------------------------------------------------------
# ensure_indexes
# ---------------------------------------------------------------------------

def test_ensure_indexes_creates_index(tmp_path):
    db = tmp_path / "test.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY, ts TEXT)")
    conn.commit()
    conn.close()

    result = ensure_indexes(db, [("t", "ts", "id")])
    assert result

    conn = sqlite3.connect(str(db))
    indexes = [r[1] for r in conn.execute("PRAGMA index_list(t)").fetchall()]
    conn.close()
    assert "idx_t_sync" in indexes


# ---------------------------------------------------------------------------
# _sync_table — unit-level tests with ATTACH
# ---------------------------------------------------------------------------

def _attach(local_path: Path, shared_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(local_path))
    conn.execute(f"ATTACH DATABASE '{shared_path}' AS shared")
    conn.execute("PRAGMA foreign_keys = OFF")
    return conn


def test_sync_table_push_new_row(tmp_path):
    cfg = _config(tmp_path)
    # Insert row into local only
    local = sqlite3.connect(str(cfg.local_db_path))
    local.execute("INSERT INTO customers VALUES ('C1', 'Alice', 0, ?)", (_ts(),))
    local.commit()
    local.close()

    conn = _attach(cfg.local_db_path, cfg.shared_db_path)
    tbl = SyncTableDef("customers", pk="customer_id", ts="updated_at")
    pushed, pulled, push_ids, pull_ids = _sync_table(conn, tbl, last_sync=None)
    conn.commit()
    conn.close()

    assert pushed == 1
    assert 'C1' in push_ids

    # Verify shared has the row
    shared = sqlite3.connect(str(cfg.shared_db_path))
    row = shared.execute("SELECT name FROM customers WHERE customer_id='C1'").fetchone()
    shared.close()
    assert row and row[0] == 'Alice'


def test_sync_table_pull_new_row(tmp_path):
    cfg = _config(tmp_path)
    # Insert row into shared only
    shared = sqlite3.connect(str(cfg.shared_db_path))
    shared.execute("INSERT INTO customers VALUES ('C2', 'Bob', 0, ?)", (_ts(),))
    shared.commit()
    shared.close()

    conn = _attach(cfg.local_db_path, cfg.shared_db_path)
    tbl = SyncTableDef("customers", pk="customer_id", ts="updated_at")
    pushed, pulled, _, pull_ids = _sync_table(conn, tbl, last_sync=None)
    conn.commit()
    conn.close()

    assert pulled == 1
    assert 'C2' in pull_ids

    local = sqlite3.connect(str(cfg.local_db_path))
    row = local.execute("SELECT name FROM customers WHERE customer_id='C2'").fetchone()
    local.close()
    assert row and row[0] == 'Bob'


def test_sync_table_newer_local_wins(tmp_path):
    cfg = _config(tmp_path)
    old_ts = _ts(-100)
    new_ts = _ts(0)

    local = sqlite3.connect(str(cfg.local_db_path))
    local.execute("INSERT INTO customers VALUES ('C3', 'NewName', 0, ?)", (new_ts,))
    local.commit()
    local.close()

    shared = sqlite3.connect(str(cfg.shared_db_path))
    shared.execute("INSERT INTO customers VALUES ('C3', 'OldName', 0, ?)", (old_ts,))
    shared.commit()
    shared.close()

    conn = _attach(cfg.local_db_path, cfg.shared_db_path)
    _sync_table(conn, SyncTableDef("customers", pk="customer_id", ts="updated_at"), None)
    conn.commit()
    conn.close()

    shared = sqlite3.connect(str(cfg.shared_db_path))
    row = shared.execute("SELECT name FROM customers WHERE customer_id='C3'").fetchone()
    shared.close()
    assert row[0] == 'NewName'


def test_sync_table_heartbeat_skip(tmp_path):
    cfg = _config(tmp_path)
    future_last_sync = datetime.now() + timedelta(seconds=3600)  # future — nothing is newer

    local = sqlite3.connect(str(cfg.local_db_path))
    local.execute("INSERT INTO customers VALUES ('C4', 'Dave', 0, ?)", (_ts(-1000),))
    local.commit()
    local.close()

    conn = _attach(cfg.local_db_path, cfg.shared_db_path)
    pushed, pulled, _, _ = _sync_table(
        conn, SyncTableDef("customers", pk="customer_id", ts="updated_at"),
        last_sync=future_last_sync
    )
    conn.commit()
    conn.close()

    assert pushed == 0  # heartbeat skip: local ts < future last_sync
    assert pulled == 0


def test_sync_table_push_only(tmp_path):
    cfg = _config(tmp_path)
    shared = sqlite3.connect(str(cfg.shared_db_path))
    shared.execute("INSERT INTO audit_log VALUES ('AL1', 'msg', ?)", (_ts(),))
    shared.commit()
    shared.close()

    conn = _attach(cfg.local_db_path, cfg.shared_db_path)
    tbl = SyncTableDef("audit_log", pk="id", ts="created_at", push_only=True)
    pushed, pulled, _, _ = _sync_table(conn, tbl, last_sync=None)
    conn.commit()
    conn.close()

    assert pulled == 0  # push_only: never pull


# ---------------------------------------------------------------------------
# SyncManager — integration tests
# ---------------------------------------------------------------------------

def test_manager_is_shared_available(tmp_path):
    cfg = _config(tmp_path)
    mgr = SyncManager(cfg)
    assert mgr.is_shared_available()


def test_manager_is_shared_unavailable(tmp_path):
    cfg = _config(tmp_path)
    cfg = SyncConfig(
        local_db_path=cfg.local_db_path,
        shared_db_path=Path("/nonexistent/share/db.sqlite"),
        tables=cfg.tables,
        meta_path=cfg.meta_path,
    )
    mgr = SyncManager(cfg)
    assert not mgr.is_shared_available()


def test_manager_sync_offline(tmp_path):
    cfg = SyncConfig(
        local_db_path=tmp_path / "local.db",
        shared_db_path=Path("/nonexistent/share/db.sqlite"),
        tables=[SyncTableDef("customers", pk="customer_id", ts="updated_at")],
        meta_path=tmp_path / "sync_meta.json",
    )
    (tmp_path / "local.db").write_bytes(b"")
    mgr = SyncManager(cfg)
    result = mgr.sync()
    assert result.offline is True
    assert result.success is False


def test_manager_full_round_trip(tmp_path):
    cfg = _config(tmp_path)
    mgr = SyncManager(cfg)

    # Seed local with two customers
    local = sqlite3.connect(str(cfg.local_db_path))
    local.execute("INSERT INTO customers VALUES ('C1', 'Alice', 0, ?)", (_ts(-2),))
    local.execute("INSERT INTO customers VALUES ('C2', 'Bob', 0, ?)", (_ts(-2),))
    local.commit()
    local.close()

    # Seed shared with one customer (newer — should pull)
    shared = sqlite3.connect(str(cfg.shared_db_path))
    shared.execute("INSERT INTO customers VALUES ('C3', 'Carol', 0, ?)", (_ts(-1),))
    shared.commit()
    shared.close()

    result = mgr.sync()

    assert result.success
    assert result.pushed == 2   # C1, C2
    assert result.pulled == 1   # C3

    local = sqlite3.connect(str(cfg.local_db_path))
    names = {r[0] for r in local.execute("SELECT name FROM customers").fetchall()}
    local.close()
    assert names == {'Alice', 'Bob', 'Carol'}


def test_manager_initial_push(tmp_path):
    local = tmp_path / "local.db"
    shared = tmp_path / "shared.db"
    _make_db(local).close()

    local_conn = sqlite3.connect(str(local))
    local_conn.execute("INSERT INTO customers VALUES ('C1', 'Alice', 0, ?)", (_ts(),))
    local_conn.commit()
    local_conn.close()

    cfg = SyncConfig(
        local_db_path=local,
        shared_db_path=shared,
        tables=[SyncTableDef("customers", pk="customer_id", ts="updated_at")],
        meta_path=tmp_path / "sync_meta.json",
    )
    mgr = SyncManager(cfg)
    result = mgr.initial_push()
    assert result.success
    assert shared.exists()

    shared_conn = sqlite3.connect(str(shared))
    row = shared_conn.execute("SELECT name FROM customers WHERE customer_id='C1'").fetchone()
    shared_conn.close()
    assert row and row[0] == 'Alice'


def test_manager_post_sync_hook(tmp_path):
    calls = []

    def hook(conn, result):
        calls.append("called")

    cfg = _config(tmp_path)
    cfg = SyncConfig(
        local_db_path=cfg.local_db_path,
        shared_db_path=cfg.shared_db_path,
        tables=cfg.tables,
        meta_path=cfg.meta_path,
        on_post_sync=hook,
    )
    mgr = SyncManager(cfg)
    mgr.sync()
    assert calls == ["called"]


def test_manager_no_tables_for_names(tmp_path):
    cfg = _config(tmp_path)
    mgr = SyncManager(cfg)
    result = mgr.sync(table_names=["nonexistent_table"])
    assert result.success  # no-op, not an error


if __name__ == "__main__":
    import sys
    raise SystemExit(pytest.main([__file__, "-v"]))
