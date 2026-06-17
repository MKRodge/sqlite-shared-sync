# sqlite-shared-sync

Bidirectional SQLite sync over a shared drive using SQLite's `ATTACH DATABASE`.

Designed for small-team desktop applications where a network share is the
natural "server" — no cloud service, no extra daemon, no special server setup.
Each user keeps a local ``.db`` file; the shared ``.db`` lives on a mapped drive.

**Zero runtime dependencies** — pure Python stdlib.

## Install

```bash
pip install sqlite-shared-sync
```

## Quick start

```python
from pathlib import Path
from sqlite_shared_sync import SyncManager, SyncConfig, SyncTableDef

config = SyncConfig(
    local_db_path=Path("local.db"),
    shared_db_path=Path("L:/OPT/shared.db"),
    tables=[
        SyncTableDef("customers", pk="customer_id", ts="updated_at"),
        SyncTableDef("orders",    pk="order_id",    ts="updated_at"),
        SyncTableDef("audit_log", pk="id",          ts="created_at",
                     push_only=True),
    ],
)

manager = SyncManager(config)

# Machine A: seed the shared DB for the first time
manager.initial_push()

# Every machine, on startup or periodically
result = manager.sync()
print(f"Pushed {result.pushed}, pulled {result.pulled}")
```

## How it works

1. **ATTACH** the shared DB as `shared` on the local connection.
2. For each table, fetch `(pk, ts)` pairs from both sides — two lightweight
   index scans, no cross-DB JOIN.
3. Determine which PKs need pushing (local newer) or pulling (shared newer).
4. Apply with `INSERT OR REPLACE … SELECT … WHERE pk IN (…)`.
5. Call your optional `on_post_sync` hook for app-specific logic.
6. Commit and detach.

Only columns common to both schemas are transferred, so a column added on one
machine via `ALTER TABLE` doesn't break the sync — it just won't be synced
until both sides have the column. Pre-flight the shared schema in your
`on_post_sync` hook if you need tighter guarantees.

### Heartbeat skip

If neither side has any row with `ts > last_sync_time`, the table is skipped
entirely — one indexed probe per side, sub-millisecond.

### Soft deletes

Use an `is_deleted` boolean column on synced tables. Setting `is_deleted = 1`
and bumping `updated_at` causes the deletion to propagate on the next sync.
Hard deletes are not detected.

### File-based lock

A `.sync.lock` JSON file in the shared directory prevents two machines from
syncing simultaneously. Stale locks (older than `lock_timeout` seconds) are
automatically overridden.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `local_db_path` | required | Path to local SQLite file |
| `shared_db_path` | required | Path to shared SQLite file |
| `tables` | required | Ordered `SyncTableDef` list (FK dependency order) |
| `meta_path` | `{local.parent}/sync_meta.json` | Last-sync timestamp storage |
| `lock_timeout` | 30 | Seconds before a stale lock is overridden |
| `lock_retries` | 3 | Lock acquisition attempts |
| `lock_retry_delay` | 2 | Seconds between retries |
| `max_backups` | 5 | Timestamped backup copies to keep |
| `on_post_sync` | None | `(conn, result) -> None` hook for extra work |

## SyncTableDef

```python
SyncTableDef(
    name="customers",
    pk="customer_id",   # primary key column
    ts="updated_at",    # datetime column — newer side wins
    push_only=False,    # True = local → shared only (e.g. audit log)
)
```

## App-specific post-processing

Use `on_post_sync` for anything that doesn't fit the standard
timestamp-based diff: junction tables, permission sets, denormalized fields.
The hook receives the open `sqlite3.Connection` (with `shared` still attached)
and the in-progress `SyncResult`.

```python
def my_hook(conn: sqlite3.Connection, result: SyncResult) -> None:
    # propagate a sticky field bidirectionally
    conn.execute(
        "UPDATE customers SET vip = 1 WHERE customer_id IN "
        "(SELECT customer_id FROM shared.customers WHERE vip = 1)"
    )

config = SyncConfig(..., on_post_sync=my_hook)
```

## Licence

Apache 2.0 — see [LICENSE](LICENSE).
