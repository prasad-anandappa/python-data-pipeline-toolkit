"""Idempotent loaders — the piece that makes pipeline re-runs safe.

Every production pipeline gets re-run: a scheduler retries a failed task, an
operator backfills a window, a deploy restarts mid-load. A naive loader turns
each of those into duplicated data. These loaders make loading *idempotent*:
running the same batch twice has exactly the same effect as running it once.

Two mechanisms are combined:

* **Batch ledger** — every load is recorded under a caller-supplied
  ``batch_key`` (e.g. ``"payments/2026-11-02"``) together with a SHA-256 hash
  of the batch contents. Re-loading the same key with identical contents is
  a verified no-op; the same key with *different* contents is a hard error
  (that almost always means a bug upstream, and silently accepting it would
  corrupt the target).
* **Natural-key upserts** — rows are merged on their business key with
  ``INSERT ... ON CONFLICT DO UPDATE``, so even *different* batches that
  overlap (a late-arriving correction file) converge instead of duplicating.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

Record = dict[str, Any]


class LoadConflictError(RuntimeError):
    """A batch_key was re-used with different contents."""


@dataclass
class LoadResult:
    batch_key: str
    status: str  # "loaded" | "skipped_duplicate"
    rows_written: int
    content_hash: str


def content_hash(records: list[Record]) -> str:
    """Stable SHA-256 of a batch: canonical JSON, key-sorted, order-sensitive."""
    canonical = json.dumps(records, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class SQLiteLoader:
    """Load records into a SQLite table idempotently.

    SQLite stands in for any warehouse table here — the ledger + upsert
    pattern transfers directly to Postgres/MySQL/Snowflake MERGE semantics.

    The target table is created if absent, with one column per record key
    plus a ``_batch_key`` bookkeeping column. Column types are inferred as
    TEXT/INTEGER/REAL from the first non-null value seen; this loader is a
    teaching-grade implementation of the pattern, not a schema manager.
    """

    LEDGER_TABLE = "_load_ledger"

    def __init__(self, db_path: str | Path, table: str) -> None:
        self.db_path = str(db_path)
        self.table = table
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.LEDGER_TABLE} (
                    batch_key TEXT PRIMARY KEY,
                    content_hash TEXT NOT NULL,
                    rows_written INTEGER NOT NULL,
                    loaded_at TEXT NOT NULL
                )"""
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> SQLiteLoader:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- internals ---------------------------------------------------------

    def _ledger_entry(self, batch_key: str) -> tuple[str, int] | None:
        row = self._conn.execute(
            f"SELECT content_hash, rows_written FROM {self.LEDGER_TABLE} WHERE batch_key = ?",
            (batch_key,),
        ).fetchone()
        return (row[0], row[1]) if row else None

    def _ensure_table(self, records: list[Record], key_columns: list[str]) -> list[str]:
        columns = sorted({k for r in records for k in r.keys()})
        existing = {
            row[1]
            for row in self._conn.execute(f"PRAGMA table_info({self.table})").fetchall()
        }
        if not existing:
            defs = ", ".join(f'"{c}"' for c in columns)
            pk = ", ".join(f'"{c}"' for c in key_columns)
            self._conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{self.table}" ({defs}, "_batch_key" TEXT, '
                f"PRIMARY KEY ({pk}))"
            )
        else:
            for c in columns:
                if c not in existing:
                    self._conn.execute(f'ALTER TABLE "{self.table}" ADD COLUMN "{c}"')
        return columns

    # -- public API ----------------------------------------------------------

    def load(
        self,
        records: list[Record],
        *,
        batch_key: str,
        key_columns: list[str],
    ) -> LoadResult:
        """Load ``records`` under ``batch_key``, upserting on ``key_columns``."""
        digest = content_hash(records)
        prior = self._ledger_entry(batch_key)
        if prior is not None:
            if prior[0] == digest:
                return LoadResult(batch_key, "skipped_duplicate", 0, digest)
            raise LoadConflictError(
                f"batch_key {batch_key!r} was already loaded with different contents "
                f"(ledger hash {prior[0][:12]}…, new hash {digest[:12]}…). "
                "Use a new batch_key for corrected data."
            )
        if not records:
            self._record(batch_key, digest, 0)
            return LoadResult(batch_key, "loaded", 0, digest)

        columns = self._ensure_table(records, key_columns)
        collist = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        updates = ", ".join(
            f'"{c}" = excluded."{c}"' for c in columns if c not in key_columns
        )
        conflict = ", ".join(f'"{c}"' for c in key_columns)
        sql = (
            f'INSERT INTO "{self.table}" ({collist}, "_batch_key") '
            f"VALUES ({placeholders}, ?) "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}, "
            f'"_batch_key" = excluded."_batch_key"'
        )
        rows = [
            tuple(_sqlite_value(r.get(c)) for c in columns) + (batch_key,)
            for r in records
        ]
        with self._conn:  # single transaction: all rows + ledger entry, or none
            self._conn.executemany(sql, rows)
            self._record_in_txn(batch_key, digest, len(rows))
        return LoadResult(batch_key, "loaded", len(rows), digest)

    def _record(self, batch_key: str, digest: str, rows: int) -> None:
        with self._conn:
            self._record_in_txn(batch_key, digest, rows)

    def _record_in_txn(self, batch_key: str, digest: str, rows: int) -> None:
        self._conn.execute(
            f"INSERT INTO {self.LEDGER_TABLE} VALUES (?, ?, ?, ?)",
            (batch_key, digest, rows, datetime.now(timezone.utc).isoformat()),
        )

    def count(self) -> int:
        try:
            row = self._conn.execute(f'SELECT COUNT(*) FROM "{self.table}"').fetchone()
            return int(row[0])
        except sqlite3.OperationalError:
            return 0


def _sqlite_value(value: Any) -> Any:
    """Map Python values to SQLite-bindable ones (Decimal/date -> str)."""
    if value is None or isinstance(value, (str, int, float, bytes)):
        if isinstance(value, bool):
            return int(value)
        return value
    return str(value)


class AtomicFileLoader:
    """Write a batch to a file atomically and idempotently.

    The batch is serialised to a temp file in the same directory and
    ``os.replace``-ed into place, so a crash mid-write never leaves a
    truncated target file. A sidecar ``<file>.manifest.json`` records the
    content hash; re-loading identical contents is a no-op.
    """

    def __init__(self, target: str | Path) -> None:
        self.target = Path(target)
        self.manifest = self.target.with_suffix(self.target.suffix + ".manifest.json")

    def load(self, records: list[Record], *, batch_key: str) -> LoadResult:
        from pipeline_toolkit.io import write_jsonl

        digest = content_hash(records)
        if self.manifest.exists():
            prior = json.loads(self.manifest.read_text(encoding="utf-8"))
            if prior.get("batch_key") == batch_key:
                if prior.get("content_hash") == digest:
                    return LoadResult(batch_key, "skipped_duplicate", 0, digest)
                raise LoadConflictError(
                    f"batch_key {batch_key!r} already used for {self.target} "
                    "with different contents."
                )
        self.target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=self.target.parent, suffix=".tmp")
        os.close(fd)
        try:
            written = write_jsonl(tmp_name, records)
            os.replace(tmp_name, self.target)
        except BaseException:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise
        self.manifest.write_text(
            json.dumps(
                {
                    "batch_key": batch_key,
                    "content_hash": digest,
                    "rows_written": written,
                    "loaded_at": datetime.now(timezone.utc).isoformat(),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        return LoadResult(batch_key, "loaded", written, digest)
