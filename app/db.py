"""SQLite persistence layer.

All access goes through a single connection guarded by ``DB_LOCK``. The
dispatcher worker threads and the FastAPI request handlers share it safely.
WAL mode allows readers to run while the dispatcher is writing.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

from . import config

DB_LOCK = threading.RLock()
_conn: sqlite3.Connection | None = None


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        raise RuntimeError("database not initialized; call init_db() first")
    return _conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url         TEXT NOT NULL,
    secret      TEXT NOT NULL DEFAULT '',
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    sequence    INTEGER NOT NULL,
    event_id    TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL DEFAULT 'null',
    created_at  TEXT NOT NULL,
    UNIQUE (source, sequence)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
    event_id        INTEGER NOT NULL REFERENCES events(id),
    source          TEXT NOT NULL,
    sequence        INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending|retrying|delivered|dead
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT NOT NULL DEFAULT '',
    last_status     INTEGER,
    next_attempt_at TEXT,
    delivered_at    TEXT,
    dead_at         TEXT,
    UNIQUE (subscription_id, event_id)
);

CREATE TABLE IF NOT EXISTS delivery_cursors (
    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
    source          TEXT NOT NULL,
    next_sequence   INTEGER NOT NULL,
    PRIMARY KEY (subscription_id, source)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_dispatch
    ON deliveries (subscription_id, source, sequence, status);
CREATE INDEX IF NOT EXISTS idx_events_source_seq
    ON events (source, sequence);
CREATE INDEX IF NOT EXISTS idx_cursors ON delivery_cursors (subscription_id, source);
"""


def init_db() -> None:
    global _conn
    import os

    os.makedirs(os.path.dirname(config.DATABASE_PATH), exist_ok=True)
    conn = sqlite3.connect(config.DATABASE_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    with conn:
        conn.executescript(SCHEMA)
    _conn = conn


# --------------------------------------------------------------------------- helpers


def query_all(sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    with DB_LOCK:
        return get_conn().execute(sql, tuple(params)).fetchall()


def query_one(sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
    with DB_LOCK:
        return get_conn().execute(sql, tuple(params)).fetchone()


def row_to_dict(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row is not None else None


_BACKFILL_SQL = """
INSERT OR IGNORE INTO deliveries
    (subscription_id, event_id, source, sequence, status)
SELECT ?, e.id, e.source, e.sequence, 'pending'
FROM events e
WHERE NOT EXISTS (
    SELECT 1 FROM deliveries d
    WHERE d.subscription_id = ? AND d.event_id = e.id
)
"""

_ENSURE_CURSOR_SQL = """
INSERT OR IGNORE INTO delivery_cursors (subscription_id, source, next_sequence)
SELECT ?, e.source, MIN(e.sequence)
FROM events e
WHERE NOT EXISTS (
    SELECT 1 FROM delivery_cursors c
    WHERE c.subscription_id = ? AND c.source = e.source
)
GROUP BY e.source
"""


def ensure_cursor_and_backfill(conn: sqlite3.Connection, sub_id: int) -> None:
    conn.execute(_BACKFILL_SQL, (sub_id, sub_id))
    conn.execute(_ENSURE_CURSOR_SQL, (sub_id, sub_id))


# --------------------------------------------------------------------------- subscriptions


def create_subscription(url: str, secret: str) -> dict:
    now = utcnow()
    with DB_LOCK:
        conn = get_conn()
        with conn:
            cur = conn.execute(
                "INSERT INTO subscriptions (url, secret, active, created_at) VALUES (?, ?, 1, ?)",
                (url, secret, now),
            )
            sub_id = cur.lastrowid
            ensure_cursor_and_backfill(conn, sub_id)
        return get_subscription(sub_id)  # type: ignore[return-value]


def get_subscription(sub_id: int) -> dict | None:
    return row_to_dict(
        query_one("SELECT * FROM subscriptions WHERE id = ?", (sub_id,))
    )


def list_subscriptions(limit: int, offset: int) -> list[dict]:
    rows = query_all(
        "SELECT * FROM subscriptions ORDER BY id LIMIT ? OFFSET ?",
        (limit, offset),
    )
    return [dict(r) for r in rows]


def set_subscription_active(sub_id: int, active: bool) -> dict | None:
    with DB_LOCK:
        conn = get_conn()
        with conn:
            row = conn.execute(
                "SELECT * FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE subscriptions SET active = ? WHERE id = ?",
                (1 if active else 0, sub_id),
            )
            if active:
                ensure_cursor_and_backfill(conn, sub_id)
        return get_subscription(sub_id)


# --------------------------------------------------------------------------- events / ingest


def get_event_by_event_id(event_id: str) -> dict | None:
    return row_to_dict(
        query_one("SELECT * FROM events WHERE event_id = ?", (event_id,))
    )


def ingest_event(source: str, sequence: int, event_id: str, payload: Any) -> dict:
    """Idempotent ingest.

    Returns ``(result_dict, status)`` style dict:
      * new event  -> existing False
      * duplicate  -> existing True
    Raises ``SequenceConflict`` when the same (source, sequence) was already
    stored under a different event_id.
    """
    now = utcnow()
    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    with DB_LOCK:
        conn = get_conn()
        with conn:
            existing = conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if existing is not None:
                event = dict(existing)
                event["duplicate"] = True
                return event

            clash = conn.execute(
                "SELECT event_id FROM events WHERE source = ? AND sequence = ?",
                (source, sequence),
            ).fetchone()
            if clash is not None:
                raise SequenceConflict(
                    f"source={source} sequence={sequence} already used by event_id={clash['event_id']}"
                )

            cur = conn.execute(
                "INSERT INTO events (source, sequence, event_id, payload_json, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (source, sequence, event_id, payload_json, now),
            )
            event_pk = cur.lastrowid

            # Fan out to every active subscription.
            conn.execute(
                """
                INSERT INTO deliveries
                    (subscription_id, event_id, source, sequence, status)
                SELECT s.id, ?, ?, ?, 'pending'
                FROM subscriptions s
                WHERE s.active = 1
                """,
                (event_pk, source, sequence),
            )

            # Anchor a cursor for every active (sub, source) pair missing one,
            # at the lowest sequence observed so far for that source.
            conn.execute(
                """
                INSERT OR IGNORE INTO delivery_cursors
                    (subscription_id, source, next_sequence)
                SELECT s.id, ?, COALESCE((
                        SELECT MIN(e2.sequence) FROM events e2 WHERE e2.source = ?
                       ), ?)
                FROM subscriptions s
                WHERE s.active = 1
                  AND NOT EXISTS (
                      SELECT 1 FROM delivery_cursors c
                      WHERE c.subscription_id = s.id AND c.source = ?
                  )
                """,
                (source, source, sequence, source),
            )

        return dict(
            query_one("SELECT * FROM events WHERE id = ?", (event_pk,)),  # type: ignore[arg-type]
            duplicate=False,
        )


class SequenceConflict(Exception):
    pass


# --------------------------------------------------------------------------- dispatcher queries / updates


_CANDIDATE_SQL = """
SELECT d.*
FROM deliveries d
JOIN subscriptions s ON s.id = d.subscription_id
WHERE d.status IN ('pending', 'retrying')
  AND s.active = 1
  AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= :now)
  AND d.sequence = (
        SELECT MIN(d2.sequence)
        FROM deliveries d2
        WHERE d2.subscription_id = d.subscription_id
          AND d2.source = d.source
          AND d2.status IN ('pending', 'retrying')
          AND (d2.next_attempt_at IS NULL OR d2.next_attempt_at <= :now)
  )
  AND NOT EXISTS (
        SELECT 1 FROM deliveries d3
        WHERE d3.subscription_id = d.subscription_id
          AND d3.source = d.source
          AND d3.sequence < d.sequence
          AND d3.status IN ('pending', 'retrying')
  )
  AND EXISTS (
        SELECT 1 FROM delivery_cursors c
        WHERE c.subscription_id = d.subscription_id
          AND c.source = d.source
          AND c.next_sequence >= d.sequence
  )
ORDER BY d.id
LIMIT :limit
"""


def claim_candidates(now_ts: str, limit: int) -> list[dict]:
    with DB_LOCK:
        rows = get_conn().execute(
            _CANDIDATE_SQL, {"now": now_ts, "limit": limit}
        ).fetchall()
        return [dict(r) for r in rows]


def record_delivery_success(delivery_id: int, attempt_no: int) -> None:
    now = utcnow()
    with DB_LOCK:
        conn = get_conn()
        with conn:
            row = conn.execute(
                "SELECT subscription_id, source, sequence, status FROM deliveries WHERE id = ?",
                (delivery_id,),
            ).fetchone()
            if row is None or row["status"] not in ("pending", "retrying"):
                return  # replayed away / deleted / concurrently handled
            conn.execute(
                """
                UPDATE deliveries
                SET status = 'delivered', attempts = ?, last_error = '',
                    last_status = 200, next_attempt_at = NULL, delivered_at = ?
                WHERE id = ?
                """,
                (attempt_no, now, delivery_id),
            )
            # Advance the cursor exactly one step only if this row was the head.
            conn.execute(
                """
                UPDATE delivery_cursors
                SET next_sequence = ?
                WHERE subscription_id = ? AND source = ? AND next_sequence = ?
                """,
                (row["sequence"] + 1, row["subscription_id"], row["source"], row["sequence"]),
            )


def record_delivery_failure(
    delivery_id: int,
    attempt_no: int,
    status_code: int | None,
    error: str,
    next_attempt_at: str | None,
) -> str:
    """Record a failed attempt.

    Returns the resulting status: 'retrying' or 'dead'. A caller that lost a
    replay race gets back the current status unchanged.
    """
    now = utcnow()
    with DB_LOCK:
        conn = get_conn()
        with conn:
            row = conn.execute(
                "SELECT subscription_id, source, sequence, status, attempts FROM deliveries WHERE id = ?",
                (delivery_id,),
            ).fetchone()
            if row is None:
                return "missing"
            if row["status"] not in ("pending", "retrying"):
                return row["status"]

            if attempt_no >= config.MAX_ATTEMPTS:
                conn.execute(
                    """
                    UPDATE deliveries
                    SET status = 'dead', attempts = ?, last_error = ?,
                        last_status = ?, next_attempt_at = NULL, dead_at = ?
                    WHERE id = ?
                    """,
                    (attempt_no, error[:2000], status_code, now, delivery_id),
                )
                # Dead head must not block later sequences: advance one step.
                conn.execute(
                    """
                    UPDATE delivery_cursors
                    SET next_sequence = ?
                    WHERE subscription_id = ? AND source = ? AND next_sequence = ?
                    """,
                    (row["sequence"] + 1, row["subscription_id"], row["source"], row["sequence"]),
                )
                return "dead"

            conn.execute(
                """
                UPDATE deliveries
                SET status = 'retrying', attempts = ?, last_error = ?,
                    last_status = ?, next_attempt_at = ?
                WHERE id = ?
                """,
                (attempt_no, error[:2000], status_code, next_attempt_at, delivery_id),
            )
            return "retrying"


# --------------------------------------------------------------------------- queries


_BACKLOG_SQL = """
SELECT d.*
FROM deliveries d
WHERE d.status IN ('pending', 'retrying')
  AND (:subscription_id IS NULL OR d.subscription_id = :subscription_id)
  AND (:source IS NULL OR d.source = :source)
ORDER BY d.subscription_id, d.source, d.sequence
LIMIT :limit OFFSET :offset
"""

_BACKLOG_COUNT_SQL = """
SELECT COUNT(*) AS n
FROM deliveries d
WHERE d.status IN ('pending', 'retrying')
  AND (:subscription_id IS NULL OR d.subscription_id = :subscription_id)
  AND (:source IS NULL OR d.source = :source)
"""


def get_backlog(subscription_id: int | None, source: str | None, limit: int, offset: int):
    params = {
        "subscription_id": subscription_id,
        "source": source,
        "limit": limit,
        "offset": offset,
    }
    with DB_LOCK:
        total = get_conn().execute(_BACKLOG_COUNT_SQL, params).fetchone()["n"]
        rows = get_conn().execute(_BACKLOG_SQL, params).fetchall()
        items = []
        for r in rows:
            item = dict(r)
            cursor = get_conn().execute(
                "SELECT next_sequence FROM delivery_cursors WHERE subscription_id = ? AND source = ?",
                (r["subscription_id"], r["source"]),
            ).fetchone()
            head_seq = cursor["next_sequence"] if cursor else None
            item["awaiting_sequence"] = head_seq
            item["buffered"] = head_seq is not None and r["sequence"] > head_seq
            event = get_conn().execute(
                "SELECT * FROM events WHERE id = ?", (r["event_id"],)
            ).fetchone()
            if event:
                item["event"] = dict(event)
            items.append(item)
    return total, items


_DEAD_SQL = """
SELECT d.* FROM deliveries d
WHERE d.status = 'dead'
  AND (:subscription_id IS NULL OR d.subscription_id = :subscription_id)
  AND (:source IS NULL OR d.source = :source)
ORDER BY d.dead_at DESC, d.id DESC
LIMIT :limit OFFSET :offset
"""

_DEAD_COUNT_SQL = """
SELECT COUNT(*) AS n FROM deliveries d
WHERE d.status = 'dead'
  AND (:subscription_id IS NULL OR d.subscription_id = :subscription_id)
  AND (:source IS NULL OR d.source = :source)
"""


def list_dead(subscription_id: int | None, source: str | None, limit: int, offset: int):
    params = {
        "subscription_id": subscription_id,
        "source": source,
        "limit": limit,
        "offset": offset,
    }
    with DB_LOCK:
        total = get_conn().execute(_DEAD_COUNT_SQL, params).fetchone()["n"]
        rows = get_conn().execute(_DEAD_SQL, params).fetchall()
        items = []
        for r in rows:
            item = dict(r)
            event = get_conn().execute(
                "SELECT * FROM events WHERE id = ?", (r["event_id"],)
            ).fetchone()
            if event:
                item["event"] = dict(event)
            items.append(item)
    return total, items


def get_delivery(delivery_id: int) -> dict | None:
    with DB_LOCK:
        row = get_conn().execute(
            "SELECT * FROM deliveries WHERE id = ?", (delivery_id,)
        ).fetchone()
        if row is None:
            return None
        item = dict(row)
        event = get_conn().execute(
            "SELECT * FROM events WHERE id = ?", (row["event_id"],)
        ).fetchone()
        if event:
            item["event"] = dict(event)
        return item


def replay_delivery(delivery_id: int) -> dict | None | str:
    """Reset a dead delivery for redelivery.

    Returns the delivery dict, None if not found, or the current status
    string if the delivery is not dead.
    """
    with DB_LOCK:
        conn = get_conn()
        with conn:
            row = conn.execute(
                "SELECT * FROM deliveries WHERE id = ?", (delivery_id,)
            ).fetchone()
            if row is None:
                return None
            if row["status"] != "dead":
                return row["status"]
            conn.execute(
                """
                UPDATE deliveries
                SET status = 'pending', attempts = 0, last_error = '',
                    last_status = NULL, next_attempt_at = NULL, dead_at = NULL
                WHERE id = ?
                """,
                (delivery_id,),
            )
        return get_delivery(delivery_id)


def stats() -> dict:
    with DB_LOCK:
        conn = get_conn()
        subs = conn.execute("SELECT COUNT(*) n FROM subscriptions").fetchone()["n"]
        active = conn.execute(
            "SELECT COUNT(*) n FROM subscriptions WHERE active = 1"
        ).fetchone()["n"]
        events = conn.execute("SELECT COUNT(*) n FROM events").fetchone()["n"]
        counts = {
            r["status"]: r["n"]
            for r in conn.execute(
                "SELECT status, COUNT(*) n FROM deliveries GROUP BY status"
            )
        }
    return {
        "subscriptions": subs,
        "active_subscriptions": active,
        "events": events,
        "deliveries": {
            "pending": counts.get("pending", 0),
            "retrying": counts.get("retrying", 0),
            "delivered": counts.get("delivered", 0),
            "dead": counts.get("dead", 0),
        },
    }
