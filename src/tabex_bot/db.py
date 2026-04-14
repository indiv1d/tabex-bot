from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str) -> None:
    with get_connection(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                timezone TEXT NOT NULL DEFAULT 'Europe/Moscow',
                plan_start TEXT
            );

            CREATE TABLE IF NOT EXISTS doses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                scheduled_at_utc TEXT NOT NULL,
                taken_at_utc TEXT,
                reminded_at_utc TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_doses_user_schedule
                ON doses(user_id, scheduled_at_utc);
            """
        )


def upsert_user(db_path: str, user_id: int, chat_id: int) -> None:
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO users(user_id, chat_id)
            VALUES (?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET chat_id = excluded.chat_id
            """,
            (user_id, chat_id),
        )


def set_user_timezone(db_path: str, user_id: int, timezone_name: str) -> None:
    with get_connection(db_path) as conn:
        conn.execute(
            "UPDATE users SET timezone = ? WHERE user_id = ?",
            (timezone_name, user_id),
        )


def get_user(db_path: str, user_id: int) -> sqlite3.Row | None:
    with get_connection(db_path) as conn:
        return conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()


def list_users(db_path: str) -> list[sqlite3.Row]:
    with get_connection(db_path) as conn:
        return conn.execute("SELECT * FROM users").fetchall()


def set_plan(db_path: str, user_id: int, plan_start: datetime, local_schedule: list[datetime]) -> int:
    with get_connection(db_path) as conn:
        conn.execute("UPDATE users SET plan_start = ? WHERE user_id = ?", (plan_start.isoformat(), user_id))
        conn.execute("DELETE FROM doses WHERE user_id = ?", (user_id,))

        payload = [
            (
                user_id,
                local_dt.astimezone(timezone.utc).isoformat(),
            )
            for local_dt in local_schedule
        ]
        conn.executemany(
            "INSERT INTO doses(user_id, scheduled_at_utc) VALUES (?, ?)",
            payload,
        )
        return len(payload)


def clear_plan(db_path: str, user_id: int) -> None:
    with get_connection(db_path) as conn:
        conn.execute("UPDATE users SET plan_start = NULL WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM doses WHERE user_id = ?", (user_id,))


def _to_local(utc_iso: str, timezone_name: str) -> datetime:
    utc_dt = datetime.fromisoformat(utc_iso)
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(ZoneInfo(timezone_name))


def get_plan_day_doses(
    db_path: str,
    user_id: int,
    timezone_name: str,
    now_local: datetime,
) -> tuple[int, datetime, datetime, list[sqlite3.Row]]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT id, scheduled_at_utc, taken_at_utc FROM doses WHERE user_id = ? ORDER BY scheduled_at_utc",
            (user_id,),
        ).fetchall()

    if not rows:
        return 0, now_local, now_local, []

    first_local = _to_local(rows[0]["scheduled_at_utc"], timezone_name)
    if now_local < first_local:
        day_number = 1
    else:
        delta = now_local - first_local
        day_number = int(delta.total_seconds() // 86400) + 1

    day_start = first_local + timedelta(days=day_number - 1)
    day_end = day_start + timedelta(days=1)

    result: list[sqlite3.Row] = []
    for row in rows:
        local_dt = _to_local(row["scheduled_at_utc"], timezone_name)
        if day_start <= local_dt < day_end:
            result.append(row)
    return day_number, day_start, day_end, result


def mark_dose_taken(db_path: str, dose_id: int, taken_at_utc: datetime) -> bool:
    with get_connection(db_path) as conn:
        cur = conn.execute(
            """
            UPDATE doses
            SET taken_at_utc = ?
            WHERE id = ? AND taken_at_utc IS NULL
            """,
            (taken_at_utc.astimezone(timezone.utc).isoformat(), dose_id),
        )
        return cur.rowcount > 0


def mark_next_pending_taken(db_path: str, user_id: int, now_utc: datetime) -> sqlite3.Row | None:
    with get_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT id, scheduled_at_utc
            FROM doses
            WHERE user_id = ?
              AND taken_at_utc IS NULL
              AND scheduled_at_utc <= ?
            ORDER BY scheduled_at_utc DESC
            LIMIT 1
            """,
            (user_id, now_utc.astimezone(timezone.utc).isoformat()),
        ).fetchone()

        if row is None:
            row = conn.execute(
                """
                SELECT id, scheduled_at_utc
                FROM doses
                WHERE user_id = ? AND taken_at_utc IS NULL
                ORDER BY scheduled_at_utc ASC
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()

        if row is None:
            return None

        conn.execute(
            "UPDATE doses SET taken_at_utc = ? WHERE id = ?",
            (now_utc.astimezone(timezone.utc).isoformat(), row["id"]),
        )
        return row


def get_dose(db_path: str, dose_id: int) -> sqlite3.Row | None:
    with get_connection(db_path) as conn:
        return conn.execute(
            "SELECT id, user_id, scheduled_at_utc, taken_at_utc FROM doses WHERE id = ?",
            (dose_id,),
        ).fetchone()


def list_pending_doses(db_path: str, from_utc: datetime) -> list[sqlite3.Row]:
    with get_connection(db_path) as conn:
        return conn.execute(
            """
            SELECT d.id, d.user_id, u.chat_id, d.scheduled_at_utc
            FROM doses d
            JOIN users u ON u.user_id = d.user_id
            WHERE d.taken_at_utc IS NULL
              AND d.scheduled_at_utc >= ?
            ORDER BY d.scheduled_at_utc ASC
            """,
            (from_utc.astimezone(timezone.utc).isoformat(),),
        ).fetchall()


def mark_reminded(db_path: str, dose_id: int, reminded_at_utc: datetime) -> None:
    with get_connection(db_path) as conn:
        conn.execute(
            "UPDATE doses SET reminded_at_utc = ? WHERE id = ?",
            (reminded_at_utc.astimezone(timezone.utc).isoformat(), dose_id),
        )


def get_missed_doses(db_path: str, user_id: int, now_utc: datetime) -> list[sqlite3.Row]:
    with get_connection(db_path) as conn:
        return conn.execute(
            """
            SELECT id, scheduled_at_utc
            FROM doses
            WHERE user_id = ?
              AND taken_at_utc IS NULL
              AND scheduled_at_utc < ?
            ORDER BY scheduled_at_utc ASC
            """,
            (user_id, now_utc.astimezone(timezone.utc).isoformat()),
        ).fetchall()


def get_stats(db_path: str, user_id: int) -> tuple[int, int]:
    with get_connection(db_path) as conn:
        total = conn.execute("SELECT COUNT(*) FROM doses WHERE user_id = ?", (user_id,)).fetchone()[0]
        taken = conn.execute(
            "SELECT COUNT(*) FROM doses WHERE user_id = ? AND taken_at_utc IS NOT NULL",
            (user_id,),
        ).fetchone()[0]
    return int(taken), int(total)
