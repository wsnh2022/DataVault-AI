"""
core/chat_store.py - SQLite chat history persistence

Stores chat sessions and messages so history survives app restarts.
Each chat maps to a list of messages (user / assistant / system).
Assistant messages store the DataFrame as JSON for full restore on reload.

No external dependencies - uses stdlib sqlite3 only.
"""

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")  # safe concurrent reads
    return conn


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Schema init
# ---------------------------------------------------------------------------

def init_db(db_path: Path) -> None:
    """Create tables if they do not exist. Safe to call on every startup."""
    with _connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS chats (
                id         TEXT PRIMARY KEY,
                title      TEXT NOT NULL,
                file_name  TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id          TEXT    NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
                role             TEXT    NOT NULL,
                content          TEXT    NOT NULL,
                sql_text         TEXT,
                df_json          TEXT,
                grounding_json   TEXT,
                retried          INTEGER DEFAULT 0,
                error            TEXT,
                is_follow_up     INTEGER DEFAULT 0,
                follow_up_depth  INTEGER DEFAULT 0,
                created_at       TEXT    NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id);

        """)


# ---------------------------------------------------------------------------
# Chat CRUD
# ---------------------------------------------------------------------------

def create_chat(db_path: Path, title: str, file_name: str | None = None) -> str:
    """Insert a new chat row. Returns the new UUID string."""
    chat_id = str(uuid.uuid4())
    now = _now()
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO chats (id, title, file_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (chat_id, title[:100], file_name, now, now),
        )
    return chat_id


def update_chat(db_path: Path, chat_id: str, title: str | None = None) -> None:
    """Update chat title and bump updated_at. Pass title=None to only bump timestamp."""
    now = _now()
    with _connect(db_path) as conn:
        if title is not None:
            conn.execute(
                "UPDATE chats SET title = ?, updated_at = ? WHERE id = ?",
                (title[:100], now, chat_id),
            )
        else:
            conn.execute(
                "UPDATE chats SET updated_at = ? WHERE id = ?",
                (now, chat_id),
            )


def list_chats(db_path: Path) -> list[dict]:
    """Return all chats sorted newest first."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, title, file_name, created_at, updated_at FROM chats ORDER BY updated_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_chat(db_path: Path, chat_id: str) -> dict | None:
    """Return a single chat dict or None if not found."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT id, title, file_name, created_at, updated_at FROM chats WHERE id = ?",
            (chat_id,),
        ).fetchone()
    return dict(row) if row else None


def delete_chat(db_path: Path, chat_id: str) -> None:
    """Delete a chat and all its messages (CASCADE)."""
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM chats WHERE id = ?", (chat_id,))


def vacuum_db(db_path: Path) -> None:
    """Reclaim free SQLite pages left behind by deletes. Safe to call at startup."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.execute("VACUUM")
    conn.close()


# ---------------------------------------------------------------------------
# Message CRUD
# ---------------------------------------------------------------------------

def save_message(
    db_path: Path,
    chat_id: str,
    role: str,
    content: str,
    sql_text: str = "",
    df_json: str = "",
    grounding_json: str = "",
    retried: bool = False,
    error: str = "",
    is_follow_up: bool = False,
    follow_up_depth: int = 0,
) -> None:
    """Insert a message and bump the parent chat's updated_at."""
    now = _now()
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO messages
               (chat_id, role, content, sql_text, df_json, grounding_json,
                retried, error, is_follow_up, follow_up_depth, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                chat_id,
                role,
                content,
                sql_text or "",
                df_json or "",
                grounding_json or "",
                int(retried),
                error or "",
                int(is_follow_up),
                follow_up_depth,
                now,
            ),
        )
        conn.execute(
            "UPDATE chats SET updated_at = ? WHERE id = ?",
            (now, chat_id),
        )


def load_messages(db_path: Path, chat_id: str) -> list[dict]:
    """Return all messages for a chat in insertion order."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            """SELECT id, chat_id, role, content, sql_text, df_json, grounding_json,
                      retried, error, is_follow_up, follow_up_depth, created_at
               FROM messages WHERE chat_id = ? ORDER BY id ASC""",
            (chat_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# DataFrame helpers (used by the UI layer when restoring old messages)
# ---------------------------------------------------------------------------

def df_to_json(df: pd.DataFrame) -> str:
    """Serialize a DataFrame to a JSON string for storage. Returns '' for empty DFs."""
    if df is None or df.empty:
        return ""
    return df.to_json(orient="records", date_format="iso")


# ---------------------------------------------------------------------------
# DataFrame helpers (used by the UI layer when restoring old messages)
# ---------------------------------------------------------------------------

def df_from_json(df_json: str) -> pd.DataFrame:
    """Deserialize a stored JSON string back to a DataFrame. Returns empty DF on falsy input."""
    if not df_json:
        return pd.DataFrame()
    return pd.read_json(StringIO(df_json), orient="records")
