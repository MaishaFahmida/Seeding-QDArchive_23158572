import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

from config import DB_PATH

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- required fields (per professor expectations)
    file_url TEXT NOT NULL UNIQUE,
    download_timestamp TEXT NOT NULL,
    local_directory TEXT NOT NULL,
    local_filename TEXT NOT NULL,

    -- useful fields for later classification / dedup / analysis
    source_name TEXT,
    dataset_url TEXT,
    record_id TEXT,
    title TEXT,
    description TEXT,
    doi TEXT,
    license TEXT,

    file_key TEXT,
    file_type TEXT,
    size_bytes INTEGER,
    sha256 TEXT
);
"""

def connect() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)

def init_db() -> None:
    with connect() as con:
        con.execute(CREATE_TABLE_SQL)
        con.commit()

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def insert_file_row(row: Dict[str, Any]) -> None:
    """
    Insert one downloaded file row (one row per file).
    Uses INSERT OR IGNORE to avoid duplicates by file_url.
    """
    cols = list(row.keys())
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT OR IGNORE INTO files ({','.join(cols)}) VALUES ({placeholders})"
    with connect() as con:
        con.execute(sql, [row[c] for c in cols])
        con.commit()