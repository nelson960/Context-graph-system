from __future__ import annotations

import sqlite3
from pathlib import Path
from urllib.parse import quote


def sqlite_readonly_uri(db_path: Path) -> str:
    resolved = db_path.resolve()
    quoted_path = quote(resolved.as_posix(), safe="/")
    return f"file:{quoted_path}?mode=ro"


def connect_readonly_sqlite(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(sqlite_readonly_uri(db_path), uri=True)


def connect_writable_sqlite(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(db_path)
