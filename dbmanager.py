import json
import os
import sqlite3
from typing import List, Dict, Any

DB_JSON = "datasets.json"
DB_SQLITE = "datasets.db"


# --------------------------------------
# JSON UTILITIES
# --------------------------------------
def ensure_json_exists():
    """Create JSON file if missing."""
    if not os.path.exists(DB_JSON):
        with open(DB_JSON, "w") as f:
            json.dump([], f)


def read_json() -> List[Dict[str, Any]]:
    ensure_json_exists()
    with open(DB_JSON, "r") as f:
        return json.load(f)


def write_json(data: List[Dict[str, Any]]):
    with open(DB_JSON, "w") as f:
        json.dump(data, f, indent=2)


# --------------------------------------
# SQLITE SETUP
# --------------------------------------
def init_sqlite():
    conn = sqlite3.connect(DB_SQLITE)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS datasets (
            id TEXT PRIMARY KEY,
            name TEXT,
            url TEXT,
            updated TEXT,
            rows INTEGER,
            columns TEXT,
            description TEXT,
            tags TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def sync_json_to_sqlite():
    """Push JSON data to SQLite (upsert for each record)."""
    init_sqlite()
    items = read_json()

    conn = sqlite3.connect(DB_SQLITE)
    c = conn.cursor()

    for item in items:
        c.execute(
            """
            INSERT INTO datasets (id, name, url, updated, rows, columns, description, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                url=excluded.url,
                updated=excluded.updated,
                rows=excluded.rows,
                columns=excluded.columns,
                description=excluded.description,
                tags=excluded.tags
            """,
            (
                item.get("id"),
                item.get("name"),
                item.get("url"),
                item.get("updated"),
                item.get("rows"),
                json.dumps(item.get("columns")),
                item.get("description"),
                json.dumps(item.get("tags")),
            ),
        )

    conn.commit()
    conn.close()


# --------------------------------------
# UPSERT
# --------------------------------------
def upsert_item(item: Dict[str, Any]):
    items = read_json()

    found = False
    for i, it in enumerate(items):
        if it.get("id") == item.get("id"):
            items[i] = item
            found = True
            break

    if not found:
        items.append(item)

    write_json(items)
    sync_json_to_sqlite()


# --------------------------------------
# QUERY FUNCTIONS
# --------------------------------------
def query_all() -> List[Dict[str, Any]]:
    return read_json()


def query_latest(_
