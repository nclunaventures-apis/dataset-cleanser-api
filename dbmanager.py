# dbmanager.py
import os
import json
import sqlite3
import datetime
import secrets
import asyncio
from typing import List, Dict, Any, Optional

DB_JSON = "datasets.json"
DB_SQLITE = "datasets.db"

# ---------------------------
# JSON helpers
# ---------------------------
def ensure_json_exists():
    if not os.path.exists(DB_JSON):
        with open(DB_JSON, "w", encoding="utf-8") as f:
            json.dump([], f, indent=2)


def read_json() -> List[Dict[str, Any]]:
    ensure_json_exists()
    with open(DB_JSON, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(data: List[Dict[str, Any]]):
    with open(DB_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ---------------------------
# SQLITE Setup & sync
# ---------------------------
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
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS api_keys (
            key TEXT PRIMARY KEY,
            label TEXT,
            created_at TEXT,
            active INTEGER DEFAULT 1,
            quota INTEGER DEFAULT NULL
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS usage_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_key TEXT,
            endpoint TEXT,
            ts INTEGER
        )
        """
    )
    conn.commit()
    conn.close()


def sync_json_to_sqlite():
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
                json.dumps(item.get("columns")) if item.get("columns") is not None else None,
                item.get("description"),
                json.dumps(item.get("tags")) if item.get("tags") is not None else None,
            ),
        )
    conn.commit()
    conn.close()


# ---------------------------
# UPSERT / CRUD
# ---------------------------
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


def query_all() -> List[Dict[str, Any]]:
    return read_json()


def query_latest(limit: int = 1) -> List[Dict[str, Any]]:
    items = read_json()
    items_sorted = sorted(items, key=lambda x: x.get("updated") or "", reverse=True)
    return items_sorted[:limit]


def search_sqlite(keyword: str, limit: int = 50) -> List[Dict[str, Any]]:
    init_sqlite()
    conn = sqlite3.connect(DB_SQLITE)
    c = conn.cursor()
    pattern = f"%{keyword}%"
    c.execute(
        "SELECT id,name,url,updated,rows,columns,description,tags FROM datasets WHERE name LIKE ? OR description LIKE ? OR tags LIKE ? LIMIT ?",
        (pattern, pattern, pattern, limit),
    )
    rows = c.fetchall()
    conn.close()
    results = []
    for r in rows:
        cols = json.loads(r[5]) if r[5] else None
        tags = json.loads(r[7]) if r[7] else None
        results.append(
            {
                "id": r[0],
                "name": r[1],
                "url": r[2],
                "updated": r[3],
                "rows": r[4],
                "columns": cols,
                "description": r[6],
                "tags": tags,
            }
        )
    return results


# ---------------------------
# API key management
# ---------------------------
def create_api_key(label: Optional[str] = None, quota: Optional[int] = None) -> str:
    init_sqlite()
    k = secrets.token_urlsafe(32)
    now = datetime.datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB_SQLITE)
    c = conn.cursor()
    c.execute(
        "INSERT INTO api_keys (key, label, created_at, active, quota) VALUES (?, ?, ?, ?, ?)",
        (k, label or "", now, 1, quota),
    )
    conn.commit()
    conn.close()
    return k


def validate_api_key(key: Optional[str]) -> bool:
    if not key:
        return False
    init_sqlite()
    conn = sqlite3.connect(DB_SQLITE)
    c = conn.cursor()
    c.execute("SELECT active FROM api_keys WHERE key = ?", (key,))
    row = c.fetchone()
    conn.close()
    return bool(row and row[0] == 1)


def deactivate_api_key(key: str):
    init_sqlite()
    conn = sqlite3.connect(DB_SQLITE)
    c = conn.cursor()
    c.execute("UPDATE api_keys SET active = 0 WHERE key = ?", (key,))
    conn.commit()
    conn.close()


# ---------------------------
# Usage logging (sync + async)
# ---------------------------
def log_usage(api_key: str, endpoint: str):
    try:
        init_sqlite()
        ts = int(time.time())
        conn = sqlite3.connect(DB_SQLITE)
        c = conn.cursor()
        c.execute("INSERT INTO usage_logs (api_key, endpoint, ts) VALUES (?, ?, ?)", (api_key, endpoint, ts))
        conn.commit()
        conn.close()
    except Exception:
        pass


async def log_usage_async(api_key: str, endpoint: str):
    # run sync log in threadpool to avoid blocking
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, log_usage, api_key, endpoint)
