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
# Sync only the changed record to SQLite
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
