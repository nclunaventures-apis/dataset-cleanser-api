# main.py
import os
import time
import json
import asyncio
from collections import defaultdict
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from models import DatasetMeta, UpdatePayload
import dbmanager as db_manager

# Optional Redis support (only used if REDIS_URL env var is set)
try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

# CONFIG
API_PORT = int(os.environ.get("PORT", 8000))
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "")  # set this in Render before enabling admin endpoints
REDIS_URL = os.environ.get("REDIS_URL")  # optional
RATE_LIMIT = int(os.environ.get("RATE_LIMIT", 120))  # requests per window
RATE_WINDOW = int(os.environ.get("RATE_WINDOW", 60))  # window seconds

app = FastAPI(title="Dataset Cleanser API — Production Pack")

# CORS - tighten in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static folder
app.mount("/static", StaticFiles(directory="static"), name="static")


# ------------------------------------------
# STARTUP
# ------------------------------------------
@app.on_event("startup")
def startup_event():
    db_manager.ensure_json_exists()
    db_manager.init_sqlite()         # creates tables (datasets, api_keys, usage_logs)
    db_manager.sync_json_to_sqlite()
    # Try connecting to Redis if configured
    if REDIS_URL and aioredis:
        try:
            app.state.redis = aioredis.from_url(REDIS_URL)
        except Exception as e:
            app.state.redis = None
    else:
        app.state.redis = None
    # in-memory rate limiter store for non-redis fallback
    app.state._rate_store = defaultdict(list)


# ------------------------------------------
# Helper: serve static index/dashboard
# ------------------------------------------
def read_static_file(path_parts):
    path = os.path.join(os.path.dirname(__file__), *path_parts)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    return None


@app.get("/", response_class=HTMLResponse)
def homepage():
    content = read_static_file(("static", "index.html"))
    if content:
        return HTMLResponse(content)
    return HTMLResponse("<h1>Index not found</h1>", status_code=404)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    content = read_static_file(("static", "dashboard.html"))
    if content:
        return HTMLResponse(content)
    return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)


@app.get("/docs-ui", response_class=HTMLResponse)
def docs_ui():
    return """
    <html><head><meta name="viewport" content="width=device-width,initial-scale=1"></head>
    <body style="font-family:Inter,Arial;padding:20px;background:#071827;color:#eaf6ff">
      <a href="/"><img src="/static/logo.png" style="width:72px"/></a>
      <h1>Dataset Cleanser API — Docs</h1>
      <p><a href="/docs">Swagger UI</a> • <a href="/redoc">ReDoc</a></p>
      <p>Protected endpoints require header <code>X-API-Key: &lt;your_key&gt;</code></p>
    </body></html>
    """


# ------------------------------------------
# Rate limiting (Redis optional; else in-memory)
# ------------------------------------------
async def _redis_rate_check(redis_client, key: str) -> bool:
    # Returns True if limited
    try:
        p = redis_client.pipeline()
        p.incr(key)
        p.expire(key, RATE_WINDOW)
        res = await p.execute()
        count = int(res[0])
        return count > RATE_LIMIT
    except Exception:
        return False


def _mem_rate_check(store, client_id: str) -> bool:
    now = time.time()
    hits = store[client_id]
    # purge old
    while hits and hits[0] <= now - RATE_WINDOW:
        hits.pop(0)
    if len(hits) >= RATE_LIMIT:
        return True
    hits.append(now)
    return False


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # allow docs and static without rate limit
    if request.url.path.startswith("/static") or request.url.path in ("/docs", "/redoc", "/openapi.json", "/docs-ui"):
        return await call_next(request)

    client_ip = request.client.host if request.client else "unknown"
    api_key = request.headers.get("X-API-Key")
    client_id = api_key or client_ip

    # prefer redis if available
    redis_client = getattr(app.state, "redis", None)
    limited = False
    if redis_client:
        key = f"rate:{client_id}"
        limited = await _redis_rate_check(redis_client, key)
    else:
        limited = _mem_rate_check(app.state._rate_store, client_id)

    if limited:
        return JSONResponse({"detail": "rate limit exceeded"}, status_code=429)

    # log usage if api_key present (async-friendly)
    if api_key:
        try:
            # don't block request; schedule background logging
            asyncio.create_task(db_manager.log_usage_async(api_key, request.url.path))
        except Exception:
            pass

    return await call_next(request)


# ------------------------------------------
# API key dependency
# ------------------------------------------
def require_api_key(request: Request):
    key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if not db_manager.validate_api_key(key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return key


# ------------------------------------------
# Health / status / stats
# ------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": int(time.time())}


@app.get("/status")
def status():
    ok = True
    try:
        db_manager.init_sqlite()
    except Exception:
        ok = False
    return {"service": "dataset-cleanser-api", "healthy": ok, "time": int(time.time())}


@app.get("/stats")
def stats():
    items = db_manager.query_all()
    count = len(items)
    last = db_manager.query_latest(1)
    last_updated = last[0]["updated"] if last else None
    tags = {}
    for it in items:
        for t in (it.get("tags") or []):
            tags[t] = tags.get(t, 0) + 1
    return {"count": count, "last_updated": last_updated, "tag_counts": tags}


# ------------------------------------------
# API endpoints (some protected)
# ------------------------------------------
@app.get("/datasets", response_model=List[DatasetMeta])
def get_datasets():
    return db_manager.query_all()


@app.get("/latest", response_model=List[DatasetMeta])
def get_latest():
    return db_manager.query_latest(1)


@app.get("/get/{dataset_id}")
def get_dataset(dataset_id: str):
    items = db_manager.query_all()
    for it in items:
        if it.get("id") == dataset_id:
            return it
    raise HTTPException(status_code=404, detail="Dataset not found")


@app.get("/search", response_model=List[DatasetMeta], dependencies=[])
def search(keyword: str, limit: int = 50):
    # public search (optionally require API key — adjust if you want)
    return db_manager.search_sqlite(keyword, limit)


@app.post("/update", dependencies=[Depends(require_api_key)])
def update(payload: UpdatePayload):
    if not payload.updated:
        payload.updated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    item = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
    db_manager.upsert_item(item)
    return {"status": "ok", "id": item.get("id")}


# ------------------------------------------
# Admin: create key (protected by ADMIN_SECRET)
# ------------------------------------------
@app.post("/admin/create_key")
def admin_create_key(label: Optional[str] = None, quota: Optional[int] = None, secret: Optional[str] = None):
    # Two-layer protection: ADMIN_SECRET env var must be set; caller must provide ?secret=<ADMIN_SECRET>
    if not ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Admin endpoints disabled (no ADMIN_SECRET set)")
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="Invalid admin secret")
    key = db_manager.create_api_key(label=label, quota=quota)
    return {"key": key}


# ------------------------------------------
# Static file fallback (for logo, etc)
# ------------------------------------------
@app.get("/static-file/{path:path}")
def static_file(path: str):
    static_path = os.path.join(os.path.dirname(__file__), "static", path)
    if os.path.exists(static_path):
        return FileResponse(static_path)
    raise HTTPException(status_code=404, detail="Not found")


# ------------------------------------------
# LOCAL DEV ENTRYPOINT
# ------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=API_PORT, reload=True)
