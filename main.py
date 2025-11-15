from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
import datetime
import os
from typing import List

from models import DatasetMeta, UpdatePayload
import dbmanager as db_manager


# PORT for Render
API_PORT = int(os.environ.get("PORT", 8000))

app = FastAPI(title="Dataset Cleanser API")


# ============================================
# CORS
# ============================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# STARTUP
# ============================================
@app.on_event("startup")
def startup_event():
    db_manager.ensure_json_exists()
    db_manager.init_sqlite()
    db_manager.sync_json_to_sqlite()


# ============================================
# STATIC FILES (logo)
# ============================================
@app.get("/static/{filename}")
def get_static(filename: str):
    static_path = os.path.join(os.path.dirname(__file__), "static", filename)
    if os.path.exists(static_path):
        return FileResponse(static_path)
    raise HTTPException(status_code=404, detail="Static file not found")


# ============================================
# HOMEPAGE ROUTE
# ============================================
@app.get("/", response_class=HTMLResponse)
def homepage():
    index_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(index_path):
        with open(index_path, "r") as f:
            return f.read()
    return "<h1>Homepage file not found</h1>"


# ============================================
# API ROUTES
# ============================================
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


@app.get("/search", response_model=List[DatasetMeta])
def search(keyword: str, limit: int = 50):
    return db_manager.search_sqlite(keyword, limit)


@app.post("/update")
def update(payload: UpdatePayload):
    if not payload.updated:
        payload.updated = datetime.datetime.utcnow().isoformat()

    item = payload.dict()
    db_manager.upsert_item(item)
    return {"status": "ok", "id": item.get("id")}


# ============================================
# RUN LOCALLY
# ============================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=API_PORT, reload=True)
