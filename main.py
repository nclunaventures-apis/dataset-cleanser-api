from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import datetime
import os

from typing import List
from models import DatasetMeta, UpdatePayload
import dbmanager as db_manager


# Render gives you PORT through env
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
# STARTUP EVENT
# ============================================
@app.on_event("startup")
def startup_event():
    db_manager.ensure_json_exists()
    db_manager.init_sqlite()
    db_manager.sync_json_to_sqlite()


# ============================================
# ROUTES
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
# LOCAL DEV ENTRYPOINT
# ============================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=API_PORT, reload=True)
