from fastapi import FastAPI, HTTPException, Request
import datetime


API_PORT = int(os.environ.get("PORT", 8000)) if 'os' in globals() else 8000


app = FastAPI(title="Dataset Cleanser API")


# CORS
app.add_middleware(
CORSMiddleware,
allow_origins=["*"],
allow_credentials=True,
allow_methods=["*"],
allow_headers=["*"],
)




@app.on_event("startup")
def startup_event():
db_manager.ensure_json_exists()
db_manager.init_sqlite()
# Make sure sqlite cache is in sync
db_manager.sync_json_to_sqlite()




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
results = db_manager.search_sqlite(keyword, limit)
return results




@app.post("/update")
def update(payload: UpdatePayload):
# Ensure payload has updated timestamp
if not payload.updated:
payload.updated = datetime.datetime.utcnow().isoformat()
item = payload.dict()
db_manager.upsert_item(item)
return {"status": "ok", "id": item.get("id")}




if __name__ == "__main__":
uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
