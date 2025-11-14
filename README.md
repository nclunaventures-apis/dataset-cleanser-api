# Dataset Cleanser API


This repository contains a FastAPI backend implementing a hybrid metadata storage
(JSON + SQLite) for the Dataset Cleanser API. It's designed to be portable and
ready to deploy to Render (or other cloud providers).


## Features
- Endpoints: `/datasets`, `/latest`, `/search`, `/get/{id}`, `/update`
- Hybrid storage: `database.json` (master) and `database.db` (SQLite cache)
- Automatic JSON → SQLite sync when `update` is called
- CORS enabled
- Lightweight and production-ready for Render deployment


## Files
- `main.py` - FastAPI application
- `models.py` - Pydantic request/response models
- `db_manager.py` - Handles JSON and SQLite sync
- `requirements.txt` - Python dependencies
- `render.yaml` - Optional Render configuration
- `database.json` - (created on first run)
- `database.db` - (created on first run)


## Local development
1. Create virtualenv and activate
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
2. Run locally
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```
3. Test endpoints:
- `GET /datasets`
- `POST /update` with JSON body (see models for schema)


## Deploy to Render (high-level)
1. Push this repo to GitHub
2. Create a Web Service on Render connected to your repo
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`


## Security & Notes
- This backend expects cleaned files to be stored on Google Drive. The API returns
the Drive URLs stored in metadata; it does not host large files.
- For production, consider adding authentication (API key) and HTTPS (Render
provides TLS automatically).
