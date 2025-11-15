# models.py
from typing import List, Optional
from pydantic import BaseModel, HttpUrl


class DatasetMeta(BaseModel):
    id: str
    name: str
    url: HttpUrl
    updated: Optional[str] = None
    rows: Optional[int] = None
    columns: Optional[List[str]] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = []


class UpdatePayload(BaseModel):
    id: str
    name: str
    url: HttpUrl
    updated: Optional[str] = None
    rows: Optional[int] = None
    columns: Optional[List[str]] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = []
