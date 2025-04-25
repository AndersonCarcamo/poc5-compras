from pydantic import BaseModel
from typing import List
from datetime import datetime

class RequestCreate(BaseModel):
    client_id: str
    product_id: str
    quantity: int

class RequestOut(RequestCreate):
    id: int
    created_at: datetime

    class Config:
        orm_mode = True

class OrderCreate(BaseModel):
    product_id: str
    total_quantity: int
    request_ids: List[int]