from pydantic import BaseModel
from typing import List, Dict
from datetime import datetime

class OrderItem(BaseModel):
    product_id: str
    quantity: int
    supplier_id: str  # id de proveedor
    
class ProviderOrderCreate(BaseModel):
    order_id: str
    vendor_id: str = None  # (se asigna automáticamente si es None)
    items: List[OrderItem]  # lista de OrderItem

class ProviderOrderOut(ProviderOrderCreate):
    id: int
    status: str
    created_at: datetime

    class Config:
        orm_mode = True

class StockReserved(BaseModel):
    order_id: str
    reserved_items: List[Dict[str, int]]