from sqlalchemy import Column, Integer, String, DateTime, func, JSON
from common.db import Base

class ProviderOrder(Base):
    __tablename__ = 'provider_orders'
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, index=True, nullable=False)  # id de orden recibida
    vendor_id = Column(String, index=True, nullable=False)  # proveedor asignado
    items = Column(JSON, nullable=False)  
    status = Column(String, default="pending")  
    created_at = Column(DateTime(timezone=True), server_default=func.now())