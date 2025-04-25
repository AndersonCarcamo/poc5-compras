from sqlalchemy import Column, Integer, String, DateTime, func
from common.db import Base

class Request(Base):
    __tablename__ = 'requests'
    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(String, index=True, nullable=False)
    product_id = Column(String, index=True, nullable=False)
    quantity = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())