from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from . import schemas, services
from services.provider_service.app.db import get_db

router = APIRouter(prefix="/provider-order", tags=["provider"])

@router.post("/", response_model=schemas.ProviderOrderOut)
def create_order(order: schemas.ProviderOrderCreate, db: Session = Depends(get_db)):
    return services.create_provider_order(db, order)