from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from services.request_service.app import db as db_module
from services.request_service.app.schemas import RequestCreate, RequestOut
from services.request_service.app.services import create_request

router = APIRouter()

def get_db():
    session = db_module.SessionLocal()
    try:
        yield session
    finally:
        session.close()

@router.post("/request", response_model=RequestOut)
def post_request(request_in: RequestCreate, db: Session = Depends(get_db)):
    return create_request(db, request_in)