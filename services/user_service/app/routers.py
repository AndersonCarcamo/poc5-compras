from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from services.user_service.app import db as db_module
from services.user_service.app.schemas import UserCreate, UserOut, UserUpdate
from services.user_service.app.services import create_user, get_user, get_user_by_username, get_user_by_email, update_user

router = APIRouter()

def get_db():
    session = db_module.SessionLocal()
    try:
        yield session
    finally:
        session.close()

@router.post("/users", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def register_user(user: UserCreate, db: Session = Depends(get_db)):
    db_user = get_user_by_username(db, user.username)
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    db_user = get_user_by_email(db, user.email)
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    return create_user(db, user)

@router.get("/users/{user_id}", response_model=UserOut)
def read_user(user_id: int, db: Session = Depends(get_db)):
    db_user = get_user(db, user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user

@router.get("/users/by-username/{username}", response_model=UserOut)
def read_user_by_username(username: str, db: Session = Depends(get_db)):
    db_user = get_user_by_username(db, username)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user

@router.patch("/users/{user_id}", response_model=UserOut)
def update_user_endpoint(user_id: int, user_data: UserUpdate, db: Session = Depends(get_db)):
    db_user = update_user(db, user_id, user_data)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user