from sqlalchemy.orm import Session
from passlib.context import CryptContext
from services.user_service.app.models import User
from services.user_service.app.schemas import UserCreate, UserUpdate, UserEvent
from services.user_service.app.events.publisher import publish_user_event

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_password_hash(password):
    return pwd_context.hash(password)

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def create_user(db: Session, user: UserCreate):
    hashed_password = get_password_hash(user.password)
    db_user = User(
        username=user.username,
        email=user.email,
        first_name=user.first_name,
        last_name=user.last_name,
        hashed_password=hashed_password
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    
    user_event = UserEvent(
        user_id=db_user.id,
        username=db_user.username,
        email=db_user.email,
        event_type="created",
        hashed_password=hashed_password  
    )
    # publish_user_event(user_event)
    
    return db_user

def get_user(db: Session, user_id: int):
    return db.query(User).filter(User.id == user_id).first()

def get_user_by_username(db: Session, username: str):
    return db.query(User).filter(User.username == username).first()

def get_user_by_email(db: Session, email: str):
    return db.query(User).filter(User.email == email).first()

def update_user(db: Session, user_id: int, user_data: UserUpdate):
    db_user = get_user(db, user_id)
    if not db_user:
        return None
    
    update_data = user_data.dict(exclude_unset=True)
    if 'password' in update_data:
        update_data['hashed_password'] = get_password_hash(update_data.pop('password'))
    
    for key, value in update_data.items():
        setattr(db_user, key, value)
    
    db.commit()
    db.refresh(db_user)
    
    user_event = UserEvent(
        user_id=db_user.id,
        username=db_user.username,
        email=db_user.email,
        event_type="updated"
    )
    # publish_user_event(user_event)
    
    return db_user