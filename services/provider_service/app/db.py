import sys
import os
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from common.db import engine, SessionLocal, Base

from . import models

def init_db():
    """Crea las tablas en la BD al iniciar el servicio."""
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()