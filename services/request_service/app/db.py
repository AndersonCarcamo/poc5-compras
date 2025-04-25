import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from common.db import engine, SessionLocal, Base

# Al iniciar la app, crea tablas si no existen
def init_db():
    Base.metadata.create_all(bind=engine)