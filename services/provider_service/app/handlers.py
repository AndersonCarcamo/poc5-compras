import sys
import os
from sqlalchemy.orm import Session

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from common.db import SessionLocal
from schemas import ProviderOrderCreate
from services import process_provider_order

def handle_provider_order(message: dict):
    db = SessionLocal()
    try:
        order_data = ProviderOrderCreate(**message)
        process_provider_order(db, order_data)
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"Error procesando orden: {str(e)}")
        raise
    finally:
        db.close()