from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import threading
from . import models
from services.provider_service.app.handlers import handle_provider_order
from services.provider_service.app.db import init_db, get_db
from consumer import EventConsumer

def startup():
    init_db()
    # Inicia RabbitMQ 
    consumer = EventConsumer()
    consumer.register_handler("provider_orders", handle_provider_order)
    threading.Thread(target=consumer.start, daemon=True).start()

app = FastAPI(title="Provider Service")
app.add_event_handler("startup", startup)

# endpoint para recibir ordenes de proveedor 
@app.get("/orders")
def list_orders(db: Session = Depends(get_db)):
    return db.query(models.ProviderOrder).all()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8004, reload=True)