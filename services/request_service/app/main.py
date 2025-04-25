from fastapi import FastAPI
from services.request_service.app.db import init_db, SessionLocal
from services.request_service.app.routers import router

# Crear tablas al iniciar
def startup():
    init_db()

app = FastAPI(title="Request Service")
app.add_event_handler("startup", startup)
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("request_service.app.main:app", host="0.0.0.0", port=8003, reload=True)
