# poc5-compras

- python -m venv venv
- venv\Scripts\activate
- source venv/bin/activate

- python event_broker.py
- python example.py


para levantar request-service (por el momento funciona)

path: ./POC5-COMPRAS
comand: uvicorn services.request_service.app.main:app --host 0.0.0.0 --port 8003 --reloadz

Datos para el .env:

DATABASE_URL=postgresql://request_user:12345@localhost:5432/request_db
RABBIT_HOST=localhost
RABBIT_PORT=5672
RABBIT_USER=guest
RABBIT_PASS=guest
MIN_THRESHOLD=30