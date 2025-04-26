from sqlalchemy.orm import Session
from services.provider_service.app.models import ProviderOrder
from services.provider_service.app.schemas import ProviderOrderCreate, StockReserved
from services.provider_service.app.events.publisher import publish_stock_reserved
from datetime import datetime

def assign_vendor(items: list) -> str:
    # Asigna un proveedor basado en los items
    return items[0]["supplier_id"]  # Lógica simplificada

def process_provider_order(order_data: ProviderOrderCreate, db: Session):
    # Guarda la orden en la base de datos (Debe persistir esta información)
    db_order = ProviderOrder(
        order_id=order_data.order_id,
        vendor_id=order_data.vendor_id,
        items=order_data.items,
        status="reserved" # campo de estado para monitorizar el estado de la orden/pedido
    )
    db.add(db_order)
    db.commit()

    # 2. Publicar evento de stock reservado
    reserved_payload = StockReserved(
        order_id=order_data.order_id,
        reserved_items=order_data.items
    )
    publish_stock_reserved(reserved_payload)

    return db_order