from sqlalchemy.orm import Session
from services.provider_service.app.models import ProviderOrder
from services.provider_service.app.schemas import ProviderOrderCreate, StockReserved
from services.provider_service.app.events.publisher import publish_stock_reserved
from datetime import datetime

def assign_vendor(items: list) -> str:
    # Asigna un proveedor basado en los items
    return items[0]["supplier_id"]

def process_provider_order(order_data: ProviderOrderCreate, db: Session):
    # asigno proveedor si no viene en el payload
    if not order_data.vendor_id:
        order_data.vendor_id = assign_vendor(order_data.items)
        
    items_json = [item.dict() for item in order_data.items]
    
    # Guarda la orden en la base de datos
    db_order = ProviderOrder(
        order_id=order_data.order_id,
        vendor_id=order_data.vendor_id,
        items=items_json,
        status="reserved" # estado de la orden/pedido
    )
    db.add(db_order)
    db.commit()

    # evento de stock reservado
    reserved_payload = StockReserved(
        order_id=order_data.order_id,
        reserved_items=items_json
    )
    publish_stock_reserved(reserved_payload)

    return db_order