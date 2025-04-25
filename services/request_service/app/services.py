from sqlalchemy.orm import Session
from sqlalchemy import func
from services.request_service.app.models import Request as RequestModel
from services.request_service.app.schemas import RequestCreate, OrderCreate
from services.request_service.app.events.publisher import publish_order
from common.settings import settings

MIN_THRESHOLD = settings.min_threshold


def create_request(db: Session, req: RequestCreate) -> RequestModel:
    # Guardar el request
    new_req = RequestModel(
        client_id=req.client_id,
        product_id=req.product_id,
        quantity=req.quantity
    )
    db.add(new_req)
    db.commit()
    db.refresh(new_req)

    # Sumar cantidades pendientes por producto
    total = db.query(func.sum(RequestModel.quantity)) \
             .filter(RequestModel.product_id == req.product_id) \
             .scalar() or 0

    if total >= MIN_THRESHOLD:
        # Recoger IDs para encapsular en la orden
        reqs = db.query(RequestModel).filter(RequestModel.product_id == req.product_id).all()
        order_payload = OrderCreate(
            product_id=req.product_id,
            total_quantity=total,
            request_ids=[r.id for r in reqs]
        )
        # Publicar evento y limpiar requests procesados
        publish_order(order_payload)
        db.query(RequestModel).filter(RequestModel.product_id == req.product_id).delete()
        db.commit()

    return new_req