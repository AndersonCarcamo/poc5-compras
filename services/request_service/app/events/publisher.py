import json
from common.rabbitmq import rabbitmq
from services.request_service.app.schemas import OrderCreate

EXCHANGE = 'order.events'
ROUTING_KEY = 'order.generated'

def publish_order(order: OrderCreate):
    channel = rabbitmq.get_channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic', durable=True)
    message = json.dumps(order.dict())
    channel.basic_publish(exchange=EXCHANGE, routing_key=ROUTING_KEY, body=message)