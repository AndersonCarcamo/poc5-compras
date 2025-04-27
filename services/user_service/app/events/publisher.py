import json
from common.rabbitmq import rabbitmq
from services.user_service.app.schemas import UserEvent

EXCHANGE = 'user.events'
ROUTING_KEY = 'user.{event_type}'  # user.created, user.updated, etc.

def publish_user_event(event: UserEvent):
    channel = rabbitmq.get_channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic', durable=True)
    
    routing_key = ROUTING_KEY.format(event_type=event.event_type)
    message = json.dumps(event.dict())
    
    channel.basic_publish(exchange=EXCHANGE, routing_key=routing_key, body=message)
    print(f"Evento publicado: {routing_key}")