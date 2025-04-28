import json
import pika
from common.rabbitmq import rabbitmq
from services.provider_service.app.schemas import StockReserved

EXCHANGE = 'provider.events'
ROUTING_KEY = 'stock.reserved'

def publish_stock_reserved(stock_data: StockReserved):
    # channel = rabbitmq.get_channel()
    # channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic', durable=True)
    # message = json.dumps(stock_data.dict())
    # channel.basic_publish(exchange=EXCHANGE, routing_key=ROUTING_KEY, body=message)
    try:
        channel = rabbitmq.get_channel()
        channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic', durable=True)
        message = json.dumps(stock_data.dict())
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)  # Mensaje persistente
        )
    except Exception as e:
        print(f"Error publicando evento: {str(e)}")
        raise