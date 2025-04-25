import pika
from common.settings import settings

class RabbitMQ:
    def __init__(self):
        creds = pika.PlainCredentials(settings.rabbit_user, settings.rabbit_pass)
        params = pika.ConnectionParameters(
            host=settings.rabbit_host,
            port=settings.rabbit_port,
            credentials=creds
        )
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

    def get_channel(self):
        return self.channel

rabbitmq = RabbitMQ()