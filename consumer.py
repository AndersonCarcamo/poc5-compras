#!/usr/bin/env python
import pika
import json
import uuid
import threading
import time
from typing import Callable, Dict, Any

class EventConsumer:
    def __init__(self, rabbitmq_host='localhost', rabbitmq_port=5672, consumer_id=None):
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.consumer_id = consumer_id or f"consumer-{uuid.uuid4().hex[:6]}"
        self.connection = None
        self.channel = None
        self.message_handlers: Dict[str, Callable[[Dict[str, Any]], None]] = {}
        self.running = False
        self.connect()

    def connect(self):
        """Establece conexión con RabbitMQ."""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.rabbitmq_host, port=self.rabbitmq_port)
            )
            self.channel = self.connection.channel()
            print(f"[{self.consumer_id}] Conectado a RabbitMQ")
        except Exception as e:
            print(f"[{self.consumer_id}] Error de conexión: {str(e)}")
            raise

    def register_handler(self, queue_name: str, handler: Callable[[Dict[str, Any]], None]):
        """Registra un manejador para una cola."""
        if not callable(handler):
            raise ValueError("El manejador debe ser una función.")
        self.message_handlers[queue_name] = handler
        self._setup_queue(queue_name)
        print(f"[{self.consumer_id}] Manejador registrado para '{queue_name}'")

    def _setup_queue(self, queue_name: str):
        """Declara una cola y la vincula al exchange."""
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.queue_bind(
            queue=queue_name,
            exchange='amq.topic',
            routing_key=queue_name
        )

    def _message_callback(self, ch, method, properties, body):
        """Callback interno para procesar mensajes."""
        queue_name = method.routing_key
        try:
            message = json.loads(body)
            print(f"[{self.consumer_id}] Mensaje recibido en '{queue_name}': {message}")
            
            if queue_name in self.message_handlers:
                self.message_handlers[queue_name](message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                print(f"[{self.consumer_id}] No hay manejador para '{queue_name}'")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except json.JSONDecodeError:
            print(f"[{self.consumer_id}] Mensaje no es JSON válido")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            print(f"[{self.consumer_id}] Error procesando mensaje: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def start(self):
        """Inicia el consumo de mensajes."""
        if not self.running:
            self.running = True
            for queue_name in self.message_handlers:
                self.channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=self._message_callback,
                    auto_ack=False
                )
            print(f"[{self.consumer_id}] Escuchando mensajes...")
            threading.Thread(target=self.channel.start_consuming, daemon=True).start()

    def stop(self):
        """Detiene el consumidor."""
        if self.running:
            self.running = False
            self.channel.stop_consuming()
            if self.connection and self.connection.is_open:
                self.connection.close()
            print(f"[{self.consumer_id}] Detenido")