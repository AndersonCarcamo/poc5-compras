#!/usr/bin/env python
import pika
import json
import uuid
import requests
import threading
import time
import sys
import os
import signal
from datetime import datetime

class EventConsumer:
    def __init__(self, rabbitmq_host='localhost', rabbitmq_port=5672, 
                 broker_api='http://localhost:5000', consumer_id=None):
        """
        Inicializa un consumidor de eventos usando RabbitMQ
        
        Args:
            rabbitmq_host: Host donde se ejecuta RabbitMQ
            rabbitmq_port: Puerto de RabbitMQ
            broker_api: URL de la API del broker
            consumer_id: Identificador único del consumidor
        """
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.broker_api = broker_api
        self.consumer_id = consumer_id or f"consumer-{str(uuid.uuid4())[:8]}"
        
        self.connection = None
        self.channel = None
        self.message_handlers = {}
        self.subscriptions = {}
        self.running = False
        
        # Iniciar conexión
        self.connect()
    
    def connect(self):
        """Establece conexión con RabbitMQ"""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.rabbitmq_host, port=self.rabbitmq_port)
            )
            self.channel = self.connection.channel()
            print(f"Consumidor {self.consumer_id} conectado a RabbitMQ en {self.rabbitmq_host}:{self.rabbitmq_port}")
            return True
        except Exception as e:
            print(f"Error al conectar a RabbitMQ: {str(e)}")
            return False
    
    def ensure_queue(self, queue_name, durable=True):
        """
        Asegura que exista una cola, creándola si es necesario
        
        Args:
            queue_name: Nombre de la cola
            durable: Si la cola debe persistir después de reiniciar el broker
        
        Returns:
            True si se creó o ya existía
        """
        try:
            # Intentar usar la API del broker primero
            response = requests.post(
                f"{self.broker_api}/queues",
                json={
                    "queueName": queue_name,
                    "durable": durable
                }
            )
            
            if response.status_code in [201, 200]:
                print(f'Cola "{queue_name}" verificada/creada mediante API')
                return True
                
            # Si falla, intentar directamente con RabbitMQ
            self.channel.queue_declare(queue=queue_name, durable=durable)
            print(f'Cola "{queue_name}" declarada directamente')
            return True
        except Exception as e:
            print(f'Error al asegurar cola "{queue_name}": {str(e)}')
            return False
    
    def ensure_binding(self, queue_name, exchange_name, routing_key=''):
        """
        Asegura que exista un binding entre una cola y un exchange
        
        Args:
            queue_name: Nombre de la cola
            exchange_name: Nombre del exchange
            routing_key: Clave de routing
        
        Returns:
            True si se creó o ya existía
        """
        try:
            # Intentar usar la API del broker primero
            response = requests.post(
                f"{self.broker_api}/bindings",
                json={
                    "queueName": queue_name,
                    "exchangeName": exchange_name,
                    "routingKey": routing_key
                }
            )
            
            if response.status_code in [201, 200]:
                print(f'Binding cola "{queue_name}" a exchange "{exchange_name}" verificado/creado mediante API')
                return True
                
            # Si falla, intentar directamente con RabbitMQ
            self.channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key
            )
            print(f'Binding cola "{queue_name}" a exchange "{exchange_name}" creado directamente')
            return True
        except Exception as e:
            print(f'Error al asegurar binding: {str(e)}')
            return False
    
    def register_handler(self, queue_name, handler):
        """
        Registra un manejador para una cola específica
        
        Args:
            queue_name: Nombre de la cola
            handler: Función que procesará los mensajes (recibe el mensaje como argumento)
        """
        if not callable(handler):
            raise ValueError("El manejador debe ser una función callable")
        
        self.message_handlers[queue_name] = handler
        print(f'Manejador registrado para cola "{queue_name}"')
    
    def subscribe(self, queue_name, exchange_name='default', routing_key='#', auto_ack=True):
        """
        Suscribe el consumidor a una cola
        
        Args:
            queue_name: Nombre de la cola
            exchange_name: Nombre del exchange al que se vinculará la cola
            routing_key: Clave de routing para la vinculación
            auto_ack: Si se debe hacer reconocimiento automático de mensajes
        
        Returns:
            True si se suscribió correctamente
        """
        try:
            # Asegurar que la cola exista
            self.ensure_queue(queue_name)
            
            # Asegurar que exista el binding
            self.ensure_binding(queue_name, exchange_name, routing_key)
            
            # Registrar la suscripción
            self.subscriptions[queue_name] = {
                'exchange': exchange_name,
                'routing_key': routing_key,
                'auto_ack': auto_ack,
                'consumer_tag': None
            }
            
            print(f'Consumidor {self.consumer_id} suscrito a cola "{queue_name}" (exchange: {exchange_name}, routing_key: {routing_key})')
            
            # Si ya estamos ejecutando, iniciar consumo inmediatamente
            if self.running:
                self.start_consuming(queue_name)
            
            return True
        except Exception as e:
            print(f'Error al suscribirse a cola "{queue_name}": {str(e)}')
            return False
    
    def start_consuming(self, queue_name):
        """
        Inicia el consumo de mensajes de una cola específica
        
        Args:
            queue_name: Nombre de la cola
        """
        if queue_name not in self.subscriptions:
            print(f'Error: No hay suscripción para cola "{queue_name}"')
            return False
        
        # Verificar si ya hay un consumidor activo para esta cola
        if self.subscriptions[queue_name].get('consumer_tag'):
            print(f'Ya hay un consumidor activo para cola "{queue_name}"')
            return True
        
        subscription = self.subscriptions[queue_name]
        auto_ack = subscription['auto_ack']
        
        # Callback para procesar mensajes
        def message_callback(ch, method, properties, body):
            try:
                # Decodificar el mensaje
                message = json.loads(body)
                
                print(f'Mensaje recibido en cola "{queue_name}": {properties.message_id}')
                
                # Procesar el mensaje con el manejador registrado
                handler = self.message_handlers.get(queue_name)
                if handler:
                    result = handler(message)
                    print(f'Mensaje {properties.message_id} procesado')
                else:
                    print(f'No hay manejador registrado para cola "{queue_name}"')
                
                # Confirmar manualmente si auto_ack es False
                if not auto_ack:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                print(f'Error al procesar mensaje: {str(e)}')
                
                # En caso de error, rechazar el mensaje
                if not auto_ack:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        # Configurar el consumidor
        consumer_tag = self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=message_callback,
            auto_ack=auto_ack
        )
        
        # Guardar el consumer_tag
        self.subscriptions[queue_name]['consumer_tag'] = consumer_tag
        
        print(f'Consumidor iniciado para cola "{queue_name}" (consumer_tag: {consumer_tag})')
        return True
    
    def stop_consuming(self, queue_name=None):
        """
        Detiene el consumo de mensajes
        
        Args:
            queue_name: Nombre de la cola. Si es None, detiene todas las colas.
        """
        if queue_name:
            # Detener consumo de una cola específica
            if queue_name in self.subscriptions and self.subscriptions[queue_name].get('consumer_tag'):
                consumer_tag = self.subscriptions[queue_name]['consumer_tag']
                self.channel.basic_cancel(consumer_tag)
                self.subscriptions[queue_name]['consumer_tag'] = None
                print(f'Consumo detenido para cola "{queue_name}"')
        else:
            # Detener consumo de todas las colas
            for queue, subscription in self.subscriptions.items():
                if subscription.get('consumer_tag'):
                    self.channel.basic_cancel(subscription['consumer_tag'])
                    subscription['consumer_tag'] = None
            
            print('Consumo detenido para todas las colas')
    
    def start(self):
        """Inicia el consumidor y comienza a recibir mensajes"""
        self.running = True
        
        # Iniciar consumo para todas las suscripciones
        for queue_name in self.subscriptions:
            self.start_consuming(queue_name)
        
        # Iniciar consumo en un hilo separado
        def consuming_thread():
            try:
                print(f'Consumidor {self.consumer_id} iniciado. Esperando mensajes...')
                self.channel.start_consuming()
            except Exception as e:
                print(f'Error en consumidor: {str(e)}')
                if self.running:
                    # Intentar reconectar si seguimos ejecutándonos
                    time.sleep(5)
                    try:
                        self.connect()
                        # Reiniciar consumidores
                        for queue_name in self.subscriptions:
                            self.start_consuming(queue_name)
                        self.channel.start_consuming()
                    except Exception as reconnect_error:
                        print(f'Error al reconectar: {str(reconnect_error)}')
        
        # Iniciar hilo de consumo
        consumer_thread = threading.Thread(target=consuming_thread)
        consumer_thread.daemon = True
        consumer_thread.start()
    
    def stop(self):
        """Detiene el consumidor"""
        if not self.running:
            return
        
        self.running = False
        
        try:
            # Detener consumo de todas las colas
            self.stop_consuming()
            
            # Detener consumo general
            if self.channel:
                self.channel.stop_consuming()
            
            # Cerrar conexión
            if self.connection and self.connection.is_open:
                self.connection.close()
                
            print(f'Consumidor {self.consumer_id} detenido')
        except Exception as e:
            print(f'Error al detener consumidor: {str(e)}')
    
    def process_one_message(self, queue_name):
        """
        Procesa un solo mensaje de la cola (útil para polling)
        
        Args:
            queue_name: Nombre de la cola
        
        Returns:
            El mensaje procesado o None si no hay mensajes
        """
        if queue_name not in self.subscriptions:
            print(f'Error: No hay suscripción para cola "{queue_name}"')
            return None
        
        try:
            # Obtener un mensaje
            method_frame, header_frame, body = self.channel.basic_get(queue=queue_name, auto_ack=False)
            
            if method_frame:
                try:
                    # Decodificar el mensaje
                    message = json.loads(body)
                    message_id = header_frame.message_id if header_frame else 'desconocido'
                    
                    print(f'Mensaje obtenido de cola "{queue_name}": {message_id}')
                    
                    # Procesar el mensaje con el manejador registrado
                    handler = self.message_handlers.get(queue_name)
                    if handler:
                        result = handler(message)
                        print(f'Mensaje {message_id} procesado')
                    else:
                        print(f'No hay manejador registrado para cola "{queue_name}"')
                    
                    # Confirmar procesamiento
                    self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    
                    return message
                except Exception as e:
                    print(f'Error al procesar mensaje: {str(e)}')
                    # Rechazar el mensaje
                    self.channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                    return None
            else:
                print(f'No hay mensajes disponibles en cola "{queue_name}"')
                return None
        except Exception as e:
            print(f'Error al obtener mensaje de cola "{queue_name}": {str(e)}')
            return None


def main():
    """Función principal para uso como script"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Consumidor de eventos para RabbitMQ')
    parser.add_argument('--host', default='localhost', help='Host de RabbitMQ')
    parser.add_argument('--port', type=int, default=5672, help='Puerto de RabbitMQ')
    parser.add_argument('--api', default='http://localhost:5000', help='URL de la API del broker')
    parser.add_argument('--queue', default='default', help='Nombre de la cola')
    parser.add_argument('--exchange', default='default', help='Nombre del exchange')
    parser.add_argument('--routing-key', default='#', help='Clave de routing')
    
    args = parser.parse_args()
    
    # Función de ejemplo para procesar mensajes
    def process_message(message):
        print(f"Procesando mensaje: {json.dumps(message, indent=2)}")
        return True
    
    # Crear consumidor
    consumer = EventConsumer(
        rabbitmq_host=args.host,
        rabbitmq_port=args.port,
        broker_api=args.api
    )
    
    # Registrar manejador
    consumer.register_handler(args.queue, process_message)
    
    # Suscribirse a la cola
    consumer.subscribe(
        queue_name=args.queue,
        exchange_name=args.exchange,
        routing_key=args.routing_key
    )
    
    # Configurar manejador de señales para detener limpiamente
    def signal_handler(sig, frame):
        print('Deteniendo consumidor...')
        consumer.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Iniciar consumidor
    consumer.start()
    
    print(f'Consumidor iniciado. Presiona Ctrl+C para detener.')
    
    # Mantener el programa en ejecución
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()