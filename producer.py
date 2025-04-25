#!/usr/bin/env python
import pika
import json
import uuid
import requests
from datetime import datetime
import argparse

class EventProducer:
    def __init__(self, rabbitmq_host='localhost', rabbitmq_port=5672, 
                 broker_api='http://localhost:5000', producer_id=None):
        """
        Inicializa un productor de eventos usando RabbitMQ
        
        Args:
            rabbitmq_host: Host donde se ejecuta RabbitMQ
            rabbitmq_port: Puerto de RabbitMQ
            broker_api: URL de la API del broker
            producer_id: Identificador único del productor
        """
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.broker_api = broker_api
        self.producer_id = producer_id or str(uuid.uuid4())
        self.connection = None
        self.channel = None
        self.connect()
    
    def connect(self):
        """Establece conexión directa con RabbitMQ"""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.rabbitmq_host, port=self.rabbitmq_port)
            )
            self.channel = self.connection.channel()
            print(f"Productor {self.producer_id} conectado a RabbitMQ en {self.rabbitmq_host}:{self.rabbitmq_port}")
            return True
        except Exception as e:
            print(f"Error al conectar a RabbitMQ: {str(e)}")
            return False
    
    def ensure_exchange(self, exchange_name, exchange_type='topic'):
        """
        Asegura que exista un exchange, creándolo si es necesario
        
        Args:
            exchange_name: Nombre del exchange
            exchange_type: Tipo de exchange (direct, fanout, topic, headers)
        
        Returns:
            True si se creó o ya existía
        """
        try:
            # Intentar usar la API del broker primero
            response = requests.post(
                f"{self.broker_api}/exchanges",
                json={
                    "exchangeName": exchange_name,
                    "type": exchange_type,
                    "durable": True
                }
            )
            
            if response.status_code in [201, 200]:
                print(f'Exchange "{exchange_name}" verificado/creado mediante API')
                return True
                
            # Si falla, intentar directamente con RabbitMQ
            self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=True
            )
            print(f'Exchange "{exchange_name}" declarado directamente')
            return True
        except Exception as e:
            print(f'Error al asegurar exchange "{exchange_name}": {str(e)}')
            return False
    
    def publish(self, message, exchange_name='default', routing_key='', use_api=True):
        """
        Publica un mensaje en un exchange
        
        Args:
            message: Mensaje a publicar (puede ser un diccionario o cualquier valor)
            exchange_name: Nombre del exchange
            routing_key: Clave de routing
            use_api: Si es True, usa la API del broker, sino usa conexión directa
        
        Returns:
            ID del mensaje si se publicó correctamente, None en caso contrario
        """
        # Asegurar que el mensaje tenga un ID y timestamp
        message_id = str(uuid.uuid4())
        
        # Si el mensaje no es un diccionario, lo encapsulamos
        if not isinstance(message, dict):
            message_data = {'data': message}
        else:
            message_data = message.copy()
        
        # Agregar metadatos al mensaje
        message_data.update({
            'message_id': message_id,
            'producer_id': self.producer_id,
            'timestamp': datetime.now().isoformat(),
        })
        
        try:
            if use_api:
                # Publicar usando la API del broker
                response = requests.post(
                    f"{self.broker_api}/messages",
                    json={
                        "exchangeName": exchange_name,
                        "routingKey": routing_key,
                        "message": message_data
                    }
                )
                
                if response.status_code == 201:
                    result = response.json()
                    print(f'Mensaje publicado con ID: {result.get("messageId")}')
                    return result.get("messageId")
                else:
                    print(f'Error al publicar mensaje mediante API: {response.text}')
                    # Si falla la API, intentar publicación directa
                    return self.publish(message, exchange_name, routing_key, use_api=False)
            else:
                # Publicar directamente usando RabbitMQ
                # Asegurar que el exchange exista
                self.ensure_exchange(exchange_name)
                
                # Propiedades del mensaje
                properties = pika.BasicProperties(
                    message_id=message_id,
                    timestamp=int(datetime.now().timestamp()),
                    content_type='application/json',
                    delivery_mode=2  # Mensaje persistente
                )
                
                # Publicar mensaje
                self.channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=json.dumps(message_data),
                    properties=properties
                )
                
                print(f'Mensaje {message_id} publicado directamente en "{exchange_name}" con clave "{routing_key}"')
                return message_id
        except Exception as e:
            print(f'Error al publicar mensaje: {str(e)}')
            return None
    
    def publish_batch(self, messages, exchange_name='default', routing_key=''):
        """
        Publica un lote de mensajes
        
        Args:
            messages: Lista de mensajes a publicar
            exchange_name: Nombre del exchange
            routing_key: Clave de routing
        
        Returns:
            Lista de IDs de mensajes publicados
        """
        message_ids = []
        
        for message in messages:
            message_id = self.publish(message, exchange_name, routing_key)
            if message_id:
                message_ids.append(message_id)
        
        return message_ids
    
    def close(self):
        """Cierra la conexión con RabbitMQ"""
        if self.connection and self.connection.is_open:
            self.connection.close()
            print(f"Productor {self.producer_id} desconectado")


def main():
    """Función principal para uso como script"""
    parser = argparse.ArgumentParser(description='Productor de eventos para RabbitMQ')
    parser.add_argument('--host', default='localhost', help='Host de RabbitMQ')
    parser.add_argument('--port', type=int, default=5672, help='Puerto de RabbitMQ')
    parser.add_argument('--api', default='http://localhost:5000', help='URL de la API del broker')
    parser.add_argument('--exchange', default='default', help='Nombre del exchange')
    parser.add_argument('--routing-key', default='', help='Clave de routing')
    parser.add_argument('--message', required=True, help='Mensaje a enviar')
    
    args = parser.parse_args()
    
    # Crear productor
    producer = EventProducer(
        rabbitmq_host=args.host,
        rabbitmq_port=args.port,
        broker_api=args.api
    )
    
    # Publicar mensaje
    message_id = producer.publish(
        message=args.message,
        exchange_name=args.exchange,
        routing_key=args.routing_key
    )
    
    if message_id:
        print(f"Mensaje publicado correctamente con ID: {message_id}")
    else:
        print("Error al publicar mensaje")
    
    # Cerrar conexión
    producer.close()


if __name__ == "__main__":
    main()