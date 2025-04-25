#!/usr/bin/env python
import pika
import json
import uuid
import threading
import time
from flask import Flask, request, jsonify

class EventBroker:
    def __init__(self, host='localhost', port=5672, management_port=5000):
        """
        Inicializa el Event Broker usando RabbitMQ
        
        Args:
            host: Host donde se ejecuta RabbitMQ
            port: Puerto de RabbitMQ
            management_port: Puerto para la API REST de gestión
        """
        self.host = host
        self.port = port
        self.connection = None
        self.channel = None
        self.exchanges = set()
        self.queues = {}  # Mapeo de nombre de cola -> info
        
        # API REST para gestión
        self.app = Flask(__name__)
        self.setup_routes()
        self.management_port = management_port
        
        # Conectar a RabbitMQ
        self.connect()
    
    def connect(self):
        """Establece conexión con RabbitMQ"""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host, port=self.port)
            )
            self.channel = self.connection.channel()
            print(f"Conectado a RabbitMQ en {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Error al conectar a RabbitMQ: {str(e)}")
            return False
    
    def setup_routes(self):
        """Configura las rutas de la API REST para gestión del broker"""
        
        @self.app.route('/queues', methods=['POST'])
        def create_queue():
            """Crear una nueva cola"""
            data = request.json
            queue_name = data.get('queueName')
            durable = data.get('durable', True)
            
            if not queue_name:
                return jsonify({"error": "Nombre de cola requerido"}), 400
            
            try:
                result = self.declare_queue(queue_name, durable)
                return jsonify({"message": f'Cola "{queue_name}" creada correctamente'}), 201
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/exchanges', methods=['POST'])
        def create_exchange():
            """Crear un nuevo exchange"""
            data = request.json
            exchange_name = data.get('exchangeName')
            exchange_type = data.get('type', 'topic')
            durable = data.get('durable', True)
            
            if not exchange_name:
                return jsonify({"error": "Nombre de exchange requerido"}), 400
            
            try:
                result = self.declare_exchange(exchange_name, exchange_type, durable)
                return jsonify({"message": f'Exchange "{exchange_name}" creado correctamente'}), 201
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/bindings', methods=['POST'])
        def create_binding():
            """Vincular una cola a un exchange con una clave de routing"""
            data = request.json
            queue_name = data.get('queueName')
            exchange_name = data.get('exchangeName')
            routing_key = data.get('routingKey', '')
            
            if not queue_name or not exchange_name:
                return jsonify({"error": "Nombre de cola y exchange requeridos"}), 400
            
            try:
                result = self.bind_queue(queue_name, exchange_name, routing_key)
                return jsonify({
                    "message": f'Cola "{queue_name}" vinculada a exchange "{exchange_name}" con clave "{routing_key}"'
                }), 201
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/messages', methods=['POST'])
        def publish_message():
            """Publicar un mensaje en un exchange"""
            data = request.json
            exchange_name = data.get('exchangeName', '')
            routing_key = data.get('routingKey', '')
            message = data.get('message')
            
            if message is None:
                return jsonify({"error": "Mensaje requerido"}), 400
            
            try:
                message_id = self.publish_message(exchange_name, routing_key, message)
                return jsonify({
                    "messageId": message_id,
                    "message": "Mensaje publicado correctamente"
                }), 201
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/queues', methods=['GET'])
        def list_queues():
            """Listar todas las colas"""
            try:
                return jsonify({"queues": list(self.queues.keys())}), 200
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/exchanges', methods=['GET'])
        def list_exchanges():
            """Listar todos los exchanges"""
            try:
                return jsonify({"exchanges": list(self.exchanges)}), 200
            except Exception as e:
                return jsonify({"error": str(e)}), 500
    
    def declare_queue(self, queue_name, durable=True):
        """
        Declara una cola en RabbitMQ
        
        Args:
            queue_name: Nombre de la cola
            durable: Si la cola debe persistir después de reiniciar el broker
        
        Returns:
            True si se creó correctamente
        """
        try:
            self.channel.queue_declare(queue=queue_name, durable=durable)
            self.queues[queue_name] = {
                'name': queue_name,
                'durable': durable,
                'created_at': time.time()
            }
            print(f'Cola "{queue_name}" declarada')
            return True
        except Exception as e:
            print(f'Error al declarar cola "{queue_name}": {str(e)}')
            raise
    
    def declare_exchange(self, exchange_name, exchange_type='topic', durable=True):
        """
        Declara un exchange en RabbitMQ
        
        Args:
            exchange_name: Nombre del exchange
            exchange_type: Tipo de exchange (direct, fanout, topic, headers)
            durable: Si el exchange debe persistir después de reiniciar el broker
        
        Returns:
            True si se creó correctamente
        """
        try:
            self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=durable
            )
            self.exchanges.add(exchange_name)
            print(f'Exchange "{exchange_name}" declarado')
            return True
        except Exception as e:
            print(f'Error al declarar exchange "{exchange_name}": {str(e)}')
            raise
    
    def bind_queue(self, queue_name, exchange_name, routing_key=''):
        """
        Vincula una cola a un exchange
        
        Args:
            queue_name: Nombre de la cola
            exchange_name: Nombre del exchange
            routing_key: Clave de routing para el binding
        
        Returns:
            True si se vinculó correctamente
        """
        try:
            self.channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key
            )
            print(f'Cola "{queue_name}" vinculada a exchange "{exchange_name}" con clave "{routing_key}"')
            return True
        except Exception as e:
            print(f'Error al vincular cola "{queue_name}" a exchange "{exchange_name}": {str(e)}')
            raise
    
    def publish_message(self, exchange_name, routing_key, message):
        """
        Publica un mensaje en un exchange
        
        Args:
            exchange_name: Nombre del exchange
            routing_key: Clave de routing para el mensaje
            message: Mensaje a publicar (será convertido a JSON)
        
        Returns:
            ID del mensaje publicado
        """
        try:
            # Generar ID único para el mensaje
            message_id = str(uuid.uuid4())
            
            # Preparar propiedades del mensaje
            properties = pika.BasicProperties(
                message_id=message_id,
                timestamp=int(time.time()),
                content_type='application/json',
                delivery_mode=2  # Mensaje persistente
            )
            
            # Si el mensaje no es un diccionario, lo encapsulamos
            if not isinstance(message, dict):
                message = {'data': message}
            
            # Asegurarnos de que tenga un ID
            message['message_id'] = message_id
            
            # Convertir mensaje a JSON y publicar
            message_json = json.dumps(message)
            self.channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=message_json,
                properties=properties
            )
            
            print(f'Mensaje {message_id} publicado en exchange "{exchange_name}" con clave "{routing_key}"')
            return message_id
        except Exception as e:
            print(f'Error al publicar mensaje: {str(e)}')
            raise
    
    def start(self):
        """Inicia el broker y su API de gestión"""
        # Iniciar API de gestión en un hilo separado
        api_thread = threading.Thread(
            target=lambda: self.app.run(host='0.0.0.0', port=self.management_port)
        )
        api_thread.daemon = True
        api_thread.start()
        
        print(f"Event Broker iniciado. API de gestión en puerto {self.management_port}")
        
        # Mantener conexión con RabbitMQ
        try:
            # Simplemente mantenemos el proceso vivo
            while True:
                # time.sleep(1)
                # Si la conexión se perdió, intentar reconectar
                if not self.connection.is_open:
                    print("Conexión perdida. Intentando reconectar...")
                    self.connect()
        except KeyboardInterrupt:
            print("Deteniendo Event Broker...")
            if self.connection and self.connection.is_open:
                self.connection.close()
            print("Event Broker detenido")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Event Broker con RabbitMQ')
    parser.add_argument('--host', default='localhost', help='Host de RabbitMQ')
    parser.add_argument('--port', type=int, default=5672, help='Puerto de RabbitMQ')
    parser.add_argument('--api-port', type=int, default=5000, help='Puerto para API de gestión')
    
    args = parser.parse_args()
    
    broker = EventBroker(
        host=args.host,
        port=args.port,
        management_port=args.api_port
    )
    
    # Declarar exchange y cola por defecto
    try:
        broker.declare_exchange('default', 'topic')
        broker.declare_queue('default')
        broker.bind_queue('default', 'default', '#')
    except Exception as e:
        print(f"Error al configurar recursos por defecto: {str(e)}")
    
    broker.start()