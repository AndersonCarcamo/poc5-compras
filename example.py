#!/usr/bin/env python
import sys
import time
import threading
import json
import random
import os
from datetime import datetime

# Importar las clases necesarias
from producer import EventProducer
from consumer import EventConsumer

# Este script muestra cómo usar el event broker con RabbitMQ

def run_example():
    # Configuración
    RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
    RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
    BROKER_API = os.environ.get('BROKER_API', 'http://localhost:5000')
    
    # Definir exchange y colas
    EXCHANGE_NAME = 'pedidos-exchange'
    QUEUE_NUEVOS_PEDIDOS = 'nuevos-pedidos'
    QUEUE_PEDIDOS_PRIORITARIOS = 'pedidos-prioritarios'
    
    print(f"Conectando a RabbitMQ en {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    
    # Crear productor
    productor = EventProducer(
        rabbitmq_host=RABBITMQ_HOST,
        rabbitmq_port=RABBITMQ_PORT,
        broker_api=BROKER_API,
        producer_id='productor-pedidos'
    )
    
    # Asegurar que existe el exchange
    productor.ensure_exchange(EXCHANGE_NAME, 'topic')
    
    # Crear consumidores
    consumidor_normal = EventConsumer(
        rabbitmq_host=RABBITMQ_HOST,
        rabbitmq_port=RABBITMQ_PORT,
        broker_api=BROKER_API,
        consumer_id='consumidor-pedidos-normal'
    )
    
    consumidor_prioritario = EventConsumer(
        rabbitmq_host=RABBITMQ_HOST,
        rabbitmq_port=RABBITMQ_PORT,
        broker_api=BROKER_API,
        consumer_id='consumidor-pedidos-prioritarios'
    )
    
    # Definir manejadores para procesar mensajes
    def procesar_pedido_normal(mensaje):
        pedido_id = mensaje.get('data', {}).get('pedido_id', 'desconocido')
        cliente = mensaje.get('data', {}).get('cliente', 'desconocido')
        print(f"\n[NORMAL] Procesando pedido #{pedido_id} de {cliente}")
        # Simular procesamiento
        time.sleep(1)
        print(f"[NORMAL] Pedido #{pedido_id} procesado correctamente\n")
        return True
    
    def procesar_pedido_prioritario(mensaje):
        pedido_id = mensaje.get('data', {}).get('pedido_id', 'desconocido')
        cliente = mensaje.get('data', {}).get('cliente', 'desconocido')
        print(f"\n[PRIORITARIO] Procesando pedido URGENTE #{pedido_id} de {cliente}")
        # Simular procesamiento rápido
        time.sleep(0.5)
        print(f"[PRIORITARIO] Pedido URGENTE #{pedido_id} procesado rápidamente\n")
        return True
    
    # Registrar manejadores
    consumidor_normal.register_handler(QUEUE_NUEVOS_PEDIDOS, procesar_pedido_normal)
    consumidor_prioritario.register_handler(QUEUE_PEDIDOS_PRIORITARIOS, procesar_pedido_prioritario)
    
    # Suscribir consumidores
    consumidor_normal.subscribe(
        queue_name=QUEUE_NUEVOS_PEDIDOS,
        exchange_name=EXCHANGE_NAME,
        routing_key='pedido.normal'
    )
    
    consumidor_prioritario.subscribe(
        queue_name=QUEUE_PEDIDOS_PRIORITARIOS,
        exchange_name=EXCHANGE_NAME,
        routing_key='pedido.prioritario'
    )
    
    # Iniciar consumidores
    print("Iniciando consumidores...")
    consumidor_normal.start()
    consumidor_prioritario.start()
    
    # Función para enviar pedidos de ejemplo
    def generar_pedidos():
        print("\nIniciando generación de pedidos...")
        
        # Esperar un momento para que los consumidores estén listos
        time.sleep(2)
        
        for i in range(1, 11):  # Generar 10 pedidos
            # Determinar si es prioritario (30% de probabilidad)
            es_prioritario = random.random() < 0.3
            
            # Crear datos del pedido
            pedido = {
                'pedido_id': i,
                'cliente': f'Cliente {i}',
                'productos': random.randint(1, 5),
                'total': round(random.uniform(10.0, 200.0), 2),
                'fecha': datetime.now().isoformat()
            }
            
            # Seleccionar routing key según prioridad
            routing_key = 'pedido.prioritario' if es_prioritario else 'pedido.normal'
            
            # Publicar mensaje
            mensaje_id = productor.publish(
                message={'data': pedido},
                exchange_name=EXCHANGE_NAME,
                routing_key=routing_key
            )
            
            if mensaje_id:
                tipo = "PRIORITARIO" if es_prioritario else "normal"
                print(f"Pedido {tipo} #{i} enviado con ID: {mensaje_id}")
            else:
                print(f"Error al enviar pedido #{i}")
            
            # Esperar entre 1 y 3 segundos entre pedidos
            time.sleep(random.uniform(1, 3))
        
        print("\nGeneración de pedidos completada")
    
    # Iniciar generación de pedidos en un hilo separado
    generator_thread = threading.Thread(target=generar_pedidos)
    generator_thread.daemon = True
    generator_thread.start()
    
    # Mantener el programa en ejecución hasta que se presione Ctrl+C
    try:
        print("\nEjemplo en ejecución. Presiona Ctrl+C para detener...\n")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDeteniendo ejemplo...")
        
        # Detener consumidores
        consumidor_normal.stop()
        consumidor_prioritario.stop()
        
        # Cerrar productor
        productor.close()
        
        print("Ejemplo finalizado")


if __name__ == "__main__":
    # Verificar que se esté ejecutando RabbitMQ y el broker
    print("Iniciando ejemplo de Event Broker con RabbitMQ")
    print("Nota: Asegúrate de que RabbitMQ y el broker estén ejecutándose")
    
    run_example()