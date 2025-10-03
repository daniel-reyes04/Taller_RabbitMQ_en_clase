# producer.py

import pika
import random
import json
import time

try:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
except pika.exceptions.AMQPConnectionError:
    print(" [ERROR] No se pudo conectar a RabbitMQ. Asegúrate de que el servidor esté corriendo en localhost:5672.")
    exit(1)
queue_name = 'tareas_distribuidas'
channel.queue_declare(queue=queue_name, durable=True)
num_tasks = 10
print(f" [P] Creando {num_tasks} tareas...")

for i in range(1, num_tasks + 1):
    complejidad = random.randint(1, 5)
    processing_time = complejidad 
    
    task = {
        'id': i,
        'complejidad': complejidad,
        'tiempo_procesamiento_s': processing_time
    }
    message = json.dumps(task)
    
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        ))
        
    print(f" [P] Tarea {i} enviada (Complejidad: {complejidad}s)")
    time.sleep(0.1) 

connection.close()
print(" [P] Productor finalizado.")
