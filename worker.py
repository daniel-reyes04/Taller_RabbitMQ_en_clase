import pika
import time
import json
import sys

worker_id = sys.argv[1] if len(sys.argv) > 1 else 'Worker-DEFAULT'
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
queue_name = 'tareas_distribuidas'
channel.queue_declare(queue=queue_name, durable=True)
channel.basic_qos(prefetch_count=1)
print(f' [{worker_id}] Esperando tareas. Para salir, presiona CTRL+C')

def callback(ch, method, properties, body):
    task = json.loads(body)
    task_id = task['id']
    processing_time = task['tiempo_procesamiento_s']
    
    print(f" [{worker_id}] 🟢 Recibida Tarea {task_id} (Tiempo: {processing_time}s)")
    
    try:
        time.sleep(processing_time)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f" [{worker_id}] ✅ Tarea {task_id} completada y reconocida (ACK)")

    except Exception as e:
        print(f" [{worker_id}] ❌ Error al procesar Tarea {task_id}: {e}")


channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback,
    auto_ack=False 
)
channel.start_consuming()
