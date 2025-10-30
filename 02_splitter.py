import pika
import json
from config import *

def callback(ch, method, properties, body):
    print("Splitter: Dividiendo caso en ofensas individuales")
    
    try:
        case_data = json.loads(body)
        case_id = case_data["caseId"]
        offenses = case_data["offenses"]
        
        print(f"Dividiendo caso {case_id} en {len(offenses)} ofensas")
        
        for offense in offenses:
            offense_message = {
                "caseId": case_id,
                "offenseId": offense["offenseId"],
                "category": offense["category"],
                "mode": offense["mode"]
            }
            
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_OFFENSE_UNIT,
                body=json.dumps(offense_message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"Ofensa {offense['offenseId']} enviada a offense.unit")
        
    except Exception as e:
        print(f"Error en Splitter: {e}")

# Configurar conexión
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    )
)

channel = connection.channel()

# Declarar colas
channel.queue_declare(queue=QUEUE_CASES_CANONICAL, durable=True)
channel.queue_declare(queue=QUEUE_OFFENSE_UNIT, durable=True)

print("Splitter escuchando en cases.canonical...")

channel.basic_consume(
    queue=QUEUE_CASES_CANONICAL,
    on_message_callback=callback,
    auto_ack=True
)

channel.start_consuming()