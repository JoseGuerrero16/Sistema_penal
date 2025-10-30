import pika
import json
from config import *

def callback(ch, method, properties, body):
    print("Mensaje recibido en Translator")
    
    try:
        case_data = json.loads(body)
        print(f"Procesando caso: {case_data['caseId']}")
        
        # NORMALIZACIÓN DE TERMINOLOGÍA
        normalized_offenses = []
        for offense in case_data["offenses"]:
            local_label = offense["localLabel"].lower()
            
            if local_label in ["crimen", "delito grave"]:
                category = "high"
            elif local_label in ["delito", "menos grave"]:
                category = "mid"
            else:  # "falta", "leve"
                category = "low"
            
            normalized_offenses.append({
                "offenseId": offense["offenseId"],
                "category": category,
                "mode": offense["mode"]
            })
        
        # Crear mensaje normalizado
        normalized_message = {
            "caseId": case_data["caseId"],
            "state": case_data["state"],
            "offender": case_data["offender"],
            "offenses": normalized_offenses,
            "ts": case_data["ts"]
        }
        
        # 1. ENVIAR A CANAL NORMALIZADO (Message Translator)
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_CASES_CANONICAL,
            body=json.dumps(normalized_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print("Caso normalizado enviado a cases.canonical")
        
        # 2. WIRE TAP - Copia para auditoría
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_CASES_AUDIT,
            body=json.dumps(normalized_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print("Copia enviada a cases.audit (Wire Tap)")
        
    except Exception as e:
        print(f"Error en Translator: {e}")

# Configurar conexión
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    )
)

channel = connection.channel()

# Declarar colas que usamos
channel.queue_declare(queue=QUEUE_CASES_IN, durable=True)
channel.queue_declare(queue=QUEUE_CASES_CANONICAL, durable=True)
channel.queue_declare(queue=QUEUE_CASES_AUDIT, durable=True)

print("Translator escuchando en cases.in...")

channel.basic_consume(
    queue=QUEUE_CASES_IN,
    on_message_callback=callback,
    auto_ack=True
)

channel.start_consuming()