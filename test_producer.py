import pika
import json
import uuid
from datetime import datetime
from config import *

def send_test_case():
    """Enviar caso de prueba al sistema"""
    try:
        # Conectar a RabbitMQ
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Asegurar que la cola existe
        channel.queue_declare(queue=QUEUE_CASES_IN, durable=True)
        
        # Crear caso de prueba
        test_case = {
            "caseId": str(uuid.uuid4()),
            "state": "Estado-A",
            "offender": {
                "name": "Juan Pérez",
                "id": "12345678-9"
            },
            "offenses": [
                {
                    "offenseId": "1",
                    "localLabel": "crimen",  # Será normalizado a "high"
                    "mode": "planificado"
                },
                {
                    "offenseId": "2", 
                    "localLabel": "falta",   # Será normalizado a "low"
                    "mode": "involuntario"
                }
            ],
            "ts": datetime.now().isoformat()
        }
        
        # Enviar caso
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_CASES_IN,
            body=json.dumps(test_case),
            properties=pika.BasicProperties(delivery_mode=2)  # Mensaje persistente
        )
        
        print(f"Caso de prueba enviado: {test_case['caseId']}")
        print("Ofensas:")
        print("   - crimen (planificado) → high")
        print("   - falta (involuntario) → low")
        
        connection.close()
        
    except Exception as e:
        print(f"Error enviando caso: {e}")
        print("Verifica que RabbitMQ esté ejecutándose")

if __name__ == "__main__":
    send_test_case()