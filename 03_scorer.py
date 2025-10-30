import pika
import json
from config import *

def process_offense(ch, method, properties, body):
    """Procesar cada ofensa individual (Scorer)"""
    try:
        offense_data = json.loads(body)
        print(f"Scorer: Procesando ofensa: {offense_data['offenseId']} del caso {offense_data['caseId']}")
        
        # Calcular puntaje de la ofensa
        base_score = SCORE_CONFIG.get(offense_data["category"], 0)
        multiplier = MULTIPLIERS.get(offense_data["mode"], 1.0)
        offense_score = base_score * multiplier
        
        # Crear mensaje con puntaje
        scored_offense = {
            "caseId": offense_data["caseId"],
            "offenseId": offense_data["offenseId"],
            "offenseScore": offense_score,
            "category": offense_data["category"],
            "mode": offense_data["mode"]
        }
        
        # Enviar a cola de puntajes
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_OFFENSE_SCORE,
            body=json.dumps(scored_offense),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f" Ofensa {offense_data['offenseId']}: {base_score} × {multiplier} = {offense_score} puntos")
        
    except Exception as e:
        print(f"Error en Scorer: {e}")

# Configurar conexión
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    )
)

channel = connection.channel()

# Asegurar que las colas existen
channel.queue_declare(queue=QUEUE_OFFENSE_UNIT, durable=True)
channel.queue_declare(queue=QUEUE_OFFENSE_SCORE, durable=True)

print("Scorer escuchando en offense.unit...")

channel.basic_consume(
    queue=QUEUE_OFFENSE_UNIT,
    on_message_callback=process_offense,
    auto_ack=True
)

channel.start_consuming()