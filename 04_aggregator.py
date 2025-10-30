import pika
import json
import time
from collections import defaultdict
from config import *

class Aggregator:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.pending_cases = defaultdict(list)
        self.setup_connection()

    def setup_connection(self):
        """Configurar conexión a RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials,
                heartbeat=600
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declarar colas
            self.channel.queue_declare(queue=QUEUE_OFFENSE_SCORE, durable=True)
            self.channel.queue_declare(queue=QUEUE_CASE_SCORE, durable=True)
            
            print("Aggregator conectado a RabbitMQ")
            
        except Exception as e:
            print(f"Error conectando a RabbitMQ: {e}")
            raise

    def process_offense_score(self, ch, method, properties, body):
        """Procesar mensaje de ofensa puntuada"""
        try:
            offense_data = json.loads(body)
            case_id = offense_data["caseId"]
            offense_id = offense_data["offenseId"]
            score = offense_data["offenseScore"]
            
            print(f"Aggregator recibió ofensa {offense_id} del caso {case_id}: {score} puntos")
            
            # Agregar ofensa al caso pendiente
            self.pending_cases[case_id].append(offense_data)
            
            # Estrategia mejorada: procesar después de timeout
            time.sleep(1)  # Espera para agrupar ofensas del mismo caso
            
            # Calcular puntaje total del caso
            total_score = sum(offense["offenseScore"] for offense in self.pending_cases[case_id])
            
            # Publicar el puntaje total del caso
            case_score_message = {
                "caseId": case_id,
                "totalScore": total_score,
                "offensesCount": len(self.pending_cases[case_id]),
                "timestamp": time.time()
            }
            
            self.channel.basic_publish(
                exchange='',
                routing_key=QUEUE_CASE_SCORE,
                body=json.dumps(case_score_message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            
            print(f"Caso {case_id}: {len(self.pending_cases[case_id])} ofensas, puntaje total: {total_score}")
            
            # Limpiar caso procesado
            if case_id in self.pending_cases:
                del self.pending_cases[case_id]
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error en Aggregator: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        """Iniciar consumo de mensajes"""
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=QUEUE_OFFENSE_SCORE,
            on_message_callback=self.process_offense_score
        )
        
        print("Aggregator escuchando en offense.score...")
        print("Presiona Ctrl+C para detener")
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("Deteniendo Aggregator...")
            self.connection.close()

def main():
    try:
        aggregator = Aggregator()
        aggregator.start_consuming()
    except Exception as e:
        print(f" No se pudo iniciar Aggregator: {e}")

if __name__ == "__main__":
    main()