import pika
import json
import time
from config import *

class PenaltyMapper:
    def __init__(self):
        self.connection = None
        self.channel = None
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
            self.channel.queue_declare(queue=QUEUE_CASE_SCORE, durable=True)
            self.channel.queue_declare(queue=QUEUE_SENTENCING, durable=True)
            
            print("Penalty Mapper conectado a RabbitMQ")
            
        except Exception as e:
            print(f"Error conectando a RabbitMQ: {e}")
            raise

    def determine_penalty(self, total_score):
        """Determinar la pena basada en el puntaje total"""
        penalties = [
            (1500, "pena de muerte"),
            (1000, "cadena perpetua"),
            (800, "25 años de cárcel"),
            (650, "10 años de cárcel"),
            (500, "3 años de cárcel"),
            (350, "8 meses de cárcel"),
            (250, "3 meses de cárcel"),
            (150, "trabajo comunitario"),
            (100, "1 semana de detención"),
            (50, "fianza $5,000"),
            (0, "sermón")
        ]
        
        for threshold, penalty in penalties:
            if total_score >= threshold:
                return penalty
        return "sin pena"

    def process_case_score(self, ch, method, properties, body):
        """Procesar mensaje con puntaje total del caso"""
        try:
            case_data = json.loads(body)
            case_id = case_data["caseId"]
            total_score = case_data["totalScore"]
            offenses_count = case_data["offensesCount"]
            
            print(f"Penalty Mapper procesando caso {case_id}")
            print(f"Puntaje total: {total_score} puntos")
            print(f"fensas: {offenses_count}")
            
            # Determinar la pena
            penalty = self.determine_penalty(total_score)
            
            # Crear mensaje de sentencia
            sentence_message = {
                "caseId": case_id,
                "totalScore": total_score,
                "offensesCount": offenses_count,
                "penalty": penalty,
                "timestamp": time.time()
            }
            
            # Publicar sentencia final
            self.channel.basic_publish(
                exchange='',
                routing_key=QUEUE_SENTENCING,
                body=json.dumps(sentence_message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            
            print(f"Sentencia para caso {case_id}: {penalty}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error en Penalty Mapper: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        """Iniciar consumo de mensajes"""
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=QUEUE_CASE_SCORE,
            on_message_callback=self.process_case_score
        )
        
        print("Penalty Mapper escuchando en case.score...")
        print("Presiona Ctrl+C para detener")
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("Deteniendo Penalty Mapper...")
            self.connection.close()

def main():
    try:
        penalty_mapper = PenaltyMapper()
        penalty_mapper.start_consuming()
    except Exception as e:
        print(f"No se pudo iniciar Penalty Mapper: {e}")

if __name__ == "__main__":
    main()