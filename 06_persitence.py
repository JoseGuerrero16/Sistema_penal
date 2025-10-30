import pika
import json
import sqlite3
import time
from datetime import datetime
from config import *

class PersistenceService:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.setup_connection()
        self.setup_database()

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
            
            # Declarar cola de sentencias
            self.channel.queue_declare(queue=QUEUE_SENTENCING, durable=True)
            
            print("Persistence Service conectado a RabbitMQ")
            
        except Exception as e:
            print(f"Error conectando a RabbitMQ: {e}")
            raise

    def setup_database(self):
        """Configurar base de datos SQLite"""
        try:
            self.conn_db = sqlite3.connect('sentencias.db', check_same_thread=False)
            cursor = self.conn_db.cursor()
            
            # Crear tabla de sentencias si no existe
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sentencias (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    case_id TEXT UNIQUE,
                    total_score REAL,
                    offenses_count INTEGER,
                    penalty TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            self.conn_db.commit()
            print("Base de datos de sentencias inicializada")
            
        except Exception as e:
            print(f"Error configurando base de datos: {e}")
            raise

    def process_sentence(self, ch, method, properties, body):
        """Procesar mensaje de sentencia y guardar en BD"""
        try:
            sentence_data = json.loads(body)
            case_id = sentence_data["caseId"]
            total_score = sentence_data["totalScore"]
            offenses_count = sentence_data["offensesCount"]
            penalty = sentence_data["penalty"]
            
            print(f"Persistiendo sentencia del caso {case_id}")
            
            # Guardar en base de datos
            cursor = self.conn_db.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO sentencias (case_id, total_score, offenses_count, penalty) VALUES (?, ?, ?, ?)",
                (case_id, total_score, offenses_count, penalty)
            )
            self.conn_db.commit()
            
            print(f"Sentencia persistida para caso {case_id}")
            print(f"Pena: {penalty}")
            print(f"Puntaje: {total_score}")
            print(f"Ofensas: {offenses_count}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error en Persistence: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        """Iniciar consumo de mensajes"""
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=QUEUE_SENTENCING,
            on_message_callback=self.process_sentence
        )
        
        print("Persistence Service escuchando en sentencing...")
        print("Presiona Ctrl+C para detener")
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("Deteniendo Persistence Service...")
            self.connection.close()
            self.conn_db.close()

def main():
    try:
        persistence = PersistenceService()
        persistence.start_consuming()
    except Exception as e:
        print(f"No se pudo iniciar Persistence Service: {e}")

if __name__ == "__main__":
    main()