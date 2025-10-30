import pika
import json
import sqlite3
from config import *

class AuditService:
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
            
            # Declarar cola de auditoría
            self.channel.queue_declare(queue=QUEUE_CASES_AUDIT, durable=True)
            
            print("Audit Service conectado a RabbitMQ")
            
        except Exception as e:
            print(f"Error conectando a RabbitMQ: {e}")
            raise

    def setup_database(self):
        """Configurar base de datos de auditoría"""
        try:
            self.conn_db = sqlite3.connect('audit.db', check_same_thread=False)
            cursor = self.conn_db.cursor()
            
            # Crear tabla de auditoría si no existe
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS audit_cases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    case_id TEXT,
                    state TEXT,
                    normalized_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            self.conn_db.commit()
            print("Base de datos de auditoría inicializada")
            
        except Exception as e:
            print(f"Error configurando base de datos de auditoría: {e}")
            raise

    def process_audit_message(self, ch, method, properties, body):
        """Procesar mensaje de auditoría y guardar en BD"""
        try:
            case_data = json.loads(body)
            case_id = case_data["caseId"]
            state = case_data["state"]
            normalized_data = json.dumps(case_data)  # Guardamos todo el mensaje normalizado
            
            print(f"Auditando caso {case_id}")
            
            # Guardar en base de datos de auditoría
            cursor = self.conn_db.cursor()
            cursor.execute(
                "INSERT INTO audit_cases (case_id, state, normalized_data) VALUES (?, ?, ?)",
                (case_id, state, normalized_data)
            )
            self.conn_db.commit()
            
            print(f"Caso auditado y persistido: {case_id}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error en Audit Service: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        """Iniciar consumo de mensajes"""
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=QUEUE_CASES_AUDIT,
            on_message_callback=self.process_audit_message
        )
        
        print("Audit Service escuchando en cases.audit...")
        print("Presiona Ctrl+C para detener")
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("Deteniendo Audit Service...")
            self.connection.close()
            self.conn_db.close()

def main():
    try:
        audit_service = AuditService()
        audit_service.start_consuming()
    except Exception as e:
        print(f"No se pudo iniciar Audit Service: {e}")

if __name__ == "__main__":
    main()