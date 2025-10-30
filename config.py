import os

# Configuración de RabbitMQ - CORREGIDO
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")  # ← CORREGIDO
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")  # ← CORREGIDO

# Nombres de las colas
QUEUE_CASES_IN = "/queue/cases.in"
QUEUE_CASES_CANONICAL = "/queue/cases.canonical"
QUEUE_CASES_AUDIT = "/queue/cases.audit"
QUEUE_OFFENSE_UNIT = "/queue/offense.unit"
QUEUE_OFFENSE_SCORE = "/queue/offense.score"
QUEUE_CASE_SCORE = "/queue/case.score"
QUEUE_SENTENCING = "/queue/sentencing"

# Configuración de puntajes
SCORE_CONFIG = {
    "high": 300,
    "mid": 120, 
    "low": 30
}

MULTIPLIERS = {
    "planificado": 1.25,
    "intencional": 1.00,
    "involuntario": 0.80,
    "defensa_propia": 0.60,
    "buenas_intenciones": 0.00
}