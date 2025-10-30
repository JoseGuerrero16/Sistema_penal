import sqlite3
import json
from config import *

def check_sentences():
    """Verificar sentencias almacenadas en la base de datos"""
    try:
        conn = sqlite3.connect('sentencias.db')
        cursor = conn.cursor()
        
        # Obtener todas las sentencias
        cursor.execute("SELECT * FROM sentencias ORDER BY created_at DESC")
        sentencias = cursor.fetchall()
        
        print("SENTENCIAS ALMACENADAS EN LA BASE DE DATOS")
        print("=" * 60)
        
        if not sentencias:
            print("No hay sentencias almacenadas aún.")
            return
        
        for sentencia in sentencias:
            id_, case_id, total_score, offenses_count, penalty, created_at = sentencia
            print(f"Case ID: {case_id}")
            print(f"Puntaje Total: {total_score}")
            print(f"Ofensas: {offenses_count}")
            print(f"Pena: {penalty}")
            print(f"Fecha: {created_at}")
            print("-" * 40)
        
        conn.close()
        
    except Exception as e:
        print(f"Error consultando base de datos: {e}")

if __name__ == "__main__":
    check_sentences()