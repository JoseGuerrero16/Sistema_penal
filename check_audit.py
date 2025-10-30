# check_audit.py
import sqlite3

def check_audit():
    conn = sqlite3.connect('audit.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM audit_cases ORDER BY created_at DESC")
    audits = cursor.fetchall()
    
    print("CASOS AUDITADOS")
    print("=" * 50)
    for audit in audits:
        print(f"Case ID: {audit[1]}")
        print(f"Estado: {audit[2]}")
        print(f"Fecha: {audit[4]}")
        print("-" * 30)
    conn.close()

if __name__ == "__main__":
    check_audit()