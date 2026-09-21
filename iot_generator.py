import os
import time
import random
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DB_HOST = os.getenv("DB_HOST", "postgres-source")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "source_db")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")

# Liczba odczytów wysyłanych w jednej paczce
BATCH_SIZE = 500
# Przerwa między kolejnymi paczkami
DELAY_SEC = 0.5

def generate_telemetry():
    print(f"Uruchamianie generatora stress test (Batch Size: {BATCH_SIZE})...")
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM liczniki WHERE status = 'ACTIVE'")
        active_meters = [row[0] for row in cursor.fetchall()]
        
        if not active_meters:
            print("Brak aktywnych liczników.")
            return

        print(f"Baza gotowa. Podpięto {len(active_meters)} liczników. Rozpoczynanie generowania danych...\n")
        
        total_sent = 0
        query = "INSERT INTO odczyty (licznik_id, zuzycie_kwh, timestamp_odczytu) VALUES %s"
        
        while True:
            batch = [
                (random.choice(active_meters), round(random.uniform(0.010, 0.500), 3), datetime.now())
                for _ in range(BATCH_SIZE)
            ]
            
            execute_values(cursor, query, batch)
            
            total_sent += BATCH_SIZE
            print(f"[Partia] Wysłano: {BATCH_SIZE} odczytów. Łącznie w bazie: {total_sent}")
            
            time.sleep(DELAY_SEC)

    except Exception as e:
        print(f"Błąd krytyczny: {e}")

if __name__ == "__main__":
    generate_telemetry()