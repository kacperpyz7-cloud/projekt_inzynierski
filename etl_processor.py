import os
import json
import psycopg2
from datetime import datetime
from confluent_kafka import Consumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-broker:9092")
KAFKA_TOPIC = "oltp.public.odczyty"

DB_HOST = os.getenv("DB_HOST", "postgres-sink")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")

def calculate_cost(zuzycie, dt, taryfa_id):
    is_night = 22 <= dt.hour or dt.hour < 6
    is_weekend = dt.weekday() >= 5

    if taryfa_id == 1:
        koszt = zuzycie * (0.45 if is_night else 1.15)
    elif taryfa_id == 3:
        koszt = zuzycie * (0.40 if (is_night or is_weekend) else 1.20)
    elif taryfa_id == 4:
        koszt = zuzycie * 1.35
    else:
        koszt = zuzycie * 0.85
            
    return round(koszt, 2)

def run_etl():
    print("Inicjalizacja ETL (Star Schema & DLQ)...")
    
    try:
        conn_source = psycopg2.connect(host="postgres-source", port=DB_PORT, user=DB_USER, password=DB_PASS, dbname="source_db")
        cursor_source = conn_source.cursor()
        
        # Wymiary są ładowane raz i używane przy obsłudze kolejnych odczytów
        cursor_source.execute("""
            SELECT l.id, k.id, k.miasto, t.nazwa_taryfy, l.model, t.id
            FROM liczniki l
            JOIN klienci k ON l.klient_id = k.id
            JOIN taryfy t ON l.taryfa_id = t.id
        """)
        
        dane_wymiarow = cursor_source.fetchall()
        
        cache = {row[0]: {'klient_id': row[1], 'miasto': row[2], 'taryfa_nazwa': row[3], 'model': row[4], 'taryfa_id': row[5]} for row in dane_wymiarow}
        conn_source.close()

        conn_sink = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname="sink_db")
        conn_sink.autocommit = True
        cursor_sink = conn_sink.cursor()

        print(f"🔄 Synchronizacja {len(cache)} wymiarów do OLAP...")
        for licznik_id, v in cache.items():
            cursor_sink.execute("INSERT INTO dim_klient (klient_id, miasto) VALUES (%s, %s) ON CONFLICT DO NOTHING", (v['klient_id'], v['miasto']))
            cursor_sink.execute("INSERT INTO dim_licznik (licznik_id, model, taryfa_nazwa) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (licznik_id, v['model'], v['taryfa_nazwa']))
            
        print("Model wymiarowy załadowany pomyślnie.")
    except Exception as e:
        print(f"Krytyczny błąd inicjalizacji: {e}")
        return

    consumer = Consumer({'bootstrap.servers': KAFKA_BROKER, 'group.id': 'python-etl-star-schema', 'auto.offset.reset': 'earliest'})
    consumer.subscribe([KAFKA_TOPIC])
    
    print("🌊 Nasłuchiwanie strumienia Kafka...")

    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is None or msg.error():
                continue

            val = msg.value().decode('utf-8')
            
            try:
                data = json.loads(val)
                if not data or 'after' not in data or not data['after']:
                    continue
                    
                wiersz = data['after']
                licznik_id = wiersz.get('licznik_id')
                
                if licznik_id not in cache:
                    raise ValueError(f"Nieznany licznik_id: {licznik_id}. Brak w systemie bilingowym.")

                zuzycie_kwh = float(wiersz.get('zuzycie_kwh'))
                
                # Odczyt powyżej progu trafia do raportu jako anomalia
                is_anomalia = zuzycie_kwh > 15.0

                timestamp_str = wiersz.get('timestamp_odczytu')
                dt = datetime.fromtimestamp(timestamp_str / 1000000.0) if isinstance(timestamp_str, int) else datetime.fromisoformat(timestamp_str.replace("Z", ""))
                
                meta = cache[licznik_id]
                koszt_pln = calculate_cost(zuzycie_kwh, dt, meta['taryfa_id'])
                
                cursor_sink.execute("""
                    INSERT INTO fakt_odczyty 
                    (licznik_id, klient_id, zuzycie_kwh, koszt_pln, is_anomalia, godzina, czy_weekend, timestamp_odczytu) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (licznik_id, meta['klient_id'], zuzycie_kwh, koszt_pln, is_anomalia, dt.hour, dt.weekday() >= 5, dt))
                
                print(f"[FAKT] Licznik: {licznik_id} | Zużyto: {zuzycie_kwh}kWh | Należność: {koszt_pln} PLN | Anomalia: {is_anomalia}")

            except Exception as e:
                # Niepoprawny rekord zapisujemy razem z przyczyną odrzucenia
                print(f" [DLQ] Odrzucono rekord. Powód: {e}")
                cursor_sink.execute("INSERT INTO log_bledow_etl (surowy_payload, powod_odrzucenia) VALUES (%s, %s)", (val, str(e)))

    except KeyboardInterrupt:
        print("Zatrzymano ETL.")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_etl()