import json, os, time, signal
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import Json
from clickhouse_driver import Client as CHClient

TOPIC = os.getenv("KAFKA_TOPIC", "demo_topic")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID_WRITER", "writer_group")
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "appdb")
PG_USER = os.getenv("POSTGRES_USER", "appuser")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "apppass")
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "9000"))
CH_DB = os.getenv("CH_DB", "app")
CH_USER = os.getenv("CH_USER")
CH_PASSWORD = os.getenv("CH_PASSWORD")

running = True
def _stop(*_): 
    global running; running = False
signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)

def ensure_pg(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS app;
        CREATE TABLE IF NOT EXISTS app.events (
            id BIGSERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            payload JSONB NOT NULL,
            received_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)
    conn.commit()

def ensure_ch(ch):
    ch.execute("CREATE DATABASE IF NOT EXISTS app")
    ch.execute("""
    CREATE TABLE IF NOT EXISTS app.events
    (
        id UInt64,
        event_type String,
        payload String,
        received_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY (received_at, id)
    """)

def connect_pg():
    for i in range(1, 31):
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)
            conn.autocommit = False
            ensure_pg(conn)
            print("[writer] PG ok")
            return conn
        except Exception as e:
            print(f"[writer] PG attempt {i} fail: {e}"); time.sleep(2)
    raise RuntimeError("PG connection failed")

def connect_ch(retries=30, delay=2.0):
    for attempt in range(1, retries+1):
        try:
            client = CHClient(
                host=CH_HOST,
                port=CH_PORT,
                database=CH_DB,
                user=CH_USER,
                password=CH_PASSWORD,
            )
            ensure_ch_schema(client)
            print("[writer] connected to ClickHouse")
            return client
        except Exception as e:
            print(f"[writer] CH connect failed (attempt {attempt}): {e}")
            time.sleep(delay)
    raise RuntimeError("CH connection failed")

def main():
    print(f"[writer] start {BOOTSTRAP=} {TOPIC=} {GROUP_ID=}")
    pg = connect_pg()
    ch = connect_ch()

    c = KafkaConsumer(TOPIC, bootstrap_servers=[BOOTSTRAP], group_id=GROUP_ID,
                      auto_offset_reset="earliest", enable_auto_commit=True,
                      value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    pg_insert = "INSERT INTO app.events (event_type, payload, received_at) VALUES (%s, %s, %s) RETURNING id;"
    ch_insert = "INSERT INTO app.events (id, event_type, payload, received_at) VALUES"
    cnt = 0
    try:
        for m in c:
            if not running: break
            val = m.value
            event_type = val.get("event", "unknown")
            recv = datetime.utcnow()

            try:
                with pg.cursor() as cur:
                    cur.execute(pg_insert, (event_type, Json(val), recv))
                    _id = cur.fetchone()[0]
                pg.commit()
            except Exception as e:
                pg.rollback(); print(f"[writer][pg] insert err: {e}")

            ch_id = int(val.get("id", 0))
            try:
                ch.execute(ch_insert, [(ch_id, event_type, json.dumps(val, ensure_ascii=False), recv)])
            except Exception as e:
                print(f"[writer][ch] insert err: {e}")

            cnt += 1
            print(f"[writer] saved offset={m.offset} total={cnt}")
            if not running: break
    finally:
        try: pg.close()
        except: pass
        try: ch.disconnect()
        except: pass
        print("[writer] stop")
if __name__ == "__main__": main()
