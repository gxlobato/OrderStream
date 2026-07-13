import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT"),
)

def criar_particao_futura():
    hoje = datetime.now()
    proximo_mes = (hoje.replace(day=1) + timedelta(days=32)).replace(day=1)
    mes_seguinte = (proximo_mes + timedelta(days=32)).replace(day=1)
    nome = f"pedidos_streaming_{proximo_mes.strftime('%Y_%m')}"

    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables WHERE table_name = %s
            );
        """, (nome,))
        existe = cur.fetchone()[0]

        if existe:
            print(f"Partição {nome} já existe, nada a fazer.")
            return

        cur.execute(f"""
            CREATE TABLE {nome} PARTITION OF pedidos_streaming
            FOR VALUES FROM (%s) TO (%s);
        """, (proximo_mes, mes_seguinte))
        print(f"Partição criada: {nome}")

    conn.commit()

criar_particao_futura()
conn.close()