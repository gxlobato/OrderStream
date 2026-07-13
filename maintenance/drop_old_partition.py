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

def apagar_particoes_antigas(meses_retencao=3):
    limite = (datetime.now().replace(day=1) - timedelta(days=32 * meses_retencao)).replace(day=1)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE tablename LIKE 'pedidos_streaming_%%';
        """)
        particoes = [row[0] for row in cur.fetchall()]

        for particao in particoes:
            try:
                ano_mes = particao.replace("pedidos_streaming_", "")
                data_particao = datetime.strptime(ano_mes, "%Y_%m")
            except ValueError:
                continue

            if data_particao < limite:
                cur.execute(f"DROP TABLE {particao};")
                print(f"Partição removida: {particao}")

    conn.commit()

apagar_particoes_antigas()
conn.close()