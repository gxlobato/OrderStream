"""
Producer: gera pedidos falsos de e-commerce e envia para o tópico Kafka.
Rode em um terminal separado do consumer.py.
"""
import json
import random
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from faker import Faker
from kafka import KafkaProducer
import os

load_dotenv()

BROKER = os.getenv("REDPANDA_BROKER")
USERNAME = os.getenv("REDPANDA_USERNAME")
PASSWORD = os.getenv("REDPANDA_PASSWORD")
TOPIC = os.getenv("TOPIC", "pedidos")

fake = Faker("pt_BR")

PRODUTOS = [
    ("Fone Bluetooth", 89.90),
    ("Mouse Gamer", 129.90),
    ("Teclado Mecânico", 259.90),
    ("Monitor 24pol", 799.90),
    ("Webcam HD", 149.90),
    ("Cadeira Gamer", 999.90),
    ("SSD 1TB", 399.90),
    ("Carregador Turbo", 59.90),
]


def criar_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BROKER,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def gerar_pedido() -> dict:
    produto, preco_base = random.choice(PRODUTOS)
    quantidade = random.randint(1, 3)
    return {
        "order_id": fake.uuid4(),
        "customer_id": fake.random_int(min=1000, max=9999),
        "produto": produto,
        "quantidade": quantidade,
        "valor_unitario": preco_base,
        "valor_total": round(preco_base * quantidade, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = criar_producer()
    print(f"Produzindo pedidos no tópico '{TOPIC}'... (Ctrl+C pra parar)")

    try:
        while True:
            pedido = gerar_pedido()
            producer.send(TOPIC, value=pedido)
            print(f"Enviado: {pedido['order_id']} | {pedido['produto']} | R$ {pedido['valor_total']}")
            time.sleep(random.uniform(1, 4))  # simula chegada aleatória de pedidos
    except KeyboardInterrupt:
        print("\nEncerrando producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
