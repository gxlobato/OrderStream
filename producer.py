"""
Producer: gera pedidos falsos de e-commerce e envia para o tópico Kafka.
Rode em um terminal separado do consumer.py.

Producer: generates fake e-commerce orders and sends them to the Kafka topic.
Run in a separate terminal from consumer.py.
"""
import json
import random
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from faker import Faker
from kafka import KafkaProducer
import os

# Carrega variáveis de ambiente do arquivo .env
# Load environment variables from .env file
load_dotenv()

# ============ CONFIGURAÇÕES DO KAFKA/REDPANDA ============
# Kafka/Redpanda broker connection settings
BROKER = os.getenv("REDPANDA_BROKER")
USERNAME = os.getenv("REDPANDA_USERNAME")
PASSWORD = os.getenv("REDPANDA_PASSWORD")
TOPIC = os.getenv("TOPIC", "pedidos")  # Tópico padrão: "pedidos" | Default topic: "orders"

# ============ CONFIGURAÇÕES DO FAKER (GERADOR DE DADOS) ============
# Faker configuration for generating realistic Brazilian Portuguese data
fake = Faker("pt_BR")  # Gera dados em português do Brasil | Generates Brazilian Portuguese data

# ============ CATÁLOGO DE PRODUTOS ============
# Product catalog with name and base price
# Lista de tuplas: (nome_do_produto, preço_unitário)
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
    """
    Configura e retorna um produtor Kafka com autenticação SASL.
    Configures and returns a Kafka producer with SASL authentication.
    
    Returns:
        KafkaProducer: Produtor configurado para enviar mensagens JSON
        Producer configured to send JSON messages
    """
    return KafkaProducer(
        bootstrap_servers=BROKER,
        security_protocol="SASL_SSL",  # Protocolo de segurança com SSL | Security protocol with SSL
        sasl_mechanism="SCRAM-SHA-256",  # Mecanismo de autenticação | Authentication mechanism
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # Converte dict para JSON bytes | Convert dict to JSON bytes
    )


def gerar_pedido() -> dict:
    """
    Gera um pedido falso simulando uma compra em e-commerce.
    Generates a fake order simulating an e-commerce purchase.
    
    Returns:
        dict: Dicionário com todos os campos do pedido
        Dictionary with all order fields
    """
    # Seleciona um produto aleatório do catálogo | Select random product from catalog
    produto, preco_base = random.choice(PRODUTOS)
    
    # Simula quantidade comprada (1 a 3 unidades) | Simulates purchased quantity (1 to 3 units)
    quantidade = random.randint(1, 3)
    
    return {
        "order_id": fake.uuid4(),  # ID único do pedido (formato UUID) | Unique order ID (UUID format)
        "customer_id": fake.random_int(min=1000, max=9999),  # ID do cliente (4 dígitos) | Customer ID (4 digits)
        "produto": produto,  # Nome do produto | Product name
        "quantidade": quantidade,  # Quantidade comprada | Quantity purchased
        "valor_unitario": preco_base,  # Preço unitário do produto | Unit product price
        "valor_total": round(preco_base * quantidade, 2),  # Valor total calculado (2 casas) | Total calculated value (2 decimals)
        "timestamp": datetime.now(timezone.utc).isoformat(),  # Timestamp UTC em formato ISO | UTC timestamp in ISO format
    }


def main():
    """
    Função principal do producer: loop infinito gerando e enviando pedidos.
    Main producer function: infinite loop generating and sending orders.
    """
    # Inicializa o produtor Kafka | Initialize Kafka producer
    producer = criar_producer()
    
    print(f"Produzindo pedidos no tópico '{TOPIC}'... (Ctrl+C pra parar)")
    print(f"Producing orders to topic '{TOPIC}'... (Ctrl+C to stop)")

    try:
        # Loop principal: gera e envia pedidos indefinidamente
        # Main loop: generates and sends orders indefinitely
        while True:
            # Gera um novo pedido falso | Generate a new fake order
            pedido = gerar_pedido()
            
            # Envia o pedido para o tópico Kafka | Send the order to the Kafka topic
            producer.send(TOPIC, value=pedido)
            
            # Log do pedido enviado | Log the sent order
            print(f"Enviado: {pedido['order_id']} | {pedido['produto']} | R$ {pedido['valor_total']}")
            print(f"Sent: {pedido['order_id']} | {pedido['produto']} | R$ {pedido['valor_total']}")
            
            # Simula intervalo aleatório entre pedidos (1 a 4 segundos)
            # Simulates random interval between orders (1 to 4 seconds)
            # Isso simula um fluxo realista de pedidos em um e-commerce
            # This simulates a realistic order flow in an e-commerce
            time.sleep(random.uniform(1, 4))
            
    except KeyboardInterrupt:
        # Interrupção manual pelo usuário (Ctrl+C) | Manual interruption by user
        print("\nEncerrando producer...")
        print("\nShutting down producer...")
    finally:
        # Garante que todas as mensagens pendentes sejam enviadas
        # Ensures all pending messages are sent
        producer.flush()
        
        # Fecha a conexão com o broker | Closes the broker connection
        producer.close()


if __name__ == "__main__":
    main()
