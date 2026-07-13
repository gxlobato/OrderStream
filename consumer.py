"""
Consumer: le pedidos do Kafka em tempo real, calcula metricas
e grava tanto o evento bruto quanto o agregado diario no Neon.
Rode em um terminal separado do producer.py.

Consumer: reads real-time orders from Kafka, calculates metrics,
and saves both raw events and daily aggregates to Neon.
Run in a separate terminal from producer.py.
"""
import json
import os
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Carrega variáveis de ambiente do arquivo .env
# Load environment variables from .env file
load_dotenv()

# ============ CONFIGURAÇÕES DO KAFKA/REDPANDA ============
# Kafka/Redpanda broker connection settings
BROKER = os.getenv("REDPANDA_BROKER")
USERNAME = os.getenv("REDPANDA_USERNAME")
PASSWORD = os.getenv("REDPANDA_PASSWORD")
TOPIC = os.getenv("TOPIC", "pedidos")  # Tópico padrão: "pedidos" | Default topic: "orders"

# ============ CONFIGURAÇÕES DO BANCO DE DADOS (NEON/POSTGRES) ============
# Database connection settings for Neon/PostgreSQL
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")  # Porta padrão do PostgreSQL | Default PostgreSQL port


def conectar_db():
    """
    Estabelece conexão com o banco de dados Neon/PostgreSQL.
    Establishes connection to Neon/PostgreSQL database.
    
    Returns:
        psycopg2.connection: Conexão ativa com autocommit habilitado
        Active connection with autocommit enabled
    """
    conn_str = (
        f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} "
        f"password={DB_PASSWORD} port={DB_PORT} sslmode=require"
    )
    conn = psycopg2.connect(conn_str)
    conn.autocommit = True  # Automaticamente confirma cada transação | Automatically commits each transaction
    return conn


def criar_tabelas(conn):
    """
    Cria as tabelas necessárias se não existirem.
    Creates necessary tables if they don't exist.
    
    Args:
        conn: Conexão ativa com o banco de dados | Active database connection
    """
    with conn.cursor() as cur:
        # Tabela para armazenar eventos brutos de pedidos
        # Table for storing raw order events
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pedidos_streaming (
                order_id UUID PRIMARY KEY,           -- Identificador único do pedido | Unique order identifier
                customer_id INT,                     -- ID do cliente | Customer ID
                produto TEXT,                        -- Nome do produto | Product name
                quantidade INT,                      -- Quantidade comprada | Quantity purchased
                valor_unitario NUMERIC(10,2),       -- Preço unitário | Unit price
                valor_total NUMERIC(10,2),          -- Valor total do pedido | Total order value
                event_timestamp TIMESTAMPTZ,        -- Timestamp do evento (do produtor) | Event timestamp (from producer)
                processed_at TIMESTAMPTZ DEFAULT now()  -- Timestamp do processamento | Processing timestamp
            );
        """)
        
        # Tabela para armazenar métricas agregadas por dia
        # Table for storing daily aggregated metrics
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agg_metricas_diarias (
                dia DATE PRIMARY KEY,                -- Data de referência | Reference date
                total_pedidos INT,                   -- Total de pedidos no dia | Total orders for the day
                valor_total_dia NUMERIC(12,2),      -- Faturamento total do dia | Total revenue for the day
                ticket_medio NUMERIC(10,2),          -- Ticket médio do dia | Average ticket for the day
                updated_at TIMESTAMPTZ DEFAULT now() -- Última atualização | Last update timestamp
            );
        """)


def criar_consumer() -> KafkaConsumer:
    """
    Configura e retorna um consumidor Kafka com autenticação SASL.
    Configures and returns a Kafka consumer with SASL authentication.
    
    Returns:
        KafkaConsumer: Consumidor configurado para o tópico de pedidos
        Consumer configured for the orders topic
    """
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        security_protocol="SASL_SSL",  # Protocolo de segurança com SSL | Security protocol with SSL
        sasl_mechanism="SCRAM-SHA-256",  # Mecanismo de autenticação | Authentication mechanism
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),  # Converte JSON para dict | Convert JSON to dict
        auto_offset_reset="latest",  # Começa a ler mensagens mais recentes | Start reading from latest messages
        group_id="grupo-metricas",  # Grupo de consumidores para balanceamento de carga | Consumer group for load balancing
    )


def gravar_pedido(conn, pedido: dict):
    """
    Insere um pedido bruto na tabela de streaming.
    Inserts a raw order into the streaming table.
    
    Args:
        conn: Conexão ativa com o banco | Active database connection
        pedido (dict): Dicionário com dados do pedido | Order data dictionary
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pedidos_streaming
                (order_id, customer_id, produto, quantidade, valor_unitario, valor_total, event_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;  -- Evita duplicatas | Prevents duplicates
        """, (
            pedido["order_id"], 
            pedido["customer_id"], 
            pedido["produto"],
            pedido["quantidade"], 
            pedido["valor_unitario"], 
            pedido["valor_total"],
            pedido["timestamp"],  # Timestamp do produtor | Producer timestamp
        ))


def atualizar_metricas_diarias(conn, dia: str):
    """
    Atualiza as métricas agregadas para um dia específico com base nos dados brutos.
    Updates aggregated metrics for a specific day based on raw data.
    
    Args:
        conn: Conexão ativa com o banco | Active database connection
        dia (str): Data no formato YYYY-MM-DD | Date in YYYY-MM-DD format
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO agg_metricas_diarias (dia, total_pedidos, valor_total_dia, ticket_medio)
            SELECT
                %s::date,  -- Converte string para date | Convert string to date
                COUNT(*),  -- Total de pedidos | Total orders
                SUM(valor_total),  -- Soma do valor total | Sum of total values
                ROUND(SUM(valor_total) / COUNT(*), 2)  -- Ticket médio com 2 casas | Average ticket with 2 decimals
            FROM pedidos_streaming
            WHERE event_timestamp::date = %s::date  -- Filtra pelo dia | Filter by day
            ON CONFLICT (dia) DO UPDATE SET
                total_pedidos = EXCLUDED.total_pedidos,
                valor_total_dia = EXCLUDED.valor_total_dia,
                ticket_medio = EXCLUDED.ticket_medio,
                updated_at = now();  -- Atualiza timestamp | Update timestamp
        """, (dia, dia))


def main():
    """
    Função principal do consumer: loop infinito processando mensagens do Kafka.
    Main consumer function: infinite loop processing messages from Kafka.
    """
    # Estabelece conexão com o banco de dados | Establish database connection
    conn = conectar_db()
    
    # Garante que as tabelas existem | Ensures tables exist
    criar_tabelas(conn)
    
    # Inicializa o consumidor Kafka | Initialize Kafka consumer
    consumer = criar_consumer()

    print(f"Consumindo do tópico '{TOPIC}'... (Ctrl+C pra parar)")
    print(f"Consuming from topic '{TOPIC}'... (Ctrl+C to stop)")
    
    try:
        # Loop principal: processa cada mensagem recebida
        # Main loop: processes each received message
        for msg in consumer:
            pedido = msg.value  # Extrai o payload da mensagem | Extract message payload
            
            # Passo 1: Grava o evento bruto | Step 1: Save raw event
            gravar_pedido(conn, pedido)
            
            # Passo 2: Atualiza métricas do dia atual | Step 2: Update current day metrics
            dia = datetime.now(timezone.utc).date().isoformat()  # Data atual UTC | Current UTC date
            atualizar_metricas_diarias(conn, dia)
            
            print(f"Processado: {pedido['order_id']} -> métricas do dia atualizadas")
            print(f"Processed: {pedido['order_id']} -> day metrics updated")
            
    except KeyboardInterrupt:
        # Interrupção manual pelo usuário (Ctrl+C) | Manual interruption by user
        print("\nEncerrando consumer...")
        print("\nShutting down consumer...")
    finally:
        # Libera recursos ao finalizar | Clean up resources on exit
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
