"""
Gerenciador de retenção de dados: remove automaticamente partições antigas da tabela pedidos_streaming.
Mantém apenas os últimos N meses de dados conforme política de retenção.
Deve ser executado periodicamente (ex: via cron/scheduler) para limpeza automática.

Data retention manager: automatically removes old partitions from the pedidos_streaming table.
Keeps only the last N months of data according to retention policy.
Should be run periodically (e.g., via cron/scheduler) for automatic cleanup.
"""
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Carrega variáveis de ambiente do arquivo .env
# Load environment variables from .env file
load_dotenv()

# ============ CONEXÃO COM O BANCO DE DADOS ============
# Database connection setup
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT"),
)


def apagar_particoes_antigas(meses_retencao=3):
    """
    Remove partições mais antigas que o período de retenção configurado.
    A política de retenção padrão mantém os últimos 3 meses de dados.
    
    Removes partitions older than the configured retention period.
    Default retention policy keeps the last 3 months of data.
    
    Args:
        meses_retencao (int): Número de meses de dados a manter (padrão: 3)
                              Number of months of data to keep (default: 3)
    
    A lógica de remoção funciona da seguinte forma:
    The removal logic works as follows:
    1. Calcula a data limite (primeiro dia do mês mais antigo a manter)
       Calculates the cutoff date (first day of the oldest month to keep)
    2. Lista todas as partições existentes no formato pedidos_streaming_YYYY_MM
       Lists all existing partitions in the format pedidos_streaming_YYYY_MM
    3. Remove aquelas com data anterior ao limite
       Removes those with date before the cutoff
    
    Benefícios do particionamento e retenção automática:
    Benefits of partitioning and automatic retention:
    - Controle de crescimento do banco de dados | Database growth control
    - Melhor performance de consultas | Better query performance
    - Conformidade com políticas de dados (GDPR, LGPD) | Compliance with data policies
    - Redução de custos de armazenamento | Storage cost reduction
    """
    # ============ CÁLCULO DA DATA LIMITE ============
    # Calculate the cutoff date
    # Exemplo: se hoje é 15/03/2026 e retenção é 3 meses, limite será 01/12/2025
    # Example: if today is 03/15/2026 and retention is 3 months, cutoff will be 12/01/2025
    limite = (datetime.now().replace(day=1) - timedelta(days=32 * meses_retencao)).replace(day=1)
    
    print(f"Política de retenção: {meses_retencao} meses")
    print(f"Retention policy: {meses_retencao} months")
    print(f"Data limite: {limite.strftime('%Y-%m-%d')} (partições anteriores a esta data serão removidas)")
    print(f"Cutoff date: {limite.strftime('%Y-%m-%d')} (partitions before this date will be removed)")
    print("-" * 50)

    with conn.cursor() as cur:
        # ============ LISTA TODAS AS PARTIÇÕES ============
        # List all partitions
        # Busca todas as tabelas que seguem o padrão de nomenclatura das partições
        # Finds all tables that follow the partition naming pattern
        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE tablename LIKE 'pedidos_streaming_%%';
        """)
        particoes = [row[0] for row in cur.fetchall()]
        
        print(f"Total de partições encontradas: {len(particoes)}")
        print(f"Total partitions found: {len(particoes)}")
        
        # ============ CONTADORES PARA LOG ============
        # Counters for logging
        removidas = 0
        mantidas = 0

        for particao in particoes:
            # ============ EXTRAI DATA DO NOME DA PARTIÇÃO ============
            # Extract date from partition name
            try:
                # Remove o prefixo "pedidos_streaming_" para obter "YYYY_MM"
                # Remove the prefix to get "YYYY_MM"
                ano_mes = particao.replace("pedidos_streaming_", "")
                
                # Converte string "YYYY_MM" para objeto datetime
                # Convert "YYYY_MM" string to datetime object
                data_particao = datetime.strptime(ano_mes, "%Y_%m")
            except ValueError:
                # Se o nome não estiver no formato esperado, pula esta partição
                # If the name is not in the expected format, skip this partition
                print(f"Aviso: Partição com nome inválido ignorada: {particao}")
                print(f"Warning: Partition with invalid name skipped: {particao}")
                continue

            # ============ DECIDE SE REMOVE OU MANTÉM ============
            # Decide whether to remove or keep
            if data_particao < limite:
                # Partição é mais antiga que o limite → remover
                # Partition is older than cutoff → remove
                try:
                    cur.execute(f"DROP TABLE {particao};")
                    print(f"🗑️  Partição removida: {particao} (período: {data_particao.strftime('%Y-%m')})")
                    print(f"🗑️  Partition dropped: {particao} (period: {data_particao.strftime('%Y-%m')})")
                    removidas += 1
                except Exception as e:
                    # Log de erro caso a remoção falhe | Error log if removal fails
                    print(f"❌ Erro ao remover partição {particao}: {e}")
                    print(f"❌ Error removing partition {particao}: {e}")
            else:
                # Partição está dentro do período de retenção → manter
                # Partition is within retention period → keep
                mantidas += 1
                print(f"✓ Partição mantida: {particao} (período: {data_particao.strftime('%Y-%m')})")
                print(f"✓ Partition kept: {particao} (period: {data_particao.strftime('%Y-%m')})")

        # ============ RESUMO DA OPERAÇÃO ============
        # Operation summary
        print("-" * 50)
        print(f"Resumo: {removidas} partição(ões) removida(s), {mantidas} partição(ões) mantida(s)")
        print(f"Summary: {removidas} partition(s) dropped, {mantidas} partition(s) kept")

    # Confirma a transação no banco de dados | Commit the transaction
    conn.commit()


# ============ EXECUÇÃO PRINCIPAL ============
# Main execution
if __name__ == "__main__":
    print("=== Gerenciador de Retenção de Dados - PostgreSQL ===")
    print("=== PostgreSQL Data Retention Manager ===")
    print(f"Executando em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Running at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    # Executa a limpeza de partições antigas (mantém últimos 3 meses por padrão)
    # Execute cleanup of old partitions (keeps last 3 months by default)
    apagar_particoes_antigas(meses_retencao=3)
    
    # Fecha a conexão com o banco | Close database connection
    conn.close()
    print("Conexão encerrada.")
    print("Connection closed.")
