"""
Gerenciador de partições: cria automaticamente partições futuras para a tabela pedidos_streaming.
Deve ser executado periodicamente (ex: via cron/scheduler) para garantir que sempre haja
partições disponíveis para novos dados.

Partition manager: automatically creates future partitions for the pedidos_streaming table.
Should be run periodically (e.g., via cron/scheduler) to ensure partitions are always
available for new incoming data.
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


def criar_particao_futura():
    """
    Cria uma partição para o próximo mês na tabela pedidos_streaming.
    A partição é criada com base no mês atual + 1, garantindo que sempre haja
    espaço para dados futuros.
    
    Creates a partition for the next month in the pedidos_streaming table.
    The partition is created based on current month + 1, ensuring there's always
    space for future data.
    
    A estratégia de particionamento é mensal (por mês), o que facilita:
    - Manutenção (excluir partições antigas)
    - Performance de consultas por período
    - Gerenciamento de dados históricos
    
    The partitioning strategy is monthly, which facilitates:
    - Maintenance (dropping old partitions)
    - Query performance by time period
    - Historical data management
    """
    # ============ CÁLCULO DOS LIMITES DA PARTIÇÃO ============
    # Calculate partition boundaries (monthly ranges)
    hoje = datetime.now()  # Data atual | Current date
    
    # Calcula o primeiro dia do próximo mês | Calculate first day of next month
    # Exemplo: se hoje é 15/01/2026, proximo_mes será 01/02/2026
    # Example: if today is 01/15/2026, proximo_mes will be 02/01/2026
    proximo_mes = (hoje.replace(day=1) + timedelta(days=32)).replace(day=1)
    
    # Calcula o primeiro dia do mês seguinte ao próximo
    # Calculate first day of the month after next
    # Exemplo: se proximo_mes é 01/02/2026, mes_seguinte será 01/03/2026
    # Example: if proximo_mes is 02/01/2026, mes_seguinte will be 03/01/2026
    mes_seguinte = (proximo_mes + timedelta(days=32)).replace(day=1)
    
    # Gera o nome da partição no formato: pedidos_streaming_ANO_MES
    # Generate partition name in format: pedidos_streaming_YEAR_MONTH
    # Exemplo: pedidos_streaming_2026_02
    nome = f"pedidos_streaming_{proximo_mes.strftime('%Y_%m')}"

    with conn.cursor() as cur:
        # ============ VERIFICA SE A PARTIÇÃO JÁ EXISTE ============
        # Check if partition already exists
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables WHERE table_name = %s
            );
        """, (nome,))
        existe = cur.fetchone()[0]  # Retorna True ou False | Returns True or False

        if existe:
            print(f"Partição {nome} já existe, nada a fazer.")
            print(f"Partition {nome} already exists, nothing to do.")
            return

        # ============ CRIA A NOVA PARTIÇÃO ============
        # Create the new partition
        # A partição herda a estrutura da tabela pai (pedidos_streaming)
        # The partition inherits the structure from the parent table
        cur.execute(f"""
            CREATE TABLE {nome} PARTITION OF pedidos_streaming
            FOR VALUES FROM (%s) TO (%s);
        """, (proximo_mes, mes_seguinte))
        
        print(f"Partição criada: {nome}")
        print(f"Partition created: {nome}")
        print(f"  - Período: {proximo_mes.strftime('%Y-%m-%d')} até {mes_seguinte.strftime('%Y-%m-%d')}")
        print(f"  - Period: {proximo_mes.strftime('%Y-%m-%d')} to {mes_seguinte.strftime('%Y-%m-%d')}")

    # Confirma a transação no banco de dados | Commit the transaction
    conn.commit()


# ============ EXECUÇÃO PRINCIPAL ============
# Main execution
if __name__ == "__main__":
    print("=== Gerenciador de Partições do PostgreSQL ===")
    print("=== PostgreSQL Partition Manager ===")
    print(f"Executando em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Running at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    # Cria a partição para o próximo mês | Create partition for next month
    criar_particao_futura()
    
    # Fecha a conexão com o banco | Close database connection
    conn.close()
    print("Conexão encerrada.")
    print("Connection closed.")
