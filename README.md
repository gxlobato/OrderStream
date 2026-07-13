🇧🇷 Português | [🇺🇸 English](#english-version)

# OrderStream - Streaming de Pedidos E-commerce (Kafka + Redpanda + Neon)

Pipeline de dados em **tempo real** que simula pedidos de e-commerce chegando um a um, processa cada evento assim que chega (não em lote) e grava métricas calculadas em um banco Postgres na nuvem.

Diferente de pipelines batch tradicionais (ex: rodar 1x por dia via GitHub Actions), aqui cada pedido é tratado individualmente no momento em que acontece — mais próximo do que se vê em sistemas de produção reais.

## Arquitetura

```
producer.py  →  Redpanda Cloud (tópico "pedidos")  →  consumer.py  →  Neon (Postgres)
```

- **`producer.py`**: gera pedidos falsos com `Faker` e publica no tópico Kafka `pedidos`
- **Redpanda Cloud Serverless**: broker compatível com a API do Kafka, sem precisar gerenciar infraestrutura
- **`consumer.py`**: consome os eventos em tempo real (consumer group `grupo-metricas`), calcula métricas (total do dia, ticket médio) e insere no Neon
- **Neon**: Postgres serverless, tabela `pedidos_streaming` particionada por mês

## Stack

| Camada | Tecnologia |
|---|---|
| Streaming | Redpanda Cloud Serverless (Kafka API) |
| Cliente Kafka | `kafka-python==3.0.8` |
| Geração de dados | Faker |
| Banco de dados | Neon (Postgres serverless) |
| Driver DB | `psycopg2` |

## Como rodar

```bash
pip install -r requirements.txt
```

Copie `.env.example` para `.env` e preencha com suas credenciais (broker e usuário/senha SASL do Redpanda, dados de conexão do Neon).

Em dois terminais separados:

```bash
python producer.py
python consumer.py
```

## Estrutura do projeto

```
OrderStream/
├── producer.py
├── consumer.py
├── maintenance/
│   ├── criar_particao_futura.py
│   └── apagar_particoes_antigas.py
├── requirements.txt
├── .env.example
└── README.md
```

## Retenção de dados

A tabela `pedidos_streaming` é **particionada por mês** (`PARTITION BY RANGE (event_timestamp)`), para evitar que uma única tabela cresça indefinidamente e fique lenta de consultar.

- `maintenance/criar_particao_futura.py`: cria a partição do próximo mês antecipadamente
- `maintenance/apagar_particoes_antigas.py`: remove partições com mais de 3 meses

**Sobre automação:** essas rotinas foram desenhadas para rodar automaticamente via **Agendador de Tarefas do Windows** (mensalmente), usando um script `.bat` que ativa o ambiente virtual e executa os dois scripts em sequência. Neste projeto específico, optei por **não agendar** e rodar os scripts manualmente quando necessário — mas a estrutura já está pronta para automação, caso o projeto evolua.

## Problemas enfrentados e soluções

Documentar isso é proposital: mostra o processo de debug, não só o resultado final.

| Problema | Causa | Solução |
|---|---|---|
| `KafkaTimeoutError` ao conectar | Porta 9092 bloqueada pela rede Wi-Fi doméstica | Confirmado com `Test-NetConnection`; contornado usando hotspot do celular |
| `GroupAuthorizationFailedError` | Usuário sem permissão configurada no Redpanda Cloud | Criação de ACLs explícitas para o tópico (`pedidos`) e para o consumer group (`grupo-metricas`) |
| Dados consumidos mas tabela vazia no Neon | Faltava `conn.commit()` após o `INSERT` | Adicionado commit explícito na função de gravação |
| Biblioteca errada testada (`aiokafka`) | Projeto usa `kafka-python`, não `aiokafka` | Ajuste dos scripts de teste para a lib correta |

## Limitações conhecidas

- O `DROP TABLE` nas partições antigas é executado sem passo de confirmação/backup — aceitável para um projeto de portfólio, mas não recomendado em produção sem uma etapa de dry-run antes
- Sem tratamento de reprocessamento em caso de falha do consumer (não há checkpoint além do offset do próprio Kafka)

# English Version

# OrderStream - Real-Time E-commerce Order Streaming (Kafka + Redpanda + Neon)

A **real-time data pipeline** that simulates e-commerce orders arriving one by one, processes each event as soon as it is received (instead of in batches), and stores calculated metrics in a cloud-hosted PostgreSQL database.

Unlike traditional batch pipelines (e.g., running once a day through GitHub Actions), this project processes each order individually at the moment it occurs, closely resembling how real production streaming systems operate.

## Architecture

```text
producer.py  →  Redpanda Cloud (topic: "pedidos")  →  consumer.py  →  Neon (Postgres)
```

- **`producer.py`**: generates fake orders using `Faker` and publishes them to the Kafka topic `pedidos`
- **Redpanda Cloud Serverless**: Kafka API-compatible broker with no infrastructure management required
- **`consumer.py`**: consumes events in real time (consumer group `grupo-metricas`), calculates metrics (daily revenue and average order value), and inserts the results into Neon
- **Neon**: Serverless PostgreSQL database with a `pedidos_streaming` table partitioned by month

## Tech Stack

| Layer | Technology |
|--------|------------|
| Streaming | Redpanda Cloud Serverless (Kafka API) |
| Kafka Client | `kafka-python==3.0.8` |
| Data Generation | Faker |
| Database | Neon (Serverless PostgreSQL) |
| Database Driver | `psycopg2` |

## Getting Started

Install the dependencies:

```bash
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your credentials:

- Redpanda broker
- SASL username and password
- Neon database connection details

Run the producer and consumer in separate terminals:

```bash
python producer.py
python consumer.py
```

## Project Structure

```text
OrderStream/
├── producer.py
├── consumer.py
├── maintenance/
│   ├── criar_particao_futura.py
│   └── apagar_particoes_antigas.py
├── requirements.txt
├── .env.example
└── README.md
```

## Data Retention

The `pedidos_streaming` table is **partitioned by month** (`PARTITION BY RANGE (event_timestamp)`), preventing a single table from growing indefinitely and improving query performance.

Maintenance scripts:

- `maintenance/criar_particao_futura.py`: creates the partition for the next month in advance
- `maintenance/apagar_particoes_antigas.py`: removes partitions older than three months

### Automation

These maintenance routines were designed to run automatically through the **Windows Task Scheduler** on a monthly basis using a `.bat` script that activates the virtual environment and executes both scripts sequentially.

For this portfolio project, I chose **not** to schedule these tasks and instead run them manually when needed. However, the project structure is already prepared for automation if it evolves into a production-like environment.

## Challenges and Solutions

Documenting these issues is intentional—it demonstrates the debugging process, not just the final implementation.

| Issue | Cause | Solution |
|-------|-------|----------|
| `KafkaTimeoutError` when connecting | Port 9092 was blocked by the home Wi-Fi network | Confirmed using `Test-NetConnection`; temporarily solved by using a mobile hotspot |
| `GroupAuthorizationFailedError` | Consumer user lacked the required permissions in Redpanda Cloud | Created explicit ACLs for both the topic (`pedidos`) and the consumer group (`grupo-metricas`) |
| Messages consumed but Neon table remained empty | Missing `conn.commit()` after the `INSERT` statement | Added an explicit commit after each database write |
| Incorrect library tested (`aiokafka`) | The project uses `kafka-python`, not `aiokafka` | Updated the test scripts to use the correct client library |

## Known Limitations

- Old partitions are removed using `DROP TABLE` without a confirmation or backup step. This is acceptable for a portfolio project but would not be recommended in production without a dry-run or backup procedure.
- There is no consumer reprocessing strategy beyond Kafka's native offsets. If the consumer fails after processing but before persisting data, additional recovery logic would be required in a production environment.
