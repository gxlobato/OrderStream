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
