# crypto-data-pipeline

Pipeline de dados de criptomoedas com execução local e arquitetura orientada a camadas:

`API -> Ingestão -> Data Lake -> Transformação -> Data Warehouse -> Dashboard`

## Visão geral
Este projeto coleta snapshots de mercado da CoinGecko, persiste dados brutos em data lake local, carrega a camada raw no PostgreSQL, transforma os dados com dbt e disponibiliza uma base analítica para dashboard.

## Arquitetura
- Fonte: CoinGecko API
- Ingestão: `ingestion/coingecko_ingestion.py`
- Data Lake raw local: `data_lake/raw/coingecko/market_snapshot/...`
- Carga raw no DW: `warehouse/load_raw_to_postgres.py`
- Orquestração: Airflow DAG `crypto_market_pipeline`
- Transformação: dbt (`staging` e `marts`)
- Consumo: Metabase (consultas iniciais em `dashboard/sql/metabase_queries.sql`)

Detalhamento textual da arquitetura: `docs/architecture.txt`.

## Stack
- Python
- Docker / Docker Compose
- Apache Airflow
- PostgreSQL
- dbt (Postgres)
- Metabase

## Estrutura do projeto
- `airflow/`: DAGs e logs locais do Airflow
- `docker/`: compose e Dockerfile do Airflow
- `ingestion/`: coleta e persistência raw
- `warehouse/`: DDL e carga no PostgreSQL
- `dbt/`: projeto dbt (staging + marts + testes)
- `dashboard/`: camada de consumo e consultas iniciais
- `docs/`: documentação técnica

## Pré-requisitos
- Docker e Docker Compose
- Python 3.11+

## Configuração
1. Copie o arquivo de ambiente:
   - `cp .env.example .env` (Linux/Mac)
   - `Copy-Item .env.example .env` (PowerShell)
2. Ajuste variáveis se necessário.

## Execução local
### 1) Subir serviços
```bash
docker compose -f docker/docker-compose.yml up -d --build
```

Serviços esperados:
- PostgreSQL: `localhost:5432`
- Airflow UI: `http://localhost:8080` (user `admin`, senha `admin`)
- Metabase: `http://localhost:3000`

### 2) Rodar ingestão manual (opcional)
```bash
python ingestion/coingecko_ingestion.py
```

### 3) Carregar raw no PostgreSQL (opcional)
```bash
python warehouse/load_raw_to_postgres.py
```

### 4) Rodar transformações dbt
```bash
pip install -r requirements.txt
set DBT_PROFILES_DIR=dbt   # PowerShell: $env:DBT_PROFILES_DIR='dbt'
dbt run --project-dir dbt
dbt test --project-dir dbt
```

### 5) Orquestração com Airflow
A DAG `crypto_market_pipeline` executa:
1. `ingest_market_snapshot`
2. `load_raw_to_postgres`

Agendamento: a cada hora (`0 * * * *`).

## Camadas de dados
- `raw.crypto_market`: tabela de snapshots brutos da CoinGecko
- `staging.stg_crypto_market`: normalização da camada raw
- `analytics.mart_crypto_daily_metrics`: métricas diárias por ativo

## Decisões técnicas
- Primeiro release com data lake local para reduzir acoplamento operacional.
- Estrutura de particionamento no raw preparada para evolução para S3.
- Pipeline com etapas separadas (ingestão, carga, transformação) para facilitar manutenção e observabilidade.

## Roadmap
1. Adicionar escrita em S3 mantendo fallback local
2. Incluir monitoramento/alertas no Airflow
3. Criar mais marts analíticos (tendência semanal, volatilidade)
4. Publicar dashboard com indicadores de preço, volume e ranking

## Estado atual
- Pipeline funcional em ambiente local com componentes principais implementados.
- Adoção de CI/CD e deploy em nuvem ainda não implementados.
