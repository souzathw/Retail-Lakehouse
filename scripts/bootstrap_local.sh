#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Bootstrap local"
echo "=================================================="

echo ""
echo "[1/6] Verificando se o arquivo .env existe..."
if [ ! -f ".env" ]; then
  echo "ERRO: arquivo .env não encontrado na raiz do projeto."
  exit 1
fi
echo "OK: .env encontrado."

echo ""
echo "[2/6] Criando diretórios necessários..."
mkdir -p airflow/dags
mkdir -p airflow/logs
mkdir -p airflow/plugins
mkdir -p airflow/config
mkdir -p ingestion/src
mkdir -p ingestion/tests
mkdir -p spark/jobs/bronze
mkdir -p spark/jobs/silver
mkdir -p spark/jobs/gold
mkdir -p spark/conf
mkdir -p spark/libs
mkdir -p sql/athena
mkdir -p sql/redshift
mkdir -p sql/quality
mkdir -p dbt/retail_analytics/models/staging
mkdir -p dbt/retail_analytics/models/intermediate
mkdir -p dbt/retail_analytics/models/marts
mkdir -p dbt/retail_analytics/tests
mkdir -p dbt/retail_analytics/macros
mkdir -p dbt/retail_analytics/seeds
mkdir -p dbt/retail_analytics/snapshots
mkdir -p tests/unit
mkdir -p tests/integration
mkdir -p .github/workflows
mkdir -p data/raw
mkdir -p data/bronze
mkdir -p data/silver
mkdir -p data/gold

echo ""
echo "[3/6] Garantindo arquivos .gitkeep nas pastas importantes..."
touch airflow/dags/.gitkeep
touch airflow/logs/.gitkeep
touch airflow/plugins/.gitkeep
touch airflow/config/.gitkeep
touch ingestion/src/.gitkeep
touch ingestion/tests/.gitkeep
touch spark/jobs/bronze/.gitkeep
touch spark/jobs/silver/.gitkeep
touch spark/jobs/gold/.gitkeep
touch spark/conf/.gitkeep
touch spark/libs/.gitkeep
touch sql/athena/.gitkeep
touch sql/redshift/.gitkeep
touch sql/quality/.gitkeep
touch dbt/retail_analytics/models/staging/.gitkeep
touch dbt/retail_analytics/models/intermediate/.gitkeep
touch dbt/retail_analytics/models/marts/.gitkeep
touch dbt/retail_analytics/tests/.gitkeep
touch dbt/retail_analytics/macros/.gitkeep
touch dbt/retail_analytics/seeds/.gitkeep
touch dbt/retail_analytics/snapshots/.gitkeep
touch tests/unit/.gitkeep
touch tests/integration/.gitkeep
touch .github/workflows/.gitkeep

echo ""
echo "[4/6] Ajustando permissões do diretório de logs do Airflow..."
chmod -R 777 airflow/logs || true

echo ""
echo "[5/6] Subindo banco Postgres do Airflow..."
docker compose up -d postgres

echo ""
echo "[6/6] Executando inicialização do Airflow..."
docker compose up airflow-init

echo ""
echo "Bootstrap concluído com sucesso."
echo ""
echo "Próximos comandos:"
echo "  make up"
echo "  make ps"
echo "  make smoke"
echo ""
echo "Depois abra no navegador:"
echo "  http://localhost:8080"
echo "Usuário: admin"
echo "Senha: admin"