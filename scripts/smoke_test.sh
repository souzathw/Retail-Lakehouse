#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Smoke test local"
echo "=================================================="

echo ""
echo "[1/7] Verificando containers..."
docker compose ps

echo ""
echo "[2/7] Verificando se o Postgres está em execução..."
if docker compose ps postgres | grep -q "Up"; then
  echo "OK: Postgres está rodando."
else
  echo "ERRO: Postgres não está rodando."
  exit 1
fi

echo ""
echo "[3/7] Verificando se o Airflow webserver está em execução..."
if docker compose ps airflow-webserver | grep -q "Up"; then
  echo "OK: Airflow webserver está rodando."
else
  echo "ERRO: Airflow webserver não está rodando."
  exit 1
fi

echo ""
echo "[4/7] Verificando se o Airflow scheduler está em execução..."
if docker compose ps airflow-scheduler | grep -q "Up"; then
  echo "OK: Airflow scheduler está rodando."
else
  echo "ERRO: Airflow scheduler não está rodando."
  exit 1
fi

echo ""
echo "[5/7] Verificando se o Spark container está em execução..."
if docker compose ps spark | grep -q "Up"; then
  echo "OK: Spark está rodando."
else
  echo "ERRO: Spark não está rodando."
  exit 1
fi

echo ""
echo "[6/7] Testando acesso HTTP ao Airflow..."
if curl -I http://localhost:8088 2>/dev/null | head -n 1 | grep -q "200\|302"; then
  echo "OK: Airflow respondeu em http://localhost:8088"
else
  echo "ERRO: Airflow não respondeu corretamente em http://localhost:8088"
  exit 1
fi

echo ""
echo "[7/7] Smoke test concluído com sucesso."
echo ""
echo "Ambiente local validado."