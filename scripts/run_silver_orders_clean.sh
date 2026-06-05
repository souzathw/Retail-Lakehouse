#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Execução manual Silver orders_clean"
echo "=================================================="

docker compose exec spark /opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/spark-apps/jobs/silver/silver_orders_clean.py

echo ""
echo "[OK] Execução manual da Silver de orders_clean concluída."