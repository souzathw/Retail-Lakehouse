#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Execução manual Bronze orders"
echo "=================================================="

docker compose exec spark spark-submit \
  --master local[*] \
  /opt/spark-apps/jobs/bronze/bronze_orders.py

echo ""
echo "[OK] Execução manual da Bronze de orders concluída."