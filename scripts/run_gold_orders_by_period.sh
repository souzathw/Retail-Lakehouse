#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Execução manual Gold orders_by_period"
echo "=================================================="

docker compose exec spark /opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/spark-apps/jobs/gold/gold_orders_by_period.py

echo ""
echo "[OK] Execução manual da Gold de orders_by_period concluída."