#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Execução manual Bronze order_products_train"
echo "=================================================="

docker compose exec spark /opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/spark-apps/jobs/bronze/bronze_order_products_train.py

echo ""
echo "[OK] Execução manual da Bronze de order_products_train concluída."