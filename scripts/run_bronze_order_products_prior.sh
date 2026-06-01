#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Execução manual Bronze order_products_prior"
echo "=================================================="

docker compose exec spark spark-submit \
  --master local[*] \
  /opt/spark-apps/jobs/bronze/bronze_order_products_prior.py

echo ""
echo "[OK] Execução manual da Bronze de order_products_prior concluída."