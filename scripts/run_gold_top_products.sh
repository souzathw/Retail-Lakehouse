#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Execução manual Gold top_products"
echo "=================================================="

docker compose exec spark /opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/spark-apps/jobs/gold/gold_top_products.py

echo ""
echo "[OK] Execução manual da Gold de top_products concluída."