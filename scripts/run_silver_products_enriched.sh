#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Execução manual Silver products_enriched"
echo "=================================================="

docker compose exec spark /opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/spark-apps/jobs/silver/silver_products_enriched.py

echo ""
echo "[OK] Execução manual da Silver de products_enriched concluída."