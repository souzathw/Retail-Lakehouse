#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Execução manual Silver order_items_train_enriched"
echo "=================================================="

docker compose exec spark /opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/spark-apps/jobs/silver/silver_order_items_train_enriched.py

echo ""
echo "[OK] Execução manual da Silver de order_items_train_enriched concluída."