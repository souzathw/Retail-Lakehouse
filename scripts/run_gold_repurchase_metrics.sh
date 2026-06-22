#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Execução manual Gold repurchase_metrics"
echo "=================================================="

docker compose exec spark /opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/spark-apps/jobs/gold/gold_repurchase_metrics.py

echo ""
echo "[OK] Execução manual da Gold de repurchase_metrics concluída."