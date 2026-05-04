#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Execução manual Bronze aisles"
echo "=================================================="

docker compose exec spark spark-submit \
  --master local[*] \
  /opt/spark-apps/jobs/bronze/bronze_aisles.py

echo ""
echo "[OK] Execução manual da Bronze de aisles concluída."