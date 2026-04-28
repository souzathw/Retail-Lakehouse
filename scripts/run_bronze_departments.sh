#!/bin/bash

set -e

echo "=================================================="
echo "Retail Lakehouse - Execução manual Bronze departments"
echo "=================================================="

docker compose exec spark spark-submit \
  --master local[*] \
  /opt/spark-apps/jobs/bronze/bronze_departments.py

echo ""
echo "[OK] Execução manual concluída."