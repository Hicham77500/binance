#!/usr/bin/env bash
set -euo pipefail

LOG_DIR=/app/logs
mkdir -p "$LOG_DIR"
TS=$(date +"%Y-%m-%d_%H-%M-%S")

echo "[$TS] Starting hourly ingestion + batch..." | tee -a "$LOG_DIR/hourly.log"

# Ingestion
/spark/bin/spark-submit --master spark://spark-master:7077 /app/get-data.py >> "$LOG_DIR/ingest_$TS.log" 2>&1

# Batch KPIs to Mongo
/spark/bin/spark-submit --master spark://spark-master:7077 /app/spark_batch.py >> "$LOG_DIR/batch_$TS.log" 2>&1

echo "[$TS] Completed." | tee -a "$LOG_DIR/hourly.log"
