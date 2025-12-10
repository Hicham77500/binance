#!/bin/bash

echo "=== Running Binance Batch Job ==="

# chemin pour spark-submit dans l'image spark-masterdocker exec -it spark-master bash

/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /app/get-data.py

echo "=== Job Finished Successfully ==="
git status
