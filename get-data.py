import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from datetime import datetime


BINANCE_URL = "https://data-api.binance.vision/api/v3/ticker/24hr"
HDFS_PATH = "hdfs://namenode:9000/users/ipssi/input/binance_batch"


# ============================================================
# 1) Spark Session (Cluster Mode)
# ============================================================
def create_spark_session():
    return (
        SparkSession.builder
        .appName("BinanceBatchIngestion")
        .master("spark://spark-master:7077")      
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


# ============================================================
# 2) Fetch data from Binance API (WITH RETRY)
# ============================================================
def fetch_binance_data(retries=3, delay=5):
    print("Fetching Binance 24h ticker data...")

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(BINANCE_URL, timeout=10)

            if r.status_code == 200:
                return r.json()

            print(f"Attempt {attempt}/{retries} failed: HTTP {r.status_code}")
        
        except Exception as e:
            print(f"Attempt {attempt}/{retries} failed: {e}")

        if attempt < retries:
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)

    raise Exception("Failed to fetch Binance data after multiple retries.")


# ============================================================
# 3) Convert JSON to Spark DataFrame
# ============================================================
def create_dataframe(spark, json_data):
    df = spark.createDataFrame(json_data)
    return df.withColumn("timestamp_ingestion", current_timestamp())


# ============================================================
# 4) Write to HDFS
# ============================================================
def write_to_hdfs(df):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{HDFS_PATH}/{timestamp}"

    print(f"Writing CSV to → {output_path}")

    df.coalesce(1).write \
        .format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .save(output_path)

    print("HDFS write COMPLETED.")


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    spark = create_spark_session()

    data = fetch_binance_data()
    df = create_dataframe(spark, data)
    write_to_hdfs(df)

    spark.stop()
    print("Batch ingestion FINISHED.")
