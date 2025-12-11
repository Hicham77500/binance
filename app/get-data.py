import requests
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime


# Configuration depuis variables d'environnement
BINANCE_URL = os.getenv("BINANCE_API_URL", "https://data-api.binance.vision/api/v3/ticker/24hr")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "namenode")
HDFS_PORT = os.getenv("HDFS_PORT", "9000")
HDFS_PATH = os.getenv("HDFS_PATH", "/users/ipssi/input/binance_batch")

HDFS_FULL_PATH = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}{HDFS_PATH}"


# ============================================================
# 1) Spark Session (Cluster Mode)
# ============================================================
def create_spark_session():
    spark_master = os.getenv("SPARK_MASTER_HOST", "spark-master")
    spark_port = os.getenv("SPARK_MASTER_PORT", "7077")
    
    return (
        SparkSession.builder
        .appName("BinanceBatchIngestion")
        .master(f"spark://{spark_master}:{spark_port}")
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )


# ============================================================
# 2) Fetch data from Binance API (WITH RETRY + EXPONENTIAL BACKOFF)
# ============================================================
def fetch_binance_data(retries=5, initial_delay=2):
    """
    Récupère les données Binance avec retry exponentiel.
    Gère les erreurs 429 (rate limit) avec backoff progressif.
    """
    print(f"[{datetime.now()}] Fetching Binance 24h ticker data from {BINANCE_URL}")

    delay = initial_delay
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(BINANCE_URL, timeout=15)

            if response.status_code == 200:
                data = response.json()
                print(f"✓ Successfully fetched {len(data)} tickers")
                return data

            elif response.status_code == 429:
                print(f"⚠ Rate limit hit (429). Waiting {delay}s before retry...")
            else:
                print(f"✗ Attempt {attempt}/{retries} failed: HTTP {response.status_code}")
        
        except requests.exceptions.Timeout:
            print(f"✗ Attempt {attempt}/{retries} - Request timeout")
        except requests.exceptions.ConnectionError as e:
            print(f"✗ Attempt {attempt}/{retries} - Connection error: {e}")
        except Exception as e:
            print(f"✗ Attempt {attempt}/{retries} - Unexpected error: {e}")

        if attempt < retries:
            print(f"   Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2  # Exponential backoff
        else:
            raise Exception(f"Failed to fetch Binance data after {retries} attempts")

    raise Exception("Failed to fetch Binance data after multiple retries.")


# ============================================================
# 3) Convert JSON to Spark DataFrame avec validation
# ============================================================
def create_dataframe(spark, json_data):
    """
    Crée un DataFrame Spark à partir des données JSON.
    Ajoute timestamp d'ingestion et date de batch.
    """
    if not json_data:
        raise ValueError("Empty data received from Binance API")
    
    print(f"Creating DataFrame from {len(json_data)} records...")
    
    df = spark.createDataFrame(json_data)
    
    # Ajout des métadonnées temporelles
    df = df.withColumn("timestamp_ingestion", current_timestamp()) \
           .withColumn("batch_date", lit(datetime.now().strftime("%Y-%m-%d")))
    
    print(f"✓ DataFrame created: {df.count()} rows, {len(df.columns)} columns")
    return df


# ============================================================
# 4) Write to HDFS en mode APPEND (historique préservé)
# ============================================================
def write_to_hdfs(df):
    """
    Écrit les données dans HDFS en mode APPEND.
    Partitionné par date pour faciliter les requêtes.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{HDFS_FULL_PATH}/{timestamp}"

    print(f"[{datetime.now()}] Writing to HDFS → {output_path}")
    print(f"   Mode: append (historical data preserved)")
    print(f"   Format: CSV with header")

    try:
        df.coalesce(1).write \
            .format("csv") \
            .mode("append") \
            .option("header", "true") \
            .option("compression", "none") \
            .save(output_path)

        print(f"✓ HDFS write COMPLETED: {df.count()} records written")
        return output_path
    
    except Exception as e:
        print(f"✗ HDFS write FAILED: {e}")
        raise


# ============================================================
# MAIN - Pipeline d'ingestion avec gestion d'erreurs
# ============================================================
if __name__ == "__main__":
    start_time = datetime.now()
    print("=" * 60)
    print(f"BINANCE BATCH INGESTION STARTED at {start_time}")
    print("=" * 60)

    spark = None
    
    try:
        # 1. Création de la session Spark
        print("\n[Step 1/4] Creating Spark session...")
        spark = create_spark_session()
        print(f"✓ Spark session created: {spark.sparkContext.appName}")

        # 2. Récupération des données Binance
        print("\n[Step 2/4] Fetching Binance data...")
        data = fetch_binance_data()

        # 3. Création du DataFrame
        print("\n[Step 3/4] Creating DataFrame...")
        df = create_dataframe(spark, data)

        # 4. Écriture dans HDFS
        print("\n[Step 4/4] Writing to HDFS...")
        output_path = write_to_hdfs(df)

        # Succès
        duration = (datetime.now() - start_time).total_seconds()
        print("\n" + "=" * 60)
        print(f"✓ BATCH INGESTION COMPLETED SUCCESSFULLY")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Output: {output_path}")
        print("=" * 60)

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ BATCH INGESTION FAILED")
        print(f"  Error: {e}")
        print("=" * 60)
        raise

    finally:
        if spark:
            spark.stop()
            print("Spark session stopped.")
