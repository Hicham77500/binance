# -*- coding: utf-8 -*-
"""
Batch KPI Spark : lit le dernier batch CSV sur HDFS, calcule des KPI, puis
les envoie vers MongoDB pour consommation (ex. Power BI). Configuration
pilotée par variables d'environnement.
"""

import os
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)
from pyspark.sql import SparkSession
from pymongo import MongoClient

def get_env(name: str, default: str) -> str:
    """Retourne une variable d'environnement avec valeur par défaut."""
    return os.getenv(name, default)

def create_spark_session():
    """Crée une session Spark cluster en lisant la config via les env vars."""
    master = get_env("SPARK_MASTER_HOST", "spark-master")
    master_port = get_env("SPARK_MASTER_PORT", "7077")
    hdfs_nn = get_env("HDFS_NAMENODE", "namenode")
    hdfs_port = get_env("HDFS_PORT", "9000")
    spark = (
        SparkSession.builder
        .appName("BinanceBatchKPI")
        .master(f"spark://{master}:{master_port}")
        .config("spark.executor.memory", get_env("SPARK_EXECUTOR_MEMORY", "2g"))
        .config("spark.driver.memory", get_env("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", get_env("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{hdfs_nn}:{hdfs_port}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

SCHEMA = StructType([
    # Colonnes prix/volumes : String -> cast plus tard pour tolérer des vides
    StructField("askPrice", StringType(), True),
    StructField("askQty", StringType(), True),
    StructField("bidPrice", StringType(), True),
    StructField("bidQty", StringType(), True),
    # Timestamps epoch millis : Long
    StructField("closeTime", LongType(), True),
    StructField("count", LongType(), True),
    StructField("firstId", LongType(), True),
    StructField("highPrice", StringType(), True),
    StructField("lastId", LongType(), True),
    StructField("lastPrice", StringType(), True),
    StructField("lastQty", StringType(), True),
    StructField("lowPrice", StringType(), True),
    StructField("openPrice", StringType(), True),
    StructField("openTime", LongType(), True),
    StructField("prevClosePrice", StringType(), True),
    StructField("priceChange", StringType(), True),
    StructField("priceChangePercent", StringType(), True),
    StructField("quoteVolume", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("weightedAvgPrice", StringType(), True),
    StructField("timestamp_ingestion", TimestampType(), True)
])

def get_latest_hdfs_subdir(spark: SparkSession) -> str:
    """Retourne le sous-dossier HDFS le plus récent (tri lexicographique)."""
    hdfs_nn = get_env("HDFS_NAMENODE", "namenode")
    hdfs_port = get_env("HDFS_PORT", "9000")
    hdfs_base = get_env("HDFS_PATH", "/users/ipssi/input/binance_batch")
    base_uri = f"hdfs://{hdfs_nn}:{hdfs_port}{hdfs_base}"

    # Liste HDFS via l'API Java (pas de subprocess) grâce à la gateway Py4J
    jsc = spark.sparkContext._jsc
    jvm = spark._jvm
    hconf = jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    Path = jvm.org.apache.hadoop.fs.Path

    status = fs.listStatus(Path(base_uri))  # FileStatus[]
    if status is None or len(status) == 0:
        raise FileNotFoundError(f"No subdirectories found in {base_uri}")

    # Ne garder que les dossiers (noms au format YYYYMMDD_HHMMSS) puis trier
    dirs = [s for s in status if s.isDirectory()]
    if not dirs:
        raise FileNotFoundError(f"No directory entries in {base_uri}")
    latest = sorted(dirs, key=lambda s: s.getPath().getName())[-1]
    latest_uri = latest.getPath().toString()
    return latest_uri

def read_latest_batch(spark: SparkSession):
    latest_uri = get_latest_hdfs_subdir(spark)
    print(f"Reading latest HDFS batch: {latest_uri}")
    df = spark.read.schema(SCHEMA).csv(latest_uri)  # Schéma explicite
    # Cast des métriques utilisées dans les KPI
    df = df.withColumn("lastPrice", col("lastPrice").cast(DoubleType()))
    df = df.withColumn("quoteVolume", col("quoteVolume").cast(DoubleType()))
    return df

def compute_kpis(df):
    print("Computing KPIs...")
    # Distinct de paires crypto
    count_cryptos = df.select(col("symbol")).where(col("symbol").isNotNull()).distinct().count()

    # Min/Max/Avg sur le prix (ignore null/0)
    last_price_min = df.where((col("lastPrice").isNotNull()) & (col("lastPrice") != 0)).agg({"lastPrice": "min"}).collect()[0][0]
    last_price_max = df.where(col("lastPrice").isNotNull()).agg({"lastPrice": "max"}).collect()[0][0]
    last_price_avg = df.where((col("lastPrice").isNotNull()) & (col("lastPrice") != 0)).agg({"lastPrice": "avg"}).collect()[0][0]

    # Timestamp d'ingestion le plus récent
    ts_row = df.select(col("timestamp_ingestion")).where(col("timestamp_ingestion").isNotNull()).orderBy(col("timestamp_ingestion").desc()).limit(1).collect()
    current_timestamp = ts_row[0][0] if ts_row else None

    # Top 20 asc/desc par volume quote (filtre valeurs nulles)
    base_sel = df.select(col("symbol"), col("lastPrice"), col("quoteVolume")).where(
        (col("lastPrice").isNotNull()) & (col("quoteVolume").isNotNull()) & (col("lastPrice") != 0)
    )
    top_asc = base_sel.orderBy(col("quoteVolume").asc()).limit(20)
    top_desc = base_sel.orderBy(col("quoteVolume").desc()).limit(20)

    return {
        "count_cryptos": int(count_cryptos) if count_cryptos is not None else 0,
        "last_price_min": float(last_price_min) if last_price_min is not None else None,
        "last_price_max": float(last_price_max) if last_price_max is not None else None,
        "last_price_avg": float(last_price_avg) if last_price_avg is not None else None,
        "current_timestamp": str(current_timestamp) if current_timestamp is not None else None,
        "top_asc": top_asc,
        "top_desc": top_desc,
    }

def get_mongo_client():
    # Connexion Mongo paramétrable (pas de secrets en dur)
    mongo_user = get_env("MONGO_INITDB_ROOT_USERNAME", "admin")
    mongo_pass = get_env("MONGO_INITDB_ROOT_PASSWORD", "SecurePassword123")
    mongo_host = get_env("MONGO_HOST", "mongo")
    mongo_port = get_env("MONGO_PORT", "27017")
    mongo_db = get_env("MONGO_DATABASE", "binance")
    uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/?authSource=admin"
    client = MongoClient(uri)
    return client, client[mongo_db]

def persist_to_mongo(kpis, db):
    print("Persisting KPIs to MongoDB...")
    # Document global des KPI
    stats = {
        "count_cryptos": kpis["count_cryptos"],
        "last_price_min": kpis["last_price_min"],
        "last_price_max": kpis["last_price_max"],
        "last_price_avg": kpis["last_price_avg"],
        "current_timestamp": kpis["current_timestamp"],
        "created_at": os.getenv("BATCH_DATE") or None,
    }
    db.binance_stats.insert_one(stats)

    def to_docs(df):
        # Collect volontaire (petits datasets : top 20)
        rows = df.collect()
        return [
            {
                "symbol": r[0],
                "lastPrice": float(r[1]) if r[1] is not None else None,
                "quoteVolume": float(r[2]) if r[2] is not None else None,
            }
            for r in rows
        ]

    asc_docs = to_docs(kpis["top_asc"])
    desc_docs = to_docs(kpis["top_desc"])
    if asc_docs:
        db.binance_batch_top_20_asc.insert_many(asc_docs)
    if desc_docs:
        db.binance_batch_top_20_desc.insert_many(desc_docs)
    print(f"Inserted {len(asc_docs)} asc and {len(desc_docs)} desc docs")

def main():
    print("=== Binance Batch KPI ===")
    spark = None
    try:
        spark = create_spark_session()
        df = read_latest_batch(spark)
        print(f"Rows: {df.count()}, Columns: {len(df.columns)}")
        kpis = compute_kpis(df)
        client, db = get_mongo_client()
        try:
            persist_to_mongo(kpis, db)
        finally:
            client.close()
            print("MongoDB connection closed")
        print("=== Completed ===")
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
