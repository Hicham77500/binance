#!/usr/bin/env python3
"""Ingestion d'un JSON local (response_1.json) vers HDFS + archivage local."""

import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Configuration (surchargée via variables d'environnement au besoin)
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "namenode")
HDFS_PORT = os.getenv("HDFS_PORT", "9000")
HDFS_PATH = os.getenv("HDFS_PATH", "/users/ipssi/input/binance_batch")

JSON_FILE_PATH = "/app/response_1.json"  # Chemin dans le conteneur (sans espace)
ARCHIVE_PATH = "/app/data/raw/response_1_archived.json"

def create_spark_session():
    """Crée une session Spark connectée au cluster (master défini par env)."""
    spark_master = os.getenv("SPARK_MASTER_HOST", "spark-master")
    spark_port = os.getenv("SPARK_MASTER_PORT", "7077")
    
    return (
        SparkSession.builder
        .appName("BinanceTestJSONIngestion")
        .master(f"spark://{spark_master}:{spark_port}")
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}")
        .getOrCreate()
    )

def ingest_json_to_hdfs():
    """Ingère le fichier JSON de test dans HDFS avec timestamp et métadonnées."""
    print(f"[{datetime.now()}] === Ingestion du fichier JSON de test ===")

    if not os.path.exists(JSON_FILE_PATH):
        print(f"[ERROR] Fichier {JSON_FILE_PATH} non trouvé!")
        return False

    # Charger le fichier en local pour éviter les problèmes d'accès des executors
    try:
        with open(JSON_FILE_PATH, "r", encoding="utf-8") as f:
            raw = f.read().rstrip()
            # Certains dumps contiennent un '%' final : on le retire si présent
            if raw.endswith("%"):
                raw = raw[:-1]
            data = json.loads(raw)
    except Exception as e:
        print(f"[ERROR] Lecture/parse du JSON échouée: {e}")
        return False

    if not isinstance(data, list) or not data:
        print("[ERROR] Le contenu JSON n'est pas une liste non vide")
        return False

    spark = None  # initialisé ici pour pouvoir le stopper en finally
    try:
        spark = create_spark_session()

        # Création du DataFrame à partir des données locales (pas de lecture distribuée)
        print(f"[{datetime.now()}] Création du DataFrame depuis {len(data)} enregistrements")
        df = spark.createDataFrame(data)

        # Ajouter métadonnées
        df_enriched = df \
            .withColumn("timestamp_ingestion", current_timestamp()) \
            .withColumn("batch_date", lit(datetime.now().strftime("%Y-%m-%d"))) \
            .withColumn("source", lit("test_response_1_json"))

        # Créer le dossier avec timestamp (suffixe _test pour tracer la source)
        batch_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        hdfs_full_path = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}{HDFS_PATH}/{batch_timestamp}_test"

        print(f"[{datetime.now()}] Écriture vers HDFS : {hdfs_full_path}")
        df_enriched.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(hdfs_full_path)

        print(f"[{datetime.now()}] ✅ Ingestion réussie : {df_enriched.count()} lignes écrites")
        return True

    except Exception as e:
        print(f"[ERROR] Échec de l'ingestion : {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if spark:
            spark.stop()

def archive_json_file():
    """Archive le fichier JSON après ingestion (copie + suppression source)."""
    try:
        # Créer le répertoire d'archive s'il n'existe pas
        os.makedirs(os.path.dirname(ARCHIVE_PATH), exist_ok=True)
        
        # Copier le fichier
        import shutil
        shutil.copy2(JSON_FILE_PATH, ARCHIVE_PATH)
        
        print(f"[{datetime.now()}] ✅ Fichier archivé : {ARCHIVE_PATH}")
        
        # Supprimer l'original
        os.remove(JSON_FILE_PATH)
        print(f"[{datetime.now()}] ✅ Fichier original supprimé : {JSON_FILE_PATH}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Échec de l'archivage : {str(e)}")
        return False

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"[{datetime.now()}] Démarrage de l'ingestion JSON de test")
    print(f"{'='*60}\n")
    
    # Étape 1 : Ingérer dans HDFS
    if ingest_json_to_hdfs():
        # Étape 2 : Archiver le fichier
        archive_json_file()
        print(f"\n[{datetime.now()}] 🎉 Processus terminé avec succès!")
    else:
        print(f"\n[{datetime.now()}] ❌ Échec du processus")
        exit(1)
