# Binance Data Pipeline

Pipeline de données pour l'ingestion, le traitement et l'analyse des données de marché Binance.

## 👥 Équipe

| Membre | Rôle | Module |
|--------|------|--------|
| **Haithem HENOUDA** | Ingestion & API | `src/ingestion/get-data.py` |
| **Hicham GUENDOUZ** | Batch Spark | `src/batch/binance-batch.py` |
| **Chaimae RAMDANI** | Docker & Cluster | `docker-compose.yml` |
| **Rayana ATTAOUI** | MongoDB & BI | Dashboards Power BI |

## 🏗️ Structure du projet

```
binance/
├── src/                    # Code source
│   ├── ingestion/         # Module d'ingestion (Haithem)
│   ├── batch/             # Module batch Spark (Hicham)
│   └── utils/             # Utilitaires partagés
├── docker/                # Configuration Docker (Chaimae)
│   ├── scripts/           # Scripts de lancement
│   ├── Dockerfile.base
│   └── Dockerfile.spark
├── config/                # Fichiers de configuration
├── data/                  # Données (gitignored)
│   ├── raw/              # CSV bruts
│   └── processed/        # Parquet traités
├── docs/                  # Documentation
├── tests/                 # Tests unitaires
├── logs/                  # Logs (gitignored)
└── scripts/               # Scripts utilitaires
```

## 🚀 Démarrage rapide

```bash
# Cloner le projet
git clone <repo-url>
cd binance

# Configuration initiale
cp .env.example .env
# Éditer .env avec vos credentials Binance

# Démarrer le cluster Docker
docker-compose up -d

# Vérifier les services
docker-compose ps
```

## 📊 Stack technique

- **Python 3.11** - Ingestion & Processing
- **Apache Spark 3.5** - Batch processing
- **HDFS** - Stockage distribué
- **MongoDB 7.0** - Base NoSQL
- **Docker** - Containerization
- **Power BI** - Visualisation

## 📚 Documentation

- Architecture détaillée: `docs/ARCHITECTURE.md`
- Guide de démarrage: `docs/GETTING_STARTED.md`
- Guides par rôle: `docs/ROLES.md`

## 📝 License

MIT License - © 2025 Chaimae RAMDANI, Rayana ATTAOUI, Haithem HENOUDA, Hicham GUENDOUZ
