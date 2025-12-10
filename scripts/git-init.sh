#!/bin/bash

##############################################################################
# Script d'initialisation et push Git pour le projet Binance
##############################################################################

echo "🚀 Initialisation du repository Git..."

# Ajouter tous les fichiers
git add .

# Vérifier ce qui sera commité
echo ""
echo "📦 Fichiers à commiter:"
git status --short

echo ""
read -p "Continuer avec le commit? (y/n) " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]
then
    # Commit initial
    git commit -m "Initial commit: Structure complète du projet Binance Data Pipeline

    Structure créée:
    - src/: Modules ingestion, batch, utils
    - docker/: Configuration Docker et scripts
    - data/: Dossiers avec .gitkeep
    - logs/: Dossiers avec .gitkeep
    - tests/: Structure de tests
    - docs/: Documentation complète
    - config/: Configurations Spark
    
    Équipe:
    - Haithem HENOUDA: Ingestion & API
    - Hicham GUENDOUZ: Batch Spark
    - Chaimae RAMDANI: Docker & Cluster
    - Rayana ATTAOUI: MongoDB & BI"
    
    echo ""
    echo "✅ Commit créé avec succès!"
    echo ""
    echo "📤 Prochaines étapes:"
    echo "1. Créer un repo sur GitHub/GitLab"
    echo "2. Ajouter le remote: git remote add origin <url>"
    echo "3. Push: git push -u origin main"
    echo ""
    echo "Exemple:"
    echo "  git remote add origin https://github.com/username/binance-pipeline.git"
    echo "  git push -u origin main"
else
    echo "❌ Commit annulé"
fi
