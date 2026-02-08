# 🛒 Instacart Real-Time Supply Chain Pipeline

![Status](https://img.shields.io/badge/Status-Completed-success)
![Python](https://img.shields.io/badge/Python-3.9-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-orange)
![ClickHouse](https://img.shields.io/badge/Database-ClickHouse-yellow)
![Kafka](https://img.shields.io/badge/Streaming-Kafka-black)

Un pipeline Big Data de bout en bout pour prédire la demande et optimiser les stocks en temps réel, basé sur le dataset public **Instacart**.

---

## 🚀 Objectif du Projet
Réduire le gaspillage alimentaire et éviter les ruptures de stock grâce à une architecture Data Streaming et au Machine Learning.

* **Problème :** Gestion statique des stocks inefficace face à la volatilité de la demande.
* **Solution :** Ingestion temps réel et prédiction du prochain achat utilisateur.
* **Performance ML :** Modèle Random Forest avec un **R² de 0.79**.

---

## 🏗️ Architecture Technique

Le projet suit un flux ETL/ELT moderne entièrement conteneurisé :

```mermaid
graph LR
A[Ingestion: Apache NiFi] -->|JSON Stream| B(Broker: Kafka)
B -->|Consumer Python| C{Processing & Enrichment}
C -->|Storage| D[(ClickHouse OLAP)]
C -->|Metadata| E[(MySQL)]
D -->|Visualization| F[Streamlit Dashboard]
D -->|BI| G[Power BI]
🛠️ Tech Stack
Ingestion : Apache NiFi (Gestion de flux, Idempotence, Backpressure)

Streaming : Apache Kafka & Zookeeper (Message Broker haute performance)

Stockage : * ClickHouse (Big Data Analytics - OLAP)

MySQL (Lookup & Métadonnées relationnelles)

Processing : Python (Pandas, Kafka-Python, OpenLineage)

Machine Learning : Scikit-learn (Random Forest pour la prédiction de demande)

Visualisation : Streamlit (Apps Data Temps Réel) & Power BI (Analyse historique)

Infrastructure : Docker & Docker Compose

📦 Installation & Démarrage
Pré-requis
Docker & Docker Compose

Python 3.9+

Git

1. Cloner le projet
Bash

git clone [https://github.com/votre-username/instacart-pipeline.git](https://github.com/votre-username/instacart-pipeline.git)
cd instacart-pipeline
2. Lancer l'infrastructure (Docker)
Assurez-vous que les ports 8080 (NiFi), 9092 (Kafka), 8123 (ClickHouse) et 3000 (Marquez) sont libres.

Bash

docker-compose up -d
Vérifiez que les conteneurs sont bien lancés via docker ps.

3. Installer les dépendances Python
Il est recommandé d'utiliser un environnement virtuel.

Bash

pip install -r requirements.txt
4. Lancer le Pipeline
Démarrer le Consumer (Enrichissement & Stockage) :

Bash

python consumer.py
Lancer le Dashboard ML (Streamlit) :

Bash

streamlit run app.py
📊 Fonctionnalités Clés
✅ Ingestion Résiliente : Gestion des doublons (Deduplication) et transformation à la volée via NiFi.

✅ Analytics Temps Réel : Calcul instantané des indicateurs clés (KPIs) via ClickHouse.

✅ Data Lineage : Traçabilité des flux de données (Compatible OpenLineage/Marquez).

✅ Prédiction de Stock : Estimation des volumes de commandes par produit et par jour pour la Supply Chain.

👥 Auteurs
Brahim DARGUI - Data Engineer & Architecture
Nouhaila BENNANI - Data Analyst & Machine Learning

Projet de fin de formation - Ynov Campus (2025)