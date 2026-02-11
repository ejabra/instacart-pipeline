# 🛒 Instacart Real-Time Supply Chain Pipeline

<div align="center">

![Status](https://img.shields.io/badge/Status-Completed-success?style=flat-square)
![Python](https://img.shields.io/badge/Python-3.9-blue?style=flat-square&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-orange?style=flat-square&logo=docker&logoColor=white)
![ClickHouse](https://img.shields.io/badge/Database-ClickHouse-yellow?style=flat-square&logo=clickhouse&logoColor=black)
![Kafka](https://img.shields.io/badge/Streaming-Kafka-black?style=flat-square&logo=apachekafka&logoColor=white)

**Un pipeline Big Data de bout en bout pour prédire la demande et optimiser les stocks en temps réel.**
*Basé sur le dataset public Instacart.*

</div>

---

## 🚀 Objectif du Projet
**Réduire le gaspillage alimentaire et éviter les ruptures de stock grâce à une architecture Data Streaming et au Machine Learning.**

* 🔴 **Problème :** Gestion statique des stocks inefficace face à la volatilité de la demande.
* 🟢 **Solution :** Ingestion temps réel et prédiction du prochain achat utilisateur.
* 📈 **Performance ML :** Modèle Random Forest avec un **R² de 0.79**.

---

## 🏗️ Architecture Technique

![Architecture Globale](architecture.png)
*(Schéma du pipeline de données : De l'ingestion NiFi à la visualisation Streamlit)*

### 🛠️ Tech Stack

| Composant | Technologies | Rôle & Caractéristiques |
| :--- | :--- | :--- |
| **Ingestion** | ![NiFi](https://img.shields.io/badge/Apache_NiFi-728e9b?style=flat-square&logo=apache-nifi&logoColor=white) | Gestion de flux, Idempotence, Backpressure |
| **Streaming** | ![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apache-kafka&logoColor=white) | Message Broker haute performance & Zookeeper |
| **Stockage** | ![ClickHouse](https://img.shields.io/badge/ClickHouse-F5475B?style=flat-square&logo=clickhouse&logoColor=white) ![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=flat-square&logo=mysql&logoColor=white) | Analytics OLAP (ClickHouse) & Métadonnées (MySQL) |
| **Processing** | ![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white) | Pandas, Kafka-Python, OpenLineage |
| **ML & AI** | ![Scikit-Learn](https://img.shields.io/badge/scikit--learn-F7931E?style=flat-square&logo=scikit-learn&logoColor=white) | Random Forest (Prédiction de demande) |
| **Visu** | ![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white) ![PowerBI](https://img.shields.io/badge/Power_BI-F2C811?style=flat-square&logo=powerbi&logoColor=black) | Apps Data Temps Réel & Analyse historique |
| **Ops** | ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white) | Conteneurisation complète |

---

## 📊 DASHBOARD POWER BI

![DASHBOARD POWER BI](powerbi_1.png)

![DASHBOARD POWER BI](powerbi_2.png)

---

## 📦 Installation & Démarrage

Suivez ces étapes pour lancer le projet en local.

### 1. Pré-requis
* **Docker** & **Docker Compose** installés.
* **Python 3.9+** installé.
* **Git** installé.

### 2. Clonage du projet
```bash
git clone https://github.com/ejabra/Instacart-Pipeline.git
cd instacart-pipeline
```
### 3. Lancement de l'infrastructure (Docker)
Démarrez les conteneurs (Kafka, NiFi, ClickHouse, Zookeeper, Marquez).
```bash
docker-compose up -d
```
⚠️ Note : Assurez-vous que les ports 8080, 9092, 8123 et 3000 sont libres sur votre machine.

---

## ▶️ Utilisation
Étape 1 : Démarrer le Consumer (Enrichissement & Stockage)
Ce script écoute Kafka, enrichit les données via MySQL et les insère dans ClickHouse.
```bash
python consumer.py
```
### Étape 2 : Lancer le Dashboard de Monitoring
Visualisez les flux de données en temps réel et les prédictions.
```bash
streamlit run app.py
```

## 📊 Fonctionnalités Clés
✅ Ingestion Résiliente : Gestion des doublons (Deduplication) et transformation à la volée via Apache NiFi.

✅ Analytics Temps Réel : Calcul instantané des KPIs (Panier moyen, Top produits) grâce à la puissance de ClickHouse.

✅ Data Lineage : Traçabilité complète des données (Provenance) compatible avec OpenLineage/Marquez.

✅ Prédiction de Stock : Algorithme de Machine Learning pour estimer les volumes de commandes futurs.

## 👥 Auteurs
Ce projet a été réalisé dans le cadre du PFE JobInTech (Ynov Campus) par :

Brahim DARGUI - Data Engineering & Architecture

Nouhaila BENNANI - Data Analysis & Machine Learning

2025 - Projet Open Source à but éducatif.
