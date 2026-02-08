#!/usr/bin/env python3
"""
Machine Learning - Prédiction de Réachat depuis MongoDB
Source: Collections MongoDB (client_profiles, product_stats)
Modèle: Random Forest pour recommandation produits
Déploiement: Modèle sauvé pour API Streamlit
"""

# ==================== IMPORTS ====================

import pandas as pd
import numpy as np
from pymongo import MongoClient
import pickle
from datetime import datetime
import json
import time
import warnings
warnings.filterwarnings('ignore')

# ML Libraries
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, 
    roc_auc_score, classification_report, confusion_matrix
)
import joblib

print("✅ Imports réussis")

# ==================== CONFIGURATION ====================

MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'instacart_cache'
}

MODEL_CONFIG = {
    'model_path': 'models/reorder_model.pkl',
    'scaler_path': 'models/scaler.pkl',
    'encoders_path': 'models/encoders.pkl',
    'metadata_path': 'models/model_metadata.json'
}

# Créer le dossier models s'il n'existe pas
import os
os.makedirs('models', exist_ok=True)

print("✅ Configuration chargée")

# ==================== 1. CONNEXION MONGODB ====================

def connect_mongodb():
    """
    Connexion à MongoDB et récupération des collections
    
    Returns:
        db: Database MongoDB 'instacart_cache'
    """
    try:
        client = MongoClient(
            host=MONGODB_CONFIG['host'],
            port=MONGODB_CONFIG['port'],
            serverSelectionTimeoutMS=5000
        )
        
        # Test de connexion
        client.admin.command('ping')
        
        db = client[MONGODB_CONFIG['database']]
        
        print(f"✅ Connexion MongoDB réussie")
        print(f"   Database: {MONGODB_CONFIG['database']}")
        
        # Afficher les collections disponibles
        collections = db.list_collection_names()
        print(f"   Collections: {collections}")
        
        return db
    
    except Exception as e:
        print(f"❌ Erreur connexion MongoDB: {e}")
        return None

# ==================== 2. CHARGEMENT DES DONNÉES ====================

def load_training_data(db):
    """
    Charger les données d'entraînement depuis MongoDB
    
    Stratégie:
    - Profils clients → Features comportementales
    - Stats produits → Features de popularité
    - Jointure sur user_id et product_id
    
    Args:
        db: Database MongoDB
        
    Returns:
        df: DataFrame avec features et target
    """
    print("\n" + "="*70)
    print("📊 CHARGEMENT DES DONNÉES DEPUIS MONGODB")
    print("="*70)
    
    # 1. Charger les profils clients
    print("\n1️⃣ Chargement des profils clients...")
    clients_cursor = db['client_profiles'].find()
    df_clients = pd.DataFrame(list(clients_cursor))
    
    if df_clients.empty:
        print("❌ Aucun profil client trouvé dans MongoDB")
        return None
    
    # Nettoyer les colonnes inutiles
    df_clients = df_clients.drop(columns=['_id', 'updated_at'], errors='ignore')
    
    print(f"   ✅ {len(df_clients)} clients chargés")
    print(f"   Colonnes: {list(df_clients.columns)}")
    
    # 2. Charger les stats produits
    print("\n2️⃣ Chargement des statistiques produits...")
    products_cursor = db['product_stats'].find()
    df_products = pd.DataFrame(list(products_cursor))
    
    if df_products.empty:
        print("❌ Aucun produit trouvé dans MongoDB")
        return None
    
    df_products = df_products.drop(columns=['_id', 'updated_at'], errors='ignore')
    
    print(f"   ✅ {len(df_products)} produits chargés")
    print(f"   Colonnes: {list(df_products.columns)}")
    
    # 3. Créer le dataset d'entraînement
    # Stratégie: Pour chaque client, générer des paires (client, produit)
    # avec un échantillonnage équilibré
    
    print("\n3️⃣ Génération du dataset d'entraînement...")
    
    # Prendre un échantillon de clients (pour éviter trop de données)
    sample_clients = df_clients.sample(min(5000, len(df_clients)), random_state=42)
    
    # Pour chaque client, prendre ses top produits (label=1) 
    # et des produits aléatoires (label=0)
    
    training_samples = []
    
    for idx, client in sample_clients.iterrows():
        user_id = client['user_id']
        
        # Top produits du client (déjà achetés = reordered=1)
        top_products = client.get('top_5_produits', [])
        
        # Nettoyer la liste (peut être une string ou une liste)
        if isinstance(top_products, str):
            top_products = [p.strip() for p in top_products.split(',') if p.strip()]
        
        # Produits positifs (achetés)
        for product_name in top_products[:5]:  # Max 5 produits positifs
            product_row = df_products[df_products['product_name'] == product_name]
            
            if not product_row.empty:
                product_id = product_row.iloc[0]['product_id']
                
                sample = {
                    'user_id': user_id,
                    'product_id': product_id,
                    'product_name': product_name,
                    
                    # Features client
                    'total_commandes': client['total_commandes'],
                    'panier_moyen': client['panier_moyen'],
                    'taux_reachat_client': client['taux_reachat'],
                    
                    # Features produit
                    'nb_ventes_produit': product_row.iloc[0]['nb_ventes'],
                    'taux_reachat_produit': product_row.iloc[0]['taux_reachat'],
                    'position_moyenne_panier': product_row.iloc[0]['position_moyenne_panier'],
                    
                    # Target
                    'reordered': 1  # Produit déjà acheté
                }
                
                training_samples.append(sample)
        
        # Produits négatifs (aléatoires, jamais achetés)
        random_products = df_products.sample(5, random_state=42)
        
        for _, product in random_products.iterrows():
            product_name = product['product_name']
            
            # Vérifier que ce produit n'est PAS dans les top produits du client
            if product_name not in top_products:
                sample = {
                    'user_id': user_id,
                    'product_id': product['product_id'],
                    'product_name': product_name,
                    
                    # Features client
                    'total_commandes': client['total_commandes'],
                    'panier_moyen': client['panier_moyen'],
                    'taux_reachat_client': client['taux_reachat'],
                    
                    # Features produit
                    'nb_ventes_produit': product['nb_ventes'],
                    'taux_reachat_produit': product['taux_reachat'],
                    'position_moyenne_panier': product['position_moyenne_panier'],
                    
                    # Target
                    'reordered': 0  # Produit jamais acheté
                }
                
                training_samples.append(sample)
    
    # Convertir en DataFrame
    df_training = pd.DataFrame(training_samples)
    
    print(f"   ✅ {len(df_training)} échantillons créés")
    print(f"   Distribution de la target:")
    print(df_training['reordered'].value_counts())
    
    return df_training

# ==================== 3. FEATURE ENGINEERING ====================

def prepare_features(df):
    """
    Préparer les features pour le ML
    
    Steps:
    1. Sélectionner les colonnes numériques
    2. Encoder les IDs si nécessaire
    3. Normaliser les features
    
    Args:
        df: DataFrame brut
        
    Returns:
        X: Features (array)
        y: Target (array)
        feature_names: Noms des features
        encoders: Dictionnaire des encoders
    """
    print("\n" + "="*70)
    print("🔧 FEATURE ENGINEERING")
    print("="*70)
    
    # Features numériques à utiliser
    feature_columns = [
        'total_commandes',
        'panier_moyen',
        'taux_reachat_client',
        'nb_ventes_produit',
        'taux_reachat_produit',
        'position_moyenne_panier'
    ]
    
    print(f"\n✅ Features sélectionnées: {feature_columns}")
    
    # Vérifier les valeurs manquantes
    missing = df[feature_columns].isnull().sum()
    if missing.any():
        print(f"\n⚠️ Valeurs manquantes détectées:")
        print(missing[missing > 0])
        
        # Remplir avec la médiane
        df[feature_columns] = df[feature_columns].fillna(df[feature_columns].median())
    
    # Features dérivées
    df['popularity_ratio'] = df['nb_ventes_produit'] / df['nb_ventes_produit'].max()
    df['client_loyalty'] = df['taux_reachat_client'] * df['total_commandes']
    df['product_affinity'] = df['taux_reachat_produit'] * df['popularity_ratio']
    
    feature_columns.extend(['popularity_ratio', 'client_loyalty', 'product_affinity'])
    
    print(f"✅ Features dérivées ajoutées")
    print(f"   Total features: {len(feature_columns)}")
    
    # Séparer X et y
    X = df[feature_columns].values
    y = df['reordered'].values
    
    print(f"\n✅ Dataset final:")
    print(f"   X shape: {X.shape}")
    print(f"   y shape: {y.shape}")
    print(f"   Positive samples: {(y == 1).sum()} ({(y == 1).sum() / len(y) * 100:.1f}%)")
    print(f"   Negative samples: {(y == 0).sum()} ({(y == 0).sum() / len(y) * 100:.1f}%)")
    
    return X, y, feature_columns, {}

# ==================== 4. ENTRAÎNEMENT DU MODÈLE ====================

def train_model(X, y):
    """
    Entraîner le modèle Random Forest
    
    Steps:
    1. Split train/test
    2. Normalisation des features
    3. Entraînement Random Forest
    4. Évaluation
    
    Args:
        X: Features
        y: Target
        
    Returns:
        model: Modèle entraîné
        scaler: Scaler pour normalisation
        metrics: Métriques de performance
    """
    print("\n" + "="*70)
    print("🎯 ENTRAÎNEMENT DU MODÈLE")
    print("="*70)
    
    # 1. Split train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    print(f"\n✅ Split des données:")
    print(f"   Train: {X_train.shape[0]} samples")
    print(f"   Test:  {X_test.shape[0]} samples")
    
    # 2. Normalisation
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    print(f"\n✅ Normalisation appliquée")
    
    # 3. Entraînement Random Forest
    print(f"\n🌲 Entraînement Random Forest...")
    
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=15,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
        verbose=1
    )
    
    import time
    start_time = time.time()
    
    model.fit(X_train_scaled, y_train)
    
    train_time = time.time() - start_time
    
    print(f"\n✅ Entraînement terminé en {train_time:.2f}s")
    
    # 4. Évaluation
    print(f"\n📊 ÉVALUATION DU MODÈLE")
    print("="*70)
    
    # Prédictions
    y_pred_train = model.predict(X_train_scaled)
    y_pred_test = model.predict(X_test_scaled)
    y_proba_test = model.predict_proba(X_test_scaled)[:, 1]
    
    # Métriques
    metrics = {
        'train_accuracy': accuracy_score(y_train, y_pred_train),
        'test_accuracy': accuracy_score(y_test, y_pred_test),
        'precision': precision_score(y_test, y_pred_test),
        'recall': recall_score(y_test, y_pred_test),
        'f1_score': f1_score(y_test, y_pred_test),
        'auc_roc': roc_auc_score(y_test, y_proba_test),
        'train_time': train_time
    }
    
    print(f"\n📈 Train Accuracy : {metrics['train_accuracy']:.4f}")
    print(f"📈 Test Accuracy  : {metrics['test_accuracy']:.4f}")
    print(f"📈 Precision      : {metrics['precision']:.4f}")
    print(f"📈 Recall         : {metrics['recall']:.4f}")
    print(f"📈 F1-Score       : {metrics['f1_score']:.4f}")
    print(f"📈 AUC-ROC        : {metrics['auc_roc']:.4f}")
    
    # Feature importance
    print(f"\n🔍 Feature Importance (Top 5):")
    feature_importance = pd.DataFrame({
        'feature': [f'feature_{i}' for i in range(X.shape[1])],
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(feature_importance.head())
    
    # Matrice de confusion
    cm = confusion_matrix(y_test, y_pred_test)
    print(f"\n📊 Confusion Matrix:")
    print(f"   TN: {cm[0,0]:<6} FP: {cm[0,1]:<6}")
    print(f"   FN: {cm[1,0]:<6} TP: {cm[1,1]:<6}")
    
    return model, scaler, metrics

# ==================== 5. SAUVEGARDE DU MODÈLE ====================

def save_model(model, scaler, encoders, feature_names, metrics):
    """
    Sauvegarder le modèle et les métadonnées
    
    Args:
        model: Modèle entraîné
        scaler: Scaler
        encoders: Encoders
        feature_names: Noms des features
        metrics: Métriques de performance
    """
    print("\n" + "="*70)
    print("💾 SAUVEGARDE DU MODÈLE")
    print("="*70)
    
    # Sauvegarder le modèle
    joblib.dump(model, MODEL_CONFIG['model_path'])
    print(f"✅ Modèle sauvegardé: {MODEL_CONFIG['model_path']}")
    
    # Sauvegarder le scaler
    joblib.dump(scaler, MODEL_CONFIG['scaler_path'])
    print(f"✅ Scaler sauvegardé: {MODEL_CONFIG['scaler_path']}")
    
    # Sauvegarder les encoders
    joblib.dump(encoders, MODEL_CONFIG['encoders_path'])
    print(f"✅ Encoders sauvegardés: {MODEL_CONFIG['encoders_path']}")
    
    # Sauvegarder les métadonnées
    metadata = {
        'training_date': datetime.now().isoformat(),
        'feature_names': feature_names,
        'metrics': metrics,
        'model_type': 'RandomForestClassifier',
        'n_features': len(feature_names)
    }
    
    with open(MODEL_CONFIG['metadata_path'], 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"✅ Métadonnées sauvegardées: {MODEL_CONFIG['metadata_path']}")
    
    print(f"\n✅ Modèle prêt pour déploiement !")

# ==================== 6. PIPELINE COMPLET ====================

def run_ml_pipeline():
    """
    Exécuter le pipeline ML complet
    
    Steps:
    1. Connexion MongoDB
    2. Chargement données
    3. Feature engineering
    4. Entraînement
    5. Sauvegarde
    """
    print("=" * 70)
    print("🚀 PIPELINE ML - PRÉDICTION DE RÉACHAT")
    print("=" * 70)
    print(f"⏰ Démarrage: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Connexion MongoDB
    db = connect_mongodb()
    if db is None:
        print("❌ Impossible de continuer sans MongoDB")
        return False
    
    # 2. Chargement données
    df = load_training_data(db)
    if df is None or df.empty:
        print("❌ Aucune donnée chargée")
        return False
    
    # 3. Feature engineering
    X, y, feature_names, encoders = prepare_features(df)
    
    # 4. Entraînement
    model, scaler, metrics = train_model(X, y)
    
    # 5. Sauvegarde
    save_model(model, scaler, encoders, feature_names, metrics)
    
    print("\n" + "=" * 70)
    print("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
    print("=" * 70)
    
    return True


# ==================== EXÉCUTION ====================

if __name__ == "__main__":
    import sys
    
    # Mode daemon (boucle infinie) ou one-shot
    if len(sys.argv) > 1 and sys.argv[1] == '--daemon':
        print("🔁 Mode DAEMON : Exécution toutes les heures")
        while True:
            run_ml_pipeline()
            print(f"\n⏳ Prochaine exécution dans 24 heures...")
            time.sleep(86400)  # 24 heures
    else:
        print("📌 Mode ONE-SHOT : Exécution unique")
        success = run_ml_pipeline()
        sys.exit(0 if success else 1)