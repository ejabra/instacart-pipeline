#!/usr/bin/env python3
"""
Interface Streamlit - Recommandation de Produits
Charge le modèle ML et fait des prédictions en temps réel
"""

import streamlit as st
import pandas as pd
import numpy as np
from pymongo import MongoClient
import joblib
import json
import os

# ==================== CONFIGURATION ====================

st.set_page_config(
    page_title="Recommandation Produits - ML",
    page_icon="🤖",
    layout="wide"
)

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

# ==================== CHARGEMENT DU MODÈLE ====================

@st.cache_resource
def load_model():
    """
    Charger le modèle ML sauvegardé
    
    Returns:
        model, scaler, metadata
    """
    try:
        model = joblib.load(MODEL_CONFIG['model_path'])
        scaler = joblib.load(MODEL_CONFIG['scaler_path'])
        
        with open(MODEL_CONFIG['metadata_path'], 'r') as f:
            metadata = json.load(f)
        
        return model, scaler, metadata
    except Exception as e:
        st.error(f"❌ Erreur chargement modèle: {e}")
        return None, None, None

# ==================== CONNEXION MONGODB ====================

@st.cache_resource
def get_mongodb_connection():
    """
    Connexion MongoDB
    
    Returns:
        db: Database
    """
    try:
        client = MongoClient(
            host=MONGODB_CONFIG['host'],
            port=MONGODB_CONFIG['port'],
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ping')
        
        return client[MONGODB_CONFIG['database']]
    except Exception as e:
        st.error(f"❌ Erreur MongoDB: {e}")
        return None

# ==================== FONCTIONS UTILITAIRES ====================

def get_client_profile(db, user_id):
    """
    Récupérer le profil d'un client depuis MongoDB
    
    Args:
        db: Database MongoDB
        user_id: ID du client
        
    Returns:
        dict: Profil du client
    """
    profile = db['client_profiles'].find_one({'user_id': int(user_id)})
    return profile

def get_all_products(db, limit=None, departments=None):
    """
    Récupérer tous les produits depuis MongoDB
    
    Args:
        db: Database MongoDB
        limit: Nombre max de produits (None = tous)
        departments: Liste des départements à filtrer
        
    Returns:
        DataFrame: Liste des produits
    """
    # Construire la requête avec filtres
    query = {}
    
    if departments:
        query['department'] = {'$in': departments}
    
    # Trier par popularité (nb_ventes) pour prendre les meilleurs
    cursor = db['product_stats'].find(query).sort('nb_ventes', -1)
    
    if limit:
        cursor = cursor.limit(limit)
    
    products = list(cursor)
    df = pd.DataFrame(products)
    
    if not df.empty:
        df = df.drop(columns=['_id', 'updated_at'], errors='ignore')
    
    return df

def predict_reorder_probability(model, scaler, client_profile, product_row):
    """
    Prédire la probabilité de réachat
    
    Args:
        model: Modèle ML
        scaler: Scaler
        client_profile: Profil du client
        product_row: Ligne du produit
        
    Returns:
        float: Probabilité de réachat (0-1)
    """
    # Construire les features
    features = [
        client_profile['total_commandes'],
        client_profile['panier_moyen'],
        client_profile['taux_reachat'],
        product_row['nb_ventes'],
        product_row['taux_reachat'],
        product_row['position_moyenne_panier']
    ]
    
    # Features dérivées
    popularity_ratio = product_row['nb_ventes'] / 50000  # Normaliser
    client_loyalty = client_profile['taux_reachat'] * client_profile['total_commandes']
    product_affinity = product_row['taux_reachat'] * popularity_ratio
    
    features.extend([popularity_ratio, client_loyalty, product_affinity])
    
    # Normaliser
    features_scaled = scaler.transform([features])
    
    # Prédire
    probability = model.predict_proba(features_scaled)[0][1]
    
    return probability

def get_recommendations(db, model, scaler, user_id, top_n=10, progress_callback=None):
    """
    Générer des recommandations pour un client (version optimisée)
    
    Args:
        db: Database MongoDB
        model: Modèle ML
        scaler: Scaler
        user_id: ID du client
        top_n: Nombre de recommandations
        progress_callback: Fonction pour afficher la progression
        
    Returns:
        DataFrame: Produits recommandés avec scores
    """
    # 1. Profil du client
    if progress_callback:
        progress_callback("Chargement du profil client...", 0.1)
    
    client_profile = get_client_profile(db, user_id)
    
    if not client_profile:
        return None, "Client introuvable"
    
    # 2. Départements préférés du client (pour filtrer)
    preferred_departments = client_profile.get('departements_preferes', [])
    if isinstance(preferred_departments, str):
        preferred_departments = [d.strip() for d in preferred_departments.split(',') if d.strip()]
    
    # Limiter à 3 départements max pour éviter trop de produits
    preferred_departments = preferred_departments[:3] if preferred_departments else None
    
    # 3. Charger SEULEMENT les produits populaires (max 500)
    if progress_callback:
        progress_callback("Chargement des produits populaires...", 0.3)
    
    df_products = get_all_products(db, limit=500, departments=preferred_departments)
    
    # Si pas assez de produits avec filtres, charger sans filtre
    if len(df_products) < 100:
        df_products = get_all_products(db, limit=500)
    
    if df_products.empty:
        return None, "Aucun produit disponible"
    
    # 4. Produits déjà achetés (à exclure)
    already_bought = client_profile.get('top_5_produits', [])
    
    if isinstance(already_bought, str):
        already_bought = [p.strip() for p in already_bought.split(',')]
    
    # 5. Calculer les scores pour chaque produit
    if progress_callback:
        progress_callback(f"Calcul des scores pour {len(df_products)} produits...", 0.5)
    
    recommendations = []
    total = len(df_products)
    
    for idx, product in df_products.iterrows():
        product_name = product['product_name']
        
        # Exclure les produits déjà achetés
        if product_name in already_bought:
            continue
        
        # Prédire la probabilité
        try:
            probability = predict_reorder_probability(model, scaler, client_profile, product)
            
            recommendations.append({
                'product_id': product['product_id'],
                'product_name': product_name,
                'department': product['department'],
                'aisle': product['aisle'],
                'probability': probability,
                'nb_ventes': product['nb_ventes'],
                'taux_reachat': product['taux_reachat']
            })
        except Exception as e:
            continue
        
        # Mise à jour de la progression tous les 50 produits
        if progress_callback and idx % 50 == 0:
            progress = 0.5 + (idx / total) * 0.4
            progress_callback(f"Traitement: {idx}/{total} produits", progress)
    
    if progress_callback:
        progress_callback("Tri des résultats...", 0.95)
    
    # 6. Trier par probabilité
    df_recommendations = pd.DataFrame(recommendations)
    df_recommendations = df_recommendations.sort_values('probability', ascending=False).head(top_n)
    
    if progress_callback:
        progress_callback("Terminé!", 1.0)
    
    return df_recommendations, client_profile

# ==================== INTERFACE STREAMLIT ====================

def main():
    """
    Interface principale
    """
    
    # Header
    st.title("🤖 Système de Recommandation ML")
    st.markdown("**Prédiction de Réachat basée sur Random Forest**")
    
    st.divider()
    
    # Charger le modèle
    model, scaler, metadata = load_model()
    
    if model is None:
        st.error("❌ Modèle non chargé. Exécutez d'abord `python ml_train.py`")
        st.stop()
    
    # Connexion MongoDB
    db = get_mongodb_connection()
    
    if db is None:
        st.error("❌ Connexion MongoDB impossible")
        st.stop()
    
    # Sidebar - Informations du modèle
    with st.sidebar:
        st.markdown("### 📊 Informations Modèle")
        
        if metadata:
            st.metric("Accuracy", f"{metadata['metrics']['test_accuracy']:.2%}")
            st.metric("F1-Score", f"{metadata['metrics']['f1_score']:.2%}")
            st.metric("AUC-ROC", f"{metadata['metrics']['auc_roc']:.2%}")
            
            st.markdown(f"**Entraîné le:** {metadata['training_date'][:10]}")
            st.markdown(f"**Features:** {metadata['n_features']}")
        
        st.divider()
        
        st.markdown("### ⚙️ Paramètres")
        top_n = st.slider("Nombre de recommandations", 5, 20, 10)
    
    # Main content
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("### 🔍 Recherche Client")
        
        # Input User ID
        user_id_input = st.text_input(
            "ID du Client",
            placeholder="Ex: 1, 2, 3...",
            help="Entrez l'ID d'un client existant"
        )
        
        search_button = st.button("🚀 Générer Recommandations", type="primary", use_container_width=True)
    
    with col2:
        st.markdown("### 📦 Recommandations")
        
        if search_button and user_id_input:
            try:
                user_id = int(user_id_input)
                
                # Créer une barre de progression
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                def update_progress(message, progress):
                    """Callback pour mettre à jour la progression"""
                    progress_bar.progress(progress)
                    status_text.text(message)
                
                # Générer les recommandations avec progression
                df_recommendations, client_profile = get_recommendations(
                    db, model, scaler, user_id, top_n, progress_callback=update_progress
                )
                
                # Effacer la barre de progression
                progress_bar.empty()
                status_text.empty()
                
                if df_recommendations is None:
                    st.error(f"❌ {client_profile}")
                else:
                    # Afficher le profil client
                    st.success(f"✅ Client trouvé : ID {user_id}")
                    
                    col_info1, col_info2, col_info3 = st.columns(3)
                    
                    with col_info1:
                        st.metric("Commandes", client_profile['total_commandes'])
                    with col_info2:
                        st.metric("Panier Moyen", f"{client_profile['panier_moyen']:.1f}")
                    with col_info3:
                        st.metric("Taux Réachat", f"{client_profile['taux_reachat']:.1f}%")
                    
                    st.divider()
                    
                    # Afficher les recommandations
                    st.markdown(f"### 🎯 Top {len(df_recommendations)} Produits Recommandés")
                    
                    for idx, row in df_recommendations.iterrows():
                        with st.expander(
                            f"#{idx+1} - {row['product_name']} (Score: {row['probability']:.1%})",
                            expanded=(idx < 3)
                        ):
                            col_detail1, col_detail2 = st.columns(2)
                            
                            with col_detail1:
                                st.markdown(f"**🏪 Rayon:** {row['department']}")
                                st.markdown(f"**📦 Allée:** {row['aisle']}")
                            
                            with col_detail2:
                                st.markdown(f"**📊 Ventes Totales:** {row['nb_ventes']:,}")
                                st.markdown(f"**🔄 Taux Réachat:** {row['taux_reachat']:.1f}%")
                            
                            # Barre de progression pour la probabilité
                            st.progress(row['probability'])
                            st.caption(f"Probabilité de réachat: {row['probability']:.1%}")
            
            except ValueError:
                st.error("❌ Veuillez entrer un ID numérique valide")
            except Exception as e:
                st.error(f"❌ Erreur: {e}")
        
        elif search_button:
            st.warning("⚠️ Veuillez entrer un ID client")
        else:
            st.info("👈 Entrez un ID client et cliquez sur 'Générer Recommandations'")
    
    st.divider()
    
    # Footer
    st.markdown("### 📚 Guide d'Utilisation")
    
    with st.expander("Comment ça marche ?"):
        st.markdown("""
        **1. Chargement du Modèle**
        - Le modèle Random Forest est chargé depuis `models/reorder_model.pkl`
        - Entraîné sur les données MongoDB (profils clients + stats produits)
        
        **2. Recherche Client**
        - Entrez l'ID d'un client existant
        - Le système récupère son profil depuis MongoDB
        
        **3. Génération Recommandations (Optimisée)**
        - Filtrage intelligent : priorité aux départements préférés du client
        - Seulement les 500 produits les plus populaires sont évalués
        - Le modèle prédit la probabilité de réachat pour chaque produit
        - Les produits sont triés par score décroissant
        - ⚡ Temps de réponse : ~5-10 secondes (au lieu de plusieurs minutes)
        
        **4. Interprétation**
        - Score > 70% : Très forte probabilité
        - Score 50-70% : Probabilité modérée
        - Score < 50% : Faible probabilité
        
        **Features utilisées:**
        - Comportement client (nb commandes, panier moyen, fidélité)
        - Popularité produit (ventes, taux réachat)
        - Affinité client-produit (calculée)
        """)
    
    with st.expander("Exemples d'IDs clients"):
        st.markdown("""
        Essayez ces IDs pour tester :
        - **1** : Client avec 10 commandes
        - **2** : Client avec 15 commandes
        - **100** : Client fréquent
        - **1000** : Client occasionnel
        
        *Note: Les IDs dépendent de vos données MongoDB*
        """)

# ==================== EXÉCUTION ====================

if __name__ == "__main__":
    main()
