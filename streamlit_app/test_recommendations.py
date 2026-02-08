#!/usr/bin/env python3
"""
Test rapide du système de recommandations
Vérifie les performances et la qualité des recommandations
"""

import sys
import time
from pymongo import MongoClient
import joblib
import json

# Configuration
MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'instacart_cache'
}

MODEL_CONFIG = {
    'model_path': 'models/reorder_model.pkl',
    'scaler_path': 'models/scaler.pkl',
    'metadata_path': 'models/model_metadata.json'
}

def test_recommendations():
    """Test rapide des recommandations"""
    
    print("=" * 70)
    print("🧪 TEST DU SYSTÈME DE RECOMMANDATIONS")
    print("=" * 70)
    
    # 1. Vérifier MongoDB
    print("\n📡 Connexion MongoDB...")
    try:
        client = MongoClient(
            host=MONGODB_CONFIG['host'],
            port=MONGODB_CONFIG['port'],
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ping')
        db = client[MONGODB_CONFIG['database']]
        print("   ✅ MongoDB OK")
    except Exception as e:
        print(f"   ❌ Erreur MongoDB: {e}")
        return False
    
    # 2. Vérifier le modèle
    print("\n🤖 Chargement du modèle...")
    try:
        model = joblib.load(MODEL_CONFIG['model_path'])
        scaler = joblib.load(MODEL_CONFIG['scaler_path'])
        
        with open(MODEL_CONFIG['metadata_path'], 'r') as f:
            metadata = json.load(f)
        
        print(f"   ✅ Modèle OK (Accuracy: {metadata['metrics']['test_accuracy']:.2%})")
    except Exception as e:
        print(f"   ❌ Erreur modèle: {e}")
        print("   💡 Exécutez d'abord: python ml_train.py")
        return False
    
    # 3. Tester avec un client aléatoire
    print("\n👤 Sélection d'un client test...")
    
    sample_client = db['client_profiles'].find_one()
    
    if not sample_client:
        print("   ❌ Aucun client dans MongoDB")
        return False
    
    user_id = sample_client['user_id']
    print(f"   ✅ Client ID: {user_id}")
    print(f"      Commandes: {sample_client['total_commandes']}")
    print(f"      Panier moyen: {sample_client['panier_moyen']:.1f}")
    
    # 4. Mesurer le temps de génération
    print("\n⏱️  Test de performance...")
    
    # Import de la fonction optimisée
    sys.path.insert(0, '.')
    from ml_app import get_recommendations
    
    def progress_callback(message, progress):
        print(f"   [{progress*100:.0f}%] {message}")
    
    start_time = time.time()
    
    df_recommendations, client_profile = get_recommendations(
        db, model, scaler, user_id, top_n=10, progress_callback=progress_callback
    )
    
    elapsed_time = time.time() - start_time
    
    # 5. Afficher les résultats
    print(f"\n⏱️  Temps de génération: {elapsed_time:.2f} secondes")
    
    if df_recommendations is None:
        print(f"   ❌ Erreur: {client_profile}")
        return False
    
    print(f"\n🎯 Top 10 Recommandations pour le client {user_id}:")
    print("-" * 70)
    print(f"{'#':<4} {'Produit':<40} {'Score':<10} {'Ventes':<10}")
    print("-" * 70)
    
    for idx, row in df_recommendations.iterrows():
        print(f"{idx+1:<4} {row['product_name'][:38]:<40} {row['probability']:.1%}      {row['nb_ventes']:>6,}")
    
    # 6. Vérifier la qualité
    print("\n📊 Analyse de qualité:")
    
    avg_score = df_recommendations['probability'].mean()
    max_score = df_recommendations['probability'].max()
    min_score = df_recommendations['probability'].min()
    
    print(f"   Score moyen: {avg_score:.1%}")
    print(f"   Score max: {max_score:.1%}")
    print(f"   Score min: {min_score:.1%}")
    
    if avg_score > 0.5:
        print("   ✅ Bonne qualité de recommandations")
    elif avg_score > 0.3:
        print("   ⚠️  Qualité moyenne (modèle à améliorer)")
    else:
        print("   ❌ Faible qualité (réentraîner le modèle)")
    
    # 7. Benchmark de temps
    print("\n⚡ Benchmark de performance:")
    
    if elapsed_time < 10:
        print(f"   ✅ Excellent ({elapsed_time:.2f}s) - Rapide")
    elif elapsed_time < 30:
        print(f"   ✅ Bon ({elapsed_time:.2f}s) - Acceptable")
    elif elapsed_time < 60:
        print(f"   ⚠️  Moyen ({elapsed_time:.2f}s) - Peut être optimisé")
    else:
        print(f"   ❌ Lent ({elapsed_time:.2f}s) - Optimisation nécessaire")
    
    print("\n" + "=" * 70)
    print("✅ TEST TERMINÉ")
    print("=" * 70)
    
    return True

if __name__ == "__main__":
    success = test_recommendations()
    sys.exit(0 if success else 1)
