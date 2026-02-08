#!/usr/bin/env python3
"""
Script de test MongoDB
Vérifie la connexion et affiche les données cachées
Usage: python test_mongodb.py
"""

from pymongo import MongoClient
from datetime import datetime
import json

# Configuration
MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'instacart_cache'
}

def test_mongodb_connection():
    """Tester la connexion MongoDB"""
    
    print("=" * 70)
    print("🔍 TEST DE CONNEXION MONGODB")
    print("=" * 70)
    
    try:
        # Connexion
        print(f"\n📡 Connexion à {MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}...")
        client = MongoClient(
            host=MONGODB_CONFIG['host'],
            port=MONGODB_CONFIG['port'],
            serverSelectionTimeoutMS=5000
        )
        
        # Test ping
        client.admin.command('ping')
        print("✅ Connexion MongoDB réussie !")
        
        # Accès à la base de données
        db = client[MONGODB_CONFIG['database']]
        
        # Lister les collections
        collections = db.list_collection_names()
        print(f"\n📦 Collections dans '{MONGODB_CONFIG['database']}': {len(collections)}")
        
        if not collections:
            print("   ⚠️ Aucune collection trouvée")
            print("   💡 Exécutez d'abord: python cache_to_mongodb.py")
            return False
        
        # Statistiques par collection
        print("\n" + "=" * 70)
        print("📊 STATISTIQUES DES COLLECTIONS")
        print("=" * 70)
        
        for coll_name in collections:
            collection = db[coll_name]
            count = collection.count_documents({})
            print(f"\n📁 {coll_name}")
            print(f"   Documents: {count:,}")
            
            if count > 0:
                # Exemple de document
                sample = collection.find_one()
                if sample:
                    # Enlever _id pour l'affichage
                    sample.pop('_id', None)
                    print(f"   Exemple (premier doc):")
                    # Afficher de manière formatée
                    for key, value in list(sample.items())[:5]:  # Premiers 5 champs
                        if isinstance(value, list):
                            print(f"      {key}: {value[:3]}..." if len(value) > 3 else f"      {key}: {value}")
                        else:
                            print(f"      {key}: {value}")
                    if len(sample) > 5:
                        print(f"      ... ({len(sample) - 5} autres champs)")
        
        # Métadonnées de cache
        print("\n" + "=" * 70)
        print("🕐 DERNIÈRE MISE À JOUR DU CACHE")
        print("=" * 70)
        
        metadata = db['cache_metadata'].find_one({'_id': 'last_update'})
        if metadata:
            print(f"\n⏰ Timestamp: {metadata.get('timestamp', 'N/A')}")
            print(f"👥 Clients cachés: {metadata.get('nb_clients', 0):,}")
            print(f"📦 Produits cachés: {metadata.get('nb_produits', 0):,}")
            print(f"🏪 Départements: {metadata.get('nb_departements', 0)}")
            print(f"📅 Patterns temporels: {metadata.get('nb_patterns', 0)}")
        else:
            print("⚠️ Aucune métadonnée de cache trouvée")
        
        # Exemple de requête
        print("\n" + "=" * 70)
        print("🔍 EXEMPLE DE REQUÊTE : Top 5 Clients")
        print("=" * 70)
        
        if 'client_profiles' in collections:
            top_clients = db['client_profiles'].find().sort('total_commandes', -1).limit(5)
            print(f"\n{'User ID':<10} {'Commandes':<12} {'Produits':<12} {'Panier Moy':<12}")
            print("-" * 46)
            for profile in top_clients:
                print(f"{profile['user_id']:<10} {profile['total_commandes']:<12} {profile['total_produits']:<12} {profile['panier_moyen']:<12}")
        
        print("\n" + "=" * 70)
        print("✅ TEST TERMINÉ AVEC SUCCÈS")
        print("=" * 70)
        
        client.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Erreur de connexion MongoDB:")
        print(f"   {str(e)}")
        print("\n💡 Vérifications:")
        print("   1. MongoDB est-il démarré ? (docker ps | grep mongodb)")
        print("   2. Le port 27017 est-il accessible ?")
        print("   3. Avez-vous exécuté 'cache_to_mongodb.py' ?")
        return False

if __name__ == "__main__":
    import sys
    success = test_mongodb_connection()
    sys.exit(0 if success else 1)
