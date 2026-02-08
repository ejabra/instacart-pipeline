#!/usr/bin/env python3

import clickhouse_connect
from pymongo import MongoClient
from datetime import datetime
import time
import json

# ==================== CONFIGURATION ====================

# Clickhouse
CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 8123,
    'database': 'default',
    'username': 'default',
    'password': ''
}

# MongoDB
MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'instacart_cache',
    'collection': 'aggregated_data'
}

# ==================== CONNEXIONS ====================

def get_clickhouse_client():
    """Connexion à Clickhouse"""
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG['host'],
            port=CLICKHOUSE_CONFIG['port'],
            database=CLICKHOUSE_CONFIG['database'],
            username=CLICKHOUSE_CONFIG['username'],
            password=CLICKHOUSE_CONFIG['password']
        )
        print("✅ Connexion Clickhouse OK")
        return client
    except Exception as e:
        print(f"❌ Erreur Clickhouse: {e}")
        return None

def get_mongodb_client():
    """Connexion à MongoDB"""
    try:
        client = MongoClient(
            host=MONGODB_CONFIG['host'],
            port=MONGODB_CONFIG['port'],
            serverSelectionTimeoutMS=5000
        )
        # Test de connexion
        client.admin.command('ping')
        print("✅ Connexion MongoDB OK")
        return client
    except Exception as e:
        print(f"❌ Erreur MongoDB: {e}")
        return None

# ==================== AGRÉGATIONS CLICKHOUSE ====================

def aggregate_client_profiles(ch_client):
    """
    Agrégation 1 : Profils Clients Complets
    Pour chaque client : comportement d'achat, préférences, récence, fréquence
    """
    print("\n📊 Agrégation des profils clients...")
    
    query = """
    SELECT 
        user_id,
        COUNT(DISTINCT order_id) AS total_commandes,
        COUNT(*) AS total_produits,
        ROUND(AVG(nb_produits_par_commande), 2) AS panier_moyen,
        MAX(order_number) AS derniere_commande_numero,
        MIN(event_time) AS premiere_commande_date,
        MAX(event_time) AS derniere_commande_date,
        ROUND(AVG(reordered) * 100, 2) AS taux_reachat,
        arrayStringConcat(groupArray(DISTINCT department), ',') AS departements_preferes,
        arrayStringConcat(topK(5)(product_name), ',') AS top_5_produits
    FROM (
        SELECT 
            user_id,
            order_id,
            order_number,
            event_time,
            department,
            product_name,
            reordered,
            COUNT(*) OVER (PARTITION BY order_id) AS nb_produits_par_commande
        FROM instacart_flat_data
    )
    GROUP BY user_id
    ORDER BY total_commandes DESC
    """
    
    result = ch_client.query(query)
    
    # Convertir en liste de dictionnaires
    profiles = []
    for row in result.result_rows:
        profile = {
            'user_id': int(row[0]),
            'total_commandes': int(row[1]),
            'total_produits': int(row[2]),
            'panier_moyen': float(row[3]),
            'derniere_commande_numero': int(row[4]),
            'premiere_commande_date': str(row[5]),
            'derniere_commande_date': str(row[6]),
            'taux_reachat': float(row[7]),
            'departements_preferes': row[8].split(',') if row[8] else [],
            'top_5_produits': row[9].split(',') if row[9] else [],
            'updated_at': datetime.now().isoformat()
        }
        profiles.append(profile)
    
    print(f"   ✅ {len(profiles)} profils clients agrégés")
    return profiles

def aggregate_product_stats(ch_client):
    """
    Agrégation 2 : Statistiques Produits
    Performance, popularité, taux de réachat par produit
    """
    print("\n📦 Agrégation des statistiques produits...")
    
    query = """
    SELECT 
        product_id,
        any(product_name) AS product_name,
        any(aisle) AS aisle,
        any(department) AS department,
        COUNT(*) AS nb_ventes,
        COUNT(DISTINCT user_id) AS nb_clients_uniques,
        COUNT(DISTINCT order_id) AS nb_commandes,
        ROUND(AVG(reordered) * 100, 2) AS taux_reachat,
        ROUND(AVG(add_to_cart_order), 2) AS position_moyenne_panier
    FROM instacart_flat_data
    GROUP BY product_id
    ORDER BY nb_ventes DESC
    """
    
    result = ch_client.query(query)
    
    products = []
    for row in result.result_rows:
        product = {
            'product_id': int(row[0]),
            'product_name': str(row[1]),
            'aisle': str(row[2]),
            'department': str(row[3]),
            'nb_ventes': int(row[4]),
            'nb_clients_uniques': int(row[5]),
            'nb_commandes': int(row[6]),
            'taux_reachat': float(row[7]),
            'position_moyenne_panier': float(row[8]),
            'updated_at': datetime.now().isoformat()
        }
        products.append(product)
    
    print(f"   ✅ {len(products)} produits agrégés")
    return products

def aggregate_department_performance(ch_client):
    """
    Agrégation 3 : Performance par Département
    Métriques clés par rayon
    """
    print("\n🏪 Agrégation des performances départements...")
    
    query = """
    SELECT 
        department,
        COUNT(*) AS nb_ventes,
        COUNT(DISTINCT product_id) AS nb_produits_uniques,
        COUNT(DISTINCT user_id) AS nb_clients,
        COUNT(DISTINCT order_id) AS nb_commandes,
        ROUND(AVG(reordered) * 100, 2) AS taux_reachat,
        ROUND(AVG(nb_produits), 2) AS produits_par_commande
    FROM (
        SELECT 
            department,
            product_id,
            user_id,
            order_id,
            reordered,
            COUNT(*) OVER (PARTITION BY order_id) AS nb_produits
        FROM instacart_flat_data
    )
    GROUP BY department
    ORDER BY nb_ventes DESC
    """
    
    result = ch_client.query(query)
    
    departments = []
    for row in result.result_rows:
        dept = {
            'department': str(row[0]),
            'nb_ventes': int(row[1]),
            'nb_produits_uniques': int(row[2]),
            'nb_clients': int(row[3]),
            'nb_commandes': int(row[4]),
            'taux_reachat': float(row[5]),
            'produits_par_commande': float(row[6]),
            'updated_at': datetime.now().isoformat()
        }
        departments.append(dept)
    
    print(f"   ✅ {len(departments)} départements agrégés")
    return departments

def aggregate_temporal_patterns(ch_client):
    """
    Agrégation 4 : Patterns Temporels
    Comportements par heure, jour de semaine
    """
    print("\n📅 Agrégation des patterns temporels...")
    
    query = """
    SELECT 
        dow AS jour_semaine,
        hour AS heure,
        COUNT(DISTINCT order_id) AS nb_commandes,
        COUNT(*) AS nb_produits,
        ROUND(AVG(nb_produits_commande), 2) AS panier_moyen
    FROM (
        SELECT 
            dow,
            hour,
            order_id,
            COUNT(*) OVER (PARTITION BY order_id) AS nb_produits_commande
        FROM instacart_flat_data
    )
    GROUP BY dow, hour
    ORDER BY dow, hour
    """
    
    result = ch_client.query(query)
    
    patterns = []
    for row in result.result_rows:
        jour = int(row[0])
        heure = int(row[1])
        pattern = {
            'pattern_id': f"{jour}_{heure}",  # Clé composite unique
            'jour_semaine': jour,
            'heure': heure,
            'nb_commandes': int(row[2]),
            'nb_produits': int(row[3]),
            'panier_moyen': float(row[4]),
            'updated_at': datetime.now().isoformat()
        }
        patterns.append(pattern)
    
    print(f"   ✅ {len(patterns)} patterns temporels agrégés")
    return patterns

# ==================== STOCKAGE MONGODB ====================

def store_in_mongodb(mongo_client, collection_name, data, key_field='user_id'):
    """
    Stocke les données dans MongoDB avec upsert par lots
    """
    from pymongo import UpdateOne
    
    db = mongo_client[MONGODB_CONFIG['database']]
    collection = db[collection_name]
    
    if not data:
        print(f"   ⚠️ Aucune donnée à stocker dans {collection_name}")
        return 0
    
    total_docs = len(data)
    batch_size = 1000  # Traiter par lots de 1000
    total_inserted = 0
    total_modified = 0
    
    print(f"   📝 {collection_name}: {total_docs:,} documents à traiter...")
    
    try:
        # Traiter par lots
        for i in range(0, total_docs, batch_size):
            batch = data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_docs + batch_size - 1) // batch_size
            
            # Créer les opérations pour ce lot
            bulk_ops = [
                UpdateOne(
                    {key_field: doc[key_field]},
                    {'$set': doc},
                    upsert=True
                )
                for doc in batch
            ]
            
            # Écriture du lot
            result = collection.bulk_write(bulk_ops, ordered=False)
            total_inserted += result.upserted_count
            total_modified += result.modified_count
            
            # Afficher la progression
            progress = (i + len(batch)) / total_docs * 100
            print(f"   ⏳ {collection_name}: Lot {batch_num}/{total_batches} ({progress:.1f}%)")
        
        print(f"   ✅ {collection_name}: {total_inserted} insérés, {total_modified} mis à jour")
        return total_inserted + total_modified
        
    except Exception as e:
        print(f"   ❌ Erreur stockage {collection_name}: {e}")
        import traceback
        traceback.print_exc()
        return 0

# ==================== FONCTION PRINCIPALE ====================

def run_caching_pipeline():
    """
    Pipeline complet de caching
    """
    print("=" * 70)
    print("🚀 DÉMARRAGE DU PIPELINE DE CACHING MONGODB")
    print("=" * 70)
    print(f"⏰ Heure : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Connexions
    ch_client = get_clickhouse_client()
    mongo_client = get_mongodb_client()
    
    if not ch_client or not mongo_client:
        print("\n❌ Impossible de continuer sans connexions valides")
        return False
    
    try:
        # Agrégations
        profiles = aggregate_client_profiles(ch_client)
        products = aggregate_product_stats(ch_client)
        departments = aggregate_department_performance(ch_client)
        patterns = aggregate_temporal_patterns(ch_client)
        
        # Stockage
        print("\n💾 Stockage dans MongoDB...")
        store_in_mongodb(mongo_client, 'client_profiles', profiles, 'user_id')
        store_in_mongodb(mongo_client, 'product_stats', products, 'product_id')
        store_in_mongodb(mongo_client, 'department_performance', departments, 'department')
        store_in_mongodb(mongo_client, 'temporal_patterns', patterns, 'pattern_id')
        
        # Métadonnées
        db = mongo_client[MONGODB_CONFIG['database']]
        metadata_collection = db['cache_metadata']
        metadata_collection.update_one(
            {'_id': 'last_update'},
            {'$set': {
                'timestamp': datetime.now().isoformat(),
                'nb_clients': len(profiles),
                'nb_produits': len(products),
                'nb_departements': len(departments),
                'nb_patterns': len(patterns)
            }},
            upsert=True
        )
        
        print("\n" + "=" * 70)
        print("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
        print("=" * 70)
        return True
        
    except Exception as e:
        print(f"\n❌ Erreur dans le pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Fermeture des connexions
        if mongo_client:
            mongo_client.close()
            print("🔌 Connexion MongoDB fermée")

# ==================== EXÉCUTION ====================

if __name__ == "__main__":
    import sys
    
    # Mode daemon (boucle infinie) ou one-shot
    if len(sys.argv) > 1 and sys.argv[1] == '--daemon':
        print("🔁 Mode DAEMON : Exécution toutes les heures")
        while True:
            run_caching_pipeline()
            print(f"\n⏳ Prochaine exécution dans 1 heure...")
            time.sleep(3600)  # 1 heure
    else:
        print("📌 Mode ONE-SHOT : Exécution unique")
        success = run_caching_pipeline()
        sys.exit(0 if success else 1)
