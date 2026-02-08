import json
import pandas as pd
import mysql.connector
import clickhouse_connect
from kafka import KafkaConsumer
from datetime import datetime
import time

# --- CONFIGURATION ---
KAFKA_BROKER = 'localhost:9092'
CH_HOST = '127.0.0.1'
PATH_DATA = 'D:/Ynov/projet-fin-formation/data/statics/'
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'instacart_db'
}

# 1. Connexion ClickHouse
try:
    client = clickhouse_connect.get_client(host=CH_HOST, port=8123, username='default', password='')
    print("• SUCCESS: Connecté à ClickHouse.")
except Exception as e:
    print(f"♦ ERROR: ClickHouse : {e}"); exit()

# Création de la Wide Table si nécessaire
print("CHECK: Vérification de la structure de la table...")
create_table_query = """
CREATE TABLE IF NOT EXISTS instacart_flat_data (
    event_time DateTime,
    order_id UInt32,
    user_id UInt32,
    order_type String,
    order_number UInt16,
    dow UInt8,
    hour UInt8,
    days_since_prior Float32,
    product_id UInt32,
    add_to_cart_order UInt8,
    reordered UInt8,
    product_name String,
    aisle String,
    department String
) ENGINE = MergeTree()
ORDER BY (event_time, order_id);
"""
try:
    client.command(create_table_query)
    print("• SUCCESS: Table 'instacart_flat_data' prête.")
except Exception as e:
    print(f"♦ ERROR: Échec lors de la création de la table : {e}"); exit()


# 2. Connexion MySQL (Lookup Database)
try:
    db_conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = db_conn.cursor(dictionary=True)
    print("• SUCCESS: Connecté à MySQL.")
except Exception as e:
    print(f"♦ ERROR: MySQL : {e}"); exit()

# --- 2. CHARGEMENT DES RÉFÉRENTIELS (Enrichissement statique) ---
print("LOADING: Chargement des dictionnaires produits depuis les CSV locaux...")

try:
    # Lecture des fichiers CSV
    df_prods = pd.read_csv(PATH_DATA + 'products.csv')
    df_aisles = pd.read_csv(PATH_DATA + 'aisles.csv')
    df_depts = pd.read_csv(PATH_DATA + 'departments.csv')

    # Fusion des DataFrames (Join)
    # On lie les produits aux rayons (aisles) et aux départements (departments)
    df_merged = df_prods.merge(df_aisles, on='aisle_id').merge(df_depts, on='department_id')

    # Création du dictionnaire de recherche (Lookup)
    # L'index est le product_id pour une recherche instantanée O(1)
    static_lookup = df_merged.set_index('product_id')[['product_name', 'aisle', 'department']].to_dict('index')
    
    print(f"• SUCCESS: {len(static_lookup)} produits chargés en mémoire.")

except FileNotFoundError as e:
    print(f"♦ ERROR: Un fichier CSV est introuvable dans {PATH_DATA} : {e}")
    exit()
except Exception as e:
    print(f"♦ ERROR lors du chargement des référentiels : {e}")
    exit()

# 4. CONSUMER KAFKA (On n'écoute QUE les produits maintenant)
consumer = KafkaConsumer(
    'order_products_topic',
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    group_id='pff-final-enricher',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("📉 START: Streaming avec Lookup MySQL en cours...")

try:
    for message in consumer:
        data = message.value
        order_id = data['order_id']
        prod_id = data['product_id']

        # --- ÉTAPE A: LOOKUP DYNAMIQUE DANS MYSQL ---
        # On cherche les métadonnées de la commande
        query = "SELECT * FROM orders_lookup WHERE order_id = %s"
        cursor.execute(query, (order_id,))
        meta = cursor.fetchone()

        # Si la commande n'est pas encore dans MySQL, on ignore ou on met 'unknown'
        if not meta:
            # print(f"SKIP: Commande {order_id} non trouvée dans MySQL")
            # meta = {}
            continue

        # --- ÉTAPE B: ENRICHISSEMENT STATIQUE (RAM) ---
        info_prod = static_lookup.get(prod_id, {
            'product_name': 'Inconnu', 
            'aisle': 'Inconnu', 
            'department': 'Inconnu'
        })

        # --- ÉTAPE C: CONSTRUCTION DE LA LIGNE POUR CLICKHOUSE ---
        # Note: 'eval_set' dans ton CSV devient 'order_type' ici
        row = [
            datetime.now(),
            int(order_id),
            int(meta.get('user_id', 0)),
            str(meta.get('eval_set') or 'unknown'),
            int(meta.get('order_number') or 0),
            int(meta.get('order_dow', 0)),
            int(meta.get('order_hour_of_day', 0)),
            float(meta.get('days_since_prior_order') if meta.get('days_since_prior_order') is not None else 0.0),
            int(prod_id),
            int(data.get('add_to_cart_order', 0)),
            int(data.get('reordered', 0)),
            str(info_prod['product_name']),
            str(info_prod['aisle']),
            str(info_prod['department'])
        ]

        # --- ÉTAPE D: INSERTION CLICKHOUSE ---
        client.insert('instacart_flat_data', [row], 
                     column_names=['event_time','order_id','user_id','order_type','order_number',
                                   'dow','hour','days_since_prior','product_id','add_to_cart_order',
                                   'reordered','product_name','aisle','department'])

        # Petite pause tous les 500 messages pour stabiliser le PC
        if message.offset % 500 == 0:
            time.sleep(0.1) 
            print(f"✅ Traitement en cours... Offset: {message.offset}")

except KeyboardInterrupt:
    print("🔴 Arrêt du script.")
finally:
    cursor.close()
    db_conn.close()
    consumer.close()