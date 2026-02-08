import clickhouse_connect
import pandas as pd
from kafka import KafkaConsumer
import json
from datetime import datetime
from config import CLICKHOUSE_CONFIG, KAFKA_CONFIG

# ==================== CLICKHOUSE ====================

def get_clickhouse_client():
    """Créer une connexion Clickhouse"""
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG['host'],
            port=CLICKHOUSE_CONFIG['port'],
            database=CLICKHOUSE_CONFIG['database'],
            username=CLICKHOUSE_CONFIG['username'],
            password=CLICKHOUSE_CONFIG['password']
        )
        return client
    except Exception as e:
        print(f"❌ Erreur connexion Clickhouse: {e}")
        return None

def execute_query(query):
    """Exécuter une requête Clickhouse et retourner un DataFrame"""
    client = get_clickhouse_client()
    if client:
        try:
            result = client.query(query)
            df = pd.DataFrame(result.result_rows, columns=result.column_names)
            return df
        except Exception as e:
            print(f"❌ Erreur requête: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# ==================== KPIs GLOBAUX ====================

def get_kpi_globaux(filtre_dept=[], date_min=None, date_max=None):
    """Récupérer les KPI principaux avec filtres optionnels"""
    where_clauses = []
    
    if filtre_dept and len(filtre_dept) > 0:
        dept_list = "', '".join(filtre_dept)
        where_clauses.append(f"department IN ('{dept_list}')")
    
    if date_min:
        where_clauses.append(f"toDate(event_time) >= '{date_min}'")
    
    if date_max:
        where_clauses.append(f"toDate(event_time) <= '{date_max}'")
    
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    query = f"""
    WITH stats_commandes AS (
        SELECT 
            order_id,
            user_id,
            COUNT(*) AS nb_produits_commande
        FROM instacart_flat_data
        {where_sql}
        GROUP BY order_id, user_id
    )
    SELECT 
        COUNT(DISTINCT order_id) AS nb_commandes,
        (SELECT COUNT(*) FROM instacart_flat_data {where_sql}) AS nb_produits_vendus,
        COUNT(DISTINCT user_id) AS nb_clients,
        ROUND(AVG(nb_produits_commande), 2) AS panier_moyen
    FROM stats_commandes
    """
    return execute_query(query)

# ==================== GRAPHIQUES ====================

def get_top_produits(limit=10, filtre_dept=[], date_min=None, date_max=None):
    """Top N produits les plus vendus avec filtres"""
    where_clauses = []
    
    if filtre_dept and len(filtre_dept) > 0:
        dept_list = "', '".join(filtre_dept)
        where_clauses.append(f"department IN ('{dept_list}')")
    
    if date_min:
        where_clauses.append(f"toDate(event_time) >= '{date_min}'")
    
    if date_max:
        where_clauses.append(f"toDate(event_time) <= '{date_max}'")
    
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    query = f"""
    SELECT 
        product_name,
        COUNT(*) AS nb_ventes
    FROM instacart_flat_data
    {where_sql}
    GROUP BY product_name
    ORDER BY nb_ventes DESC
    LIMIT {limit}
    """
    return execute_query(query)

def get_ventes_par_departement(filtre_dept=[], date_min=None, date_max=None):
    """Répartition des ventes par département avec filtres"""
    where_clauses = []
    
    if date_min:
        where_clauses.append(f"toDate(event_time) >= '{date_min}'")
    
    if date_max:
        where_clauses.append(f"toDate(event_time) <= '{date_max}'")
    
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    query = f"""
    SELECT 
        department,
        COUNT(*) AS nb_ventes,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pourcentage
    FROM instacart_flat_data
    {where_sql}
    GROUP BY department
    ORDER BY nb_ventes DESC
    """
    return execute_query(query)

def get_commandes_par_heure(filtre_dept=[], date_min=None, date_max=None):
    """Distribution des commandes par heure avec filtres"""
    where_clauses = []
    
    if filtre_dept and len(filtre_dept) > 0:
        dept_list = "', '".join(filtre_dept)
        where_clauses.append(f"department IN ('{dept_list}')")
    
    if date_min:
        where_clauses.append(f"toDate(event_time) >= '{date_min}'")
    
    if date_max:
        where_clauses.append(f"toDate(event_time) <= '{date_max}'")
    
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    query = f"""
    SELECT 
        hour,
        COUNT(DISTINCT order_id) AS nb_commandes
    FROM instacart_flat_data
    {where_sql}
    GROUP BY hour
    ORDER BY hour
    """
    return execute_query(query)

def get_evolution_temps_reel(limit=50, filtre_dept=[], date_min=None, date_max=None):
    """Dernières commandes en temps réel avec filtres"""
    where_clauses = []
    
    if filtre_dept and len(filtre_dept) > 0:
        dept_list = "', '".join(filtre_dept)
        where_clauses.append(f"department IN ('{dept_list}')")
    
    if date_min:
        where_clauses.append(f"toDate(event_time) >= '{date_min}'")
    
    if date_max:
        where_clauses.append(f"toDate(event_time) <= '{date_max}'")
    
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    query = f"""
    SELECT 
        event_time,
        order_id,
        user_id,
        product_name,
        department
    FROM instacart_flat_data
    {where_sql}
    ORDER BY event_time DESC
    LIMIT {limit}
    """
    return execute_query(query)

# ==================== KAFKA ====================

def create_kafka_consumer():
    """Créer un consumer Kafka"""
    try:
        consumer = KafkaConsumer(
            KAFKA_CONFIG['topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            group_id=KAFKA_CONFIG['group_id'],
            auto_offset_reset=KAFKA_CONFIG['auto_offset_reset'],
            enable_auto_commit=KAFKA_CONFIG['enable_auto_commit'],
            consumer_timeout_ms=KAFKA_CONFIG['consumer_timeout_ms'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        return consumer
    except Exception as e:
        print(f"❌ Erreur connexion Kafka: {e}")
        return None

def get_latest_kafka_messages(consumer, max_messages=10):
    """Récupérer les derniers messages Kafka"""
    messages = []
    if consumer:
        try:
            # Poll avec timeout court
            message_batch = consumer.poll(timeout_ms=1000, max_records=max_messages)
            
            # Extraire les messages de toutes les partitions
            for topic_partition, records in message_batch.items():
                for record in records:
                    messages.append(record.value)
                    if len(messages) >= max_messages:
                        break
                if len(messages) >= max_messages:
                    break
                    
        except Exception as e:
            print(f"⚠️ Erreur lors de la lecture Kafka: {e}")
    
    return messages
