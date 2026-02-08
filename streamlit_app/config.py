# Configuration Clickhouse
CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 8123,
    'database': 'default',
    'username': 'default',
    'password': ''
}

# Configuration Kafka
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic': 'order_products_topic',
    'group_id': 'streamlit_consumer',
    'auto_offset_reset': 'latest',
    'enable_auto_commit': True,
    'consumer_timeout_ms': 1000  # Timeout pour ne pas bloquer
}

# Configuration de l'application
APP_CONFIG = {
    'page_title': 'InstaCart - Dashboard Temps Réel',
    'page_icon': '📊',
    'layout': 'wide',
    'refresh_interval': 3  # Secondes entre chaque actualisation
}