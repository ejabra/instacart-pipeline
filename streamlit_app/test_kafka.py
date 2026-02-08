#!/usr/bin/env python3
"""
Script de test pour vérifier la connexion Kafka
Usage: python test_kafka.py
"""

from kafka import KafkaConsumer
import json
import sys

# Configuration
KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'order_products_topic'  # Topic utilisé par l'application

def test_kafka_connection():
    """Tester la connexion Kafka"""
    
    print("=" * 60)
    print("🔍 TEST DE CONNEXION KAFKA")
    print("=" * 60)
    
    print(f"\n📡 Tentative de connexion à {KAFKA_BOOTSTRAP}...")
    print(f"📋 Topic : {KAFKA_TOPIC}")
    
    try:
        # Créer un consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='test_consumer',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        print("✅ Connexion Kafka réussie !")
        
        # Vérifier les partitions
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if partitions:
            print(f"✅ Topic '{KAFKA_TOPIC}' trouvé avec {len(partitions)} partition(s)")
        else:
            print(f"❌ Topic '{KAFKA_TOPIC}' introuvable")
            print("\n💡 Créez le topic avec :")
            print(f"   docker exec kafka kafka-topics --create --topic {KAFKA_TOPIC} --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1")
            return False
        
        # Essayer de lire des messages
        print(f"\n⏳ Écoute de messages pendant 5 secondes...")
        message_count = 0
        
        for message in consumer:
            message_count += 1
            print(f"\n📦 Message {message_count} reçu :")
            print(f"   Offset: {message.offset}")
            print(f"   Partition: {message.partition}")
            print(f"   Données: {message.value}")
            
            if message_count >= 5:
                break
        
        if message_count == 0:
            print("⚠️  Aucun message reçu")
            print("\n💡 Vérifications :")
            print("   1. NiFi envoie-t-il des messages vers Kafka ?")
            print("   2. Le producer est-il bien configuré sur le bon topic ?")
            print("   3. Y a-t-il des données dans le fichier source ?")
        else:
            print(f"\n✅ {message_count} message(s) lu(s) avec succès !")
        
        consumer.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Erreur de connexion Kafka :")
        print(f"   {str(e)}")
        print("\n💡 Vérifications :")
        print("   1. Kafka est-il démarré ? (docker ps | grep kafka)")
        print("   2. Le port 9092 est-il accessible ?")
        print("   3. Le topic existe-t-il ?")
        return False

if __name__ == "__main__":
    success = test_kafka_connection()
    sys.exit(0 if success else 1)
