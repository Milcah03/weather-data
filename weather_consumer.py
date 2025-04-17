import os
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException
from cassandra.cluster import Cluster
from json import loads
from datetime import datetime
import uuid

#Load environment variables 
load_dotenv()

# Confluent Kafka Consumer Configuration 
conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'group.id': 'weather-group-id',
    'auto.offset.reset': 'earliest'
}

# Initialize Kafka consumer
consumer = Consumer(conf)
topic = 'weather'  # Topic name
consumer.subscribe([topic])
print(f"✅ Subscribed to topic: {topic}")

#Cassandra Setup 
try:
    cluster = Cluster(['127.0.0.1']) 
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS weather
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace("weather_data")

    session.execute("""
        CREATE TABLE IF NOT EXISTS weather_stream (
            id UUID PRIMARY KEY,
            city_name TEXT,
            weather_main TEXT,
            weather_description TEXT,
            temperature FLOAT,
            timestamp TIMESTAMP
        )
    """)
    print("✅ Cassandra table ready")
except Exception as e:
    print("❌ Error setting up Cassandra:", e)
    session = None

#Read from Kafka and Insert into Cassandra
if session:
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                try:
                    data = loads(msg.value().decode('utf-8'))

                    # Extract required fields
                    record = {
                        "id": uuid.uuid4(),
                        "city_name": data.get("extracted_city", "Unknown"),
                        "weather_main": data["weather"][0]["main"],
                        "weather_description": data["weather"][0]["description"],
                        "temperature": data["main"]["temp"],
                        "timestamp": datetime.fromtimestamp(data["dt"])
                    }

                    # Insert into Cassandra
                    session.execute("""
                        INSERT INTO weather_stream (id, city_name, weather_main, weather_description, temperature, timestamp)
                        VALUES (%(id)s, %(city_name)s, %(weather_main)s, %(weather_description)s, %(temperature)s, %(timestamp)s)
                    """, record)

                    print(f"✅ Inserted weather for {record['city_name']} at {record['timestamp']}")

                except Exception as e:
                    print("❌ Error processing message:", e)

    except KeyboardInterrupt:
        print("🛑 Consumer stopped manually")

    finally:
        consumer.close()
        print("🔒 Kafka consumer closed")

