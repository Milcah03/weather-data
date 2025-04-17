🌦️ Real-Time Weather Data Pipeline using Apache Kafka, Cassandra & Confluent

This project demonstrates how to stream real-time weather data using Apache Kafka, process it with Python, and store it in Apache Cassandra. It utilizes Confluent Platform for simplified Kafka setup and management.

🛠️ Tech Stack

- Python  
- Apache Kafka (for real-time data streaming)  
- Confluent Platform (for Kafka management)  
- Apache Cassandra (NoSQL database for storing weather data)  
- Kafka-Python (Kafka client library)  
- JSON (data format)

📌 Project Structure

├── weather_producer.py       # Fetches weather data and sends it to Kafka topic  
├── cassandra_consumer.py     # Consumes data from Kafka and inserts into Cassandra  
├── README.md                 # Project documentation

🔁 How It Works

1. **Producer**  
   - Fetches real-time weather data from a public API  
   - Publishes each record to Kafka topic `weather_data`

2. **Kafka (via Confluent Platform)**  
   - Acts as the message broker between producer and consumer  

3. **Consumer**  
   - Subscribes to `weather_data` topic  
   - Parses weather records and inserts them into Apache Cassandra  

✅ Use Cases

- Real-time weather dashboards  
- Environmental monitoring systems  
- Scalable IoT data pipelines  
- Weather-based alerts and analytics

📖 Article
Want a step-by-step walkthrough? Check out the full write-up on: https://dev.to/milcah03/real-time-weather-data-pipeline-using-kafka-confluent-and-cassandra-4425 

