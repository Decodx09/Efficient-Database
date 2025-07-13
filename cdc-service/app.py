# cdc-service/app.py
from kafka import KafkaConsumer
import json
import mysql.connector
from mysql.connector import pooling
import os
import time
from datetime import datetime

# Kafka Consumer for CDC
consumer = KafkaConsumer(
    'db-shard-1', 'db-shard-2', 'db-shard-3',
    bootstrap_servers=[os.getenv('KAFKA_BROKERS', 'kafka:9092')],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='cdc-consumer-group',
    auto_offset_reset='earliest'
)

# Read Database Connection Pool
read_db_pool = pooling.MySQLConnectionPool(
    pool_name="read_pool",
    pool_size=10,
    user='root',
    password='rootpass',
    host=os.getenv('MYSQL_READ', 'mysql-read:3306').split(':')[0],
    database='readdb',
    charset='utf8mb4',
    collation='utf8mb4_unicode_ci',
    autocommit=True
)

def init_read_database():
    """Initialize read database tables"""
    try:
        conn = read_db_pool.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                age INT,
                shard_id INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                event_type VARCHAR(50),
                event_data JSON,
                shard_id INT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_user_id (user_id),
                INDEX idx_event_type (event_type)
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Read database initialized successfully")
        
    except Exception as e:
        print(f"Error initializing read database: {e}")

def replicate_to_read_db(event):
    """Replicate changes to read database"""
    try:
        user_id = event['userId']
        event_type = event['eventType']
        data = event['data']
        shard = event['shard']
        
        conn = read_db_pool.get_connection()
        cursor = conn.cursor()
        
        # Insert event log
        cursor.execute(
            "INSERT INTO user_events (user_id, event_type, event_data, shard_id) VALUES (%s, %s, %s, %s)",
            (user_id, event_type, json.dumps(data), shard)
        )
        
        # Update/Insert user record
        if event_type == 'user_created':
            cursor.execute(
                "INSERT INTO users (user_id, name, email, age, shard_id) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name=%s, email=%s, age=%s, shard_id=%s",
                (user_id, data['name'], data['email'], data['age'], shard, data['name'], data['email'], data['age'], shard)
            )
        elif event_type == 'user_updated':
            update_fields = []
            update_values = []
            for key, value in data.items():
                update_fields.append(f"{key} = %s")
                update_values.append(value)
            
            if update_fields:
                update_values.extend([shard, user_id])
                cursor.execute(
                    f"UPDATE users SET {', '.join(update_fields)}, shard_id = %s WHERE user_id = %s",
                    update_values
                )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"CDC: Replicated event {event_type} for user {user_id} to read database")
        
    except Exception as e:
        print(f"CDC Error: {e}")

def main():
    print("Starting CDC service...")
    init_read_database()
    
    while True:
        try:
            time.sleep(5)  # Wait for services to be ready
            
            for message in consumer:
                event = message.value
                replicate_to_read_db(event)
                
        except Exception as e:
            print(f"CDC Consumer error: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()