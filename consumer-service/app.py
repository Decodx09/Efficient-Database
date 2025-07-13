# consumer-service/app.py
from kafka import KafkaConsumer
import json
import mysql.connector
from mysql.connector import pooling
import os
import time
from datetime import datetime

# Kafka Consumer Setup
consumer = KafkaConsumer(
    'db-shard-1', 'db-shard-2', 'db-shard-3',
    bootstrap_servers=[os.getenv('KAFKA_BROKERS', 'kafka:9092')],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='db-consumer-group',
    auto_offset_reset='latest'
)

# MySQL Connection Configuration
mysql_config = {
    'user': 'root',
    'password': 'rootpass',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
    'autocommit': True
}

def create_connection_pool(host, database):
    config = mysql_config.copy()
    config['host'] = host
    config['database'] = database
    return pooling.MySQLConnectionPool(
        pool_name=f"pool_{database}",
        pool_size=5,
        **config
    )

# Initialize connection pools
shard_pools = {
    1: create_connection_pool(os.getenv('MYSQL_SHARD_1', 'mysql-shard-1:3306').split(':')[0], 'shard1'),
    2: create_connection_pool(os.getenv('MYSQL_SHARD_2', 'mysql-shard-2:3306').split(':')[0], 'shard2'),
    3: create_connection_pool(os.getenv('MYSQL_SHARD_3', 'mysql-shard-3:3306').split(':')[0], 'shard3')
}

def process_event(event):
    """Process incoming Kafka events"""
    try:
        user_id = event['userId']
        event_type = event['eventType']
        data = event['data']
        shard = event['shard']
        
        print(f"Processing event: {event_type} for user {user_id} in shard {shard}")
        
        # Get connection from appropriate shard pool
        pool = shard_pools[shard]
        conn = pool.get_connection()
        cursor = conn.cursor()
        
        if event_type == 'user_created':
            cursor.execute(
                "INSERT INTO users (user_id, name, email, age) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE name=%s, email=%s, age=%s",
                (user_id, data['name'], data['email'], data['age'], data['name'], data['email'], data['age'])
            )
        elif event_type == 'user_updated':
            update_fields = []
            update_values = []
            for key, value in data.items():
                update_fields.append(f"{key} = %s")
                update_values.append(value)
            
            if update_fields:
                update_values.append(user_id)
                cursor.execute(
                    f"UPDATE users SET {', '.join(update_fields)} WHERE user_id = %s",
                    update_values
                )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Successfully processed event for user {user_id}")
        
    except Exception as e:
        print(f"Error processing event: {e}")

def main():
    print("Starting Kafka consumer...")
    
    while True:
        try:
            # Wait for Kafka to be ready
            time.sleep(10)
            
            for message in consumer:
                event = message.value
                process_event(event)
                
        except Exception as e:
            print(f"Consumer error: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()