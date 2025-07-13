# producer-service/app.py
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import hashlib
import mysql.connector
from mysql.connector import pooling
import os
from datetime import datetime

app = Flask(__name__)

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=[os.getenv('KAFKA_BROKERS', 'kafka:9092')],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# MySQL Connection Pools
mysql_config = {
    'user': 'root',
    'password': 'rootpass',
    'host': '',
    'database': '',
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
        pool_size=10,
        **config
    )

# Initialize connection pools
shard_pools = {
    1: create_connection_pool(os.getenv('MYSQL_SHARD_1', 'mysql-shard-1:3306').split(':')[0], 'shard1'),
    2: create_connection_pool(os.getenv('MYSQL_SHARD_2', 'mysql-shard-2:3306').split(':')[0], 'shard2'),
    3: create_connection_pool(os.getenv('MYSQL_SHARD_3', 'mysql-shard-3:3306').split(':')[0], 'shard3')
}

def get_shard(user_id):
    """Hash-based sharding"""
    return int(hashlib.md5(str(user_id).encode()).hexdigest(), 16) % 3 + 1

def init_database_tables():
    """Initialize tables in all shards"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255),
        age INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """
    
    for shard_id, pool in shard_pools.items():
        try:
            conn = pool.get_connection()
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Table created in shard {shard_id}")
        except Exception as e:
            print(f"Error creating table in shard {shard_id}: {e}")

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/produce', methods=['POST'])
def produce_event():
    try:
        data = request.get_json()
        user_id = data.get('userId')
        event_type = data.get('eventType')
        event_data = data.get('data')
        
        if not all([user_id, event_type, event_data]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Determine shard
        shard = get_shard(user_id)
        
        # Create event message
        event_message = {
            'userId': user_id,
            'eventType': event_type,
            'data': event_data,
            'shard': shard,
            'timestamp': datetime.now().isoformat()
        }
        
        # Send to Kafka
        topic = f'db-shard-{shard}'
        producer.send(topic, value=event_message)
        producer.flush()
        
        # Also write directly to shard (for immediate consistency)
        write_to_shard(user_id, event_type, event_data, shard)
        
        return jsonify({
            "success": True,
            "shard": shard,
            "topic": topic,
            "message": "Event produced successfully"
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def write_to_shard(user_id, event_type, data, shard):
    """Write directly to MySQL shard"""
    try:
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
        
    except Exception as e:
        print(f"Error writing to shard {shard}: {e}")

if __name__ == '__main__':
    init_database_tables()
    app.run(host='0.0.0.0', port=8080, debug=True)