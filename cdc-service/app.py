# cdc-service/app.py
from kafka import KafkaConsumer
import json
import mysql.connector
from mysql.connector import pooling
import os
import time
from datetime import datetime
from flask import Flask, jsonify, request
import threading

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

# Flask app for API endpoints
app = Flask(__name__)

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

@app.route('/', methods=['GET'])
def index():
    """Index endpoint to fetch all users"""
    try:
        conn = read_db_pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 10, type=int)
        offset = (page - 1) * limit
        
        # Get total count
        cursor.execute("SELECT COUNT(*) as total FROM users")
        total = cursor.fetchone()['total']
        
        # Get paginated users
        cursor.execute(
            "SELECT user_id, name, email, age, shard_id, created_at, updated_at FROM users ORDER BY user_id LIMIT %s OFFSET %s",
            (limit, offset)
        )
        users = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'data': {
                'users': users,
                'pagination': {
                    'page': page,
                    'limit': limit,
                    'total': total,
                    'pages': (total + limit - 1) // limit
                }
            }
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/users/<int:user_id>', methods=['GET'])
def get_user(user_id):
    """Get specific user by ID"""
    try:
        conn = read_db_pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(
            "SELECT user_id, name, email, age, shard_id, created_at, updated_at FROM users WHERE user_id = %s",
            (user_id,)
        )
        user = cursor.fetchone()
        
        if not user:
            cursor.close()
            conn.close()
            return jsonify({'status': 'error', 'message': 'User not found'}), 404
        
        # Get user events
        cursor.execute(
            "SELECT event_type, event_data, processed_at FROM user_events WHERE user_id = %s ORDER BY processed_at DESC LIMIT 10",
            (user_id,)
        )
        events = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'data': {
                'user': user,
                'recent_events': events
            }
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/events', methods=['GET'])
def get_events():
    """Get recent events"""
    try:
        conn = read_db_pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        limit = request.args.get('limit', 20, type=int)
        event_type = request.args.get('type')
        
        query = "SELECT id, user_id, event_type, event_data, shard_id, processed_at FROM user_events"
        params = []
        
        if event_type:
            query += " WHERE event_type = %s"
            params.append(event_type)
        
        query += " ORDER BY processed_at DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        events = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'data': {
                'events': events
            }
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'cdc-service',
        'timestamp': datetime.now().isoformat()
    })

def cdc_consumer():
    """CDC consumer function to run in separate thread"""
    print("Starting CDC consumer...")
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

def main():
    print("Starting CDC service with API endpoints...")
    
    # Start CDC consumer in a separate thread
    consumer_thread = threading.Thread(target=cdc_consumer, daemon=True)
    consumer_thread.start()
    
    # Start Flask API server
    app.run(host='0.0.0.0', port=5000, debug=False)

if __name__ == '__main__':
    main()