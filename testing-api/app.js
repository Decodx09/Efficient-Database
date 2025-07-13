// testing-api/app.js
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const mysql = require('mysql2/promise');
const { Kafka } = require('kafkajs');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Kafka Setup
const kafka = new Kafka({
  clientId: 'testing-api',
  brokers: [process.env.KAFKA_BROKERS || 'kafka:9092']
});

const producer = kafka.producer();

// MySQL Connection Pools
const readDbPool = mysql.createPool({
  host: process.env.MYSQL_READ || 'mysql-read',
  user: 'root',
  password: 'rootpass',
  database: 'readdb',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Initialize Kafka Producer
async function initKafka() {
  await producer.connect();
  console.log('Kafka producer connected');
}

// Health Check
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// Test User Creation
app.post('/api/users', async (req, res) => {
  try {
    const { name, email, age } = req.body;
    const userId = Math.floor(Math.random() * 1000000);
    
    // Send to Producer Service
    const response = await axios.post('http://producer-service:8080/produce', {
      userId,
      eventType: 'user_created',
      data: { name, email, age }
    });
    
    res.json({ 
      success: true, 
      userId, 
      message: 'User creation event sent',
      shard: response.data.shard 
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Test User Update
app.put('/api/users/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const updateData = req.body;
    
    const response = await axios.post('http://producer-service:8080/produce', {
      userId: parseInt(userId),
      eventType: 'user_updated',
      data: updateData
    });
    
    res.json({ 
      success: true, 
      message: 'User update event sent',
      shard: response.data.shard 
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Read from Read Database
app.get('/api/users/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    
    const [rows] = await readDbPool.execute(
      'SELECT * FROM users WHERE user_id = ?',
      [userId]
    );
    
    res.json({ user: rows[0] || null });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get All Users from Read Database
app.get('/api/users', async (req, res) => {
  try {
    const [rows] = await readDbPool.execute('SELECT * FROM users LIMIT 100');
    res.json({ users: rows });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Performance Test - Bulk Insert
app.post('/api/test/bulk-users', async (req, res) => {
  try {
    const { count = 1000 } = req.body;
    const promises = [];
    
    for (let i = 0; i < count; i++) {
      const userId = Math.floor(Math.random() * 1000000);
      promises.push(
        axios.post('http://producer-service:8080/produce', {
          userId,
          eventType: 'user_created',
          data: {
            name: `User ${i}`,
            email: `user${i}@example.com`,
            age: Math.floor(Math.random() * 80) + 18
          }
        })
      );
    }
    
    await Promise.all(promises);
    res.json({ message: `${count} users created successfully` });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Initialize and Start Server
async function startServer() {
  await initKafka();
  app.listen(PORT, () => {
    console.log(`Testing API running on port ${PORT}`);
  });
}

startServer().catch(console.error);