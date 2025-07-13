require('dotenv').config();
const database = require('./config/database');
const kafkaConfig = require('./config/kafka');
const createApp = require('./app');
const DbWriterWorker = require('./workers/dbWriter');
const logger = require('./utils/logger');

const PORT = process.env.PORT || 3000;

async function testDatabaseConnections(dbConnections) {
  try {
    console.log('Testing database connections...');
    
    // Test shard1
    const [rows1] = await dbConnections.shard1.master.execute('SELECT 1 as test');
    console.log('Shard1 connection test:', rows1);
    
    // Test shard2
    const [rows2] = await dbConnections.shard2.master.execute('SELECT 1 as test');
    console.log('Shard2 connection test:', rows2);
    
    console.log('All database connections working!');
  } catch (error) {
    console.error('Database connection test failed:', error);
    throw error;
  }
}

async function startServer() {
  try {
    // Initialize database
    logger.info('Connecting to databases...');
    await database.initialize();
    const dbConnections = database.getConnections();
    
    // Test connections
    await testDatabaseConnections(dbConnections);

    // Initialize Kafka (but don't fail if it doesn't work)
    try {
      logger.info('Connecting to Kafka...');
      await kafkaConfig.connectProducer();
      
      // Start worker
      const dbWriter = new DbWriterWorker();
      dbWriter.start().catch(logger.error);
    } catch (kafkaError) {
      logger.warn('Kafka connection failed, continuing without Kafka:', kafkaError.message);
    }

    // Create and start Express app
    const app = createApp(dbConnections);
    
    const server = app.listen(PORT, () => {
      logger.info(`Server running on http://localhost:${PORT}`);
      console.log('=== Server Ready ===');
    });

    // Graceful shutdown
    process.on('SIGTERM', async () => {
      logger.info('Shutting down gracefully...');
      server.close();
      await kafkaConfig.disconnect();
      process.exit(0);
    });

  } catch (error) {
    logger.error('Failed to start server:', error);
    console.error('Startup error:', error);
    process.exit(1);
  }
}

startServer();