const kafkaConfig = require('../config/kafka');
const logger = require('../utils/logger');

class DbWriterWorker {
  constructor() {
    this.isRunning = false;
  }

  async start() {
    await kafkaConfig.connectConsumer('db-writer-group');
    const consumer = kafkaConfig.getConsumer();

    await consumer.subscribe({ 
      topics: ['user.created', 'user.updated', 'user.deleted'], 
      fromBeginning: false 
    });

    this.isRunning = true;

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const event = JSON.parse(message.value.toString());
          logger.info(`Processing event from topic ${topic}:`, event);
          // Additional async processing can be done here
        } catch (error) {
          logger.error('Error processing message:', error);
        }
      }
    });
  }

  async stop() {
    this.isRunning = false;
    await kafkaConfig.disconnect();
  }
}

module.exports = DbWriterWorker;