const kafkaConfig = require('../config/kafka');
const logger = require('../utils/logger');

class KafkaProducer {
  async publishEvent(topic, eventData) {
    try {
      const producer = kafkaConfig.getProducer();
      
      if (!producer) {
        logger.warn('Kafka producer not initialized, skipping event publish');
        return;
      }
      
      await producer.send({
        topic,
        messages: [{
          key: eventData.id ? eventData.id.toString() : null,
          value: JSON.stringify({
            ...eventData,
            timestamp: new Date().toISOString()
          })
        }]
      });

      logger.info(`Event published to ${topic}`, eventData);
    } catch (error) {
      logger.error('Failed to publish event:', error);
      // Don't throw - just log the error
      // This prevents Kafka issues from breaking the API
    }
  }
}

module.exports = new KafkaProducer();