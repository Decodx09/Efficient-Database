const { Kafka } = require('kafkajs');

class KafkaConfig {
  constructor() {
    this.kafka = new Kafka({
      clientId: 'api-service',
      brokers: process.env.KAFKA_BROKERS.split(',')
    });
    this.producer = null;
    this.consumer = null;
  }

  async connectProducer() {
    this.producer = this.kafka.producer();
    await this.producer.connect();
  }

  async connectConsumer(groupId) {
    this.consumer = this.kafka.consumer({ groupId });
    await this.consumer.connect();
  }

  getProducer() {
    return this.producer;
  }

  getConsumer() {
    return this.consumer;
  }

  async disconnect() {
    if (this.producer) await this.producer.disconnect();
    if (this.consumer) await this.consumer.disconnect();
  }
}

module.exports = new KafkaConfig();