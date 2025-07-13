# MySQL Sharded API

Node.js REST API with MySQL sharding, replication, and Kafka integration.

## Architecture

- 2 MySQL shards with 2 replicas each
- 1 global read replica
- Kafka for event streaming
- Hash-based sharding

## Setup

1. Install dependencies:
```bash
npm install