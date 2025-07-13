const mysql = require('mysql2/promise');

class Database {
  constructor() {
    this.connections = {
      shard1: { master: null, replicas: [] },
      shard2: { master: null, replicas: [] },
      readReplica: null
    };
  }

  async initialize() {
    try {
      // Shard 1 connections
      this.connections.shard1.master = await mysql.createConnection({
        host: process.env.SHARD1_MASTER_HOST,
        port: parseInt(process.env.SHARD1_MASTER_PORT),
        user: process.env.SHARD1_MASTER_USER,
        password: process.env.SHARD1_MASTER_PASSWORD,
        database: process.env.SHARD1_MASTER_DATABASE
      });

      this.connections.shard1.replicas = [
        await mysql.createConnection({
          host: process.env.SHARD1_REPLICA1_HOST,
          port: parseInt(process.env.SHARD1_REPLICA1_PORT),
          user: process.env.SHARD1_MASTER_USER,
          password: process.env.SHARD1_MASTER_PASSWORD,
          database: process.env.SHARD1_MASTER_DATABASE
        }),
        await mysql.createConnection({
          host: process.env.SHARD1_REPLICA2_HOST,
          port: parseInt(process.env.SHARD1_REPLICA2_PORT),
          user: process.env.SHARD1_MASTER_USER,
          password: process.env.SHARD1_MASTER_PASSWORD,
          database: process.env.SHARD1_MASTER_DATABASE
        })
      ];

      // Shard 2 connections
      this.connections.shard2.master = await mysql.createConnection({
        host: process.env.SHARD2_MASTER_HOST,
        port: parseInt(process.env.SHARD2_MASTER_PORT),
        user: process.env.SHARD2_MASTER_USER,
        password: process.env.SHARD2_MASTER_PASSWORD,
        database: process.env.SHARD2_MASTER_DATABASE
      });

      this.connections.shard2.replicas = [
        await mysql.createConnection({
          host: process.env.SHARD2_REPLICA1_HOST,
          port: parseInt(process.env.SHARD2_REPLICA1_PORT),
          user: process.env.SHARD2_MASTER_USER,
          password: process.env.SHARD2_MASTER_PASSWORD,
          database: process.env.SHARD2_MASTER_DATABASE
        }),
        await mysql.createConnection({
          host: process.env.SHARD2_REPLICA2_HOST,
          port: parseInt(process.env.SHARD2_REPLICA2_PORT),
          user: process.env.SHARD2_MASTER_USER,
          password: process.env.SHARD2_MASTER_PASSWORD,
          database: process.env.SHARD2_MASTER_DATABASE
        })
      ];

      // Read replica
      this.connections.readReplica = await mysql.createConnection({
        host: process.env.READ_REPLICA_HOST,
        port: parseInt(process.env.READ_REPLICA_PORT),
        user: process.env.READ_REPLICA_USER,
        password: process.env.READ_REPLICA_PASSWORD,
        database: process.env.READ_REPLICA_DATABASE
      });

      console.log('All database connections established');

      // Now create tables after connections are established
      await this.createTables();
      await this.setupShardAutoIncrement();

    } catch (error) {
      console.error('Database connection error:', error);
      throw error;
    }
  }

  async createTables() {
    const createUserTableSQL = `
      CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;

    // Create tables on both shards
    await this.connections.shard1.master.execute(createUserTableSQL);
    await this.connections.shard2.master.execute(createUserTableSQL);
  }

  async setupShardAutoIncrement() {
    // Set shard1 to start from 1
    await this.connections.shard1.master.execute(`
      ALTER TABLE users AUTO_INCREMENT = 1
    `);
    
    // Set shard2 to start from 1000000
    await this.connections.shard2.master.execute(`
      ALTER TABLE users AUTO_INCREMENT = 1000000
    `);
  }

  getConnections() {
    return this.connections;
  }
}

module.exports = new Database();