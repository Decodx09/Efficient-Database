const ShardingConfig = require('../config/sharding');

class UserModel {
  constructor(dbConnections) {
    console.log('=== UserModel constructor ===');
    console.log('Received connections:', Object.keys(dbConnections));
    
    // Check if connections exist
    if (dbConnections.shard1) {
      console.log('Shard1 master exists:', !!dbConnections.shard1.master);
      console.log('Shard1 replicas count:', dbConnections.shard1.replicas?.length || 0);
    }
    if (dbConnections.shard2) {
      console.log('Shard2 master exists:', !!dbConnections.shard2.master);
      console.log('Shard2 replicas count:', dbConnections.shard2.replicas?.length || 0);
    }
    
    this.connections = dbConnections;
  }

  async create(userData) {
    console.log('=== UserModel.create called ===');
    console.log('userData:', userData);
    
    const { name, email } = userData;
    
    try {
      console.log('Getting shard key for email:', email);
      const shardKey = ShardingConfig.getShardKeyByEmail(email);
      console.log('Selected shard:', shardKey);
      
      console.log('Getting connection for shard:', shardKey);
      const connection = ShardingConfig.getConnection(this.connections, shardKey, 'write');
      console.log('Got connection:', !!connection);
      
      console.log('Executing INSERT query...');
      const [result] = await connection.execute(
        'INSERT INTO users (name, email) VALUES (?, ?)',
        [name, email]
      );
      
      console.log('Insert successful, result:', result);
      
      return { 
        id: result.insertId, 
        name, 
        email, 
        shard: shardKey 
      };
    } catch (error) {
      console.error('=== Error in UserModel.create ===');
      console.error('Error code:', error.code);
      console.error('Error message:', error.message);
      console.error('Error stack:', error.stack);
      throw error;
    }
  }

  async findAll() {
    console.log('=== UserModel.findAll called ===');
    const allUsers = [];
    
    // Query both shards
    for (const shardKey of ['shard1', 'shard2']) {
      try {
        console.log(`Querying ${shardKey}...`);
        const connection = ShardingConfig.getConnection(this.connections, shardKey, 'read');
        const [rows] = await connection.execute(
          'SELECT * FROM users ORDER BY id DESC LIMIT 100'
        );
        
        console.log(`Found ${rows.length} users in ${shardKey}`);
        const usersWithShard = rows.map(user => ({ ...user, shard: shardKey }));
        allUsers.push(...usersWithShard);
      } catch (error) {
        console.error(`Error querying ${shardKey}:`, error);
      }
    }
    
    allUsers.sort((a, b) => b.id - a.id);
    console.log(`Total users found: ${allUsers.length}`);
    
    return allUsers;
  }

  // ... other methods remain the same
}

module.exports = UserModel;