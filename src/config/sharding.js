class ShardingConfig {
    static getShardKey(userId) {
      return userId % 2 === 0 ? 'shard1' : 'shard2';
    }
  
    static getConnection(connections, shardKey, operationType) {
      const shard = connections[shardKey];
      
      if (operationType === 'write') {
        return shard.master;
      } else {
        // Simple round-robin for read replicas
        const replicaIndex = Math.floor(Math.random() * shard.replicas.length);
        return shard.replicas[replicaIndex];
      }
    }
  }
  
  module.exports = ShardingConfig;