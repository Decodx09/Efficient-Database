const mysql = require('mysql2/promise');

async function quickDBTest() {
  const configs = [
    { host: 'localhost', port: 3306, database: 'shard1' },
    { host: 'localhost', port: 3307, database: 'shard2' },
    { host: 'localhost', port: 3308, database: 'shard3' }
  ];

  console.log('🔍 Testing Database Performance...');
  
  for (let i = 0; i < configs.length; i++) {
    try {
      const conn = await mysql.createConnection({
        ...configs[i],
        user: 'root',
        password: 'rootpass'
      });
      
      const start = Date.now();
      await conn.execute('SELECT COUNT(*) FROM users');
      const time = Date.now() - start;
      
      console.log(`✅ Shard ${i + 1}: ${time}ms`);
      await conn.end();
    } catch (error) {
      console.log(`❌ Shard ${i + 1}: ERROR - ${error.message}`);
    }
  }
}

quickDBTest();