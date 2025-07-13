-- Run on each master to create replication user
CREATE USER 'repl'@'%' IDENTIFIED BY 'password';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;

-- Get master status (run on master)
SHOW MASTER STATUS;

-- Configure replica (run on each replica)
-- Replace LOG_FILE and LOG_POS with values from SHOW MASTER STATUS
CHANGE MASTER TO
  MASTER_HOST='mysql-shard1-master',
  MASTER_USER='repl',
  MASTER_PASSWORD='password',
  MASTER_LOG_FILE='mysql-bin.000001',
  MASTER_LOG_POS=0;

START SLAVE;
SHOW SLAVE STATUS\G;