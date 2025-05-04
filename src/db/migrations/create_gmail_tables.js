import { pool } from '../init.js';

export async function createGmailTables() {
  const connection = await pool.getConnection();
  
  try {
    console.log('Creating Gmail tables if they don\'t exist...');
    
    // Create Gmail Credentials table
    await connection.query(`
      CREATE TABLE IF NOT EXISTS gmail_credentials (
        id VARCHAR(36) PRIMARY KEY,
        client_id TEXT NOT NULL,
        client_secret TEXT NOT NULL,
        redirect_uri TEXT NOT NULL,
        active BOOLEAN DEFAULT TRUE,
        usage_count INT DEFAULT 0,
        last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      ) ENGINE=InnoDB;
    `);
    
    // Create Gmail Accounts table
    await connection.query(`
      CREATE TABLE IF NOT EXISTS gmail_accounts (
        id VARCHAR(36) PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        refresh_token TEXT,
        access_token TEXT,
        expires_at BIGINT,
        quota_used INT DEFAULT 0,
        alias_count INT DEFAULT 0,
        status VARCHAR(20) DEFAULT 'active',
        last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY email_unique (email)
      ) ENGINE=InnoDB;
    `);
    
    console.log('Gmail tables created successfully');
    
  } catch (error) {
    console.error('Failed to create Gmail tables:', error);
    throw error;
  } finally {
    connection.release();
  }
}
