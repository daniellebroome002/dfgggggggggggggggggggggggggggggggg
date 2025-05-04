import mysql from 'mysql2/promise';
import dotenv from 'dotenv';

dotenv.config();

// Create the pool with optimized settings for DigitalOcean MySQL
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT || 25060, // DigitalOcean's default MySQL port
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 150, // Optimized for DigitalOcean MySQL
  maxIdle: 50, // Keep fewer idle connections
  idleTimeout: 30000, // Reduce idle timeout to 30 seconds
  queueLimit: 0, // No limit on queue size
  enableKeepAlive: true,
  keepAliveInitialDelay: 10000,
  connectTimeout: 10000, // Connection timeout in milliseconds
  ssl: {
    // For DigitalOcean Managed MySQL
    rejectUnauthorized: false, // Required for DigitalOcean's self-signed certificates
    // Fix TLS issues by specifying min and max versions
    minVersion: 'TLSv1.2',
    maxVersion: 'TLSv1.3',
    // Explicitly specify ciphers to avoid decode error
    ciphers: 'TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384'
  }
});

// Connection monitoring and error handling
pool.on('connection', (connection) => {
  console.log('New database connection established');
  
  connection.on('error', (err) => {
    console.error('Database connection error:', err);
    if (err.code === 'PROTOCOL_CONNECTION_LOST') {
      console.error('Database connection was closed');
    }
    if (err.code === 'ER_CON_COUNT_ERROR') {
      console.error('Database has too many connections');
    }
    if (err.code === 'ECONNREFUSED') {
      console.error('Database connection was refused');
    }
  });

  // Monitor query execution time
  connection.on('query', (query) => {
    const start = Date.now();
    connection.once('result', () => {
      const duration = Date.now() - start;
      if (duration > 1000) { // Log slow queries (>1s)
        console.warn('Slow query detected:', {
          query: query.sql,
          duration: duration + 'ms'
        });
      }
    });
  });
});



// Enhanced health check with connection metrics
export async function checkDatabaseConnection() {
  try {
    const connection = await pool.getConnection();
    
    // Get connection stats
    const [threadStatus] = await connection.query('SHOW STATUS LIKE "Threads_connected"');
    const [maxConnections] = await connection.query('SHOW VARIABLES LIKE "max_connections"');
    const [waitEvents] = await connection.query('SHOW STATUS LIKE "Threads_waiting_for_connection_count"');
    
    const stats = {
      activeConnections: parseInt(threadStatus[0].Value),
      maxAllowed: parseInt(maxConnections[0].Value),
      waitingThreads: parseInt(waitEvents[0].Value),
      poolSize: pool.pool.config.connectionLimit,
      queueSize: pool.pool.waitingClientsCount()
    };
    
    connection.release();
    
    return {
      healthy: true,
      timestamp: new Date().toISOString(),
      metrics: stats,
      warning: stats.activeConnections > (stats.poolSize * 0.8) ? 'High connection usage' : null
    };
  } catch (error) {
    console.error('Database health check failed:', error);
    return {
      healthy: false,
      timestamp: new Date().toISOString(),
      error: error.message
    };
  }
}

export async function initializeDatabase() {
  try {
    console.log('Attempting to connect to database...');
    console.log('Database host:', process.env.DB_HOST);
    
    const connection = await pool.getConnection();
    console.log('Successfully connected to database');
    
    // Test the connection
    await connection.query('SELECT 1');
    
    // Create tables
    await createTables(connection);
    
    connection.release();
    console.log('Database initialized successfully');
    return pool;
  } catch (error) {
    console.error('Error initializing database:', error);
    throw error;
  }
}

async function createTables(connection) {
  // Users table with optimized settings
  await connection.query(`
    CREATE TABLE IF NOT EXISTS users (
      id VARCHAR(36) PRIMARY KEY,
      email VARCHAR(255) UNIQUE NOT NULL,
      password VARCHAR(255) NOT NULL,
      google_id VARCHAR(255) UNIQUE,
      is_admin BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      last_login TIMESTAMP,
      INDEX idx_user_email (email),
      INDEX idx_google_id (google_id)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
  `);

  // Domains table
  await connection.query(`
    CREATE TABLE IF NOT EXISTS domains (
      id VARCHAR(36) PRIMARY KEY,
      domain VARCHAR(255) UNIQUE NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_domain_name (domain)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
  `);

  // Temporary emails table with partitioning by year
  await connection.query(`
    CREATE TABLE IF NOT EXISTS temp_emails (
      id VARCHAR(36) PRIMARY KEY,
      user_id VARCHAR(36),
      email VARCHAR(255) UNIQUE NOT NULL,
      domain_id VARCHAR(36) NOT NULL,
      expires_at TIMESTAMP NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
      FOREIGN KEY (domain_id) REFERENCES domains(id) ON DELETE CASCADE,
      INDEX idx_temp_email (email),
      INDEX idx_user_id (user_id),
      INDEX idx_expiry (expires_at),
      INDEX idx_user_expiry (user_id, expires_at),
      INDEX idx_domain_expiry (domain_id, expires_at)
    ) ENGINE=InnoDB
    PARTITION BY RANGE (YEAR(expires_at)) (
      PARTITION p2024 VALUES LESS THAN (2025),
      PARTITION p2025 VALUES LESS THAN (2026),
      PARTITION p_future VALUES LESS THAN MAXVALUE
    );
  `);

  // Received emails table with partitioning by month
  await connection.query(`
    CREATE TABLE IF NOT EXISTS received_emails (
      id VARCHAR(36) PRIMARY KEY,
      temp_email_id VARCHAR(36) NOT NULL,
      from_email VARCHAR(255) NOT NULL,
      from_name VARCHAR(255) NOT NULL,
      subject TEXT,
      body_html LONGTEXT,
      body_text LONGTEXT,
      received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (temp_email_id) REFERENCES temp_emails(id) ON DELETE CASCADE,
      INDEX idx_temp_email_id (temp_email_id),
      INDEX idx_received_at (received_at),
      INDEX idx_email_received (temp_email_id, received_at),
      INDEX idx_from_received (from_email, received_at)
    ) ENGINE=InnoDB
    PARTITION BY RANGE (MONTH(received_at)) (
      PARTITION p1 VALUES LESS THAN (2),
      PARTITION p2 VALUES LESS THAN (3),
      PARTITION p3 VALUES LESS THAN (4),
      PARTITION p4 VALUES LESS THAN (5),
      PARTITION p5 VALUES LESS THAN (6),
      PARTITION p6 VALUES LESS THAN (7),
      PARTITION p7 VALUES LESS THAN (8),
      PARTITION p8 VALUES LESS THAN (9),
      PARTITION p9 VALUES LESS THAN (10),
      PARTITION p10 VALUES LESS THAN (11),
      PARTITION p11 VALUES LESS THAN (12),
      PARTITION p12 VALUES LESS THAN MAXVALUE
    );
  `);

  // Email attachments table
  await connection.query(`
    CREATE TABLE IF NOT EXISTS email_attachments (
      id VARCHAR(36) PRIMARY KEY,
      email_id VARCHAR(36) NOT NULL,
      filename VARCHAR(255) NOT NULL,
      content_type VARCHAR(100),
      size BIGINT,
      content LONGTEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (email_id) REFERENCES received_emails(id) ON DELETE CASCADE,
      INDEX idx_email_id (email_id),
      INDEX idx_attachment_filename (filename)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
  `);

  // Custom messages table
  await connection.query(`
    CREATE TABLE IF NOT EXISTS custom_messages (
      id VARCHAR(36) PRIMARY KEY,
      content TEXT NOT NULL,
      type ENUM('info', 'warning', 'success', 'error') NOT NULL DEFAULT 'info',
      is_active BOOLEAN DEFAULT TRUE,
      created_by VARCHAR(36),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE,
      INDEX idx_active_messages (is_active),
      INDEX idx_created_at (created_at)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
  `);

  // User dismissed messages table
  await connection.query(`
    CREATE TABLE IF NOT EXISTS user_dismissed_messages (
      user_id VARCHAR(36) NOT NULL,
      message_id VARCHAR(36) NOT NULL,
      dismissed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (user_id, message_id),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
      FOREIGN KEY (message_id) REFERENCES custom_messages(id) ON DELETE CASCADE,
      INDEX idx_user_dismissals (user_id, dismissed_at)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
  `);

  // Blog posts table
  await connection.query(`
    CREATE TABLE IF NOT EXISTS blog_posts (
      id VARCHAR(36) PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      slug VARCHAR(255) NOT NULL UNIQUE,
      content LONGTEXT NOT NULL,
      category VARCHAR(100) NOT NULL,
      meta_title VARCHAR(255),
      meta_description TEXT,
      keywords TEXT,
      featured_image VARCHAR(255),
      status ENUM('draft', 'published') DEFAULT 'draft',
      author_email VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      is_featured BOOLEAN DEFAULT FALSE,
      is_trending BOOLEAN DEFAULT FALSE,
      featured_order INT,
      trending_order INT,
      INDEX idx_slug (slug),
      INDEX idx_category (category),
      INDEX idx_status (status),
      INDEX idx_created_at (created_at),
      INDEX idx_featured (is_featured, featured_order),
      INDEX idx_trending (is_trending, trending_order)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
  `);

  // Blog images table
  await connection.query(`
    CREATE TABLE IF NOT EXISTS blog_images (
      id VARCHAR(36) PRIMARY KEY,
      post_id VARCHAR(36) NOT NULL,
      url VARCHAR(255) NOT NULL,
      alt_text TEXT,
      caption TEXT,
      is_featured BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      \`order\` INT DEFAULT 0,
      FOREIGN KEY (post_id) REFERENCES blog_posts(id) ON DELETE CASCADE,
      INDEX idx_post_id (post_id),
      INDEX idx_is_featured (is_featured)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
  `);

  // Request logs table for IP and Request ID tracking
  await connection.query(`
    CREATE TABLE IF NOT EXISTS request_logs (
      id VARCHAR(36) PRIMARY KEY,
      request_id VARCHAR(64) NOT NULL,
      client_ip VARCHAR(45) NOT NULL,
      user_id VARCHAR(36),
      user_agent TEXT,
      request_path VARCHAR(255) NOT NULL,
      request_method VARCHAR(10) NOT NULL,
      status_code INT,
      response_time INT,
      geo_country VARCHAR(100),
      geo_city VARCHAR(100),
      geo_region VARCHAR(100),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      referer VARCHAR(512),
      is_bot BOOLEAN DEFAULT FALSE,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
      INDEX idx_request_id (request_id),
      INDEX idx_client_ip (client_ip),
      INDEX idx_created_at (created_at),
      INDEX idx_path (request_path),
      INDEX idx_user_id (user_id)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
  `);
  
  // Gmail accounts table
  await connection.query(`
    CREATE TABLE IF NOT EXISTS gmail_accounts (
      id VARCHAR(36) PRIMARY KEY,
      email VARCHAR(255) UNIQUE NOT NULL,
      app_password TEXT,
      quota_used INT DEFAULT 0,
      alias_count INT DEFAULT 0,
      status VARCHAR(20) DEFAULT 'active',
      last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_email (email),
      INDEX idx_status (status)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
  `);
}

// Cleanup function with stats logging
async function cleanup() {
  try {
    const stats = await checkDatabaseConnection();
    console.log('Connection stats before cleanup:', stats);
    
    await pool.end();
    console.log('All database connections closed');
  } catch (err) {
    console.error('Error closing database connections:', err);
  }
}

// Handle graceful shutdown
process.on('SIGTERM', cleanup);
process.on('SIGINT', cleanup);

export { pool };
