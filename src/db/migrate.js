import mysql from 'mysql2/promise';
import dotenv from 'dotenv';

dotenv.config();

const migration = `
  -- Add meta tags columns
  ALTER TABLE blog_posts
  ADD COLUMN IF NOT EXISTS og_title VARCHAR(255),
  ADD COLUMN IF NOT EXISTS og_description TEXT,
  ADD COLUMN IF NOT EXISTS og_image VARCHAR(255),
  ADD COLUMN IF NOT EXISTS og_type VARCHAR(50) DEFAULT 'article',
  ADD COLUMN IF NOT EXISTS twitter_title VARCHAR(255),
  ADD COLUMN IF NOT EXISTS twitter_description TEXT,
  ADD COLUMN IF NOT EXISTS twitter_image VARCHAR(255),
  ADD COLUMN IF NOT EXISTS twitter_card VARCHAR(50) DEFAULT 'summary_large_image',
  ADD COLUMN IF NOT EXISTS canonical_url VARCHAR(255),
  ADD COLUMN IF NOT EXISTS structured_data JSON,
  ADD COLUMN IF NOT EXISTS meta_tags TEXT;

  -- Add indexes
  CREATE INDEX idx_blog_meta_title ON blog_posts(meta_title);
  CREATE INDEX idx_blog_canonical ON blog_posts(canonical_url);
  CREATE INDEX idx_blog_og_type ON blog_posts(og_type);
`;

async function runMigration() {
  try {
    const connection = await mysql.createConnection({
      host: process.env.DB_HOST,
      port: process.env.DB_PORT || 25060,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      ssl: {
        rejectUnauthorized: false
      }
    });

    console.log('Running migration...');
    
    // Split migration into individual statements
    const statements = migration.split(';').filter(stmt => stmt.trim());
    
    // Execute each statement separately
    for (const stmt of statements) {
      try {
        await connection.query(stmt);
      } catch (err) {
        // Ignore errors for "column/index already exists"
        if (!err.message.includes('Duplicate') && !err.message.includes('already exists')) {
          throw err;
        }
      }
    }
    
    console.log('Migration completed successfully!');
    await connection.end();
  } catch (error) {
    console.error('Migration failed:', error);
    process.exit(1);
  }
}

runMigration();
