import nodemailer from 'nodemailer';
import { pool } from '../db/init.js';

class SMTPManager {
  constructor() {
    this.currentSMTP = null;
    this.transporter = null;
    this.smtpConfigs = [];
    this.initializeConfigs();
  }

  initializeConfigs() {
    // Load SMTP configurations from environment variables
    for (let i = 1; i <= 20; i++) {
      const username = process.env[`SMTP_USER_${i}`];
      const password = process.env[`SMTP_PASS_${i}`];
      const fromEmail = process.env[`SMTP_FROM_${i}`];
      const fromName = process.env[`SMTP_NAME_${i}`];

      // Skip if required credentials are missing
      if (!username || !password || !fromEmail) continue;

      this.smtpConfigs.push({
        id: `smtp-${i}`,
        host: process.env.SMTP_HOST,
        port: parseInt(process.env.SMTP_PORT || '587'),
        username,
        password,
        fromEmail,
        fromName: fromName || 'Boomlify',
        rotationOrder: i,
        dailyLimit: 450,
        monthlyLimit: 9500,
        dailyCount: 0,
        monthlyCount: 0,
        errorCount: 0,
        lastError: null,
        lastErrorAt: null,
        lastUsedAt: null,
        isActive: true
      });
    }

    if (this.smtpConfigs.length === 0) {
      throw new Error('No SMTP configurations found in environment variables');
    }

    console.log(`Loaded ${this.smtpConfigs.length} SMTP configurations`);
  }

  async initialize() {
    try {
      // Initialize database records for SMTP configs
      await this.syncConfigsWithDB();
      
      // Get the first active SMTP server
      await this.rotateToNextSMTP();
    } catch (error) {
      console.error('Failed to initialize SMTP Manager:', error);
      throw error;
    }
  }

  async syncConfigsWithDB() {
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      // Get existing SMTP servers
      const [existingServers] = await connection.query(
        'SELECT id FROM smtp_servers WHERE id LIKE "smtp-%"'
      );
      const existingIds = new Set(existingServers.map(s => s.id));

      // Insert or update SMTP configurations
      for (const config of this.smtpConfigs) {
        if (existingIds.has(config.id)) {
          await connection.query(`
            UPDATE smtp_servers 
            SET 
              host = ?,
              port = ?,
              username = ?,
              password = ?,
              from_email = ?,
              from_name = ?,
              rotation_order = ?,
              daily_limit = ?,
              monthly_limit = ?,
              is_active = true
            WHERE id = ?
          `, [
            config.host,
            config.port,
            config.username,
            config.password,
            config.fromEmail,
            config.fromName,
            config.rotationOrder,
            config.dailyLimit,
            config.monthlyLimit,
            config.id
          ]);
        } else {
          await connection.query(`
            INSERT INTO smtp_servers (
              id, host, port, username, password, from_email, from_name,
              rotation_order, daily_limit, monthly_limit, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true)
          `, [
            config.id,
            config.host,
            config.port,
            config.username,
            config.password,
            config.fromEmail,
            config.fromName,
            config.rotationOrder,
            config.dailyLimit,
            config.monthlyLimit
          ]);
        }
      }

      // Deactivate removed configurations
      const configIds = this.smtpConfigs.map(c => c.id);
      await connection.query(
        'UPDATE smtp_servers SET is_active = false WHERE id LIKE "smtp-%" AND id NOT IN (?)',
        [configIds]
      );

      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  async rotateToNextSMTP() {
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      // Get next available SMTP server
      const [servers] = await connection.query(`
        SELECT * FROM smtp_servers
        WHERE is_active = true
          AND daily_email_count < daily_limit
          AND monthly_email_count < monthly_limit
          AND (error_count < 3 OR last_error_at < DATE_SUB(NOW(), INTERVAL 1 HOUR))
        ORDER BY 
          CASE 
            WHEN last_used_at < DATE(NOW()) THEN rotation_order -- New day, start from beginning
            ELSE rotation_order 
          END,
          last_used_at ASC
        LIMIT 1
      `);

      if (servers.length === 0) {
        throw new Error('No available SMTP servers');
      }

      const smtp = servers[0];

      // Update last used timestamp
      await connection.query(
        'UPDATE smtp_servers SET last_used_at = NOW() WHERE id = ?',
        [smtp.id]
      );

      await connection.commit();

      // Create new transporter
      this.transporter = nodemailer.createTransport({
        host: smtp.host,
        port: smtp.port,
        secure: false,
        auth: {
          user: smtp.username,
          pass: smtp.password
        },
        tls: {
          rejectUnauthorized: false
        }
      });

      this.currentSMTP = smtp;

      return smtp;
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  async sendEmail(to, subject, html) {
    if (!this.transporter || !this.currentSMTP) {
      await this.initialize();
    }

    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      // Check if we need to rotate SMTP
      const [currentStats] = await connection.query(
        'SELECT daily_email_count, monthly_email_count FROM smtp_servers WHERE id = ?',
        [this.currentSMTP.id]
      );

      if (
        currentStats[0].daily_email_count >= this.currentSMTP.daily_limit ||
        currentStats[0].monthly_email_count >= this.currentSMTP.monthly_limit
      ) {
        await this.rotateToNextSMTP();
      }

      // Send email
      const result = await this.transporter.sendMail({
        from: `"${this.currentSMTP.from_name}" <${this.currentSMTP.from_email}>`,
        to,
        subject,
        html
      });

      // Update counters
      await connection.query(`
        UPDATE smtp_servers 
        SET 
          daily_email_count = daily_email_count + 1,
          monthly_email_count = monthly_email_count + 1,
          error_count = 0,
          last_error = NULL,
          last_error_at = NULL
        WHERE id = ?
      `, [this.currentSMTP.id]);

      await connection.commit();
      return result;
    } catch (error) {
      await connection.rollback();

      // Update error stats
      await connection.query(`
        UPDATE smtp_servers 
        SET 
          error_count = error_count + 1,
          last_error = ?,
          last_error_at = NOW()
        WHERE id = ?
      `, [error.message, this.currentSMTP.id]);

      // Try to rotate to next SMTP
      await this.rotateToNextSMTP();
      throw error;
    } finally {
      connection.release();
    }
  }

  async resetDailyCounts() {
    const connection = await pool.getConnection();
    try {
      await connection.query('UPDATE smtp_servers SET daily_email_count = 0');
    } catch (error) {
      console.error('Failed to reset daily counts:', error);
      throw error;
    } finally {
      connection.release();
    }
  }

  async resetMonthlyCounts() {
    const connection = await pool.getConnection();
    try {
      await connection.query('UPDATE smtp_servers SET monthly_email_count = 0');
    } catch (error) {
      console.error('Failed to reset monthly counts:', error);
      throw error;
    } finally {
      connection.release();
    }
  }
}

// Create singleton instance
const smtpManager = new SMTPManager();

export default smtpManager;