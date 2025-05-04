import { pool } from '../db/init.js';
import smtpManager from './smtpManager.js';
import {
  inactivityReminderTemplate,
  newEmailNotificationTemplate,
  tempEmailExpiryTemplate
} from '../templates/emails/index.js';

class NotificationManager {
  async sendInactivityReminders() {
    const connection = await pool.getConnection();
    try {
      // Get inactive users who haven't been notified recently
      const [inactiveUsers] = await connection.query(`
        SELECT 
          u.id,
          u.email,
          COUNT(te.id) as temp_email_count,
          es.unsubscribe_token
        FROM users u
        JOIN temp_emails te ON u.id = te.user_id
        LEFT JOIN email_subscriptions es ON u.id = es.user_id
        WHERE 
          u.last_activity_at < DATE_SUB(NOW(), INTERVAL 3 DAY)
          AND (u.last_notification_at IS NULL OR u.last_notification_at < DATE_SUB(NOW(), INTERVAL 7 DAY))
          AND (es.is_subscribed IS NULL OR es.is_subscribed = true)
          AND 'inactivity_reminder' = ANY(es.subscription_type)
        GROUP BY u.id
      `);

      // Send notifications
      for (const user of inactiveUsers) {
        try {
          const html = inactivityReminderTemplate.html
            .replace('{{name}}', user.email.split('@')[0])
            .replace('{{email_count}}', user.temp_email_count)
            .replace('{{dashboard_url}}', `${process.env.FRONTEND_URL}/dashboard`)
            .replace('{{unsubscribe_url}}', `${process.env.FRONTEND_URL}/unsubscribe/${user.unsubscribe_token}`);

          await smtpManager.sendEmail(
            user.email,
            inactivityReminderTemplate.subject,
            html
          );

          // Record notification
          await connection.query(`
            INSERT INTO email_notifications (
              user_id, 
              email_type,
              smtp_server_id,
              sent_to,
              subject,
              content,
              status,
              sent_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'sent', NOW())
          `, [
            user.id,
            'inactivity_reminder',
            smtpManager.currentSMTP.id,
            user.email,
            inactivityReminderTemplate.subject,
            html
          ]);

          // Update last notification time
          await connection.query(
            'UPDATE users SET last_notification_at = NOW() WHERE id = ?',
            [user.id]
          );
        } catch (error) {
          console.error(`Failed to send notification to ${user.email}:`, error);
          
          // Record failed notification
          await connection.query(`
            INSERT INTO email_notifications (
              user_id,
              email_type,
              smtp_server_id,
              sent_to,
              subject,
              content,
              status,
              error_message
            ) VALUES (?, ?, ?, ?, ?, ?, 'failed', ?)
          `, [
            user.id,
            'inactivity_reminder',
            smtpManager.currentSMTP?.id,
            user.email,
            inactivityReminderTemplate.subject,
            html,
            error.message
          ]);
        }
      }
    } catch (error) {
      console.error('Failed to send inactivity reminders:', error);
      throw error;
    } finally {
      connection.release();
    }
  }

  async sendNewEmailNotifications(tempEmailId, receivedEmail) {
    const connection = await pool.getConnection();
    try {
      // Get temp email owner and subscription status
      const [users] = await connection.query(`
        SELECT 
          u.id,
          u.email,
          te.email as temp_email,
          es.unsubscribe_token
        FROM users u
        JOIN temp_emails te ON u.id = te.user_id
        LEFT JOIN email_subscriptions es ON u.id = es.user_id
        WHERE 
          te.id = ?
          AND (es.is_subscribed IS NULL OR es.is_subscribed = true)
          AND 'new_email_notification' = ANY(es.subscription_type)
      `, [tempEmailId]);

      if (users.length === 0) return;

      const user = users[0];

      // Send notification
      const html = newEmailNotificationTemplate.html
        .replace('{{name}}', user.email.split('@')[0])
        .replace('{{temp_email}}', user.temp_email)
        .replace('{{sender}}', receivedEmail.from_email)
        .replace('{{subject}}', receivedEmail.subject)
        .replace('{{email_url}}', `${process.env.FRONTEND_URL}/dashboard/email/${tempEmailId}`)
        .replace('{{unsubscribe_url}}', `${process.env.FRONTEND_URL}/unsubscribe/${user.unsubscribe_token}`);

      const subject = newEmailNotificationTemplate.subject.replace('{{temp_email}}', user.temp_email);

      await smtpManager.sendEmail(
        user.email,
        subject,
        html
      );

      // Record notification
      await connection.query(`
        INSERT INTO email_notifications (
          user_id,
          email_type,
          smtp_server_id,
          sent_to,
          subject,
          content,
          status,
          sent_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'sent', NOW())
      `, [
        user.id,
        'new_email_notification',
        smtpManager.currentSMTP.id,
        user.email,
        subject,
        html
      ]);

    } catch (error) {
      console.error('Failed to send new email notification:', error);
      throw error;
    } finally {
      connection.release();
    }
  }

  async sendExpiryNotifications() {
    const connection = await pool.getConnection();
    try {
      // Get temp emails expiring in 7 days
      const [expiringEmails] = await connection.query(`
        SELECT 
          u.id as user_id,
          u.email as user_email,
          te.id as temp_email_id,
          te.email as temp_email,
          DATEDIFF(te.expires_at, NOW()) as days_left,
          es.unsubscribe_token
        FROM temp_emails te
        JOIN users u ON te.user_id = u.id
        LEFT JOIN email_subscriptions es ON u.id = es.user_id
        WHERE 
          te.expires_at BETWEEN NOW() AND DATE_ADD(NOW(), INTERVAL 7 DAY)
          AND (es.is_subscribed IS NULL OR es.is_subscribed = true)
          AND 'temp_email_expiry' = ANY(es.subscription_type)
      `);

      // Send notifications
      for (const email of expiringEmails) {
        try {
          const html = tempEmailExpiryTemplate.html
            .replace('{{name}}', email.user_email.split('@')[0])
            .replace('{{temp_email}}', email.temp_email)
            .replace('{{days_left}}', email.days_left)
            .replace('{{dashboard_url}}', `${process.env.FRONTEND_URL}/dashboard`)
            .replace('{{unsubscribe_url}}', `${process.env.FRONTEND_URL}/unsubscribe/${email.unsubscribe_token}`);

          const subject = tempEmailExpiryTemplate.subject.replace('{{temp_email}}', email.temp_email);

          await smtpManager.sendEmail(
            email.user_email,
            subject,
            html
          );

          // Record notification
          await connection.query(`
            INSERT INTO email_notifications (
              user_id,
              email_type,
              smtp_server_id,
              sent_to,
              subject,
              content,
              status,
              sent_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'sent', NOW())
          `, [
            email.user_id,
            'temp_email_expiry',
            smtpManager.currentSMTP.id,
            email.user_email,
            subject,
            html
          ]);

        } catch (error) {
          console.error(`Failed to send expiry notification for ${email.temp_email}:`, error);
        }
      }
    } catch (error) {
      console.error('Failed to send expiry notifications:', error);
      throw error;
    } finally {
      connection.release();
    }
  }
}

export default new NotificationManager();