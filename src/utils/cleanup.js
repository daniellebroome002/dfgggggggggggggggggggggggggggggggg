import { pool } from '../db/init.js';

export async function cleanupOldEmails() {
  try {
    console.log('Starting cleanup process for old received emails...');
    
    // Delete received emails older than 14 days
    const [result] = await pool.query(`
      DELETE re FROM received_emails re
      WHERE re.received_at < DATE_SUB(NOW(), INTERVAL 14 DAY)
    `);

    console.log(`Cleanup completed. Deleted ${result.affectedRows} old received emails.`);
    
    // Clean up orphaned attachments but keep temp emails
    const [attachmentResult] = await pool.query(`
      DELETE ea FROM email_attachments ea
      LEFT JOIN received_emails re ON ea.email_id = re.id
      WHERE re.id IS NULL
    `);

    console.log(`Cleaned up ${attachmentResult.affectedRows} orphaned attachments.`);
    
    return {
      deletedEmails: result.affectedRows,
      deletedAttachments: attachmentResult.affectedRows
    };
  } catch (error) {
    console.error('Error during email cleanup:', error);
    throw error;
  }
}

export async function manualCleanup(days = 10) {
  try {
    console.log(`Starting manual cleanup process for emails older than ${days} days...`);
    
    // Delete received emails older than specified days
    const [result] = await pool.query(`
      DELETE re FROM received_emails re
      WHERE re.received_at < DATE_SUB(NOW(), INTERVAL ? DAY)
    `, [days]);

    console.log(`Manual cleanup completed. Deleted ${result.affectedRows} old received emails.`);
    
    // Clean up orphaned attachments
    const [attachmentResult] = await pool.query(`
      DELETE ea FROM email_attachments ea
      LEFT JOIN received_emails re ON ea.email_id = re.id
      WHERE re.id IS NULL
    `);

    console.log(`Cleaned up ${attachmentResult.affectedRows} orphaned attachments.`);
    
    return {
      deletedEmails: result.affectedRows,
      deletedAttachments: attachmentResult.affectedRows
    };
  } catch (error) {
    console.error('Error during manual cleanup:', error);
    throw error;
  }
}
