import cron from 'node-cron';
import smtpManager from '../services/smtpManager.js';
import notificationManager from '../services/notificationManager.js';

// Reset SMTP daily counts at midnight
cron.schedule('0 0 * * *', async () => {
  try {
    await smtpManager.resetDailyCounts();
    console.log('Daily SMTP counts reset successfully');
  } catch (error) {
    console.error('Failed to reset daily SMTP counts:', error);
  }
});

// Reset SMTP monthly counts on the first of each month
cron.schedule('0 0 1 * *', async () => {
  try {
    await smtpManager.resetMonthlyCounts();
    console.log('Monthly SMTP counts reset successfully');
  } catch (error) {
    console.error('Failed to reset monthly SMTP counts:', error);
  }
});

// Send inactivity reminders daily at 10 AM
cron.schedule('0 10 * * *', async () => {
  try {
    await notificationManager.sendInactivityReminders();
    console.log('Inactivity reminders sent successfully');
  } catch (error) {
    console.error('Failed to send inactivity reminders:', error);
  }
});

// Check for expiring emails daily at 9 AM
cron.schedule('0 9 * * *', async () => {
  try {
    await notificationManager.sendExpiryNotifications();
    console.log('Expiry notifications sent successfully');
  } catch (error) {
    console.error('Failed to send expiry notifications:', error);
  }
});