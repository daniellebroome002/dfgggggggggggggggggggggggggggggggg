import express from 'express';
import { pool } from '../db/init.js';
import { authenticateToken } from '../middleware/auth.js';

const router = express.Router();

// Update subscription preferences
router.post('/preferences', authenticateToken, async (req, res) => {
  const { subscriptionTypes } = req.body;
  const userId = req.user.id;

  const connection = await pool.getConnection();
  try {
    // Check if subscription exists
    const [existing] = await connection.query(
      'SELECT id FROM email_subscriptions WHERE user_id = ?',
      [userId]
    );

    if (existing.length > 0) {
      // Update existing subscription
      await connection.query(
        'UPDATE email_subscriptions SET subscription_type = ?, updated_at = NOW() WHERE user_id = ?',
        [subscriptionTypes, userId]
      );
    } else {
      // Create new subscription
      await connection.query(
        'INSERT INTO email_subscriptions (user_id, subscription_type) VALUES (?, ?)',
        [userId, subscriptionTypes]
      );
    }

    res.json({ message: 'Subscription preferences updated' });
  } catch (error) {
    console.error('Failed to update subscription preferences:', error);
    res.status(500).json({ error: 'Failed to update preferences' });
  } finally {
    connection.release();
  }
});

// Unsubscribe using token
router.get('/unsubscribe/:token', async (req, res) => {
  const { token } = req.params;

  const connection = await pool.getConnection();
  try {
    // Update subscription status
    const [result] = await connection.query(
      'UPDATE email_subscriptions SET is_subscribed = false, updated_at = NOW() WHERE unsubscribe_token = ?',
      [token]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Invalid unsubscribe token' });
    }

    res.json({ message: 'Successfully unsubscribed from notifications' });
  } catch (error) {
    console.error('Failed to unsubscribe:', error);
    res.status(500).json({ error: 'Failed to unsubscribe' });
  } finally {
    connection.release();
  }
});

// Get subscription status
router.get('/status', authenticateToken, async (req, res) => {
  const userId = req.user.id;

  const connection = await pool.getConnection();
  try {
    const [subscriptions] = await connection.query(
      'SELECT * FROM email_subscriptions WHERE user_id = ?',
      [userId]
    );

    if (subscriptions.length === 0) {
      return res.json({
        is_subscribed: true,
        subscription_type: ['inactivity_reminder', 'new_email_notification', 'temp_email_expiry']
      });
    }

    res.json(subscriptions[0]);
  } catch (error) {
    console.error('Failed to get subscription status:', error);
    res.status(500).json({ error: 'Failed to get subscription status' });
  } finally {
    connection.release();
  }
});

export default router;