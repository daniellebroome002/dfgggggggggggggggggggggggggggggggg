import express from 'express';
import { pool } from '../db/init.js';
import {
  lookupRequestById,
  lookupRequestsByIp,
  getIpStats,
  getRecentIps
} from '../middleware/requestTracker.js';
import { manualCleanup } from '../utils/cleanup.js';

const router = express.Router();

// Helper function to check admin passphrase
const checkAdminPassphrase = (req) => {
  return req.headers['admin-access'] === process.env.ADMIN_PASSPHRASE;
};

// Get overall statistics
router.get('/stats', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const connection = await pool.getConnection();
  try {
    // Get total users count
    const [usersCount] = await connection.query(
      'SELECT COUNT(*) as total FROM users'
    );

    // Get users registered today
    const [todayUsers] = await connection.query(
      'SELECT COUNT(*) as total FROM users WHERE DATE(created_at) = CURDATE()'
    );

    // Get total temp emails count
    const [emailsCount] = await connection.query(
      'SELECT COUNT(*) as total FROM temp_emails'
    );

    // Get active temp emails count
    const [activeEmailsCount] = await connection.query(
      'SELECT COUNT(*) as total FROM temp_emails WHERE expires_at > NOW()'
    );

    // Get total received emails count
    const [receivedEmailsCount] = await connection.query(
      'SELECT COUNT(*) as total FROM received_emails'
    );

    // Get today's received emails count
    const [todayReceivedCount] = await connection.query(
      'SELECT COUNT(*) as total FROM received_emails WHERE DATE(received_at) = CURDATE()'
    );

    // Get request logs stats
    const [requestsCount] = await connection.query(
      'SELECT COUNT(*) as total FROM request_logs'
    );

    const [todayRequestsCount] = await connection.query(
      'SELECT COUNT(*) as total FROM request_logs WHERE DATE(created_at) = CURDATE()'
    );

    const [uniqueIpsCount] = await connection.query(
      'SELECT COUNT(DISTINCT client_ip) as total FROM request_logs'
    );

    res.json({
      users: {
        total: usersCount[0].total,
        today: todayUsers[0].total
      },
      tempEmails: {
        total: emailsCount[0].total,
        active: activeEmailsCount[0].total
      },
      receivedEmails: {
        total: receivedEmailsCount[0].total,
        today: todayReceivedCount[0].total
      },
      requests: {
        total: requestsCount[0].total,
        today: todayRequestsCount[0].total,
        uniqueIps: uniqueIpsCount[0].total
      }
    });
  } catch (error) {
    console.error('Failed to fetch stats:', error);
    res.status(500).json({ error: 'Failed to fetch statistics' });
  } finally {
    connection.release();
  }
});

// Manual cleanup endpoint (Updated with explicit fields and message from below code)
router.post('/cleanup', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  try {
    const days = parseInt(req.query.days) || 10;
    const result = await manualCleanup(days);
    res.json({
      message: 'Cleanup completed successfully',
      deletedEmails: result.deletedEmails,
      deletedAttachments: result.deletedAttachments
    });
  } catch (error) {
    console.error('Failed to perform cleanup:', error);
    res.status(500).json({ error: 'Failed to perform cleanup' });
  }
});

// Get recent user registrations
router.get('/recent-users', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  try {
    const [users] = await pool.query(
      `SELECT id, email, created_at, last_login, 
      (SELECT COUNT(*) FROM temp_emails WHERE user_id = users.id) as email_count
      FROM users 
      ORDER BY created_at DESC 
      LIMIT 50`
    );
    res.json(users);
  } catch (error) {
    console.error('Failed to fetch recent users:', error);
    res.status(500).json({ error: 'Failed to fetch recent users' });
  }
});

// Get user activity
router.get('/user-activity', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  try {
    const [activity] = await pool.query(
      `SELECT 
        DATE(created_at) as date,
        COUNT(*) as registrations,
        COUNT(DISTINCT user_id) as active_users
      FROM users
      WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
      GROUP BY DATE(created_at)
      ORDER BY date DESC`
    );
    res.json(activity);
  } catch (error) {
    console.error('Failed to fetch user activity:', error);
    res.status(500).json({ error: 'Failed to fetch user activity' });
  }
});

// Get top users by email count
router.get('/top-users', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  try {
    const [topUsers] = await pool.query(
      `SELECT 
        u.id,
        u.email,
        COUNT(te.id) as email_count,
        COUNT(DISTINCT re.id) as received_count
      FROM users u
      LEFT JOIN temp_emails te ON u.id = te.user_id
      LEFT JOIN received_emails re ON te.id = re.temp_email_id
      GROUP BY u.id, u.email
      ORDER BY email_count DESC
      LIMIT 20`
    );
    res.json(topUsers);
  } catch (error) {
    console.error('Failed to fetch top users:', error);
    res.status(500).json({ error: 'Failed to fetch top users' });
  }
});

// Lookup temporary email to find owner
router.get('/lookup-temp-email', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const { email } = req.query;
  
  if (!email) {
    return res.status(400).json({ error: 'Email parameter is required' });
  }

  const connection = await pool.getConnection();
  try {
    // Find the temporary email and join with the users table to get the owner
    const [result] = await connection.query(`
      SELECT 
        te.email as tempEmail,
        u.email as ownerEmail,
        te.created_at,
        te.expires_at,
        (te.expires_at > NOW()) as isActive
      FROM temp_emails te
      LEFT JOIN users u ON te.user_id = u.id
      WHERE te.email = ?
    `, [email]);

    if (result.length === 0) {
      return res.status(404).json({ error: 'Temporary email not found' });
    }

    // For anonymous/public emails (no user_id), indicate it's a public email
    if (!result[0].ownerEmail) {
      result[0].ownerEmail = 'Public/Anonymous Email (No registered user)';
    }

    res.json(result[0]);
  } catch (error) {
    console.error('Failed to lookup temporary email:', error);
    res.status(500).json({ error: 'Failed to lookup temporary email owner' });
  } finally {
    connection.release();
  }
});

// Lookup IP address by temporary email
router.get('/lookup-temp-email-ip', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const { email } = req.query;
  
  if (!email) {
    return res.status(400).json({ error: 'Email parameter is required' });
  }

  const connection = await pool.getConnection();
  try {
    // First get the temp email details
    const [tempEmailResult] = await connection.query(`
      SELECT 
        te.id,
        te.email as tempEmail,
        te.created_at,
        te.expires_at,
        u.email as ownerEmail
      FROM temp_emails te
      LEFT JOIN users u ON te.user_id = u.id
      WHERE te.email = ?
    `, [email]);

    if (tempEmailResult.length === 0) {
      return res.status(404).json({ error: 'Temporary email not found' });
    }

    // Get the IP address from request logs when the email was created
    const [ipResult] = await connection.query(`
      SELECT 
        rl.client_ip,
        rl.created_at,
        rl.user_agent,
        rl.geo_country,
        rl.geo_city,
        rl.geo_region
      FROM request_logs rl
      WHERE rl.request_path LIKE '%/create%' 
      AND rl.created_at BETWEEN DATE_SUB(?, INTERVAL 5 MINUTE) AND DATE_ADD(?, INTERVAL 5 MINUTE)
      ORDER BY rl.created_at ASC
      LIMIT 1
    `, [tempEmailResult[0].created_at, tempEmailResult[0].created_at]);

    // If no IP found in the 10-minute window, try to find any requests related to this email
    if (ipResult.length === 0) {
      const [recentIpResult] = await connection.query(`
        SELECT 
          rl.client_ip,
          rl.created_at,
          rl.user_agent,
          rl.geo_country,
          rl.geo_city,
          rl.geo_region
        FROM request_logs rl
        WHERE rl.request_path LIKE ?
        AND rl.created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        ORDER BY rl.created_at DESC
        LIMIT 1
      `, [`%${email}%`]);

      if (recentIpResult.length > 0) {
        ipResult.push(recentIpResult[0]);
      }
    }

    res.json({
      tempEmail: tempEmailResult[0],
      ipInfo: ipResult.length > 0 ? ipResult[0] : null
    });
  } catch (error) {
    console.error('Failed to lookup temporary email IP:', error);
    res.status(500).json({ error: 'Failed to lookup temporary email IP' });
  } finally {
    connection.release();
  }
});

// NEW API ENDPOINTS FOR IP AND REQUEST TRACKING

// Get recent IPs
router.get('/recent-ips', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  try {
    const limit = parseInt(req.query.limit) || 50;
    const recentIps = await getRecentIps(limit);
    res.json(recentIps);
  } catch (error) {
    console.error('Failed to fetch recent IPs:', error);
    res.status(500).json({ error: 'Failed to fetch recent IPs' });
  }
});

// Lookup IP and get detailed statistics
router.get('/lookup-ip', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const { ip } = req.query;
  
  if (!ip) {
    return res.status(400).json({ error: 'IP parameter is required' });
  }

  try {
    // Get IP statistics
    const ipStats = await getIpStats(ip);
    
    // Get recent requests from this IP
    const limit = parseInt(req.query.limit) || 50;
    const recentRequests = await lookupRequestsByIp(ip, limit);
    
    // Get associated user information
    const userDetails = [];
    if (ipStats.associatedUsers && ipStats.associatedUsers.length > 0) {
      const placeholders = ipStats.associatedUsers.map(() => '?').join(',');
      const [users] = await pool.query(
        `SELECT id, email, created_at, last_login 
         FROM users
         WHERE id IN (${placeholders})`,
        ipStats.associatedUsers
      );
      
      for (const user of users) {
        const [emailCount] = await pool.query(
          'SELECT COUNT(*) as total FROM temp_emails WHERE user_id = ?',
          [user.id]
        );
        
        userDetails.push({
          ...user,
          emailCount: emailCount[0].total
        });
      }
    }
    
    res.json({
      ip,
      stats: ipStats,
      recentRequests,
      associatedUsers: userDetails
    });
  } catch (error) {
    console.error('Failed to lookup IP:', error);
    res.status(500).json({ error: 'Failed to lookup IP information' });
  }
});

// Lookup request by ID
router.get('/lookup-request', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const { requestId } = req.query;
  
  if (!requestId) {
    return res.status(400).json({ error: 'Request ID parameter is required' });
  }

  try {
    const request = await lookupRequestById(requestId);
    
    if (!request) {
      return res.status(404).json({ error: 'Request not found' });
    }
    
    // If the request has a user ID, get user information
    let userInfo = null;
    if (request.user_id) {
      const [users] = await pool.query(
        'SELECT id, email, created_at, last_login FROM users WHERE id = ?',
        [request.user_id]
      );
      
      if (users.length > 0) {
        const [emailCount] = await pool.query(
          'SELECT COUNT(*) as total FROM temp_emails WHERE user_id = ?',
          [request.user_id]
        );
        
        userInfo = {
          ...users[0],
          emailCount: emailCount[0].total
        };
      }
    }
    
    res.json({
      request,
      userInfo
    });
  } catch (error) {
    console.error('Failed to lookup request:', error);
    res.status(500).json({ error: 'Failed to lookup request information' });
  }
});

// Get request statistics
router.get('/request-stats', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  try {
    // Get hourly request counts for the last 24 hours
    const [hourlyStats] = await pool.query(`
      SELECT 
        DATE_FORMAT(created_at, '%Y-%m-%d %H:00:00') as hour,
        COUNT(*) as count
      FROM request_logs
      WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
      GROUP BY hour
      ORDER BY hour DESC
    `);
    
    // Get top paths
    const [topPaths] = await pool.query(`
      SELECT request_path, COUNT(*) as count
      FROM request_logs
      GROUP BY request_path
      ORDER BY count DESC
      LIMIT 20
    `);
    
    // Get response time stats
    const [responseTimeStats] = await pool.query(`
      SELECT 
        MIN(response_time) as min,
        MAX(response_time) as max,
        AVG(response_time) as avg,
        COUNT(*) as total
      FROM request_logs
    `);
    
    // Get error rate
    const [errorRateStats] = await pool.query(`
      SELECT 
        SUM(CASE WHEN status_code < 400 THEN 1 ELSE 0 END) as success,
        SUM(CASE WHEN status_code >= 400 AND status_code < 500 THEN 1 ELSE 0 END) as client_error,
        SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) as server_error,
        COUNT(*) as total
      FROM request_logs
    `);
    
    // Get geographic distribution
    const [geoStats] = await pool.query(`
      SELECT geo_country, COUNT(*) as count
      FROM request_logs
      WHERE geo_country != ''
      GROUP BY geo_country
      ORDER BY count DESC
      LIMIT 20
    `);
    
    // Get bot vs human ratio
    const [botStats] = await pool.query(`
      SELECT is_bot, COUNT(*) as count
      FROM request_logs
      GROUP BY is_bot
    `);
    
    res.json({
      hourlyStats,
      topPaths,
      responseTimeStats: responseTimeStats[0],
      errorRateStats: errorRateStats[0],
      geoStats,
      botStats: {
        bots: botStats.find(stat => stat.is_bot === 1)?.count || 0,
        humans: botStats.find(stat => stat.is_bot === 0)?.count || 0
      }
    });
  } catch (error) {
    console.error('Failed to fetch request stats:', error);
    res.status(500).json({ error: 'Failed to fetch request statistics' });
  }
});

// Get IP behaviors
router.get('/ip-behaviors', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  try {
    const [behaviors] = await pool.query(
      `SELECT * FROM ip_behaviors 
       WHERE detected_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
       ORDER BY detected_at DESC`
    );
    res.json(behaviors);
  } catch (error) {
    console.error('Failed to fetch IP behaviors:', error);
    res.status(500).json({ error: 'Failed to fetch IP behaviors' });
  }
});

// Get blocked IPs
router.get('/blocked-ips', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  try {
    const [blockedIps] = await pool.query(
      `SELECT * FROM blocked_ips 
       WHERE expires_at IS NULL OR expires_at > NOW()
       ORDER BY blocked_at DESC`
    );
    res.json(blockedIps);
  } catch (error) {
    console.error('Failed to fetch blocked IPs:', error);
    res.status(500).json({ error: 'Failed to fetch blocked IPs' });
  }
});

// Block an IP
router.post('/block-ip', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const { ip, reason, duration } = req.body;
  
  try {
    let expiresAt = null;
    
    // Calculate expiration based on duration
    if (duration !== 'permanent') {
      const durationMap = {
        '1h': '1 HOUR',
        '24h': '24 HOUR',
        '7d': '7 DAY',
        '30d': '30 DAY'
      };
      expiresAt = `DATE_ADD(NOW(), INTERVAL ${durationMap[duration]})`;
    }

    await pool.query(
      `INSERT INTO blocked_ips (ip_address, reason, expires_at) 
       VALUES (?, ?, ${expiresAt || 'NULL'})
       ON DUPLICATE KEY UPDATE 
       reason = VALUES(reason),
       expires_at = ${expiresAt || 'NULL'},
       blocked_at = NOW()`,
      [ip, reason]
    );

    res.json({ message: 'IP blocked successfully' });
  } catch (error) {
    console.error('Failed to block IP:', error);
    res.status(500).json({ error: 'Failed to block IP' });
  }
});

// Unblock an IP
router.post('/unblock-ip', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const { ip } = req.body;
  
  try {
    await pool.query(
      'DELETE FROM blocked_ips WHERE ip_address = ?',
      [ip]
    );

    res.json({ message: 'IP unblocked successfully' });
  } catch (error) {
    console.error('Failed to unblock IP:', error);
    res.status(500).json({ error: 'Failed to unblock IP' });
  }
});

// Lookup IP information by email
router.get('/lookup-email-ip', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const { email } = req.query;
  
  if (!email) {
    return res.status(400).json({ error: 'Email parameter is required' });
  }

  const connection = await pool.getConnection();
  try {
    console.log('Looking up IP for email:', email);

    // First check if email exists in temp_emails
    const [tempEmailResult] = await connection.query(
      'SELECT id, email, created_at, expires_at FROM temp_emails WHERE email = ?',
      [email]
    );

    if (tempEmailResult.length === 0) {
      console.log('Email not found in temp_emails:', email);
      return res.status(404).json({ error: 'Email not found' });
    }

    // Then get IP history
    const [ipHistoryResult] = await connection.query(`
      SELECT 
        eih.email,
        eih.client_ip,
        eih.email_type,
        eih.first_seen,
        eih.last_seen,
        eih.request_count,
        eih.behavior_score,
        eih.is_suspicious
      FROM email_ip_history eih
      WHERE eih.email = ?
      ORDER BY eih.first_seen DESC
      LIMIT 1
    `, [email]);

    if (ipHistoryResult.length === 0) {
      console.log('No IP history found for email:', email);
      return res.status(404).json({ 
        error: 'No IP history found',
        message: 'Email exists but no IP history found'
      });
    }

    // Get user info if available
    const [userResult] = await connection.query(`
      SELECT u.email as owner_email
      FROM users u
      JOIN temp_emails te ON u.id = te.user_id
      WHERE te.email = ?
    `, [email]);

    const response = {
      ...ipHistoryResult[0],
      email_expires_at: tempEmailResult[0].expires_at,
      owner_email: userResult.length > 0 ? userResult[0].owner_email : null
    };

    console.log('Found IP info:', response);
    res.json(response);

  } catch (error) {
    console.error('Failed to lookup email IP:', error);
    console.error('Error stack:', error.stack);
    res.status(500).json({ 
      error: 'Failed to lookup email IP',
      details: error.message
    });
  } finally {
    connection.release();
  }
});

export default router;
