// In middleware/ipBlocker.js

import { pool } from '../db/init.js';

export async function checkBlockedIp(req, res, next) {
  const clientIp = req.headers['x-forwarded-for']?.split(',')[0].trim() || 
                   req.socket.remoteAddress;
  
  try {
    const [blockedIp] = await pool.query(
      `SELECT * FROM blocked_ips 
       WHERE ip_address = ? 
       AND (expires_at IS NULL OR expires_at > NOW())
       LIMIT 1`,
      [clientIp]
    );

    if (blockedIp.length > 0) {
      return res.status(403).json({
        error: 'IP_BLOCKED',
        message: 'Your IP address has been blocked',
        reason: blockedIp[0].reason,
        expiresAt: blockedIp[0].expires_at
      });
    }

    next();
  } catch (error) {
    console.error('Error checking blocked IP:', error);
    next();
  }
}
