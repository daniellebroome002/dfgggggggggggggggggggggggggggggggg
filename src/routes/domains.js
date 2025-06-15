import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { authenticateToken, requireAdmin } from '../middleware/auth.js';
import { pool } from '../db/init.js';
import dns from 'dns';
import axios from 'axios';
import { validateDomain, sanitizeText, createValidationMiddleware } from '../utils/inputValidation.js';
import { checkCustomDomainLimits } from '../middleware/customDomainRateLimit.js';
import { syncAllDomainsToMailserver, checkMailserverHealth } from '../services/domainSyncService.js';

const router = express.Router();

// Mailserver push functions
const pushDomainToMailserver = async (domain, action) => {
  try {
    const mailserverUrl = process.env.MAILSERVER_URL;
    const mailserverToken = process.env.MAILSERVER_TOKEN;
    
    if (!mailserverUrl || !mailserverToken) {
      console.warn('Mailserver URL or token not configured, skipping push');
      return { success: false, error: 'Mailserver not configured' };
    }

    const endpoint = action === 'add' ? '/domain/add' : '/domain/delete';
    const response = await axios.post(`${mailserverUrl}${endpoint}`, 
      { domain },
      {
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${mailserverToken}`
        },
        timeout: 10000 // 10 second timeout
      }
    );

    console.log(`✅ Domain ${domain} ${action} pushed to mailserver successfully`);
    return { success: true, result: response.data };
  } catch (error) {
    if (error.response) {
      console.error(`❌ Failed to push domain ${domain} ${action} to mailserver:`, error.response.data);
      return { success: false, error: error.response.data };
    } else {
      console.error(`❌ Error pushing domain ${domain} ${action} to mailserver:`, error.message);
      return { success: false, error: error.message };
    }
  }
};

// DNS verification function with timeout
const verifyDomainDNS = (domain) => {
  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      resolve(false);
    }, 10000); // 10 second timeout
    
    dns.resolveMx(domain, (err, addresses) => {
      clearTimeout(timeout);
      
      if (err) {
        console.error(`DNS verification failed for ${domain}:`, err.message);
        resolve(false);
      } else {
        try {
          // Check if MX record points to our mail server
          const isVerified = addresses.some(mx => 
            mx.exchange.toLowerCase().includes('custom.boomlify.com') || 
            mx.exchange.toLowerCase().includes('boomlify.com')
          );
          resolve(isVerified);
        } catch (error) {
          console.error(`Error processing MX records for ${domain}:`, error);
          resolve(false);
        }
      }
    });
  });
};

// Get public domains (no auth required)
router.get('/public', async (req, res) => {
  try {
    // Remove authentication requirement for this route
    const [domains] = await pool.query(
      'SELECT * FROM domains ORDER BY created_at DESC'
    );
    res.json(domains);
  } catch (error) {
    console.error('Failed to fetch public domains:', error);
    res.status(500).json({ error: 'Failed to fetch domains' });
  }
});

// Protected routes
router.get('/', authenticateToken, async (req, res) => {
  try {
    // Get regular domains
    const [domains] = await pool.query('SELECT * FROM domains ORDER BY created_at DESC');
    
    // Get user's verified custom domains
    const [customDomains] = await pool.query(
      'SELECT id, domain FROM custom_domains WHERE user_id = ? AND status = ? ORDER BY created_at DESC',
      [req.user.id, 'verified']
    );
    
    // Add isCustom flag to custom domains
    const customDomainsWithFlag = customDomains.map(domain => ({
      ...domain,
      isCustom: true
    }));
    
    // Combine regular domains and verified custom domains
    const allDomains = [...domains, ...customDomainsWithFlag];
    
    res.json(allDomains);
  } catch (error) {
    console.error('Failed to fetch domains:', error);
    res.status(500).json({ error: 'Failed to fetch domains' });
  }
});

router.post('/add', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { domain } = req.body;
    const id = uuidv4();

    await pool.query(
      'INSERT INTO domains (id, domain) VALUES (?, ?)',
      [id, domain]
    );

    res.json({ id, domain });
  } catch (error) {
    res.status(400).json({ error: 'Failed to add domain' });
  }
});

// Custom domain routes
// Get user's custom domains
router.get('/custom', authenticateToken, async (req, res) => {
  try {
    const [customDomains] = await pool.query(
      'SELECT * FROM custom_domains WHERE user_id = ? ORDER BY created_at DESC',
      [req.user.id]
    );
    
    // Add usage information (shared across all verified domains for this user)
    const verifiedDomains = customDomains.filter(domain => domain.status === 'verified');
    let sharedUsage = null;
    
    if (verifiedDomains.length > 0) {
      // Get shared usage across all custom domains for this user (FROM CACHE - INSTANT)
      const limits = await checkCustomDomainLimits(req.user.id);
      sharedUsage = {
        daily: {
          current: limits.dailyCount,
          limit: limits.dailyLimit,
          remaining: Math.max(0, limits.dailyLimit - limits.dailyCount)
        },
        total: {
          current: limits.totalCount,
          limit: limits.totalLimit,
          remaining: Math.max(0, limits.totalLimit - limits.totalCount)
        },
        resetTime: limits.resetTime,
        canCreate: limits.canCreate,
        cached: limits.cached, // Show if data is from cache
        lastUpdated: new Date().toISOString() // Show when data was fetched
      };
    }
    
    // Add shared usage to all verified domains
    const domainsWithUsage = customDomains.map(domain => {
      if (domain.status === 'verified' && sharedUsage) {
        return {
          ...domain,
          usage: sharedUsage
        };
      }
      return domain;
    });
    
    // Add domain count information
    const DOMAIN_LIMIT = 2;
    const response = {
      domains: domainsWithUsage,
      limits: {
        current: customDomains.length,
        limit: DOMAIN_LIMIT,
        remaining: Math.max(0, DOMAIN_LIMIT - customDomains.length),
        canAdd: customDomains.length < DOMAIN_LIMIT
      },
      meta: {
        cached: sharedUsage?.cached || false,
        lastUpdated: new Date().toISOString()
      }
    };
    
    res.json(response);
  } catch (error) {
    console.error('Failed to fetch custom domains:', error);
    res.status(500).json({ error: 'Failed to fetch custom domains' });
  }
});

// Manual flush endpoint for testing and immediate synchronization
router.post('/custom/flush-usage', authenticateToken, async (req, res) => {
  try {
    // We need to access the cache directly, so let's create a simple flush trigger
    // This will be handled by the next scheduled flush (within 30 seconds)
    
    // Get current usage from cache
    const limits = await checkCustomDomainLimits(req.user.id);
    
    res.json({
      message: 'Usage data will be synchronized with database within 30 seconds',
      usage: {
        daily: {
          current: limits.dailyCount,
          limit: limits.dailyLimit,
          remaining: Math.max(0, limits.dailyLimit - limits.dailyCount)
        },
        total: {
          current: limits.totalCount,
          limit: limits.totalLimit,
          remaining: Math.max(0, limits.totalLimit - limits.totalCount)
        },
        resetTime: limits.resetTime,
        canCreate: limits.canCreate,
        cached: limits.cached,
        requestedAt: new Date().toISOString()
      }
    });
  } catch (error) {
    console.error('Failed to get usage data:', error);
    res.status(500).json({ error: 'Failed to get usage data' });
  }
});

// Add custom domain
router.post('/custom/add', authenticateToken, createValidationMiddleware({
  domain: { required: true, type: 'domain', maxLength: 253 }
}), async (req, res) => {
  try {
    const { domain } = req.body;
    
    // Use centralized validation
    const { isValid, sanitized } = validateDomain(domain);
    if (!isValid) {
      return res.status(400).json({ 
        error: 'Invalid domain format. Subdomains are supported (e.g., mail.example.com)' 
      });
    }

    const cleanDomain = sanitized;
    const id = uuidv4();

    // Check custom domain count limit (2 per user)
    const [userDomains] = await pool.query(
      'SELECT COUNT(*) as domain_count FROM custom_domains WHERE user_id = ?',
      [req.user.id]
    );
    
    const DOMAIN_LIMIT = 2;
    if (userDomains[0].domain_count >= DOMAIN_LIMIT) {
      return res.status(429).json({ 
        error: 'CUSTOM_DOMAIN_COUNT_LIMIT_EXCEEDED',
        message: `You can only add ${DOMAIN_LIMIT} custom domains per account`,
        limits: {
          current: userDomains[0].domain_count,
          limit: DOMAIN_LIMIT,
          remaining: 0
        }
      });
    }

    // Check if domain already exists
    const [existing] = await pool.query(
      'SELECT * FROM custom_domains WHERE domain = ?',
      [cleanDomain]
    );

    if (existing.length > 0) {
      return res.status(400).json({ error: 'Domain already exists' });
    }

    // Add domain with pending status
    await pool.query(
      'INSERT INTO custom_domains (id, user_id, domain, status, created_at) VALUES (?, ?, ?, ?, NOW())',
      [id, req.user.id, cleanDomain, 'pending']
    );

    res.json({ 
      id, 
      domain: cleanDomain, 
      status: 'pending',
      message: 'Domain added successfully. Please configure your DNS settings.' 
    });
  } catch (error) {
    console.error('Failed to add custom domain:', error);
    res.status(500).json({ error: 'Failed to add custom domain' });
  }
});

// Verify custom domain DNS
router.post('/custom/:id/verify', authenticateToken, async (req, res) => {
  try {
    const domainId = req.params.id;
    
    // Get domain info
    const [domains] = await pool.query(
      'SELECT * FROM custom_domains WHERE id = ? AND user_id = ?',
      [domainId, req.user.id]
    );

    if (domains.length === 0) {
      return res.status(404).json({ error: 'Domain not found' });
    }

    const domain = domains[0];

    // Check if MX records point to our mail server
    const isVerified = await verifyDomainDNS(domain.domain);

    if (isVerified) {
      await pool.query(
        'UPDATE custom_domains SET status = ?, verified_at = NOW(), last_check_at = NOW() WHERE id = ?',
        ['verified', domainId]
      );

      // Push verified domain to mailserver
      const pushResult = await pushDomainToMailserver(domain.domain, 'add');
      if (!pushResult.success) {
        console.warn(`Domain verified but failed to push to mailserver: ${pushResult.error}`);
      }

      res.json({ 
        success: true, 
        message: 'Domain verified successfully!' 
      });
    } else {
      await pool.query(
        'UPDATE custom_domains SET status = ?, last_check_at = NOW() WHERE id = ?',
        ['failed', domainId]
      );

      res.json({ 
        success: false, 
        message: 'Domain verification failed. Please check your DNS settings.' 
      });
    }
  } catch (error) {
    console.error('Failed to verify domain:', error);
    res.status(500).json({ error: 'Failed to verify domain' });
  }
});

// Delete custom domain
router.delete('/custom/:id', authenticateToken, async (req, res) => {
  try {
    const domainId = req.params.id;
    
    // Get domain info before deletion
    const [domains] = await pool.query(
      'SELECT * FROM custom_domains WHERE id = ? AND user_id = ?',
      [domainId, req.user.id]
    );

    if (domains.length === 0) {
      return res.status(404).json({ error: 'Domain not found' });
    }

    const domain = domains[0];
    
    // Delete from database
    const [result] = await pool.query(
      'DELETE FROM custom_domains WHERE id = ? AND user_id = ?',
      [domainId, req.user.id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Domain not found' });
    }

    // Push domain deletion to mailserver (only if it was verified)
    if (domain.status === 'verified') {
      const pushResult = await pushDomainToMailserver(domain.domain, 'delete');
      if (!pushResult.success) {
        console.warn(`Domain deleted from DB but failed to remove from mailserver: ${pushResult.error}`);
      }
    }

    res.json({ success: true, message: 'Domain deleted successfully' });
  } catch (error) {
    console.error('Failed to delete custom domain:', error);
    res.status(500).json({ error: 'Failed to delete custom domain' });
  }
});

// Manual domain sync endpoint (admin only)
router.post('/sync', async (req, res) => {
  // Check admin access
  const adminAccess = req.headers['admin-access'];
  if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
    return res.status(403).json({ error: 'Admin access required' });
  }
  try {
    console.log('🔄 Manual domain sync triggered by admin');
    
    // Check mailserver health first
    const healthCheck = await checkMailserverHealth();
    
    if (!healthCheck.healthy) {
      return res.status(503).json({
        error: 'Mailserver is not healthy',
        details: healthCheck.error,
        url: healthCheck.url
      });
    }
    
    // Perform domain sync
    const syncResult = await syncAllDomainsToMailserver();
    
    if (syncResult.success) {
      res.json({
        success: true,
        message: 'Domain sync completed successfully',
        synced: syncResult.synced,
        failed: syncResult.failed,
        failedDomains: syncResult.failedDomains,
        mailserverUrl: healthCheck.url
      });
    } else {
      res.status(500).json({
        error: 'Domain sync failed',
        details: syncResult.error,
        synced: syncResult.synced,
        failed: syncResult.failed
      });
    }
    
  } catch (error) {
    console.error('Manual domain sync error:', error);
    res.status(500).json({
      error: 'Failed to perform domain sync',
      details: error.message
    });
  }
});

// Mailserver health check endpoint (admin only)
router.get('/mailserver/health', async (req, res) => {
  // Check admin access
  const adminAccess = req.headers['admin-access'];
  if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
    return res.status(403).json({ error: 'Admin access required' });
  }
  try {
    const healthCheck = await checkMailserverHealth();
    
    if (healthCheck.healthy) {
      res.json({
        healthy: true,
        status: healthCheck.status,
        url: healthCheck.url,
        message: 'Mailserver is healthy and accessible'
      });
    } else {
      res.status(503).json({
        healthy: false,
        error: healthCheck.error,
        url: healthCheck.url,
        message: 'Mailserver is not accessible'
      });
    }
    
  } catch (error) {
    console.error('Mailserver health check error:', error);
    res.status(500).json({
      healthy: false,
      error: error.message,
      message: 'Failed to check mailserver health'
    });
  }
});

export default router;
