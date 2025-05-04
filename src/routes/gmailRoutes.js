import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { pool } from '../db/init.js';
import { authenticateToken, requireAdmin, authenticateMasterPassword } from '../middleware/auth.js';
import { 
  addGmailAccount, 
  generateGmailAlias, 
  fetchGmailEmails, 
  getUserAliases,
  rotateUserAlias,
  getGmailAccountStats,
  getEmailCacheStats,
  initializeImapService
} from '../services/gmailImapService.js';

const router = express.Router();

// Initialize IMAP service when the server starts
initializeImapService().catch(error => {
  console.error('Failed to initialize IMAP service:', error);
});

// ==================== User Routes ====================

// Create a new Gmail alias
router.post('/create', async (req, res) => {
  try {
    // Allow both authenticated and unauthenticated users
    const userId = req.user?.id || `anon_${uuidv4()}`;
    const { strategy, domain } = req.body; // 'dot' or 'plus', 'gmail.com' or 'googlemail.com'
    
    const result = await generateGmailAlias(
      userId, 
      strategy || 'dot', 
      domain || 'gmail.com'
    );
    
    res.json(result);
  } catch (error) {
    console.error('Failed to create Gmail alias:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to create Gmail alias',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Get all Gmail aliases for the user
router.get('/aliases', async (req, res) => {
  try {
    // Allow both authenticated and unauthenticated users
    const userId = req.user?.id || req.query.userId || `anon_${uuidv4()}`;
    const aliases = await getUserAliases(userId);
    
    res.json({ aliases });
  } catch (error) {
    console.error('Failed to fetch Gmail aliases:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to fetch Gmail aliases',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Fetch emails for a specific alias
router.get('/:alias/emails', async (req, res) => {
  try {
    // Allow both authenticated and unauthenticated users
    const userId = req.user?.id || req.query.userId || `anon_${uuidv4()}`;
    const { alias } = req.params;
    
    const emails = await fetchGmailEmails(userId, alias);
    
    res.json({ emails });
  } catch (error) {
    console.error('Failed to fetch emails:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to fetch emails',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Rotate to a new Gmail alias
router.post('/rotate', async (req, res) => {
  try {
    // Allow both authenticated and unauthenticated users
    const userId = req.user?.id || req.body.userId || `anon_${uuidv4()}`;
    const { strategy, domain } = req.body;
    
    const result = await rotateUserAlias(
      userId, 
      strategy || 'dot', 
      domain || 'gmail.com'
    );
    
    res.json(result);
  } catch (error) {
    console.error('Failed to rotate Gmail alias:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to rotate Gmail alias',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// ==================== Public Routes ====================

// Public routes for non-authenticated users
router.post('/public/create', async (req, res) => {
  try {
    const userId = req.query.userId || `anon_${uuidv4()}`;
    const { strategy, domain, version } = req.body;
    
    // Check if version matches
    if (version !== '1.0.0') {
      return res.status(400).json({ 
        error: 'Version mismatch',
        requiresReset: true
      });
    }
    
    const result = await generateGmailAlias(
      userId, 
      strategy || 'dot', 
      domain || 'gmail.com'
    );
    
    res.json(result);
  } catch (error) {
    console.error('Failed to create Gmail alias:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to create Gmail alias',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

router.get('/public/aliases/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { version } = req.query;
    
    // Check if version matches
    if (version !== '1.0.0') {
      return res.status(400).json({ 
        error: 'Version mismatch',
        requiresReset: true
      });
    }
    
    const aliases = await getUserAliases(userId);
    res.json({ aliases });
  } catch (error) {
    console.error('Failed to fetch Gmail aliases:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to fetch Gmail aliases',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

router.get('/public/emails/:alias', async (req, res) => {
  try {
    const { alias } = req.params;
    const userId = req.query.userId || `anon_${uuidv4()}`;
    
    const emails = await fetchGmailEmails(userId, alias);
    
    res.json({ emails });
  } catch (error) {
    console.error('Failed to fetch emails:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to fetch emails',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

router.post('/public/rotate', async (req, res) => {
  try {
    const { userId, strategy, domain } = req.body;
    
    if (!userId) {
      return res.status(400).json({ error: 'User ID is required' });
    }
    
    const result = await rotateUserAlias(
      userId, 
      strategy || 'dot', 
      domain || 'gmail.com'
    );
    
    res.json(result);
  } catch (error) {
    console.error('Failed to rotate Gmail alias:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to rotate Gmail alias',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Add version check endpoint
router.get('/public/version', (req, res) => {
  res.json({
    version: '1.0.0',
    timestamp: Date.now(),
    requiresReset: false
  });
});

// ==================== Admin Routes ====================

// Add a new Gmail account with IMAP
router.post('/admin/accounts', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { email, appPassword } = req.body;
    
    if (!email || !appPassword) {
      return res.status(400).json({ error: 'Email and app password are required' });
    }
    
    const result = await addGmailAccount(email, appPassword);
    
    res.json(result);
  } catch (error) {
    console.error('Failed to add Gmail account:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to add Gmail account',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Admin route with passphrase for adding Gmail account (alternative auth)
router.post('/admin/accounts-alt', async (req, res) => {
  try {
    // Check admin passphrase
    const adminAccess = req.headers['admin-access'];
    if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
      return res.status(403).json({ error: 'Unauthorized' });
    }
    
    const { email, appPassword } = req.body;
    
    if (!email || !appPassword) {
      return res.status(400).json({ error: 'Email and app password are required' });
    }
    
    const result = await addGmailAccount(email, appPassword);
    
    res.json(result);
  } catch (error) {
    console.error('Failed to add Gmail account:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to add Gmail account',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Update Gmail account (update app password)
router.patch('/admin/accounts/:accountId', async (req, res) => {
  try {
    // Check admin passphrase
    const adminAccess = req.headers['admin-access'];
    if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
      return res.status(403).json({ error: 'Unauthorized' });
    }
    
    const { accountId } = req.params;
    const { appPassword } = req.body;
    
    if (!appPassword) {
      return res.status(400).json({ error: 'App password is required' });
    }
    
    // Check if account exists
    const [accounts] = await pool.query(
      'SELECT id FROM gmail_accounts WHERE id = ?',
      [accountId]
    );
    
    if (accounts.length === 0) {
      return res.status(404).json({ error: 'Gmail account not found' });
    }
    
    // Update the app password
    await pool.query(
      'UPDATE gmail_accounts SET app_password = ?, updated_at = NOW() WHERE id = ?',
      [appPassword, accountId]
    );
    
    res.json({ message: 'Gmail account password updated successfully' });
  } catch (error) {
    console.error('Failed to update Gmail account:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to update Gmail account',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Delete a Gmail account
router.delete('/admin/accounts/:accountId', async (req, res) => {
  try {
    // Check admin passphrase
    const adminAccess = req.headers['admin-access'];
    if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
      return res.status(403).json({ error: 'Unauthorized' });
    }
    
    const { accountId } = req.params;
    
    // Check if account exists
    const [accounts] = await pool.query(
      'SELECT id FROM gmail_accounts WHERE id = ?',
      [accountId]
    );
    
    if (accounts.length === 0) {
      return res.status(404).json({ error: 'Gmail account not found' });
    }
    
    // Delete the account
    await pool.query(
      'DELETE FROM gmail_accounts WHERE id = ?',
      [accountId]
    );
    
    res.json({ message: 'Gmail account deleted successfully' });
  } catch (error) {
    console.error('Failed to delete Gmail account:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to delete Gmail account',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Toggle Gmail account status
router.patch('/admin/accounts/:accountId/status', async (req, res) => {
  try {
    // Check admin passphrase
    const adminAccess = req.headers['admin-access'];
    if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
      return res.status(403).json({ error: 'Unauthorized' });
    }
    
    const { accountId } = req.params;
    const { status } = req.body;
    
    if (!status) {
      return res.status(400).json({ error: 'Status is required' });
    }
    
    // Check if account exists
    const [accounts] = await pool.query(
      'SELECT id, email, status FROM gmail_accounts WHERE id = ?',
      [accountId]
    );
    
    if (accounts.length === 0) {
      return res.status(404).json({ error: 'Gmail account not found' });
    }
    
    const account = accounts[0];
    
    // Update account status
    await pool.query(
      'UPDATE gmail_accounts SET status = ?, updated_at = NOW() WHERE id = ?',
      [status, accountId]
    );
    
    // If we're activating an account that was previously inactive, we may need to restart polling
    if (status === 'active' && account.status !== 'active') {
      // You may need to call a function from your gmailImapService here
      // For example: await gmailImapService.restartPolling(account.email);
      console.log(`Account ${account.email} status changed to active`);
    }
    
    res.json({ message: 'Gmail account status updated successfully', status });
  } catch (error) {
    console.error('Failed to update Gmail account status:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to update Gmail account status',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Get Gmail accounts statistics
router.get('/admin/stats', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const accountStats = await getGmailAccountStats();
    const cacheStats = getEmailCacheStats();
    
    res.json({
      accounts: accountStats,
      cache: cacheStats
    });
  } catch (error) {
    console.error('Failed to get Gmail stats:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to get Gmail stats',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Admin route with passphrase for stats (alternative auth)
router.get('/admin/stats-alt', async (req, res) => {
  try {
    // Check admin passphrase
    const adminAccess = req.headers['admin-access'];
    if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
      return res.status(403).json({ error: 'Unauthorized' });
    }
    
    const accountStats = await getGmailAccountStats();
    const cacheStats = getEmailCacheStats();
    
    res.json({
      accounts: accountStats,
      cache: cacheStats
    });
  } catch (error) {
    console.error('Failed to get Gmail stats:', error);
    res.status(400).json({ 
      error: error.message || 'Failed to get Gmail stats',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

export default router;
