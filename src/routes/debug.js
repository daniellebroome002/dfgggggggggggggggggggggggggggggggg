import express from 'express';
import { authenticateMasterPassword } from '../middleware/auth.js';
import { 
  generateGmailDebugReport, 
  dumpGmailServiceStatus,
  checkAccountHealth,
  checkUserAliases,
  findAccountAliases,
  recoverFailedAccount,
  reassignAlias
} from '../utils/gmailServiceDebug.js';

const router = express.Router();

// Apply admin authentication to all routes
router.use(authenticateMasterPassword);
router.use((req, res, next) => {
  if (!req.isAdminAuth) {
    return res.status(403).json({ error: 'Admin access required' });
  }
  next();
});

// Get full Gmail service debug report
router.get('/gmail', (req, res) => {
  try {
    const report = generateGmailDebugReport();
    res.json({ report });
  } catch (error) {
    console.error('Failed to generate Gmail debug report:', error);
    res.status(500).json({ error: 'Failed to generate debug report' });
  }
});

// Dump Gmail service status to console
router.post('/gmail/dump', (req, res) => {
  try {
    dumpGmailServiceStatus();
    res.json({ message: 'Gmail service status dumped to console' });
  } catch (error) {
    console.error('Failed to dump Gmail service status:', error);
    res.status(500).json({ error: 'Failed to dump status' });
  }
});

// Check health of specific account
router.get('/gmail/account/:email', (req, res) => {
  try {
    const { email } = req.params;
    const health = checkAccountHealth(email);
    res.json({ health });
  } catch (error) {
    console.error('Failed to check account health:', error);
    res.status(500).json({ error: 'Failed to check account health' });
  }
});

// Check aliases for specific user
router.get('/gmail/user/:userId/aliases', (req, res) => {
  try {
    const { userId } = req.params;
    const aliases = checkUserAliases(userId);
    res.json({ aliases });
  } catch (error) {
    console.error('Failed to check user aliases:', error);
    res.status(500).json({ error: 'Failed to check user aliases' });
  }
});

// Find all aliases for an account
router.get('/gmail/account/:email/aliases', (req, res) => {
  try {
    const { email } = req.params;
    const aliases = findAccountAliases(email);
    res.json({ aliases });
  } catch (error) {
    console.error('Failed to find account aliases:', error);
    res.status(500).json({ error: 'Failed to find account aliases' });
  }
});

// Recover a failed account
router.post('/gmail/account/:email/recover', (req, res) => {
  try {
    const { email } = req.params;
    const result = recoverFailedAccount(email);
    res.json({ result });
  } catch (error) {
    console.error('Failed to recover account:', error);
    res.status(500).json({ error: 'Failed to recover account' });
  }
});

// Reassign an alias to a different account
router.post('/gmail/alias/reassign', (req, res) => {
  try {
    const { aliasEmail, targetAccountEmail } = req.body;
    
    if (!aliasEmail || !targetAccountEmail) {
      return res.status(400).json({ 
        error: 'Missing required parameters', 
        message: 'Both aliasEmail and targetAccountEmail are required' 
      });
    }
    
    const result = reassignAlias(aliasEmail, targetAccountEmail);
    res.json({ result });
  } catch (error) {
    console.error('Failed to reassign alias:', error);
    res.status(500).json({ error: 'Failed to reassign alias' });
  }
});

export default router;