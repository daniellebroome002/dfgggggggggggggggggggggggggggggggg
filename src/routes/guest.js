import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import jwt from 'jsonwebtoken';
import bcrypt from 'bcryptjs';
import { pool } from '../db/init.js';
import { authenticateGuestToken, authenticateAnyToken } from '../middleware/auth.js';
import { 
  generateGuestJWT, 
  getTempEmails, 
  getTempEmailById,
  storeTempEmail, 
  getInbox, 
  storeReceivedEmail,
  migrateGuestSessionToUser,
  deleteGuestSession,
  isEmailAddressInUse,
  isValidGuestToken
} from '../guestSessionHandler.js';

// Import these directly from the file since they're not exported
// This requires modifying guestSessionHandler.js to export these
import { guestSessions, emailToGuestMap } from '../guestSessionHandler.js';

const router = express.Router();

// Initialize guest session
router.post('/init', async (req, res) => {
  try {
    // Generate a guest JWT token
    const token = generateGuestJWT();
    
    res.json({ 
      token,
      isGuest: true
    });
  } catch (error) {
    console.error('Guest session initialization error:', error);
    res.status(500).json({ 
      error: 'Failed to initialize guest session',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Get guest temp emails
router.get('/emails', authenticateGuestToken, async (req, res) => {
  try {
    const emails = getTempEmails(req.guestToken);
    
    res.json({
      data: emails,
      metadata: {
        total: emails.length,
        page: 1,
        limit: emails.length,
        pages: 1
      }
    });
  } catch (error) {
    console.error('Get guest emails error:', error);
    res.status(500).json({ 
      error: 'Failed to retrieve guest emails',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Get a single temp email by ID
router.get('/emails/:id', authenticateGuestToken, async (req, res) => {
  try {
    const emailId = req.params.id;
    const email = getTempEmailById(req.guestToken, emailId);
    
    if (!email) {
      return res.status(404).json({ error: 'Email not found' });
    }
    
    res.json(email);
  } catch (error) {
    console.error('Get guest email error:', error);
    res.status(500).json({ 
      error: 'Failed to retrieve guest email',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Create a new temporary email for guest
router.post('/emails/create', authenticateGuestToken, async (req, res) => {
  try {
    const { email, domain_id, expires_at } = req.body;
    
    if (!email || !domain_id || !expires_at) {
      return res.status(400).json({ error: 'Missing required fields' });
    }
    
    // Only check for uniqueness with the smart approach that minimizes DB load
    const isInUse = await isEmailAddressInUse(email);
    if (isInUse) {
      return res.status(409).json({ 
        error: 'Email address already in use',
        suggestion: `Try ${email.split('@')[0]}_${Math.floor(Math.random() * 1000)}@${email.split('@')[1]}`
      });
    }
    
    // Format the date for MySQL format (useful if we need it later)
    // But we'll store the original ISO format in memory since it's easier to work with
    const formatDate = (dateString) => {
      try {
        const date = new Date(dateString);
        // MySQL TIMESTAMP format: YYYY-MM-DD HH:MM:SS
        return date.toISOString().slice(0, 19).replace('T', ' ');
      } catch (error) {
        console.error('Error formatting date:', error);
        const now = new Date();
        return now.toISOString().slice(0, 19).replace('T', ' ');
      }
    };
    
    // Keep date validation but don't convert yet since guest emails are stored in memory
    const mysqlExpiresAt = formatDate(expires_at);
    
    const emailData = {
      id: uuidv4(),
      email,
      domain_id,
      expires_at, // Store original ISO format for frontend and in-memory storage
      created_at: new Date().toISOString()
    };
    
    const emailId = storeTempEmail(req.guestToken, emailData);
    
    if (!emailId) {
      return res.status(500).json({ error: 'Failed to store temporary email' });
    }
    
    res.json(emailData);
  } catch (error) {
    console.error('Create guest email error:', error);
    res.status(500).json({ 
      error: 'Failed to create temporary email',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Get inbox for a temp email
router.get('/emails/:id/received', authenticateGuestToken, async (req, res) => {
  try {
    const emailId = req.params.id;
    const emails = getInbox(req.guestToken, emailId);
    
    res.json({
      data: emails,
      metadata: {
        total: emails.length,
        page: 1,
        limit: emails.length,
        pages: 1
      }
    });
  } catch (error) {
    console.error('Get guest inbox error:', error);
    res.status(500).json({ 
      error: 'Failed to retrieve inbox',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Save inbox (register user and migrate data)
router.post('/save-inbox', authenticateGuestToken, async (req, res) => {
  try {
    const { email, password } = req.body;
    
    // Validate input
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required' });
    }
    
    // Check if email is already registered
    const [existingUsers] = await pool.query(
      'SELECT * FROM users WHERE email = ?',
      [email]
    );
    
    if (existingUsers.length > 0) {
      return res.status(409).json({ error: 'Email already registered' });
    }
    
    // Create a new user ID
    const userId = uuidv4();
    
    // Migrate guest session data to the new user
    const migrationResult = await migrateGuestSessionToUser(
      req.guestToken,
      userId,
      email,
      password
    );
    
    if (!migrationResult.success) {
      return res.status(500).json({ 
        error: 'Failed to migrate guest session',
        details: migrationResult.error
      });
    }
    
    // Create a new token for the registered user
    const token = jwt.sign(
      { id: userId, email, isAdmin: false },
      process.env.JWT_SECRET,
      { expiresIn: '6h' }
    );
    
    res.json({ 
      token, 
      user: { id: userId, email, isAdmin: false },
      message: 'Account created successfully',
      migrationDetails: {
        totalEmails: migrationResult.results.totalEmails,
        migratedEmails: migrationResult.results.migratedEmails,
        renamedEmails: migrationResult.results.renamedEmails,
        skippedEmails: migrationResult.results.skippedEmails
      }
    });
  } catch (error) {
    console.error('Save inbox error:', error);
    res.status(500).json({ 
      error: 'Failed to save inbox',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Endpoint to add a received email to a temp email inbox
// This would typically be called by your mail server
router.post('/receive-email/:emailId', async (req, res) => {
  try {
    const { guestToken, from_email, from_name, subject, body_html, body_text } = req.body;
    const emailId = req.params.emailId;
    
    if (!guestToken || !from_email || !emailId) {
      return res.status(400).json({ error: 'Missing required fields' });
    }
    
    // Verify this is a valid guest token
    if (!isValidGuestToken(guestToken)) {
      return res.status(403).json({ error: 'Invalid guest token' });
    }
    
    const emailData = {
      id: uuidv4(),
      from_email,
      from_name: from_name || '',
      subject: subject || '',
      body_html: body_html || '',
      body_text: body_text || '',
      received_at: new Date().toISOString()
    };
    
    const success = storeReceivedEmail(guestToken, emailId, emailData);
    
    if (!success) {
      return res.status(500).json({ error: 'Failed to store received email' });
    }
    
    res.json({ success: true, message: 'Email received successfully' });
  } catch (error) {
    console.error('Receive email error:', error);
    res.status(500).json({ 
      error: 'Failed to process received email',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Delete a temp email for guest user
router.delete('/emails/delete/:id', authenticateGuestToken, async (req, res) => {
  try {
    const emailId = req.params.id;
    const email = getTempEmailById(req.guestToken, emailId);
    
    if (!email) {
      return res.status(404).json({ error: 'Email not found' });
    }
    
    // Get the guest session from the token
    const session = guestSessions.get(req.guestToken);
    if (!session) {
      return res.status(404).json({ error: 'Guest session not found' });
    }
    
    // Get the email data to remove from lookup map
    const emailData = session.emails.get(emailId);
    if (emailData) {
      emailToGuestMap.delete(emailData.email);
    }
    
    // Remove the email and its inbox
    session.emails.delete(emailId);
    session.inbox.delete(emailId);
    
    res.json({ message: 'Email deleted successfully' });
  } catch (error) {
    console.error('Delete guest email error:', error);
    res.status(500).json({ 
      error: 'Failed to delete guest email',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

export default router; 
