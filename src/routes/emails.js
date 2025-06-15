import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { authenticateToken, authenticateAnyToken } from '../middleware/auth.js';
import { pool } from '../db/init.js';
import compression from 'compression';
import { rateLimitMiddleware, verifyCaptcha, checkCaptchaRequired, rateLimitStore } from '../middleware/rateLimit.js';
import { customDomainRateLimitMiddleware, incrementCustomDomainUsage, decrementCustomDomainUsage } from '../middleware/customDomainRateLimit.js';
import nodemailer from 'nodemailer';
import { validateEmail, sanitizeText, validateInteger, validateUUID, createValidationMiddleware } from '../utils/inputValidation.js';
import { 
  getTempEmails, 
  getTempEmailById, 
  getInbox, 
  storeTempEmail, 
  isEmailAddressInUse,
  // New cache functions for registered users
  getCachedUserEmails,
  getCachedUserInbox,
  cacheUserEmails,
  cacheReceivedEmail,
  cacheAddedEmail,
  removeCachedEmail,
  clearUserCache,
  findRegisteredUserByEmail
} from '../guestSessionHandler.js';

const router = express.Router();

// Get a specific temporary email
router.get('/:id', authenticateAnyToken, async (req, res) => {
  try {
    const emailId = req.params.id;
    
    // Check if this is a guest user
    if (req.user.isGuest) {
      const email = getTempEmailById(req.guestToken, emailId);
      
      if (!email) {
        return res.status(404).json({ error: 'Email not found' });
      }
      
      res.json(email);
      return;
    }
    
    // For regular authenticated users
    const [emails] = await pool.query(
      'SELECT * FROM temp_emails WHERE id = ? AND user_id = ?',
      [emailId, req.user.id]
    );
    
    if (emails.length === 0) {
      return res.status(404).json({ error: 'Email not found' });
    }
    
    res.json(emails[0]);
  } catch (error) {
    console.error('Failed to fetch email:', error);
    res.status(400).json({ error: 'Failed to fetch email' });
  }
});

// Admin route for batch processing bulk emails
router.post('/admin/batch-bulk-send', async (req, res) => {
  try {
    // Check admin passphrase
    const adminAccess = req.headers['admin-access'];
    if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    const { 
      emails, 
      email, 
      smtp, 
      batchSize = 50, 
      batchDelay = 1000, // Delay between batches in ms
      throttleDelay = 300 // Delay between individual emails in ms
    } = req.body;

    if (!emails?.length || !email?.subject || !email?.body || !smtp) {
      return res.status(400).json({ error: 'Missing required parameters' });
    }

    console.log('Starting batch email processing:', {
      totalEmails: emails.length,
      batchSize,
      batchDelay,
      throttleDelay
    });

    // Create transporter with provided SMTP settings
    const transporter = nodemailer.createTransport({
      host: smtp.host,
      port: parseInt(smtp.port) || 587,
      secure: false,
      auth: {
        user: smtp.username,
        pass: smtp.password
      },
      tls: {
        rejectUnauthorized: false
      }
    });

    // Verify SMTP connection
    await transporter.verify();
    console.log('SMTP connection verified successfully');

    const results = [];
    let succeeded = 0;
    let failed = 0;
    let currentBatch = 0;
    const totalBatches = Math.ceil(emails.length / batchSize);

    // Process emails in batches
    for (let i = 0; i < emails.length; i += batchSize) {
      currentBatch++;
      const batch = emails.slice(i, i + batchSize);
      console.log(`Processing batch ${currentBatch}/${totalBatches} (${batch.length} emails)`);

      // Process emails within the batch
      for (const recipientEmail of batch) {
        try {
          console.log(`Sending email to ${recipientEmail}`);
          const result = await transporter.sendMail({
            from: `"${smtp.from_name}" <${smtp.from_email}>`,
            to: recipientEmail,
            subject: email.subject,
            html: email.body
          });
          console.log(`Email sent successfully to ${recipientEmail}`);
          results.push({ status: 'fulfilled', value: result, email: recipientEmail });
          succeeded++;

          // Apply throttling delay between individual emails
          if (batch.indexOf(recipientEmail) < batch.length - 1) {
            await new Promise(resolve => setTimeout(resolve, throttleDelay));
          }
        } catch (error) {
          console.error(`Failed to send email to ${recipientEmail}:`, error);
          results.push({ status: 'rejected', reason: error, email: recipientEmail });
          failed++;
        }
      }

      // Apply delay between batches (skip for the last batch)
      if (currentBatch < totalBatches) {
        console.log(`Waiting ${batchDelay}ms before next batch...`);
        await new Promise(resolve => setTimeout(resolve, batchDelay));
      }
    }

    console.log(`Batch email processing complete: ${succeeded} succeeded, ${failed} failed`);

    // Generate detailed report
    const report = {
      totalEmails: emails.length,
      totalBatches,
      batchSize,
      batchDelay,
      throttleDelay,
      succeeded,
      failed,
      startTime: new Date().toISOString(),
      endTime: new Date().toISOString(),
      details: results.map((result) => ({
        email: result.email,
        status: result.status,
        error: result.status === 'rejected' ? result.reason?.message || 'Unknown error' : null
      }))
    };

    res.json({
      message: `Batch processing complete: ${succeeded} succeeded, ${failed} failed`,
      ...report
    });
  } catch (error) {
    console.error('Failed to process batch emails:', error);
    res.status(500).json({ 
      error: 'Failed to process batch emails',
      details: error.message || 'Unknown error'
    });
  }
});

// Get received emails for a specific temporary email with pagination
router.get('/:id/received', authenticateAnyToken, async (req, res) => {
  try {
    const emailId = req.params.id;
    
    // Check if this is a guest user
    if (req.user.isGuest) {
      const emails = getInbox(req.guestToken, emailId);
      
      // Sort by received_at in descending order
      const sortedEmails = [...emails].sort((a, b) => 
        new Date(b.received_at).getTime() - new Date(a.received_at).getTime()
      );
      
      res.json({
        data: sortedEmails,
        metadata: {
          total: sortedEmails.length,
          page: 1,
          limit: sortedEmails.length,
          pages: 1
        }
      });
      return;
    }
    
    // Check if we should skip the cache
    const skipCache = req.query.skipCache === 'true';
    
    // For registered users, try to use the cache first (unless skipCache is true)
    const userId = req.user.id;
    let cachedEmails = skipCache ? null : getCachedUserInbox(userId, emailId);
    
    if (cachedEmails && cachedEmails.length > 0) {
      // Get pagination parameters with defaults
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const offset = (page - 1) * limit;
      
      // Paginate the results (emails are already sorted newest first)
      const paginatedEmails = cachedEmails.slice(offset, offset + limit);
      
      // Return the data with pagination metadata
      res.json({
        data: paginatedEmails,
        metadata: {
          total: cachedEmails.length,
          page: page,
          limit: limit,
          pages: Math.ceil(cachedEmails.length / limit),
          cached: true
        }
      });
      return;
    }
    
    // If not in cache or empty, fetch from database
    // For regular authenticated users, continue with the existing logic
    // Get pagination parameters with defaults
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const offset = (page - 1) * limit;

    // First get the total count
    const [countResult] = await pool.query(`
      SELECT COUNT(*) as total
      FROM received_emails re
      JOIN temp_emails te ON re.temp_email_id = te.id
      WHERE te.id = ? AND te.user_id = ?
    `, [req.params.id, req.user.id]);

    const totalCount = countResult[0].total;

    // Then get the paginated data
    const [emails] = await pool.query(`
      SELECT re.*, te.email as temp_email
      FROM received_emails re
      JOIN temp_emails te ON re.temp_email_id = te.id
      WHERE te.id = ? AND te.user_id = ?
      ORDER BY re.received_at DESC
      LIMIT ? OFFSET ?
    `, [req.params.id, req.user.id, limit, offset]);

    // Return the data with pagination metadata
    res.json({
      data: emails,
      metadata: {
        total: totalCount,
        page: page,
        limit: limit,
        pages: Math.ceil(totalCount / limit)
      }
    });
  } catch (error) {
    console.error('Failed to fetch received emails:', error);
    res.status(400).json({ error: 'Failed to fetch received emails' });
  }
});

// Create a new temporary email
router.post('/create', authenticateAnyToken, rateLimitMiddleware, checkCaptchaRequired, verifyCaptcha, customDomainRateLimitMiddleware, async (req, res) => {
  try {
    const { email, domainId, expiresAt, captchaResponse } = req.body;
    
    // Validate the required fields
    if (!email || !domainId) {
      return res.status(400).json({ error: 'Email and domain are required' });
    }
    
    // Set default expiration if not provided (60 days from now)
    const defaultExpiry = new Date();
    defaultExpiry.setDate(defaultExpiry.getDate() + 60);
    const validExpiresAt = expiresAt || defaultExpiry.toISOString();
    
    // Format date for MySQL (convert ISO string to MySQL datetime format)
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
    
    // Convert the ISO date to MySQL format
    const mysqlExpiresAt = formatDate(validExpiresAt);
    
    // Smart check if email is in use (minimizes DB load)
    const isInUse = await isEmailAddressInUse(email);
    if (isInUse) {
      // Generate a suggestion for an alternative email
      const localPart = email.split('@')[0];
      const domain = email.split('@')[1];
      const suggestion = `${localPart}_${Math.floor(Math.random() * 1000)}@${domain}`;
      
      return res.status(409).json({ 
        error: 'Email address already in use',
        suggestion 
      });
    }
    
    // Get client IP for rate limiting and tracking
    const clientIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    
    // Check if this is a guest user
    if (req.user.isGuest) {
      // Create email ID
      const id = uuidv4();
      
      // Prepare email data
      const emailData = {
        id,
        email,
        domain_id: domainId,
        expires_at: validExpiresAt, // Keep ISO format for in-memory storage
        created_at: new Date().toISOString()
      };
      
      // Store in guest session
      const emailId = storeTempEmail(req.guestToken, emailData);
      
      if (!emailId) {
        return res.status(500).json({ error: 'Failed to create temporary email' });
      }
      
      res.json(emailData);
      return;
    }
    
    // For regular authenticated users, continue with database storage
    const connection = await pool.getConnection();
    
    try {
      await connection.beginTransaction();
      
      // Create email ID
      const id = uuidv4();
      
      // Verify the domain exists (check both regular domains and custom domains)
      const [regularDomains] = await connection.query(
        'SELECT * FROM domains WHERE id = ?',
        [domainId]
      );
      
      const [customDomains] = await connection.query(
        'SELECT * FROM custom_domains WHERE id = ? AND user_id = ? AND status = ?',
        [domainId, req.user.id, 'verified']
      );
      
      if (regularDomains.length === 0 && customDomains.length === 0) {
        return res.status(400).json({ error: 'Invalid domain or domain not verified' });
      }
      
      // Determine if this is a regular domain or custom domain
      const isCustomDomain = customDomains.length > 0;
      
      // Insert temp email with proper domain handling
      if (isCustomDomain) {
        // For custom domains: set domain_id to NULL, use custom_domain_id
      await connection.query(
          'INSERT INTO temp_emails (id, user_id, email, domain_id, custom_domain_id, expires_at) VALUES (?, ?, ?, NULL, ?, ?)',
        [id, req.user.id, email, domainId, mysqlExpiresAt]
      );
      } else {
        // For regular domains: use domain_id, set custom_domain_id to NULL
        await connection.query(
          'INSERT INTO temp_emails (id, user_id, email, domain_id, custom_domain_id, expires_at) VALUES (?, ?, ?, ?, NULL, ?)',
          [id, req.user.id, email, domainId, mysqlExpiresAt]
        );
      }
      
      // Store IP history
      await connection.query(
        `INSERT INTO email_ip_history 
        (email, client_ip, email_type, first_seen, last_seen, request_count) 
        VALUES (?, ?, 'temp', NOW(), NOW(), 1)`,
        [email, clientIp]
      );
      
      await connection.commit();
      
      const [createdEmail] = await pool.query(
        'SELECT * FROM temp_emails WHERE id = ?',
        [id]
      );
      
      // Add to the cache for fast access
      const newEmail = createdEmail[0];
      cacheAddedEmail(req.user.id, newEmail);
      
      // Increment custom domain usage if applicable
      if (req.customDomainInfo) {
        incrementCustomDomainUsage(req.customDomainInfo.id);
      }
      
      res.json(newEmail);
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error('Create email error:', error);
    res.status(400).json({ error: 'Failed to create temporary email' });
  }
});

router.delete('/delete/:id', authenticateToken, async (req, res) => {
  const connection = await pool.getConnection();
  
  try {
    await connection.beginTransaction();

    // Check if this is a custom domain email before deletion (for usage decrement)
    const [tempEmailCheck] = await connection.query(
      'SELECT custom_domain_id FROM temp_emails WHERE id = ? AND user_id = ?',
      [req.params.id, req.user.id]
    );

    if (tempEmailCheck.length === 0) {
      await connection.rollback();
      return res.status(404).json({ error: 'Email not found' });
    }

    const customDomainId = tempEmailCheck[0].custom_domain_id;
    const isCustomDomainEmail = customDomainId !== null;

    // First, delete all received emails
    const [deleteReceivedResult] = await connection.query(
      'DELETE FROM received_emails WHERE temp_email_id = ?',
      [req.params.id]
    );

    // Then, delete the temporary email
    const [deleteTempResult] = await connection.query(
      'DELETE FROM temp_emails WHERE id = ? AND user_id = ?',
      [req.params.id, req.user.id]
    );

    if (deleteTempResult.affectedRows === 0) {
      await connection.rollback();
      return res.status(404).json({ error: 'Email not found' });
    }

    await connection.commit();
    
    // Remove from cache
    removeCachedEmail(req.user.id, req.params.id);
    
    // Decrement custom domain usage if this was a custom domain email
    if (isCustomDomainEmail && customDomainId) {
      await decrementCustomDomainUsage(customDomainId, req.user.id);
    }
    
    res.json({ message: 'Email deleted successfully' });
  } catch (error) {
    await connection.rollback();
    console.error('Delete email error:', error);
    res.status(400).json({ error: 'Failed to delete email' });
  } finally {
    connection.release();
  }
});

// Get user emails with pagination
router.get('/', authenticateAnyToken, async (req, res) => {
  try {
    // Check if this is a guest user
    if (req.user.isGuest) {
      // Get emails from guest session
      const emails = getTempEmails(req.guestToken);
      
      // Filter by search term if provided
      const search = req.query.search || '';
      let filteredEmails = emails;
      
      if (search) {
        filteredEmails = emails.filter(email => 
          email.email.toLowerCase().includes(search.toLowerCase())
        );
      }
      
      // Get pagination parameters with defaults
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const offset = (page - 1) * limit;
      
      // Paginate the results
      const paginatedEmails = filteredEmails.slice(offset, offset + limit);
      
      // Return the data with pagination metadata
      res.json({
        data: paginatedEmails,
        metadata: {
          total: filteredEmails.length,
          page: page,
          limit: limit,
          pages: Math.ceil(filteredEmails.length / limit)
        }
      });
      return;
    }
    
    // For registered users, check if we should skip the cache
    const skipCache = req.query.skipCache === 'true';
    
    // For registered users, try to use the cache first (unless skipCache is true)
    const userId = req.user.id;
    let cachedEmails = skipCache ? null : getCachedUserEmails(userId);
    let fromCache = true;
    
    // If not in cache, fetch from database
    if (!cachedEmails) {
      // Get pagination parameters with defaults
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const offset = (page - 1) * limit;
      const search = req.query.search || '';

      // First get the total count with search
      let countQuery = 'SELECT COUNT(*) as total FROM temp_emails WHERE user_id = ?';
      let countParams = [req.user.id];
      
      // Add search condition if search term is provided
      if (search) {
        countQuery += ' AND email LIKE ?';
        countParams.push(`%${search}%`);
      }
      
      const [countResult] = await pool.query(countQuery, countParams);
      const totalCount = countResult[0].total;

      // Get ALL emails for caching (not just the paginated ones)
      // This improves performance for subsequent requests
      const [allEmails] = await pool.query(
        'SELECT * FROM temp_emails WHERE user_id = ? ORDER BY created_at DESC',
        [req.user.id]
      );
      
      // Cache the emails for future requests
      await cacheUserEmails(userId, allEmails);
      
      // Now get emails with search and pagination
      let dataQuery = 'SELECT * FROM temp_emails WHERE user_id = ?';
      let dataParams = [req.user.id];
      
      // Add search condition if search term is provided
      if (search) {
        dataQuery += ' AND email LIKE ?';
        dataParams.push(`%${search}%`);
      }
      
      dataQuery += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
      dataParams.push(limit, offset);
      
      const [paginatedEmails] = await pool.query(dataQuery, dataParams);
      
      // Return the data with pagination metadata
      res.json({
        data: paginatedEmails,
        metadata: {
          total: totalCount,
          page: page,
          limit: limit,
          pages: Math.ceil(totalCount / limit),
          cached: false
        }
      });
      return;
    }
    
    // If we have cached data, use it for pagination and filtering
    const search = req.query.search || '';
    let filteredEmails = cachedEmails;
    
    if (search) {
      filteredEmails = cachedEmails.filter(email => 
        email.email.toLowerCase().includes(search.toLowerCase())
      );
    }
    
    // Get pagination parameters with defaults
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const offset = (page - 1) * limit;
    
    // Sort by created_at desc
    filteredEmails.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
    
    // Paginate the results
    const paginatedEmails = filteredEmails.slice(offset, offset + limit);
    
    // Return the data with pagination metadata
    res.json({
      data: paginatedEmails,
      metadata: {
        total: filteredEmails.length,
        page: page,
        limit: limit,
        pages: Math.ceil(filteredEmails.length / limit),
        cached: true
      }
    });
  } catch (error) {
    console.error('Failed to fetch emails:', error);
    res.status(400).json({ error: 'Failed to fetch emails' });
  }
});

// Delete a received email
router.delete('/:tempEmailId/received/:emailId', authenticateToken, async (req, res) => {
  try {
    // First check if the temp email belongs to the user
    const [tempEmails] = await pool.query(
      'SELECT id FROM temp_emails WHERE id = ? AND user_id = ?',
      [req.params.tempEmailId, req.user.id]
    );

    if (tempEmails.length === 0) {
      return res.status(404).json({ error: 'Temporary email not found' });
    }

    // Delete the received email
    const [result] = await pool.query(
      'DELETE FROM received_emails WHERE id = ? AND temp_email_id = ?',
      [req.params.emailId, req.params.tempEmailId]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Received email not found' });
    }

    res.json({ message: 'Email deleted successfully' });
  } catch (error) {
    console.error('Failed to delete received email:', error);
    res.status(400).json({ error: 'Failed to delete received email' });
  }
});

// Bulk delete received emails
router.post('/:tempEmailId/received/bulk/delete', authenticateToken, async (req, res) => {
  const { emailIds } = req.body;
  
  if (!emailIds || !Array.isArray(emailIds) || emailIds.length === 0) {
    return res.status(400).json({ error: 'Invalid email IDs' });
  }

  try {
    // First check if the temp email belongs to the user
    const [tempEmails] = await pool.query(
      'SELECT id FROM temp_emails WHERE id = ? AND user_id = ?',
      [req.params.tempEmailId, req.user.id]
    );

    if (tempEmails.length === 0) {
      return res.status(404).json({ error: 'Temporary email not found' });
    }

    // Delete the received emails
    const [result] = await pool.query(
      'DELETE FROM received_emails WHERE id IN (?) AND temp_email_id = ?',
      [emailIds, req.params.tempEmailId]
    );

    res.json({ 
      message: 'Emails deleted successfully',
      count: result.affectedRows
    });
  } catch (error) {
    console.error('Failed to delete received emails:', error);
    res.status(400).json({ error: 'Failed to delete received emails' });
  }
});

// Get public emails (no auth required)
router.get('/public/:email', async (req, res) => {
  try {
    res.setHeader('Cache-Control', 'public, max-age=5'); // Cache for 5 seconds
    const [emails] = await pool.query(`
      SELECT re.*, te.email as temp_email
      FROM received_emails re
      JOIN temp_emails te ON re.temp_email_id = te.id
      WHERE te.email = ?
      ORDER BY re.received_at DESC
    `, [req.params.email]);

    res.json(emails);
  } catch (error) {
    console.error('Failed to fetch public emails:', error);
    res.status(400).json({ error: 'Failed to fetch emails' });
  }
});

// Create public temporary email (no auth required) with rate limiting and CAPTCHA
router.post('/public/create', rateLimitMiddleware, checkCaptchaRequired, verifyCaptcha, async (req, res) => {
  try {
    const { email, domainId } = req.body;
    const id = uuidv4();
    
    // Add CAPTCHA information to response if required
    if (res.locals.captchaRequired && !req.body.captchaResponse) {
      return res.status(400).json({
        error: 'CAPTCHA_REQUIRED',
        captchaRequired: true,
        captchaSiteKey: res.locals.captchaSiteKey,
        message: 'You have exceeded the rate limit. Please complete the CAPTCHA.'
      });
    }
    
    // Set expiry date to 48 hours from now
    const expiresAt = new Date();
    expiresAt.setHours(expiresAt.getHours() + 48);
    
    // If CAPTCHA was provided and successfully verified, reset rate limit counter
    const clientIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    if (req.body.captchaResponse) {
      if (rateLimitStore.limits[clientIp]) {
        rateLimitStore.limits[clientIp].count = 0; // Reset counter
        rateLimitStore.limits[clientIp].captchaRequired = false; // No longer require CAPTCHA
      }
    }

    // Start transaction
    const connection = await pool.getConnection();
    await connection.beginTransaction();

    try {
      // Insert temp email (public endpoint only uses regular domains)
      await connection.query(
        'INSERT INTO temp_emails (id, email, domain_id, custom_domain_id, expires_at) VALUES (?, ?, ?, NULL, ?)',
        [id, email, domainId, expiresAt]
      );

      // Store IP history
      await connection.query(
        `INSERT INTO email_ip_history 
        (email, client_ip, email_type, first_seen, last_seen, request_count) 
        VALUES (?, ?, 'temp', NOW(), NOW(), 1)`,
        [email, clientIp]
      );

      await connection.commit();

      const [createdEmail] = await pool.query(
        'SELECT * FROM temp_emails WHERE id = ?',
        [id]
      );

      res.json(createdEmail[0]);
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error('Create public email error:', error);
    res.status(400).json({ error: 'Failed to create temporary email' });
  }
});

// Admin route to fetch all emails (admin-only)
router.get('/admin/all', async (req, res) => {
  try {
    // Check admin passphrase
    const adminAccess = req.headers['admin-access'];
    if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    // Get pagination parameters
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 100;
    const offset = (page - 1) * limit;

    // Get total count
    const [countResult] = await pool.query(`
      SELECT COUNT(*) as total
      FROM received_emails
    `);

    const totalCount = countResult[0].total;

    // Fetch paginated emails
    const [emails] = await pool.query(`
      SELECT re.*, te.email as temp_email
      FROM received_emails re
      JOIN temp_emails te ON re.temp_email_id = te.id
      ORDER BY re.received_at DESC
      LIMIT ? OFFSET ?
    `, [limit, offset]);

    res.json({
      data: emails,
      metadata: {
        total: totalCount,
        page: page,
        limit: limit,
        pages: Math.ceil(totalCount / limit)
      }
    });
  } catch (error) {
    console.error('Failed to fetch admin emails:', error);
    res.status(500).json({ error: 'Failed to fetch emails' });
  }
});

// Get all users with advanced filtering (admin-only)
router.get('/admin/users', async (req, res) => {
  try {
    const adminAccess = req.headers['admin-access'];
    if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    const {
      search,
      sortBy = 'created_at',
      sortOrder = 'desc',
      page = 1,
      limit = 50,
      emailCountMin,
      emailCountMax,
      dateStart,
      dateEnd,
      isActive,
      hasCustomDomain
    } = req.query;

    // Validate sortBy to prevent SQL injection - remove last_activity_at
    const allowedSortFields = ['created_at', 'last_login', 'email_count', 'received_email_count', 'custom_domain_count'];
    const validSortBy = allowedSortFields.includes(sortBy) ? sortBy : 'created_at';
    
    // Validate sortOrder
    const validSortOrder = sortOrder === 'asc' ? 'ASC' : 'DESC';

    let query = `
      SELECT 
        u.id,
        u.email,
        u.created_at,
        u.last_login,
        COUNT(DISTINCT te.id) as email_count,
        COUNT(DISTINCT cd.id) as custom_domain_count,
        (
          SELECT COUNT(*)
          FROM received_emails re
          JOIN temp_emails te2 ON re.temp_email_id = te2.id
          WHERE te2.user_id = u.id
        ) as received_email_count
      FROM users u
      LEFT JOIN temp_emails te ON u.id = te.user_id
      LEFT JOIN user_domains cd ON u.id = cd.user_id
      WHERE 1=1
    `;

    const queryParams = [];

    // Apply filters
    if (search) {
      query += ` AND u.email LIKE ?`;
      queryParams.push(`%${search}%`);
    }

    if (emailCountMin !== undefined && emailCountMin !== '') {
      query += ` AND (SELECT COUNT(*) FROM temp_emails WHERE user_id = u.id) >= ?`;
      queryParams.push(parseInt(emailCountMin) || 0);
    }

    if (emailCountMax !== undefined && emailCountMax !== '') {
      query += ` AND (SELECT COUNT(*) FROM temp_emails WHERE user_id = u.id) <= ?`;
      queryParams.push(parseInt(emailCountMax) || 0);
    }

    if (dateStart) {
      query += ` AND u.created_at >= ?`;
      queryParams.push(dateStart);
    }

    if (dateEnd) {
      query += ` AND u.created_at <= ?`;
      queryParams.push(dateEnd);
    }

    // Fix activity checks to use last_login instead of last_activity_at
    if (isActive === 'true') {
      query += ` AND u.last_login >= DATE_SUB(NOW(), INTERVAL 7 DAY)`;
    } else if (isActive === 'false') {
      query += ` AND (u.last_login IS NULL OR u.last_login < DATE_SUB(NOW(), INTERVAL 7 DAY))`;
    }

    if (hasCustomDomain === 'true') {
      query += ` AND EXISTS (SELECT 1 FROM user_domains WHERE user_id = u.id)`;
    } else if (hasCustomDomain === 'false') {
      query += ` AND NOT EXISTS (SELECT 1 FROM user_domains WHERE user_id = u.id)`;
    }

    // Group by user
    query += ` GROUP BY u.id`;

    // Get total count for pagination - separate query for better reliability
    const countQuery = `
      SELECT COUNT(DISTINCT u.id) as total
      FROM users u
      LEFT JOIN temp_emails te ON u.id = te.user_id
      LEFT JOIN user_domains cd ON u.id = cd.user_id
      WHERE 1=1
    `;
    
    // Copy filters to count query
    let countQueryComplete = countQuery;
    const countParams = [...queryParams]; // Clone the params array
    
    if (search) countQueryComplete += ` AND u.email LIKE ?`;
    if (emailCountMin !== undefined && emailCountMin !== '') countQueryComplete += ` AND (SELECT COUNT(*) FROM temp_emails WHERE user_id = u.id) >= ?`;
    if (emailCountMax !== undefined && emailCountMax !== '') countQueryComplete += ` AND (SELECT COUNT(*) FROM temp_emails WHERE user_id = u.id) <= ?`;
    if (dateStart) countQueryComplete += ` AND u.created_at >= ?`;
    if (dateEnd) countQueryComplete += ` AND u.created_at <= ?`;
    
    // Fix activity checks in count query to use last_login
    if (isActive === 'true') countQueryComplete += ` AND u.last_login >= DATE_SUB(NOW(), INTERVAL 7 DAY)`;
    else if (isActive === 'false') countQueryComplete += ` AND (u.last_login IS NULL OR u.last_login < DATE_SUB(NOW(), INTERVAL 7 DAY))`;
    
    if (hasCustomDomain === 'true') countQueryComplete += ` AND EXISTS (SELECT 1 FROM user_domains WHERE user_id = u.id)`;
    else if (hasCustomDomain === 'false') countQueryComplete += ` AND NOT EXISTS (SELECT 1 FROM user_domains WHERE user_id = u.id)`;

    // Apply safe sorting with validated values
    query += ` ORDER BY ${validSortBy} ${validSortOrder}`;

    // Add pagination
    const pageNum = parseInt(page) || 1;
    const limitNum = parseInt(limit) || 50;
    const offset = (pageNum - 1) * limitNum;
    query += ` LIMIT ? OFFSET ?`;
    queryParams.push(limitNum, offset);

    // Execute count query first
    const [countResult] = await pool.query(countQueryComplete, countParams);
    const total = countResult[0]?.total || 0;

    // Execute main query
    const [users] = await pool.query(query, queryParams);

    res.json({
      data: users || [],
      metadata: {
        total,
        page: pageNum,
        limit: limitNum,
        pages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('Failed to fetch users:', error);
    res.status(500).json({ error: 'Failed to fetch users', details: error.message });
  }
});

// Admin route to send bulk emails (updated with detailed implementation)
router.post('/admin/bulk-send', async (req, res) => {
  try {
    // Check admin passphrase
    const adminAccess = req.headers['admin-access'];
    if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    const { userIds, email, smtp, throttleDelay = 300 } = req.body;

    if (!userIds?.length || !email?.subject || !email?.body || !smtp) {
      return res.status(400).json({ error: 'Missing required parameters' });
    }

    console.log('Creating transporter with SMTP settings:', {
      host: smtp.host,
      port: smtp.port,
      auth: {
        user: smtp.username
      }
    });
    
    console.log(`Using throttle delay of ${throttleDelay}ms between emails`);

    // Create transporter with provided SMTP settings
    const transporter = nodemailer.createTransport({
      host: smtp.host,
      port: parseInt(smtp.port) || 587,
      secure: false, // true for 465, false for other ports
      auth: {
        user: smtp.username,
        pass: smtp.password
      },
      tls: {
        rejectUnauthorized: false // Only use this in development!
      }
    });

    // Verify SMTP connection
    await transporter.verify();
    console.log('SMTP connection verified successfully');

    // Fix for the IN clause with array parameter
    let query = 'SELECT email FROM users WHERE id IN (?)';
    let params = [userIds];
    
    // For MySQL, if we have multiple IDs, we need to use a different approach
    if (userIds.length > 1) {
      query = `SELECT email FROM users WHERE id IN (${userIds.map(() => '?').join(',')})`;
      params = userIds;
    }

    // Get users' emails
    const [users] = await pool.query(query, params);

    console.log(`Found ${users.length} users to send emails to`);

    // Modified: Send emails one by one with delay instead of Promise.allSettled
    const results = [];
    let succeeded = 0;
    let failed = 0;
    
    for (const user of users) {
      try {
        console.log(`Sending email to ${user.email}`);
        const result = await transporter.sendMail({
          from: `"${smtp.from_name}" <${smtp.from_email}>`,
          to: user.email,
          subject: email.subject,
          html: email.body
        });
        console.log(`Email sent successfully to ${user.email}`);
        results.push({ status: 'fulfilled', value: result, email: user.email });
        succeeded++;
        
        // Apply throttling delay between sends (skip delay for the last email)
        if (users.indexOf(user) < users.length - 1) {
          await new Promise(resolve => setTimeout(resolve, throttleDelay));
        }
      } catch (error) {
        console.error(`Failed to send email to ${user.email}:`, error);
        results.push({ status: 'rejected', reason: error, email: user.email });
        failed++;
      }
    }

    console.log(`Email sending complete: ${succeeded} succeeded, ${failed} failed`);

    res.json({
      message: `Sent ${succeeded} emails successfully, ${failed} failed`,
      succeeded,
      failed,
      details: results.map((result) => ({
        email: result.email,
        status: result.status,
        error: result.status === 'rejected' ? result.reason?.message || 'Unknown error' : null
      }))
    });
  } catch (error) {
    console.error('Failed to send bulk emails:', error);
    res.status(500).json({ 
      error: 'Failed to send emails',
      details: error.message || 'Unknown error'
    });
  }
});

// Admin route to send bulk emails to external recipients
router.post('/admin/bulk-send-external', async (req, res) => {
  try {
    // Check admin passphrase
    const adminAccess = req.headers['admin-access'];
    if (adminAccess !== process.env.ADMIN_PASSPHRASE) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    const { emails, email, smtp, throttleDelay = 300 } = req.body;

    if (!emails?.length || !email?.subject || !email?.body || !smtp) {
      return res.status(400).json({ error: 'Missing required parameters' });
    }

    console.log('Creating transporter with SMTP settings:', {
      host: smtp.host,
      port: smtp.port,
      auth: {
        user: smtp.username
      }
    });
    
    console.log(`Using throttle delay of ${throttleDelay}ms between emails`);

    // Create transporter with provided SMTP settings
    const transporter = nodemailer.createTransport({
      host: smtp.host,
      port: parseInt(smtp.port) || 587,
      secure: false, // true for 465, false for other ports
      auth: {
        user: smtp.username,
        pass: smtp.password
      },
      tls: {
        rejectUnauthorized: false // Only use this in development!
      }
    });

    // Verify SMTP connection
    await transporter.verify();
    console.log('SMTP connection verified successfully');

    console.log(`Sending emails to ${emails.length} external recipients`);

    // Modified: Send emails one by one with delay instead of Promise.allSettled
    const results = [];
    let succeeded = 0;
    let failed = 0;
    
    for (const recipientEmail of emails) {
      try {
        console.log(`Sending email to ${recipientEmail}`);
        const result = await transporter.sendMail({
          from: `"${smtp.from_name}" <${smtp.from_email}>`,
          to: recipientEmail,
          subject: email.subject,
          html: email.body
        });
        console.log(`Email sent successfully to ${recipientEmail}`);
        results.push({ status: 'fulfilled', value: result, email: recipientEmail });
        succeeded++;
        
        // Apply throttling delay between sends (skip delay for the last email)
        if (emails.indexOf(recipientEmail) < emails.length - 1) {
          await new Promise(resolve => setTimeout(resolve, throttleDelay));
        }
      } catch (error) {
        console.error(`Failed to send email to ${recipientEmail}:`, error);
        results.push({ status: 'rejected', reason: error, email: recipientEmail });
        failed++;
      }
    }

    console.log(`Email sending complete: ${succeeded} succeeded, ${failed} failed`);

    res.json({
      message: `Sent ${succeeded} emails successfully, ${failed} failed`,
      succeeded,
      failed,
      details: results.map((result) => ({
        email: result.email,
        status: result.status,
        error: result.status === 'rejected' ? result.reason?.message || 'Unknown error' : null
      }))
    });
  } catch (error) {
    console.error('Failed to send bulk emails to external recipients:', error);
    res.status(500).json({ 
      error: 'Failed to send emails',
      details: error.message || 'Unknown error'
    });
  }
});

// Compress responses
router.use(compression());

export default router;
