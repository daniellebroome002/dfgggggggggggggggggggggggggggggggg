// guestSessionHandler.js
import jwt from 'jsonwebtoken';
import { v4 as uuidv4 } from 'uuid';
import bcrypt from 'bcryptjs';
import { pool } from './db/init.js';
import { recentRequests } from './middleware/requestTracker.js'; // Import recentRequests for migration tracking

// In-memory storage for guest sessions
// Using Map for better performance
export const guestSessions = new Map();

// Email address to guest session lookup for webhooks
// This map allows quickly finding which guest session an email belongs to
// Format: email address → { token, emailId }
export const emailToGuestMap = new Map();

// In-memory cache for registered users (similar to guest sessions)
// This dramatically reduces database load for frequent dashboard views
// Format: userId → { emails: Map<emailId, emailData>, inbox: Map<emailId, emails[]>, lastFetched: Date }
const registeredUserCache = new Map();

// Email address to registered user lookup (similar to emailToGuestMap)
// Format: email address → { userId, emailId }
const emailToRegisteredMap = new Map();

// Cache expiration time (10 minutes)
const CACHE_EXPIRY = 10 * 60 * 1000;

// Set of reserved/popular local parts that should always be checked in DB
// These are commonly used email prefixes that are likely to be taken
const RESERVED_LOCALPARTS = new Set([
  'admin', 'info', 'support', 'contact', 'help', 'sales', 
  'test', 'demo', 'noreply', 'no-reply', 'account', 'billing',
  'hello', 'team', 'service', 'mail', 'user', 'webmaster',
  'postmaster', 'hostmaster', 'security', 'abuse'
]);

// Clean up expired sessions every hour
setInterval(() => {
  const now = new Date();
  
  // Clean guest sessions
  for (const [token, session] of guestSessions.entries()) {
    // Check if the token is expired by decoding it
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET || 'default_secret');
      if (decoded.exp * 1000 < now.getTime()) {
        // Remove all email mappings for this session before deleting
        for (const [emailId, emailData] of session.emails.entries()) {
          emailToGuestMap.delete(emailData.email);
        }
        guestSessions.delete(token);
      }
    } catch (err) {
      // If verification fails, token is invalid or expired
      // Remove all email mappings for this session before deleting
      if (session) {
        for (const [emailId, emailData] of session.emails.entries()) {
          emailToGuestMap.delete(emailData.email);
        }
      }
      guestSessions.delete(token);
    }
  }
  
  // Clean registered user cache entries that are older than CACHE_EXPIRY
  for (const [userId, userData] of registeredUserCache.entries()) {
    if (now.getTime() - userData.lastFetched.getTime() > CACHE_EXPIRY) {
      // Remove all email mappings for this user before deleting
      for (const [emailId, emailData] of userData.emails.entries()) {
        emailToRegisteredMap.delete(emailData.email);
      }
      registeredUserCache.delete(userId);
    }
  }
}, 60 * 60 * 1000); // Run every hour

/**
 * Cache temp emails for a registered user to reduce database load
 * @param {string} userId - User ID
 * @param {Array} emails - Array of temp emails from database
 */
export async function cacheUserEmails(userId, emails) {
  // Clear any existing cache entries for this user
  const existingCache = registeredUserCache.get(userId);
  if (existingCache) {
    // Remove old email mappings
    for (const [emailId, emailData] of existingCache.emails.entries()) {
      emailToRegisteredMap.delete(emailData.email);
    }
  }
  
  // Create a new cache entry
  const userCache = {
    emails: new Map(),
    inbox: new Map(),
    lastFetched: new Date()
  };
  
  // Add each email to the cache
  for (const email of emails) {
    userCache.emails.set(email.id, email);
    userCache.inbox.set(email.id, []);
    
    // Add to email lookup map
    emailToRegisteredMap.set(email.email, { userId, emailId: email.id });
  }
  
  // Store the cache
  registeredUserCache.set(userId, userCache);
  
  // Optional: Pre-fetch and cache received emails for each temp email
  // This is a performance optimization that loads inbox data in the background
  for (const email of emails) {
    try {
      const [receivedEmails] = await pool.query(
        'SELECT * FROM received_emails WHERE temp_email_id = ? ORDER BY received_at DESC LIMIT 50',
        [email.id]
      );
      
      if (receivedEmails.length > 0) {
        userCache.inbox.set(email.id, receivedEmails);
      }
    } catch (error) {
      console.error(`Failed to cache received emails for ${email.id}:`, error);
    }
  }
  
  return userCache;
}

/**
 * Get cached temp emails for a registered user
 * @param {string} userId - User ID
 * @returns {Array|null} - Array of cached temp emails or null if not cached
 */
export function getCachedUserEmails(userId) {
  const userCache = registeredUserCache.get(userId);
  if (!userCache || new Date().getTime() - userCache.lastFetched.getTime() > CACHE_EXPIRY) {
    return null; // Cache miss or expired
  }
  
  return Array.from(userCache.emails.values());
}

/**
 * Get cached inbox for a registered user's temp email
 * @param {string} userId - User ID
 * @param {string} emailId - Temp email ID
 * @returns {Array|null} - Array of received emails or null if not cached
 */
export function getCachedUserInbox(userId, emailId) {
  const userCache = registeredUserCache.get(userId);
  if (!userCache || new Date().getTime() - userCache.lastFetched.getTime() > CACHE_EXPIRY) {
    return null; // Cache miss or expired
  }
  
  return userCache.inbox.get(emailId) || [];
}

/**
 * Cache a received email for a registered user
 * @param {string} userId - User ID
 * @param {string} tempEmailId - Temp email ID 
 * @param {object} emailData - Received email data
 * @returns {boolean} - Success status
 */
export function cacheReceivedEmail(userId, tempEmailId, emailData) {
  const userCache = registeredUserCache.get(userId);
  if (!userCache) {
    return false; // Cache miss
  }
  
  // Get the inbox for this email
  const inbox = userCache.inbox.get(tempEmailId) || [];
  
  // Add the new email to the beginning of the inbox (newest first)
  inbox.unshift(emailData);
  
  // Update the cache
  userCache.inbox.set(tempEmailId, inbox);
  userCache.lastFetched = new Date(); // Refresh the cache timestamp
  
  return true;
}

/**
 * Find registered user by email address (for webhook handling)
 * @param {string} emailAddress - The email address to look up
 * @returns {object|null} - User info {userId, emailId} or null if not found
 */
export function findRegisteredUserByEmail(emailAddress) {
  return emailToRegisteredMap.get(emailAddress) || null;
}

/**
 * Cache an added temp email for a registered user
 * @param {string} userId - User ID
 * @param {object} emailData - Temp email data
 * @returns {boolean} - Success status
 */
export function cacheAddedEmail(userId, emailData) {
  const userCache = registeredUserCache.get(userId);
  if (!userCache) {
    return false; // Cache miss
  }
  
  // Add the new email to the cache
  userCache.emails.set(emailData.id, emailData);
  
  // Initialize empty inbox
  userCache.inbox.set(emailData.id, []);
  
  // Add to email lookup map
  emailToRegisteredMap.set(emailData.email, { userId, emailId: emailData.id });
  
  // Refresh cache timestamp
  userCache.lastFetched = new Date();
  
  return true;
}

/**
 * Remove a cached email for a registered user (on delete)
 * @param {string} userId - User ID
 * @param {string} emailId - Temp email ID
 * @returns {boolean} - Success status
 */
export function removeCachedEmail(userId, emailId) {
  const userCache = registeredUserCache.get(userId);
  if (!userCache) {
    return false; // Cache miss
  }
  
  // Get the email data to remove from lookup map
  const emailData = userCache.emails.get(emailId);
  if (emailData) {
    emailToRegisteredMap.delete(emailData.email);
  }
  
  // Remove the email and its inbox
  userCache.emails.delete(emailId);
  userCache.inbox.delete(emailId);
  
  // Refresh cache timestamp
  userCache.lastFetched = new Date();
  
  return true;
}

// Clear cache for a user
export function clearUserCache(userId) {
  const userCache = registeredUserCache.get(userId);
  if (userCache) {
    // Remove all email mappings
    for (const [emailId, emailData] of userCache.emails.entries()) {
      emailToRegisteredMap.delete(emailData.email);
    }
    registeredUserCache.delete(userId);
  }
}

/**
 * Generates a guest JWT token with 24h expiration
 * @returns {string} JWT token for guest session
 */
export function generateGuestJWT() {
  const id = uuidv4();
  const token = jwt.sign(
    { 
      id, 
      isGuest: true,
      isAdmin: false 
    },
    process.env.JWT_SECRET || 'default_secret',
    { expiresIn: '24h' }
  );

  // Create a new guest session
  guestSessions.set(token, {
    id,
    emails: new Map(),
    inbox: new Map(),
    created_at: new Date()
  });

  return token;
}

/**
 * Checks if an email address belongs to a guest user
 * @param {string} emailAddress - The email address to check
 * @returns {object|null} - Guest info {token, emailId} or null if not found
 */
export function findGuestByEmail(emailAddress) {
  return emailToGuestMap.get(emailAddress) || null;
}

/**
 * Stores temporary email data in memory for a guest session
 * @param {string} token - Guest JWT token
 * @param {object} emailData - Email data to store
 * @returns {string|null} - ID of stored email or null if failed
 */
export function storeTempEmail(token, emailData) {
  try {
    const session = guestSessions.get(token);
    if (!session) return null;

    const emailId = emailData.id || uuidv4();
    emailData.id = emailId;
    
    // Add created_at timestamp if not provided
    if (!emailData.created_at) {
      emailData.created_at = new Date().toISOString();
    }

    // Store email data in session
    session.emails.set(emailId, emailData);
    
    // Initialize empty inbox for this email
    if (!session.inbox.has(emailId)) {
      session.inbox.set(emailId, []);
    }
    
    // Add to the email lookup map for webhook to find guest emails quickly
    emailToGuestMap.set(emailData.email, { token, emailId });

    return emailId;
  } catch (error) {
    console.error('Error storing temp email:', error);
    return null;
  }
}

/**
 * Retrieves all temporary emails for a guest session
 * @param {string} token - Guest JWT token
 * @returns {Array} - Array of temp emails or empty array if none found
 */
export function getTempEmails(token) {
  try {
    const session = guestSessions.get(token);
    if (!session) return [];
    
    return Array.from(session.emails.values());
  } catch (error) {
    console.error('Error retrieving temp emails:', error);
    return [];
  }
}

/**
 * Gets a single temporary email by ID
 * @param {string} token - Guest JWT token
 * @param {string} emailId - ID of the temporary email
 * @returns {object|null} - Temp email data or null if not found
 */
export function getTempEmailById(token, emailId) {
  try {
    const session = guestSessions.get(token);
    if (!session) return null;

    return session.emails.get(emailId) || null;
  } catch (error) {
    console.error('Error retrieving temp email by ID:', error);
    return null;
  }
}

/**
 * Stores a received email in the guest's inbox
 * @param {string} token - Guest JWT token
 * @param {string} tempEmailId - ID of the temporary email
 * @param {object} emailData - Received email data
 * @returns {boolean} - Success status
 */
export function storeReceivedEmail(token, tempEmailId, emailData) {
  try {
    const session = guestSessions.get(token);
    if (!session) return false;

    if (!session.emails.has(tempEmailId)) return false;

    // Ensure inbox exists for this email
    if (!session.inbox.has(tempEmailId)) {
      session.inbox.set(tempEmailId, []);
    }

    // Add ID if not provided
    const emailId = emailData.id || uuidv4();
    emailData.id = emailId;
    emailData.temp_email_id = tempEmailId;

    // Add timestamp if not provided
    if (!emailData.received_at) {
      emailData.received_at = new Date().toISOString();
    }

    // Add email to inbox
    const inbox = session.inbox.get(tempEmailId);
    inbox.push(emailData);

    return true;
  } catch (error) {
    console.error('Error storing received email:', error);
    return false;
  }
}

/**
 * Gets inbox content for a temporary email
 * @param {string} token - Guest JWT token
 * @param {string} tempEmailId - ID of the temporary email
 * @returns {Array} - Array of received emails
 */
export function getInbox(token, tempEmailId) {
  try {
    const session = guestSessions.get(token);
    if (!session) return [];

    return session.inbox.get(tempEmailId) || [];
  } catch (error) {
    console.error('Error retrieving inbox:', error);
    return [];
  }
}

/**
 * Migrates guest session data to a registered user
 * @param {string} token - Guest JWT token
 * @param {string} userId - ID of the registered user
 * @param {string} realEmail - User's real email
 * @param {string} realPassword - User's real password
 * @returns {Promise<object>} - Success status and migration results
 */
export async function migrateGuestSessionToUser(token, userId, realEmail, realPassword) {
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    const session = guestSessions.get(token);
    if (!session) return { success: false, error: 'Session not found' };

    // First check if the user already exists
    const [existingUsers] = await connection.query(
      'SELECT id FROM users WHERE email = ?',
      [realEmail]
    );

    if (existingUsers && existingUsers.length > 0) {
      // User already exists, we can't migrate
      await connection.rollback();
      return { success: false, error: 'User already exists' };
    }

    // Hash the password
    const hashedPassword = await bcrypt.hash(realPassword, 10);

    // Insert the new user
    await connection.query(
      'INSERT INTO users (id, email, password) VALUES (?, ?, ?)',
      [userId, realEmail, hashedPassword]
    );
    
    // Track migration results
    const migrationResults = {
      totalEmails: session.emails.size,
      migratedEmails: 0,
      renamedEmails: 0,
      skippedEmails: 0,
      details: []
    };

    // Migrate all temporary emails
    for (const [emailId, emailData] of session.emails) {
      // Quick check if this email exists in the database
      const [existingEmails] = await connection.query(
        'SELECT id FROM temp_emails WHERE email = ?',
        [emailData.email]
      );
      
      let emailToInsert = emailData.email;
      let status = 'migrated';
      
      if (existingEmails && existingEmails.length > 0) {
        // Try to create a renamed version if there's a conflict
        const localPart = emailData.email.split('@')[0];
        const domain = emailData.email.split('@')[1];
        const newEmail = `${localPart}_${userId.substring(0, 6)}@${domain}`;
        
        // Check if the renamed email also exists
        const [checkRenamed] = await connection.query(
          'SELECT id FROM temp_emails WHERE email = ?',
          [newEmail]
        );
        
        if (checkRenamed && checkRenamed.length > 0) {
          // Skip this email if even the renamed version exists
          migrationResults.skippedEmails++;
          migrationResults.details.push({
            id: emailId,
            email: emailData.email,
            status: 'skipped',
            reason: 'Both original and renamed versions exist'
          });
          continue;
        }
        
        // Use the renamed email
        emailToInsert = newEmail;
        status = 'renamed';
        migrationResults.renamedEmails++;
      } else {
        migrationResults.migratedEmails++;
      }

      // Format dates for MySQL (convert ISO string to MySQL datetime format)
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
      
      const formattedExpiresAt = formatDate(emailData.expires_at);
      const formattedCreatedAt = formatDate(emailData.created_at);

      // Insert the temp email (guest migration only uses regular domains)
      await connection.query(
        'INSERT INTO temp_emails (id, user_id, email, domain_id, custom_domain_id, expires_at, created_at) VALUES (?, ?, ?, ?, NULL, ?, ?)',
        [emailId, userId, emailToInsert, emailData.domain_id, formattedExpiresAt, formattedCreatedAt]
      );
      
      // Record the migration result
      migrationResults.details.push({
        id: emailId,
        originalEmail: emailData.email,
        migratedEmail: emailToInsert,
        status
      });

      // Get the inbox for this email
      const inbox = session.inbox.get(emailId) || [];
      
      // Migrate all received emails for this temp email
      for (const receivedEmail of inbox) {
        const formattedReceivedAt = formatDate(receivedEmail.received_at);
        
        // Insert the received email
        await connection.query(
          'INSERT INTO received_emails (id, temp_email_id, from_email, from_name, subject, body_html, body_text, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
          [
            receivedEmail.id,
            emailId,
            receivedEmail.from_email,
            receivedEmail.from_name,
            receivedEmail.subject,
            receivedEmail.body_html,
            receivedEmail.body_text,
            formattedReceivedAt
          ]
        );
      }
    }

    // Record the migration in user_migrations table
    const migrationId = uuidv4();
    
    // Try to get client IP and user agent from token or req
    let clientIp = '';
    let userAgent = '';
    let country = '';
    
    // Check if there are any recent requests in the memory cache that match this user
    // This uses the existing request tracking infrastructure
    try {
      // Look for request info in recent requests that might match this session
      // This is an approximation, as we don't have direct access to request data here
      const requests = Array.from(recentRequests?.byId?.values() || []);
      if (requests.length > 0) {
        // Find the most recent request that could be associated with this session
        const recentRequest = requests
          .filter(req => req.requestPath?.includes('/guest/save-inbox'))
          .sort((a, b) => b.timestamp - a.timestamp)[0];
          
        if (recentRequest) {
          clientIp = recentRequest.clientIp || '';
          userAgent = recentRequest.userAgent || '';
          country = recentRequest.geoCountry || '';
        }
      }
    } catch (error) {
      console.error('Error getting request info for migration logging:', error);
    }
    
    // Record the migration in the database
    await connection.query(
      `INSERT INTO user_migrations (
        id, user_id, user_email, migration_date, 
        emails_migrated, emails_renamed, emails_skipped,
        client_ip, user_agent, country, success
      ) VALUES (?, ?, ?, NOW(), ?, ?, ?, ?, ?, ?, ?)`,
      [
        migrationId,
        userId,
        realEmail,
        migrationResults.migratedEmails,
        migrationResults.renamedEmails,
        migrationResults.skippedEmails,
        clientIp,
        userAgent,
        country,
        true
      ]
    );

    // Commit the transaction
    await connection.commit();
    
    // Clean up the guest session
    for (const [emailId, emailData] of session.emails.entries()) {
      emailToGuestMap.delete(emailData.email);
    }
    guestSessions.delete(token);
    
    return { success: true, results: migrationResults };
  } catch (error) {
    await connection.rollback();
    console.error('Error migrating guest session:', error);
    
    // Try to record failed migration
    try {
      const migrationId = uuidv4();
      await pool.query(
        `INSERT INTO user_migrations (
          id, user_id, user_email, migration_date, 
          emails_migrated, emails_renamed, emails_skipped,
          client_ip, user_agent, country, success, error_message
        ) VALUES (?, ?, ?, NOW(), ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          migrationId,
          userId,
          realEmail,
          0, 0, 0,
          '', '', '',
          false,
          error.message.substring(0, 1000) // Limit error message length
        ]
      );
    } catch (logError) {
      console.error('Failed to log failed migration:', logError);
    }
    
    return { success: false, error: error.message };
  } finally {
    connection.release();
  }
}

/**
 * Checks if a token is a valid guest token
 * @param {string} token - JWT token to verify
 * @returns {boolean} - Whether the token is a valid guest token
 */
export function isValidGuestToken(token) {
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET || 'default_secret');
    return !!decoded.isGuest && guestSessions.has(token);
  } catch (error) {
    return false;
  }
}

/**
 * Deletes a guest session
 * @param {string} token - Guest JWT token
 * @returns {boolean} - Success status
 */
export function deleteGuestSession(token) {
  const session = guestSessions.get(token);
  if (session) {
    // Remove all email mappings from the lookup map
    for (const [emailId, emailData] of session.emails.entries()) {
      emailToGuestMap.delete(emailData.email);
    }
  }
  return guestSessions.delete(token);
}

/**
 * Determines if an email should be checked in the database
 * Uses heuristics to minimize DB load while still catching likely conflicts
 * @param {string} email - The email address to analyze
 * @returns {boolean} - Whether this email should be checked in DB
 */
function shouldCheckEmailInDB(email) {
  if (!email || typeof email !== 'string') return false;
  
  // Extract local part (before @)
  const localPart = email.split('@')[0].toLowerCase();
  
  // Check if local part is in reserved list
  if (RESERVED_LOCALPARTS.has(localPart)) {
    return true;
  }
  
  // Check for simple email patterns that are likely to be taken
  if (localPart.length <= 5) {
    return true; // Short email addresses are more likely to be taken
  }
  
  // Check for common patterns (firstname.lastname, etc.)
  if (localPart.includes('.') || localPart.includes('_')) {
    return true;
  }
  
  // Check if domain is popular (these have higher chance of conflicts)
  const domain = email.split('@')[1]?.toLowerCase();
  if (domain) {
    const popularDomains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com'];
    if (popularDomains.some(d => domain === d || domain.endsWith('.' + d))) {
      return true;
    }
  }
  
  // For all other cases, don't check DB to reduce load
  return false;
}

/**
 * Checks if an email address might be in use by a registered user
 * Uses smart heuristics to reduce database load
 * @param {string} emailAddress - The email address to check 
 * @returns {Promise<boolean>} - True if email is likely in use, false otherwise
 */
export async function isEmailAddressInUse(emailAddress) {
  try {
    // Always check if the email exists in the guest system first (in-memory, fast)
    const guestEmail = emailToGuestMap.get(emailAddress);
    if (guestEmail) {
      return true;
    }
    
    // Apply heuristics to determine if we should check the database
    if (shouldCheckEmailInDB(emailAddress)) {
      // Only hit the database for emails that are likely to be taken
      const [emails] = await pool.query(
        'SELECT id FROM temp_emails WHERE email = ? LIMIT 1',
        [emailAddress]
      );
      
      return emails.length > 0;
    }
    
    // For emails unlikely to cause conflicts, skip DB check
    return false;
  } catch (error) {
    console.error('Error checking email address usage:', error);
    // In case of an error, allow the email to avoid blocking users unnecessarily
    return false;
  }
} 
