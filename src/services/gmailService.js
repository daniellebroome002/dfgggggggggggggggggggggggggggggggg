// backend/src/services/gmailService.js

import { google } from 'googleapis';
import crypto from 'crypto';
import { v4 as uuidv4 } from 'uuid';
import { pool } from '../db/init.js';

// In-memory storage
const emailCache = new Map(); // Cache for fetched emails
const aliasCache = new Map(); // Cache for active aliases during runtime
const activePollingAccounts = new Set(); // Track which accounts are being actively polled

// Configuration
const MAX_CACHE_SIZE = 10000; // Maximum number of emails to cache
const ALIAS_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days in milliseconds for in-memory cache
const GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly'];
const POLLING_INTERVALS = {
  high: 5000,     // 5 seconds for high priority accounts
  medium: 15000,  // 15 seconds for medium priority
  low: 30000      // 30 seconds for low priority accounts
};

// Encryption utilities for token security
const encryptionKey = process.env.ENCRYPTION_KEY || 'default-encryption-key';

function encrypt(text) {
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-cbc', Buffer.from(encryptionKey), iv);
  let encrypted = cipher.update(text);
  encrypted = Buffer.concat([encrypted, cipher.final()]);
  return iv.toString('hex') + ':' + encrypted.toString('hex');
}

function decrypt(text) {
  const textParts = text.split(':');
  const iv = Buffer.from(textParts[0], 'hex');
  const encryptedText = Buffer.from(textParts[1], 'hex');
  const decipher = crypto.createDecipheriv('aes-256-cbc', Buffer.from(encryptionKey), iv);
  let decrypted = decipher.update(encryptedText);
  decrypted = Buffer.concat([decrypted, decipher.final()]);
  return decrypted.toString();
}

// OAuth client setup
async function getOAuthClient(specificCredentialId = null) {
  let credential;
  
  try {
    if (specificCredentialId) {
      // Use the specified credential
      const [credentials] = await pool.query(
        'SELECT * FROM gmail_credentials WHERE id = ?',
        [specificCredentialId]
      );
      
      if (credentials.length === 0) {
        throw new Error(`Credential with ID ${specificCredentialId} not found`);
      }
      
      credential = credentials[0];
    } else {
      // Get the next available credential using round-robin
      const [credentials] = await pool.query(
        'SELECT * FROM gmail_credentials WHERE active = true ORDER BY usage_count, last_used LIMIT 1'
      );
      
      if (credentials.length === 0) {
        console.error('No active Gmail credentials found in database');
        
        // Fallback to environment variables if no credentials in database
        return new google.auth.OAuth2(
          process.env.GMAIL_CLIENT_ID,
          process.env.GMAIL_CLIENT_SECRET,
          process.env.GMAIL_REDIRECT_URI
        );
      }
      
      credential = credentials[0];
      
      // Update usage count
      await pool.query(
        'UPDATE gmail_credentials SET usage_count = usage_count + 1, last_used = NOW() WHERE id = ?',
        [credential.id]
      );
    }
    
    return new google.auth.OAuth2(
      credential.client_id,
      decrypt(credential.client_secret),
      credential.redirect_uri
    );
  } catch (error) {
    console.error('Error getting OAuth client:', error);
    
    // Fallback to environment variables
    return new google.auth.OAuth2(
      process.env.GMAIL_CLIENT_ID,
      process.env.GMAIL_CLIENT_SECRET,
      process.env.GMAIL_REDIRECT_URI
    );
  }
}

// Gmail Account Management
export async function addGmailAccount(code) {
  const oauth2Client = await getOAuthClient();
  
  try {
    console.log('Exchanging authorization code for tokens...');
    // Exchange authorization code for tokens
    const { tokens } = await oauth2Client.getToken(code);
    
    // Set credentials to get user info
    oauth2Client.setCredentials(tokens);
    
    // Get Gmail profile to identify the email
    const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
    const profile = await gmail.users.getProfile({ userId: 'me' });
    
    const email = profile.data.emailAddress;
    console.log(`Successfully obtained profile for email: ${email}`);
    
    // Start a transaction
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();
      
      // Check if account already exists
      const [existingAccounts] = await connection.query(
        'SELECT * FROM gmail_accounts WHERE email = ?',
        [email]
      );
      
      const id = existingAccounts.length > 0 ? existingAccounts[0].id : uuidv4();
      const encryptedRefreshToken = tokens.refresh_token ? encrypt(tokens.refresh_token) : 
                                   (existingAccounts.length > 0 ? existingAccounts[0].refresh_token : null);
      
      // Calculate expiry time with a valid default if expires_in is not provided
      const expiresIn = typeof tokens.expires_in === 'number' ? tokens.expires_in : 3600; // Default to 1 hour
      const expiresAt = Date.now() + (expiresIn * 1000);
      
      const wasInAuthError = existingAccounts.length > 0 && existingAccounts[0].status === 'auth-error';

      if (existingAccounts.length > 0) {
        // Update existing account
        await connection.query(
          `UPDATE gmail_accounts SET 
           access_token = ?,
           expires_at = ?,
           refresh_token = COALESCE(?, refresh_token),
           status = 'active',
           last_used = NOW(),
           updated_at = NOW()
           WHERE id = ?`,
          [
            tokens.access_token,
            expiresAt, // Using the fixed expires_at value
            encryptedRefreshToken,
            id
          ]
        );
        console.log(`Updated existing Gmail account: ${email}`);
      } else {
        // Insert new account
        await connection.query(
          `INSERT INTO gmail_accounts (
            id, email, refresh_token, access_token, expires_at, quota_used, status, last_used
          ) VALUES (?, ?, ?, ?, ?, 0, 'active', NOW())`,
          [
            id,
            email,
            encryptedRefreshToken,
            tokens.access_token,
            expiresAt // Using the fixed expires_at value
          ]
        );
        console.log(`Added new Gmail account: ${email}`);
      }
      
      await connection.commit();
      
      // Specifically handle reauthorized accounts that had auth errors before
      if (wasInAuthError) {
        console.log(`Account ${email} was previously in auth-error state, restarting polling...`);
        // Remove from active polling if it was erroneously there
        activePollingAccounts.delete(email);
        
        // Start fresh polling for this account
        if (!activePollingAccounts.has(email)) {
          schedulePolling(email);
          activePollingAccounts.add(email);
        }
      } else {
        // Standard polling start for new or active accounts
        if (!activePollingAccounts.has(email)) {
          schedulePolling(email);
          activePollingAccounts.add(email);
        }
      }
      
      return { email, id };
      
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
    
  } catch (error) {
    console.error('Failed to add Gmail account:', error);
    throw new Error('Failed to authenticate with Gmail: ' + error.message);
  }
}

// Get or refresh access token with enhanced error handling and recovery
export async function getValidAccessToken(accountEmail) {
  console.log(`Getting valid access token for ${accountEmail}...`);
  
  try {
    // Get account from database
    const [accounts] = await pool.query(
      'SELECT * FROM gmail_accounts WHERE email = ?',
      [accountEmail]
    );
    
    if (accounts.length === 0) {
      throw new Error(`Gmail account ${accountEmail} not found`);
    }
    
    const account = accounts[0];
    
    // Check if token is still valid
    if (account.access_token && account.expires_at > Date.now()) {
      console.log('Using existing valid access token');
      return account.access_token;
    }
    
    // Refresh the token
    console.log('Refreshing access token...');
    const oauth2Client = await getOAuthClient();
    
    if (!account.refresh_token) {
      throw new Error(`No refresh token available for ${accountEmail}`);
    }
    
    const refreshToken = decrypt(account.refresh_token);
    
    oauth2Client.setCredentials({ refresh_token: refreshToken });
    const { credentials } = await oauth2Client.refreshAccessToken();
    
    // Make sure we have a valid expiration time (never NaN)
    const expiresAt = credentials.expires_in 
      ? Date.now() + (Number(credentials.expires_in) * 1000) 
      : Date.now() + (3600 * 1000); // Default to 1 hour if no expiration provided
    
    // Update account with new tokens in database
    await pool.query(
      `UPDATE gmail_accounts SET 
       access_token = ?,
       expires_at = ?,
       status = 'active',
       last_used = NOW(),
       updated_at = NOW()
       WHERE email = ?`,
      [
        credentials.access_token,
        expiresAt,
        accountEmail
      ]
    );
    
    console.log('Token refreshed successfully');
    return credentials.access_token;
    
  } catch (error) {
    console.error(`Failed to refresh access token for ${accountEmail}:`, error);
    
    // Update account status in database with more detailed status
    let statusUpdate = 'error';
    if (error.message.includes('invalid_grant') || error.message.includes('unauthorized_client')) {
      statusUpdate = 'auth-error';
    } else if (error.message.includes('quota') || error.message.includes('rate limit')) {
      statusUpdate = 'rate-limited';
    } else if (error.message.includes('network') || error.message.includes('timeout')) {
      statusUpdate = 'network-error';
    }
    
    try {
      await pool.query(
        'UPDATE gmail_accounts SET status = ?, updated_at = NOW() WHERE email = ?',
        [statusUpdate, accountEmail]
      );
      
      // Attempt auto-recovery for certain types of errors
      if (statusUpdate !== 'auth-error') {
        // Schedule a retry after a delay
        setTimeout(async () => {
          try {
            console.log(`Attempting auto-recovery for ${accountEmail}...`);
            await pool.query(
              'UPDATE gmail_accounts SET status = \'active\', updated_at = NOW() WHERE email = ?',
              [accountEmail]
            );
            
            // Restart polling for this account
            if (!activePollingAccounts.has(accountEmail)) {
              console.log(`Restarting polling for recovered account ${accountEmail}`);
              schedulePolling(accountEmail);
              activePollingAccounts.add(accountEmail);
            }
          } catch (retryError) {
            console.error(`Failed to auto-recover ${accountEmail}:`, retryError);
          }
        }, 15 * 60 * 1000); // Try to recover after 15 minutes
      }
      
    } catch (dbError) {
      console.error('Error updating account status:', dbError);
    }
    
    throw new Error('Failed to refresh access token: ' + error.message);
  }
}

// Alias Generation with improved reliability
export async function generateGmailAlias(userId, strategy = 'dot') {
  // Get next available account using load balancing from database
  try {
    const account = await getNextAvailableAccount();
    
    if (!account) {
      console.error('No Gmail accounts available. Active accounts:', [...activePollingAccounts]);
      throw new Error('No Gmail accounts available');
    }
    
    console.log(`Generating ${strategy} alias using account: ${account.email}`);
    
    // Generate unique alias based on strategy
    const alias = strategy === 'dot' 
      ? generateDotAlias(account.email)
      : generatePlusAlias(account.email);
    
    console.log(`Generated alias: ${alias}`);
    
    // Update alias count in database
    const connection = await pool.getConnection();
    
    try {
      await connection.beginTransaction();
      
      // Update alias count in gmail_accounts
      await connection.query(
        'UPDATE gmail_accounts SET alias_count = alias_count + 1, last_used = NOW(), updated_at = NOW() WHERE id = ?',
        [account.id]
      );
      
      await connection.commit();
      
      // Store in-memory alias cache
      aliasCache.set(alias, {
        parentAccount: account.email,
        parentAccountId: account.id,
        created: Date.now(),
        lastAccessed: Date.now(),
        userId: userId || null,
        expires: new Date(Date.now() + ALIAS_TTL)
      });
      
      return { alias };
      
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
    
  } catch (error) {
    console.error('Failed to generate Gmail alias:', error);
    throw new Error('Failed to generate alias: ' + error.message);
  }
}

function generateDotAlias(email) {
  // Extract username and domain
  const [username, domain] = email.split('@');
  
  // Insert dots randomly in username
  let dotUsername = '';
  for (let i = 0; i < username.length - 1; i++) {
    dotUsername += username[i];
    // Random chance to insert a dot, but ensure no consecutive dots
    if (Math.random() > 0.5 && username[i] !== '.' && username[i+1] !== '.') {
      dotUsername += '.';
    }
  }
  // Add last character
  dotUsername += username[username.length - 1];
  
  return `${dotUsername}@${domain}`;
}

function generatePlusAlias(email) {
  // Extract username and domain
  const [username, domain] = email.split('@');
  
  // Add random tag
  const tag = Math.random().toString(36).substring(2, 8);
  
  return `${username}+${tag}@${domain}`;
}

// Email Fetching with improved caching and retrieval
export async function fetchGmailEmails(userId, aliasEmail) {
  console.log(`Fetching emails for ${aliasEmail}, requested by user ${userId || 'anonymous'}`);
  
  try {
    // Check if alias exists in memory cache first
    let parentAccount = null;
    
    if (aliasCache.has(aliasEmail)) {
      const cachedAlias = aliasCache.get(aliasEmail);
      parentAccount = cachedAlias.parentAccount;
      
      // Update last accessed timestamp in memory
      cachedAlias.lastAccessed = Date.now();
      aliasCache.set(aliasEmail, cachedAlias);
      
      console.log(`Found alias ${aliasEmail} in memory cache, parent account: ${parentAccount}`);
    } else {
      console.log(`Alias ${aliasEmail} not found in memory cache, checking user permissions`);
      
      // Modified permission check for aliases not in memory
      // Check if this user has permission to access this alias (only for non-anonymous users)
      if (userId && !userId.startsWith('anon_')) {
        // For authenticated users, we'll be more permissive since we don't have DB records
        // We'll generate a new alias for them instead of failing
        console.log(`Authorized user ${userId} requesting missing alias, will create new one`);
        const result = await generateGmailAlias(userId);
        return fetchGmailEmails(userId, result.alias); // Recursive call with new alias
      }
      
      // For anonymous users with missing alias, also generate a new one
      if (userId && userId.startsWith('anon_')) {
        console.log(`Anonymous user ${userId} requesting missing alias, will create new one`);
        const result = await generateGmailAlias(userId);
        return fetchGmailEmails(userId, result.alias); // Recursive call with new alias
      }
      
      throw new Error('Alias not found');
    }
    
    if (!parentAccount) {
      throw new Error('Parent account not found for alias');
    }
    
    // Check parent account status
    const [accounts] = await pool.query(
      'SELECT * FROM gmail_accounts WHERE email = ?',
      [parentAccount]
    );
    
    if (accounts.length === 0) {
      throw new Error('Gmail account unavailable');
    }
    
    const account = accounts[0];
    
    if (account.status !== 'active') {
      console.error(`Account ${parentAccount} is not active. Current status: ${account.status}`);
      
      // Auto-recovery: Try to reactivate account if it's not in auth-error state
      if (account.status !== 'auth-error') {
        console.log(`Attempting to reactivate account ${parentAccount}`);
        await pool.query(
          'UPDATE gmail_accounts SET status = \'active\', updated_at = NOW() WHERE id = ?',
          [account.id]
        );
        
        // If the account was inactive, reactivate polling for it
        if (!activePollingAccounts.has(parentAccount)) {
          console.log(`Restarting polling for reactivated account ${parentAccount}`);
          schedulePolling(parentAccount);
          activePollingAccounts.add(parentAccount);
        }
      } else {
        throw new Error('Gmail account unavailable - authentication error');
      }
    }
    
    // Get cached emails
    console.log(`Looking for cached emails for alias ${aliasEmail}`);
    const cachedEmails = [];
    for (const [key, email] of emailCache.entries()) {
      if (key.startsWith(`${aliasEmail}:`)) {
        cachedEmails.push(email);
      }
    }
    
    // Return cached emails sorted by date (newest first)
    console.log(`Found ${cachedEmails.length} cached emails for ${aliasEmail}`);
    return cachedEmails.sort((a, b) => 
      new Date(b.internalDate) - new Date(a.internalDate)
    );
  
  } catch (error) {
    console.error(`Error fetching Gmail emails for ${aliasEmail}:`, error);
    throw error;
  }
}

// Only return most recent one
export async function getUserAliases(userId) {
  if (!userId) return [];
  
  try {
    // Get only the most recent alias from memory cache for this user
    const userAliases = [];
    let mostRecentAlias = null;
    let mostRecentTime = 0;
    
    for (const [alias, data] of aliasCache.entries()) {
      if (data.userId === userId && data.created > mostRecentTime) {
        mostRecentAlias = alias;
        mostRecentTime = data.created;
      }
    }
    
    if (mostRecentAlias) {
      userAliases.push(mostRecentAlias);
    }
    
    console.log(`User ${userId} has ${userAliases.length} recent alias in memory cache: ${mostRecentAlias}`);
    return userAliases;
  } catch (error) {
    console.error('Failed to get user aliases from memory:', error);
    return [];
  }
}

export async function rotateUserAlias(userId) {
  try {
    // Generate a new alias for the user (will use load balancing)
    return await generateGmailAlias(userId);
  } catch (error) {
    console.error('Failed to rotate Gmail alias:', error);
    throw error;
  }
}

// Email Polling (Background Task) with improved error handling and frequency
async function pollForNewEmails(accountEmail) {
  try {
    // Get account from database
    const [accounts] = await pool.query(
      'SELECT * FROM gmail_accounts WHERE email = ?',
      [accountEmail]
    );
    
    if (accounts.length === 0) {
      console.log(`Account ${accountEmail} not found, skipping polling`);
      return;
    }
    
    const account = accounts[0];
    
    if (account.status !== 'active') {
      console.log(`Skipping polling for inactive account ${accountEmail} (status: ${account.status})`);
      
      // Only attempt recovery if the account has been inactive for at least 10 minutes
      // (but not if it's in auth-error state)
      if (account.status !== 'auth-error') {
        const tenMinutesAgo = new Date(Date.now() - 10 * 60 * 1000);
        if (new Date(account.updated_at) < tenMinutesAgo) {
          console.log(`Attempting to auto-recover account ${accountEmail}`);
          await pool.query(
            'UPDATE gmail_accounts SET status = \'active\', updated_at = NOW() WHERE id = ?',
            [account.id]
          );
        }
      }
      return;
    }
    
    // Get all aliases associated with this account from in-memory cache only
    const accountAliases = [];
    for (const [alias, data] of aliasCache.entries()) {
      if (data.parentAccount === accountEmail) {
        accountAliases.push(alias);
      }
    }
    
    if (accountAliases.length === 0) {
      console.log(`Skipping polling for ${accountEmail}: no aliases in memory cache`);
      return;
    }
    
    // Get fresh access token
    console.log(`Polling for new emails for account ${accountEmail} with ${accountAliases.length} aliases...`);
    const accessToken = await getValidAccessToken(accountEmail);
    
    // Initialize Gmail API client
    const oauth2Client = await getOAuthClient();
    oauth2Client.setCredentials({ access_token: accessToken });
    const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
    
    // Fetch emails for all aliases associated with this account
    for (const alias of accountAliases) {
      // Build query to get recent emails sent to this alias including spam/trash
      const query = `to:${alias} newer_than:2m in:anywhere`; // Search everywhere, including spam
      
      try {
        // List messages matching query
        const response = await gmail.users.messages.list({
          userId: 'me',
          q: query,
          maxResults: 10
        });
        
        const messages = response.data.messages || [];
        console.log(`Found ${messages.length} new messages for alias ${alias}`);
        
        // Fetch full message details for each message
        for (const message of messages) {
          // Check if already in cache
          const cacheKey = `${alias}:${message.id}`;
          
          if (!emailCache.has(cacheKey)) {
            // Fetch full message
            const fullMessage = await gmail.users.messages.get({
              userId: 'me',
              id: message.id,
              format: 'full'
            });
            
            // Process message to extract headers, body, etc.
            const processedEmail = processGmailMessage(fullMessage.data, alias);
            
            // Add to cache
            addToEmailCache(cacheKey, processedEmail);
          }
        }
      } catch (error) {
        console.error(`Error fetching emails for alias ${alias}:`, error);
      }
    }
    
    // Update account metrics in database
    await pool.query(
      'UPDATE gmail_accounts SET quota_used = quota_used + 1, last_used = NOW(), updated_at = NOW() WHERE id = ?',
      [account.id]
    );
    
  } catch (error) {
    console.error(`Error polling Gmail account ${accountEmail}:`, error);
    
    // Update account status in database with more detailed status
    let statusUpdate = 'error';
    if (error.message.includes('quota') || error.message.includes('rate limit')) {
      statusUpdate = 'rate-limited';
    } else if (error.message.includes('auth') || error.message.includes('token')) {
      statusUpdate = 'auth-error';
      // Remove from active polling set when auth error is detected
      activePollingAccounts.delete(accountEmail);
    } else if (error.message.includes('network') || error.message.includes('timeout')) {
      statusUpdate = 'network-error';
    }
    
    try {
      await pool.query(
        'UPDATE gmail_accounts SET status = ?, updated_at = NOW() WHERE email = ?',
        [statusUpdate, accountEmail]
      );
    } catch (dbError) {
      console.error('Error updating account status:', dbError);
    }
  }
}

function schedulePolling(accountEmail) {
  console.log(`Setting up polling schedule for ${accountEmail}`);
  
  // Check if account is active (in a self-executing async function)
  (async () => {
    try {
      // Get latest account state from database to ensure we have the most current info
      const [accounts] = await pool.query(
        'SELECT * FROM gmail_accounts WHERE email = ?',
        [accountEmail]
      );
      
      if (accounts.length === 0) {
        console.log(`Not scheduling polling for missing account: ${accountEmail}`);
        activePollingAccounts.delete(accountEmail); // Ensure it's removed from active set
        return;
      }
      
      const accountStatus = accounts[0].status;
      if (accountStatus !== 'active') {
        console.log(`Not scheduling polling for inactive account (${accountStatus}): ${accountEmail}`);
        activePollingAccounts.delete(accountEmail); // Ensure it's removed from active set
        return;
      }
      
      console.log(`Scheduling polling for ${accountEmail}`);
      
      // Determine polling interval based on activity
      let interval = POLLING_INTERVALS.medium; // Default to medium priority
      
      if (accounts[0].alias_count > 10) {
        interval = POLLING_INTERVALS.high;
      } else if (accounts[0].alias_count > 5) {
        interval = POLLING_INTERVALS.medium;
      } else {
        interval = POLLING_INTERVALS.low;
      }
      
      console.log(`Using polling interval of ${interval}ms for ${accountEmail}`);
      
      // Only add to active set if it's not already being polled
      if (!activePollingAccounts.has(accountEmail)) {
        activePollingAccounts.add(accountEmail);
      }
      
      // Schedule first poll
      setTimeout(() => {
        pollForNewEmails(accountEmail)
          .finally(() => {
            // Only schedule next poll if account is still in active set and polling should continue
            if (activePollingAccounts.has(accountEmail)) {
              schedulePolling(accountEmail);
            } else {
              console.log(`Stopped polling for ${accountEmail} as it's no longer in active set`);
            }
          });
      }, interval);
      
    } catch (error) {
      console.error(`Error setting up polling for ${accountEmail}:`, error);
      // Don't remove from active polling on setup error - it may be temporary
    }
  })();
}

// Process Gmail message into standardized format
function processGmailMessage(message, recipientAlias) {
  // Extract headers
  const headers = {};
  message.payload.headers.forEach(header => {
    headers[header.name.toLowerCase()] = header.value;
  });
  
  // Extract body
  let bodyHtml = '';
  let bodyText = '';
  
  // Process parts recursively to find the body
  function processMessageParts(parts) {
    if (!parts) return;
    
    for (const part of parts) {
      if (part.mimeType === 'text/html' && part.body.data) {
        bodyHtml = Buffer.from(part.body.data, 'base64').toString('utf-8');
      } else if (part.mimeType === 'text/plain' && part.body.data) {
        bodyText = Buffer.from(part.body.data, 'base64').toString('utf-8');
      }
      
      if (part.parts) {
        processMessageParts(part.parts);
      }
    }
  }
  
  if (message.payload.parts) {
    processMessageParts(message.payload.parts);
  } else if (message.payload.body && message.payload.body.data) {
    // Handle single-part messages
    if (message.payload.mimeType === 'text/html') {
      bodyHtml = Buffer.from(message.payload.body.data, 'base64').toString('utf-8');
    } else {
      bodyText = Buffer.from(message.payload.body.data, 'base64').toString('utf-8');
    }
  }
  
  // Extract attachments
  const attachments = [];
  
  // Format the processed email
  return {
    id: message.id,
    threadId: message.threadId,
    labelIds: message.labelIds,
    from: headers.from,
    to: headers.to,
    subject: headers.subject || '(No Subject)',
    bodyHtml,
    bodyText,
    internalDate: new Date(parseInt(message.internalDate)).toISOString(),
    timestamp: Date.now(),
    snippet: message.snippet,
    recipientAlias,
    attachments
  };
}

// Cache Management with improved efficiency
function addToEmailCache(key, email) {
  // If cache is at capacity, remove oldest entries
  if (emailCache.size >= MAX_CACHE_SIZE) {
    const oldestKeys = [...emailCache.keys()]
      .map(k => ({ key: k, timestamp: emailCache.get(k).timestamp }))
      .sort((a, b) => a.timestamp - b.timestamp)
      .slice(0, Math.ceil(MAX_CACHE_SIZE * 0.2)) // Remove oldest 20%
      .map(item => item.key);
    
    oldestKeys.forEach(key => emailCache.delete(key));
  }
  
  // Add new email to cache
  emailCache.set(key, {
    ...email,
    timestamp: Date.now()
  });
}

// Cleanup and maintenance
export async function cleanupInactiveAliases() {
  console.log('Running in-memory alias cleanup...');
  
  // Clean up in-memory cache
  const now = Date.now();
  let inMemoryCleanupCount = 0;
  
  for (const [alias, data] of aliasCache.entries()) {
    if (now - data.lastAccessed > ALIAS_TTL) {
      // Also keep track of account aliases to update count in DB
      if (data.parentAccountId) {
        try {
          // Decrement alias count in the database
          await pool.query(
            'UPDATE gmail_accounts SET alias_count = GREATEST(0, alias_count - 1), updated_at = NOW() WHERE id = ?',
            [data.parentAccountId]
          );
        } catch (error) {
          console.error(`Error updating alias count for account ${data.parentAccountId}:`, error);
        }
      }
      
      aliasCache.delete(alias);
      inMemoryCleanupCount++;
    }
  }
  
  if (inMemoryCleanupCount > 0) {
    console.log(`Cleaned up ${inMemoryCleanupCount} inactive aliases from memory cache`);
  }
}

// Run alias cleanup every hour
setInterval(cleanupInactiveAliases, 60 * 60 * 1000);

// Function to check for Gmail accounts that need polling restart
export async function checkAndRestartPolling() {
  try {
    console.log("Running scheduled check for accounts needing polling restart...");
    
    // Get all active accounts that aren't currently being polled
    const [accounts] = await pool.query(`
      SELECT email FROM gmail_accounts 
      WHERE status = 'active' AND updated_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)
    `);
    
    if (!accounts || accounts.length === 0) {
      console.log("No recently active accounts found that need polling restart");
      return;
    }
    
    // Start polling for any active accounts that aren't currently being polled
    let restartCount = 0;
    for (const account of accounts) {
      if (!activePollingAccounts.has(account.email)) {
        console.log(`Restarting polling for active account: ${account.email}`);
        schedulePolling(account.email);
        activePollingAccounts.add(account.email);
        restartCount++;
      }
    }
    
    console.log(`Restarted polling for ${restartCount} accounts`);
  } catch (error) {
    console.error("Error checking for accounts needing polling restart:", error);
  }
}

// Run the polling restart check every 15 minutes
setInterval(checkAndRestartPolling, 15 * 60 * 1000);

// Setup auto-recovery for non-auth-error accounts
setInterval(async () => {
  try {
    const [result] = await pool.query(`
      UPDATE gmail_accounts 
      SET status = 'active', updated_at = NOW() 
      WHERE status NOT IN ('active', 'auth-error') 
      AND updated_at < DATE_SUB(NOW(), INTERVAL 30 MINUTE)
    `);
    
    if (result.affectedRows > 0) {
      console.log(`Auto-recovered ${result.affectedRows} Gmail accounts`);
      
      // Get the list of accounts that were auto-recovered so we can restart polling for them
      const [recoveredAccounts] = await pool.query(`
        SELECT email FROM gmail_accounts 
        WHERE status = 'active'
        AND updated_at > DATE_SUB(NOW(), INTERVAL 1 MINUTE)
      `);
      
      // Restart polling for recovered accounts
      for (const account of recoveredAccounts) {
        if (!activePollingAccounts.has(account.email)) {
          console.log(`Restarting polling for auto-recovered account: ${account.email}`);
          schedulePolling(account.email);
          activePollingAccounts.add(account.email);
        }
      }
    }
  } catch (error) {
    console.error('Error in auto-recovery process:', error);
  }
}, 15 * 60 * 1000); // Run every 15 minutes

// Load Balancing with improved account selection
async function getNextAvailableAccount() {
  try {
    // Get available accounts with balancing strategy
    const [accounts] = await pool.query(`
      SELECT * 
      FROM gmail_accounts a
      WHERE a.status = 'active' OR (a.status = 'rate-limited' AND a.updated_at < DATE_SUB(NOW(), INTERVAL 30 MINUTE))
      ORDER BY 
        CASE WHEN a.status = 'active' THEN 0 ELSE 1 END,  -- Active accounts first
        a.alias_count ASC,                                -- Accounts with fewer aliases
        a.quota_used ASC,                                 -- Accounts with less quota usage
        a.last_used ASC                                   -- Least recently used accounts
      LIMIT 1
    `);
    
    if (accounts.length === 0) {
      console.error('No available Gmail accounts');
      
      // Attempt to auto-recover accounts that haven't been updated in a while
      const [recoveryResult] = await pool.query(`
        UPDATE gmail_accounts 
        SET status = 'active', updated_at = NOW() 
        WHERE status != 'auth-error' 
        AND updated_at < DATE_SUB(NOW(), INTERVAL 1 HOUR)
        LIMIT 3
      `);
      
      if (recoveryResult.affectedRows > 0) {
        console.log(`Auto-recovered ${recoveryResult.affectedRows} Gmail accounts`);
        
        // Try again after recovery
        const [recoveredAccounts] = await pool.query(`
          SELECT * FROM gmail_accounts
          WHERE status = 'active'
          ORDER BY alias_count ASC, quota_used ASC, last_used ASC
          LIMIT 1
        `);
        
        if (recoveredAccounts.length > 0) {
          return recoveredAccounts[0];
        }
      }
      
      return null;
    }

    const selectedAccount = accounts[0];
    
    // Auto-recover rate-limited accounts after a cool-down period
    if (selectedAccount.status !== 'active') {
      await pool.query(
        'UPDATE gmail_accounts SET status = \'active\', updated_at = NOW() WHERE id = ?',
        [selectedAccount.id]
      );
    }
    
    console.log(`Selected account for new alias: ${selectedAccount.email} (aliases: ${selectedAccount.alias_count}, quota: ${selectedAccount.quota_used})`);
    
    return selectedAccount;
  } catch (error) {
    console.error('Error selecting next available account:', error);
    return null;
  }
}

// Credential Management
export async function addGmailCredential(credential) {
  const id = uuidv4();
  const connection = await pool.getConnection();
  
  try {
    await connection.beginTransaction();
    
    // Encrypt client secret for database storage
    const encryptedSecret = encrypt(credential.clientSecret);
    
    await connection.query(`
      INSERT INTO gmail_credentials (
        id, client_id, client_secret, redirect_uri, active, usage_count, last_used
      ) VALUES (?, ?, ?, ?, ?, ?, NOW())
    `, [
      id,
      credential.clientId,
      encryptedSecret,
      credential.redirectUri,
      credential.active !== false,
      credential.usageCount || 0
    ]);
    
    await connection.commit();
    
    // Return credential without the actual secret
    return { 
      ...credential, 
      id,
      clientSecret: '***********' 
    };
  } catch (error) {
    await connection.rollback();
    console.error('Failed to add Gmail credential:', error);
    throw error;
  } finally {
    connection.release();
  }
}

export async function updateGmailCredential(id, updates) {
  const connection = await pool.getConnection();
  
  try {
    await connection.beginTransaction();
    
    // Get current credential to ensure it exists
    const [credentials] = await connection.query(
      'SELECT * FROM gmail_credentials WHERE id = ?',
      [id]
    );
    
    if (credentials.length === 0) {
      throw new Error('Credential not found');
    }
    
    // Prepare update fields
    const updateFields = [];
    const updateValues = [];
    
    if (updates.clientId) {
      updateFields.push('client_id = ?');
      updateValues.push(updates.clientId);
    }
    
    if (updates.clientSecret) {
      updateFields.push('client_secret = ?');
      updateValues.push(encrypt(updates.clientSecret));
    }
    
    if (updates.redirectUri) {
      updateFields.push('redirect_uri = ?');
      updateValues.push(updates.redirectUri);
    }
    
    if (typeof updates.active !== 'undefined') {
      updateFields.push('active = ?');
      updateValues.push(updates.active);
    }
    
    // Only update if there are fields to update
    if (updateFields.length > 0) {
      updateFields.push('updated_at = NOW()');
      updateValues.push(id);
      
      await connection.query(
        `UPDATE gmail_credentials SET ${updateFields.join(', ')} WHERE id = ?`,
        updateValues
      );
    }
    
    await connection.commit();
    
    // Return updated credential
    const [updatedCredentials] = await connection.query(
      'SELECT id, client_id, redirect_uri, active, usage_count, last_used FROM gmail_credentials WHERE id = ?',
      [id]
    );
    
    return {
      id: updatedCredentials[0].id,
      clientId: updatedCredentials[0].client_id,
      redirectUri: updatedCredentials[0].redirect_uri,
      active: updatedCredentials[0].active === 1,
      usageCount: updatedCredentials[0].usage_count,
      lastUsed: updatedCredentials[0].last_used,
      clientSecret: '***********'
    };
  } catch (error) {
    await connection.rollback();
    console.error('Failed to update Gmail credential:', error);
    throw error;
  } finally {
    connection.release();
  }
}

export async function deleteGmailCredential(id) {
  try {
    const [result] = await pool.query('DELETE FROM gmail_credentials WHERE id = ?', [id]);
    
    if (result.affectedRows === 0) {
      throw new Error('Credential not found');
    }
  } catch (error) {
    console.error('Failed to delete Gmail credential:', error);
    throw error;
  }
}

export async function updateGmailCredentialStatus(id, active) {
  try {
    const [result] = await pool.query(
      'UPDATE gmail_credentials SET active = ?, updated_at = NOW() WHERE id = ?',
      [active, id]
    );
    
    if (result.affectedRows === 0) {
      throw new Error('Credential not found');
    }
  } catch (error) {
    console.error('Failed to update Gmail credential status:', error);
    throw error;
  }
}

export async function getGmailCredentials() {
  try {
    const [credentials] = await pool.query(
      'SELECT id, client_id, redirect_uri, active, usage_count, last_used, created_at, updated_at FROM gmail_credentials'
    );
    
    return credentials.map(cred => ({
      id: cred.id,
      clientId: cred.client_id,
      redirectUri: cred.redirect_uri,
      active: cred.active === 1,
      usageCount: cred.usage_count,
      lastUsed: cred.last_used,
      clientSecret: '***********'
    }));
  } catch (error) {
    console.error('Failed to get Gmail credentials:', error);
    throw error;
  }
}

// Verify a credential with enhanced security checks
export async function verifyCredential(credentialId) {
  try {
    // Get credential from database
    const [credentials] = await pool.query(
      'SELECT * FROM gmail_credentials WHERE id = ?',
      [credentialId]
    );
    
    if (credentials.length === 0) {
      throw new Error('Credential not found');
    }
    
    const credential = credentials[0];
    
    // Create OAuth client with this credential
    const oauth2Client = new google.auth.OAuth2(
      credential.client_id,
      decrypt(credential.client_secret),
      credential.redirect_uri
    );
    
    // Get the auth URL to verify the credentials are valid
    const authUrl = oauth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: GMAIL_SCOPES,
      prompt: 'consent' // Force to get refresh token
    });
    
    // If we can generate an auth URL, the credentials are valid
    return {
      valid: true,
      authUrl: authUrl
    };
  } catch (error) {
    console.error('Credential verification failed:', error);
    throw new Error(`Credential verification failed: ${error.message}`);
  }
}

// Initialize OAuth URL generator
export function getAuthUrl(credentialId = null) {
  return new Promise(async (resolve, reject) => {
    try {
      let oauth2Client;
      
      if (credentialId) {
        oauth2Client = await getOAuthClient(credentialId);
      } else {
        oauth2Client = await getOAuthClient();
      }
      
      const authUrl = oauth2Client.generateAuthUrl({
        access_type: 'offline',
        scope: GMAIL_SCOPES,
        prompt: 'consent' // Force to get refresh token
      });
      
      resolve(authUrl);
    } catch (error) {
      console.error('Failed to generate auth URL:', error);
      reject(error);
    }
  });
}

// Function to force check and update all accounts
export async function forceCheckAllAccounts() {
  console.log("Performing forced check of all Gmail accounts...");
  
  try {
    // Get all accounts from the database
    const [allAccounts] = await pool.query(`SELECT * FROM gmail_accounts`);
    
    console.log(`Found ${allAccounts.length} Gmail accounts to check`);
    
    for (const account of allAccounts) {
      const accountEmail = account.email;
      
      // If account is active but not being polled, start polling
      if (account.status === 'active' && !activePollingAccounts.has(accountEmail)) {
        console.log(`Starting polling for active account ${accountEmail} that wasn't being polled`);
        schedulePolling(accountEmail);
        activePollingAccounts.add(accountEmail);
      }
      // If account is inactive but being polled, stop polling
      else if (account.status !== 'active' && activePollingAccounts.has(accountEmail)) {
        console.log(`Removing inactive account ${accountEmail} from polling`);
        activePollingAccounts.delete(accountEmail);
      }
    }
    
    // Log account status
    console.log(`After check: ${activePollingAccounts.size} accounts actively polling`);
    return {
      totalAccounts: allAccounts.length,
      activelyPolling: activePollingAccounts.size,
      pollingAccounts: Array.from(activePollingAccounts)
    };
  } catch (error) {
    console.error("Error performing forced account check:", error);
    throw error;
  }
}

// Check all accounts every 30 minutes to catch any polling issues
setInterval(forceCheckAllAccounts, 30 * 60 * 1000);

// Admin functions
export async function getGmailAccountStats() {
  try {
    // Get overall stats
    const [accountsCount] = await pool.query(
      'SELECT COUNT(*) as count FROM gmail_accounts'
    );
    
    // Count aliases from in-memory cache
    const aliasCount = aliasCache.size;
    
    // Count unique users from in-memory cache
    const userIds = new Set();
    for (const data of aliasCache.values()) {
      if (data.userId) {
        userIds.add(data.userId);
      }
    }
    
    // Get account details
    const [accounts] = await pool.query(`
      SELECT id, email, status, quota_used, alias_count, last_used, updated_at
      FROM gmail_accounts
      ORDER BY last_used DESC
    `);
    
    return {
      totalAccounts: accountsCount[0].count,
      totalAliases: aliasCount,
      totalUsers: userIds.size,
      active: accounts.filter(a => a.status === 'active').length,
      auth_error: accounts.filter(a => a.status === 'auth-error').length,
      rate_limited: accounts.filter(a => a.status === 'rate-limited').length,
      accounts: accounts.map(account => ({
        id: account.id,
        email: account.email,
        status: account.status,
        aliasCount: account.alias_count,
        quotaUsed: account.quota_used,
        lastUsed: account.last_used
      }))
    };
  } catch (error) {
    console.error('Failed to get Gmail account stats:', error);
    return {
      totalAccounts: 0,
      totalAliases: 0,
      totalUsers: 0,
      accounts: []
    };
  }
}

export function getEmailCacheStats() {
  return {
    size: emailCache.size,
    maxSize: MAX_CACHE_SIZE
  };
}

// Export for testing and monitoring
export const stores = {
  emailCache,
  aliasCache
};
