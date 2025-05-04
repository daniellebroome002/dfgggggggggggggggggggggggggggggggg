import { ImapFlow } from 'imapflow';
import { v4 as uuidv4 } from 'uuid';
import { pool } from '../db/init.js';
import crypto from 'crypto';
import { WebSocketServer } from 'ws';
import { simpleParser } from 'mailparser';  // For better email parsing

// In-memory storage
const emailCache = new Map(); // Cache for fetched emails
const aliasCache = new Map(); // Cache for active aliases during runtime
const activeImapAccounts = new Set(); // Track which accounts are being actively polled
const imapClients = new Map(); // Store active IMAP clients
const connectedClients = new Map(); // Map of userId:alias -> Set of websocket connections
const reconnectionAttempts = new Map(); // Track reconnection attempts for exponential backoff
const pendingDatabaseUpdates = new Map(); // Batch DB updates to reduce database load
const aliasToAccountMap = new Map(); // Quick lookup of alias to account

// Configuration
const MAX_CACHE_SIZE = 10000; // Maximum number of emails to cache
const ALIAS_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days in milliseconds for in-memory cache
const MAX_RECONNECTION_ATTEMPTS = 5; // Maximum number of reconnection attempts
const BASE_RECONNECTION_DELAY = 2000; // Base delay for reconnection (2 seconds)
const MAX_RECONNECTION_DELAY = 10 * 60 * 1000; // Maximum delay (10 minutes)
const POLLING_INTERVALS = {
  high: 10000,     // 10 seconds for high priority accounts
  medium: 20000,   // 20 seconds for medium priority
  low: 30000       // 30 seconds for low priority accounts
};
const CONNECTION_TIMEOUT = 30000; // 30 second timeout for IMAP connections
const IDLE_TIMEOUT = 300000; // 5 minutes idle timeout (Gmail drops at ~10 min)
const MAX_EMAILS_PER_FETCH = 50; // Maximum number of emails to fetch at once
const DB_FLUSH_INTERVAL = 120000; // Flush DB updates every 2 minutes
const CONNECTION_POOL_SIZE = 5; // Maximum concurrent connections per account
const SEARCH_WINDOW = 14; // Number of days to search for emails

// Setup DB flushing interval
let dbFlushIntervalId = null;
let lastDbFlushTime = Date.now();

// Encryption utilities for password security
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

// Setup WebSocket Server
export function setupWebSocketServer(server) {
  const wss = new WebSocketServer({ 
    server,
    perMessageDeflate: {
      zlibDeflateOptions: { chunkSize: 1024, memLevel: 7, level: 3 },
      zlibInflateOptions: { chunkSize: 10 * 1024 },
      clientNoContextTakeover: true,
      serverNoContextTakeover: true,
      clientMaxWindowBits: 10,
      serverMaxWindowBits: 10,
      concurrencyLimit: 10,
      threshold: 1024
    }
  });
  
  console.log('WebSocket server created for real-time email updates');
  
  wss.on('connection', (ws, req) => {
    // Extract userId and alias from URL parameters
    const url = new URL(req.url, `http://${req.headers.host}`);
    const userId = url.searchParams.get('userId');
    const alias = url.searchParams.get('alias');
    
    if (!userId || !alias) {
      console.log('WebSocket connection rejected: missing userId or alias');
      ws.close();
      return;
    }
    
    const clientKey = `${userId}:${alias}`;
    console.log(`WebSocket client connected: ${clientKey}`);
    
    // Add to connected clients
    if (!connectedClients.has(clientKey)) {
      connectedClients.set(clientKey, new Set());
    }
    connectedClients.get(clientKey).add(ws);
    
    // Send initial message
    ws.send(JSON.stringify({
      type: 'connected',
      message: 'Connected successfully to real-time email updates',
      timestamp: new Date().toISOString(),
      alias
    }));
    
    // Trigger immediate polling for this alias to get the latest emails
    if (aliasToAccountMap.has(alias)) {
      const accountEmail = aliasToAccountMap.get(alias);
      // Trigger a specific fetch for this alias (in background)
      fetchEmailsForAlias(accountEmail, alias)
        .catch(err => console.error(`Error in immediate fetch for ${alias}:`, err));
    }
    
    // Handle client disconnect
    ws.on('close', () => {
      console.log(`WebSocket client disconnected: ${clientKey}`);
      if (connectedClients.has(clientKey)) {
        connectedClients.get(clientKey).delete(ws);
        if (connectedClients.get(clientKey).size === 0) {
          connectedClients.delete(clientKey);
        }
      }
    });
    
    // Handle client messages
    ws.on('message', (message) => {
      try {
        const data = JSON.parse(message.toString());
        
        // Handle ping message to keep connection alive
        if (data.type === 'ping') {
          ws.send(JSON.stringify({
            type: 'pong',
            timestamp: Date.now()
          }));
        }
      } catch (err) {
        console.error('Invalid WebSocket message:', err);
      }
    });
    
    // Handle errors
    ws.on('error', (err) => {
      console.error(`WebSocket error for client ${clientKey}:`, err);
      try {
        ws.close();
      } catch (closeErr) {
        console.error(`Error closing WebSocket for ${clientKey}:`, closeErr);
      }
    });
  });
  
  // Handle server errors
  wss.on('error', (err) => {
    console.error('WebSocket server error:', err);
  });
  
  // Log websocket stats every 5 minutes
  setInterval(() => {
    console.log(`WebSocket stats: ${connectedClients.size} unique aliases connected`);
    let totalConnections = 0;
    for (const clients of connectedClients.values()) {
      totalConnections += clients.size;
    }
    console.log(`Total WebSocket connections: ${totalConnections}`);
  }, 5 * 60 * 1000);
  
  return wss;
}

// Start DB flush interval
function startDbFlushInterval() {
  if (dbFlushIntervalId === null) {
    dbFlushIntervalId = setInterval(flushPendingDatabaseUpdates, DB_FLUSH_INTERVAL);
    console.log(`Started DB flush interval (every ${DB_FLUSH_INTERVAL / 1000} seconds)`);
  }
}

// Stop DB flush interval
function stopDbFlushInterval() {
  if (dbFlushIntervalId !== null) {
    clearInterval(dbFlushIntervalId);
    dbFlushIntervalId = null;
    console.log('Stopped DB flush interval');
  }
}

// Flush pending database updates
async function flushPendingDatabaseUpdates() {
  if (pendingDatabaseUpdates.size === 0) {
    return;
  }
  
  console.log(`Flushing ${pendingDatabaseUpdates.size} pending DB updates`);
  lastDbFlushTime = Date.now();
  
  const updates = [...pendingDatabaseUpdates.entries()];
  pendingDatabaseUpdates.clear();
  
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    
    let updatedCount = 0;
    for (const [accountId, data] of updates) {
      try {
        // Update account metrics in database
        await connection.query(
          `UPDATE gmail_accounts SET 
           quota_used = quota_used + ?,
           last_used = NOW(),
           updated_at = NOW()
           WHERE id = ?`,
          [data.quotaIncrement, accountId]
        );
        updatedCount++;
      } catch (error) {
        console.error(`Error updating account ${accountId}:`, error);
      }
    }
    
    await connection.commit();
    console.log(`Successfully committed ${updatedCount} account updates`);
    
  } catch (error) {
    await connection.rollback();
    console.error('Failed to flush database updates:', error);
    
    // Re-add failed updates back to the pending queue
    for (const [accountId, data] of updates) {
      if (!pendingDatabaseUpdates.has(accountId)) {
        pendingDatabaseUpdates.set(accountId, { quotaIncrement: 0 });
      }
      
      pendingDatabaseUpdates.get(accountId).quotaIncrement += data.quotaIncrement;
    }
    
  } finally {
    connection.release();
  }
}

// Queue a database update
function queueDatabaseUpdate(accountId, quotaIncrement = 1) {
  if (!pendingDatabaseUpdates.has(accountId)) {
    pendingDatabaseUpdates.set(accountId, { quotaIncrement: 0 });
  }
  
  pendingDatabaseUpdates.get(accountId).quotaIncrement += quotaIncrement;
  
  // Start the flush interval if not already running
  if (dbFlushIntervalId === null) {
    startDbFlushInterval();
  }
  
  // If we have a lot of pending updates or it's been too long since last flush, do it immediately
  const pendingCount = pendingDatabaseUpdates.size;
  const timeSinceLastFlush = Date.now() - lastDbFlushTime;
  
  if (pendingCount > 100 || timeSinceLastFlush > 10 * 60 * 1000) { // 10 minutes
    console.log(`Forcing immediate DB flush (${pendingCount} pending, ${Math.round(timeSinceLastFlush/1000)}s since last flush)`);
    flushPendingDatabaseUpdates();
  }
}

// Utility function to notify all connected clients for an alias
function notifyClients(alias, email) {
  for (const [clientKey, clients] of connectedClients.entries()) {
    const [userId, clientAlias] = clientKey.split(':');
    
    if (clientAlias === alias) {
      const notification = {
        type: 'new_email',
        email,
        alias,
        timestamp: new Date().toISOString()
      };
      
      const payload = JSON.stringify(notification);
      
      for (const client of clients) {
        if (client.readyState === 1) { // OPEN
          try {
            client.send(payload);
            console.log(`Notified client ${clientKey} about new email`);
          } catch (err) {
            console.error(`Error notifying client ${clientKey}:`, err);
          }
        }
      }
    }
  }
}

// Gmail Account Management
export async function addGmailAccount(email, appPassword) {
  try {
    console.log(`Adding Gmail account: ${email}`);
    
    // Encrypt the app password
    const encryptedPassword = encrypt(appPassword);
    
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
      
      if (existingAccounts.length > 0) {
        // Update existing account
        await connection.query(
          `UPDATE gmail_accounts SET 
           app_password = ?,
           status = 'active',
           last_used = NOW(),
           updated_at = NOW()
           WHERE id = ?`,
          [
            encryptedPassword,
            id
          ]
        );
        console.log(`Updated existing Gmail account: ${email}`);
      } else {
        // Insert new account
        await connection.query(
          `INSERT INTO gmail_accounts (
            id, email, app_password, quota_used, alias_count, status, last_used
          ) VALUES (?, ?, ?, 0, 0, 'active', NOW())`,
          [
            id,
            email,
            encryptedPassword
          ]
        );
        console.log(`Added new Gmail account: ${email}`);
      }
      
      await connection.commit();
      
      // Test the IMAP connection to verify credentials
      try {
        await testImapConnection(email, appPassword);
        console.log(`Successfully verified IMAP connection for ${email}`);
        
        // Reset reconnection attempts counter for this account
        reconnectionAttempts.delete(email);
      } catch (imapError) {
        console.error(`IMAP connection test failed for ${email}:`, imapError);
        throw new Error(`Failed to connect to IMAP server: ${imapError.message}`);
      }
      
      // Start polling for this account
      if (!activeImapAccounts.has(email)) {
        console.log(`Starting polling for account: ${email}`);
        schedulePolling(email);
        activeImapAccounts.add(email);
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
    throw new Error('Failed to add Gmail account: ' + error.message);
  }
}

// Test IMAP connection for an account
async function testImapConnection(email, appPassword) {
  // Create a temporary client for testing
  const testClient = new ImapFlow({
    host: 'imap.gmail.com',
    port: 993,
    secure: true,
    auth: {
      user: email,
      pass: appPassword
    },
    logger: false,
    emitLogs: false,
    timeoutConnection: 20000, // 20 seconds timeout
    tls: {
      rejectUnauthorized: true,
      enableTrace: false
    }
  });
  
  try {
    console.log(`Testing IMAP connection for ${email}...`);
    await testClient.connect();
    
    // List mailboxes to verify connection works
    const mailboxes = await testClient.list();
    console.log(`IMAP connection test successful for ${email}, found ${mailboxes.length} mailboxes`);
    
    // Clean logout
    await testClient.logout();
    return true;
  } catch (error) {
    console.error(`IMAP connection test failed for ${email}:`, error);
    // Ensure client is properly closed on error
    try {
      if (testClient.authenticated || testClient.usable) {
        await testClient.logout();
      } else if (testClient._socket && testClient._socket.writable) {
        await testClient.close();
      }
    } catch (closeError) {
      console.warn(`Error closing IMAP test client for ${email}:`, closeError);
    }
    throw error;
  }
}

// Connection pool management
const connectionPools = new Map(); // Map of account email -> array of connections

// Get a connection from the pool or create a new one
async function getImapConnection(accountEmail) {
  // Initialize pool for this account if it doesn't exist
  if (!connectionPools.has(accountEmail)) {
    connectionPools.set(accountEmail, []);
  }
  
  const pool = connectionPools.get(accountEmail);
  
  // Check if we have an available connection in the pool
  for (let i = 0; i < pool.length; i++) {
    const conn = pool[i];
    if (conn.available) {
      try {
        // Test if connection is still usable with NOOP
        await conn.client.noop();
        console.log(`Reusing existing IMAP connection for ${accountEmail}`);
        
        // Mark as in use
        conn.available = false;
        conn.lastUsed = Date.now();
        
        return {
          client: conn.client,
          release: () => {
            // Release connection back to pool
            conn.available = true;
            conn.lastUsed = Date.now();
          }
        };
      } catch (error) {
        console.warn(`Connection in pool for ${accountEmail} is no longer usable:`, error.message);
        // Remove bad connection from pool and continue to next
        try {
          await conn.client.logout();
        } catch (err) {
          // Ignore errors during logout
        }
        pool.splice(i, 1);
        i--; // Adjust index after removal
      }
    }
  }
  
  // If we have capacity, create a new connection
  if (pool.length < CONNECTION_POOL_SIZE) {
    try {
      const client = await createImapClient(accountEmail);
      const connIndex = pool.length;
      
      // Add to pool as in-use
      const conn = { 
        client, 
        available: false,
        lastUsed: Date.now()
      };
      pool.push(conn);
      
      return {
        client,
        release: () => {
          // If the connection still exists in the pool
          if (connIndex < pool.length && pool[connIndex] === conn) {
            conn.available = true;
            conn.lastUsed = Date.now();
          }
        }
      };
    } catch (error) {
      console.error(`Failed to create new IMAP connection for ${accountEmail}:`, error.message);
      throw error;
    }
  } else {
    // If pool is at capacity, wait for a connection to become available
    console.log(`Connection pool at capacity for ${accountEmail}, waiting for available connection`);
    
    // Find the oldest in-use connection to reuse
    let oldestConn = null;
    let oldestTime = Infinity;
    
    for (const conn of pool) {
      if (conn.lastUsed < oldestTime) {
        oldestConn = conn;
        oldestTime = conn.lastUsed;
      }
    }
    
    if (oldestConn) {
      // Force release the oldest connection
      console.log(`Forcing reuse of oldest connection (${Date.now() - oldestTime}ms old) for ${accountEmail}`);
      oldestConn.available = false;
      oldestConn.lastUsed = Date.now();
      
      return {
        client: oldestConn.client,
        release: () => {
          oldestConn.available = true;
          oldestConn.lastUsed = Date.now();
        }
      };
    }
    
    throw new Error(`No IMAP connections available for ${accountEmail}`);
  }
}

// Create a new IMAP client
async function createImapClient(accountEmail) {
  const attempts = reconnectionAttempts.get(accountEmail) || 0;
  
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
    
    if (!account.app_password) {
      throw new Error(`No app password available for ${accountEmail}`);
    }
    
    console.log(`Creating new IMAP client for ${accountEmail}`);
    const appPassword = decrypt(account.app_password);
    
    // Create new IMAP client
    const client = new ImapFlow({
      host: 'imap.gmail.com',
      port: 993,
      secure: true,
      auth: {
        user: accountEmail,
        pass: appPassword
      },
      logger: false,
      emitLogs: false,
      disableAutoIdle: true,
      timeoutConnection: CONNECTION_TIMEOUT,
      timeoutIdle: IDLE_TIMEOUT,
      tls: {
        rejectUnauthorized: true,
        enableTrace: false,
        minVersion: 'TLSv1.2',
        maxVersion: 'TLSv1.3'
      }
    });
    
    // Connect to the server
    await client.connect();
    console.log(`IMAP client connected for ${accountEmail}`);
    
    // Reset reconnection counter on successful connection
    reconnectionAttempts.delete(accountEmail);
    
    // Setup event listeners for connection monitoring
    client.on('error', err => {
      console.error(`IMAP client error for ${accountEmail}:`, err);
    });
    
    client.on('close', () => {
      console.log(`IMAP connection closed for ${accountEmail}`);
    });
    
    client.on('end', () => {
      console.log(`IMAP connection ended for ${accountEmail}`);
    });
    
    return client;
  } catch (error) {
    // Increment reconnection attempts
    reconnectionAttempts.set(accountEmail, attempts + 1);
    
    console.error(`IMAP client creation error for ${accountEmail} (attempt ${attempts + 1}):`, error);
    
    // Update account status based on error type
    let statusUpdate = 'error';
    if (error.message?.includes('Invalid credentials') || 
        error.message?.includes('authentication failed') ||
        error.message?.includes('[AUTH]')) {
      statusUpdate = 'auth-error';
      
      try {
        const connection = await pool.getConnection();
        await connection.query(
          'UPDATE gmail_accounts SET status = ?, updated_at = NOW() WHERE email = ?',
          [statusUpdate, accountEmail]
        );
        connection.release();
      } catch (dbError) {
        console.error(`Error updating status for ${accountEmail}:`, dbError);
      }
    }
    
    throw error;
  }
}

// Clean up connection pools
function cleanupConnectionPools() {
  for (const [accountEmail, pool] of connectionPools.entries()) {
    // Find stale connections (unused for more than 5 minutes)
    const now = Date.now();
    const staleTimeout = 5 * 60 * 1000; // 5 minutes
    
    for (let i = pool.length - 1; i >= 0; i--) {
      const conn = pool[i];
      if (conn.available && (now - conn.lastUsed > staleTimeout)) {
        console.log(`Closing stale connection for ${accountEmail} (unused for ${Math.round((now - conn.lastUsed)/1000)}s)`);
        
        try {
          // Remove from pool first to prevent reuse
          pool.splice(i, 1);
          
          // Close connection
          conn.client.logout().catch(err => {
            console.warn(`Error closing stale connection for ${accountEmail}:`, err);
          });
        } catch (err) {
          console.warn(`Error during connection cleanup for ${accountEmail}:`, err);
        }
      }
    }
    
    // If pool is empty, remove it
    if (pool.length === 0) {
      connectionPools.delete(accountEmail);
    }
  }
}

// Run connection pool cleanup every 5 minutes
setInterval(cleanupConnectionPools, 5 * 60 * 1000);

// Alias Generation with improved reliability and account rotation
export async function generateGmailAlias(userId, strategy = 'dot', domain = 'gmail.com') {
  // Get next available account using load balancing from database
  try {
    const account = await getNextAvailableAccount();
    
    if (!account) {
      console.error('No Gmail accounts available. Active accounts:', [...activeImapAccounts]);
      throw new Error('No Gmail accounts available');
    }
    
    // Track how many users are assigned to each account (for rotation fairness)
    const accountUserCounts = new Map();
    
    for (const [alias, data] of aliasCache.entries()) {
      if (data.userId === userId && data.parentAccountId) {
        if (!accountUserCounts.has(data.parentAccountId)) {
          accountUserCounts.set(data.parentAccountId, 0);
        }
        accountUserCounts.set(data.parentAccountId, accountUserCounts.get(data.parentAccountId) + 1);
      }
    }
    
    // If user already has an alias with this account and there are other accounts,
    // try to get a different account for better distribution
    let selectedAccount = account;
    if (accountUserCounts.has(account.id) && accountUserCounts.get(account.id) > 0) {
      console.log(`User ${userId} already has ${accountUserCounts.get(account.id)} aliases with account ${account.email}, looking for another account`);
      
      try {
        // Get all available accounts
        const [accounts] = await pool.query(`
          SELECT * FROM gmail_accounts
          WHERE status = 'active'
          ORDER BY alias_count ASC, quota_used ASC, last_used ASC
        `);
        
        if (accounts.length > 1) {
          // Find an account this user doesn't have or has fewer aliases with
          for (const acc of accounts) {
            if (acc.id !== account.id && (!accountUserCounts.has(acc.id) || accountUserCounts.get(acc.id) < accountUserCounts.get(account.id))) {
              selectedAccount = acc;
              console.log(`Assigned account ${acc.email} to user ${userId} for better distribution`);
              break;
            }
          }
        }
      } catch (error) {
        console.warn('Error finding alternate account for user, using original selection:', error);
      }
    }
    
    console.log(`Assigned account ${selectedAccount.email} to user ${userId}, current user count: ${accountUserCounts.get(selectedAccount.id) || 0}`);
    
    // Generate unique alias based on strategy
    const alias = strategy === 'dot' 
      ? generateDotAlias(selectedAccount.email, domain)
      : generatePlusAlias(selectedAccount.email, domain);
    
    console.log(`Generated alias: ${alias} for user ${userId}`);
    
    // Update alias count in database
    const connection = await pool.getConnection();
    
    try {
      await connection.beginTransaction();
      
      // Update alias count in gmail_accounts
      await connection.query(
        'UPDATE gmail_accounts SET alias_count = alias_count + 1, last_used = NOW(), updated_at = NOW() WHERE id = ?',
        [selectedAccount.id]
      );
      
      await connection.commit();
      
      // Store in-memory alias cache
      aliasCache.set(alias, {
        parentAccount: selectedAccount.email,
        parentAccountId: selectedAccount.id,
        created: Date.now(),
        lastAccessed: Date.now(),
        userId: userId || null,
        expires: new Date(Date.now() + ALIAS_TTL)
      });
      
      // Also store in quick lookup map
      aliasToAccountMap.set(alias, selectedAccount.email);
      
      // Make sure this account is being polled
      if (!activeImapAccounts.has(selectedAccount.email)) {
        console.log(`Starting polling for account ${selectedAccount.email} after alias generation`);
        schedulePolling(selectedAccount.email);
        activeImapAccounts.add(selectedAccount.email);
      }
      
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

function generateDotAlias(email, domain = 'gmail.com') {
  // Extract username from email
  const username = email.split('@')[0];
  
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
  
  // Use the specified domain (gmail.com or googlemail.com)
  return `${dotUsername}@${domain}`;
}

function generatePlusAlias(email, domain = 'gmail.com') {
  // Extract username from email
  const username = email.split('@')[0];
  
  // Add random tag
  const tag = Math.random().toString(36).substring(2, 8);
  
  // Use the specified domain (gmail.com or googlemail.com)
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
      
      console.log(`Found alias ${aliasEmail} in cache, parent account: ${parentAccount}`);
    } else {
      console.log(`Alias ${aliasEmail} not found in cache, checking user permissions`);
      
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
        if (!activeImapAccounts.has(parentAccount)) {
          console.log(`Restarting polling for reactivated account ${parentAccount}`);
          schedulePolling(parentAccount);
          activeImapAccounts.add(parentAccount);
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
    
    // If we have very few emails in cache, trigger a fresh fetch
    if (cachedEmails.length < 3) {
      console.log(`Only ${cachedEmails.length} emails in cache for ${aliasEmail}, triggering background fetch`);
      
      // This happens in the background and won't block the response
      fetchEmailsForAlias(parentAccount, aliasEmail).catch(error => {
        console.error(`Background fetch failed for ${aliasEmail}:`, error);
      });
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

// Optimized batch fetch for emails specifically for an alias
async function fetchEmailsForAlias(accountEmail, alias) {
  let connection = null;
  
  try {
    // Get IMAP client from the connection pool
    connection = await getImapConnection(accountEmail);
    const client = connection.client;
    
    // Get the two-weeks-ago date for efficient searching
    const searchDate = new Date();
    searchDate.setDate(searchDate.getDate() - SEARCH_WINDOW);
    
    // List folders to search - prioritize INBOX for faster initial results
    const folders = ['INBOX', '[Gmail]/Spam'];
    let totalEmails = 0;
    let newEmails = [];
    
    for (const folder of folders) {
      try {
        console.log(`Searching ${folder} for emails to ${alias}...`);
        
        // Select the mailbox
        const mailbox = await client.mailboxOpen(folder);
        console.log(`Opened ${folder} for ${accountEmail}, message count: ${mailbox.exists}`);
        
        // If no messages, skip to next folder
        if (mailbox.exists === 0) {
          console.log(`No messages in ${folder} for ${accountEmail}`);
          continue;
        }
        
        // Search for emails specifically for this alias first
        // Use the since parameter to limit search to recent emails
        const specificSearch = await client.search({
          to: alias,
          since: searchDate
        });
        
        console.log(`Found ${specificSearch.length} new messages for ${alias} in ${folder} since ${searchDate.toISOString()}`);
        
        if (specificSearch.length === 0) {
          // Skip processing messages for this folder if none found
          continue;
        }
        
        // Process emails in reverse order (newest first) to prioritize recent emails
        const uids = specificSearch.slice().reverse();
        const batchSize = Math.min(10, uids.length); // Process 10 at a time
        const messagesToProcess = uids.slice(0, batchSize);
        
        // Fetch email details for matching messages
        for await (const message of client.fetch(messagesToProcess, { envelope: true, source: true, uid: true })) {
          try {
            // Parse the message
            const email = await parseImapMessage(message.source.toString(), alias);
            
            // Add to cache
            const cacheKey = `${alias}:${message.uid}`;
            
            // Only add if not already in cache
            if (!emailCache.has(cacheKey)) {
              addToEmailCache(cacheKey, email);
              totalEmails++;
              newEmails.push(email);
              
              // Notify connected clients about the new email in real-time
              notifyClients(alias, email);
              console.log(`Found and cached new email for ${alias}, UID: ${message.uid}`);
            }
          } catch (messageError) {
            console.error(`Error processing message ${message.uid}:`, messageError);
          }
        }
      } catch (folderError) {
        console.error(`Error processing folder ${folder} for ${accountEmail}:`, folderError);
        // Continue to next folder even if one fails
      }
    }
    
    console.log(`Found ${totalEmails} new emails for ${alias}`);
    
    if (totalEmails > 0) {
      // Update account usage metrics in database (batch update)
      queueDatabaseUpdate(aliasCache.get(alias).parentAccountId, 1);
    }
    
    return newEmails;
  } catch (error) {
    console.error(`Error fetching emails for alias ${alias}:`, error);
    throw error;
  } finally {
    // Always release the connection back to the pool
    if (connection) {
      connection.release();
    }
  }
}

// Parse an IMAP message using mailparser for better results
async function parseImapMessage(source, recipientAlias) {
  try {
    // Use mailparser to parse the email properly
    const parsed = await simpleParser(source);
    
    // Extract information
    const fromText = parsed.from?.text || '';
    const fromEmail = parsed.from?.value?.[0]?.address || fromText;
    const fromName = parsed.from?.value?.[0]?.name || parsed.from?.value?.[0]?.address?.split('@')[0] || 'Unknown Sender';
    const to = recipientAlias;
    const subject = parsed.subject || '(No Subject)';
    const bodyHtml = parsed.html || '';
    const bodyText = parsed.text || '';
    const date = parsed.date || new Date();
    
    // Get attachments info
    const attachments = parsed.attachments?.map(att => ({
      filename: att.filename || 'attachment',
      contentType: att.contentType || 'application/octet-stream',
      size: att.size || 0,
      // We don't store attachment content in memory, just metadata
      contentId: att.contentId || '',
      disposition: att.disposition || 'attachment'
    })) || [];
    
    // Generate a unique ID based on message ID and date
    const id = parsed.messageId || `${date.getTime()}-${Math.random().toString(36).substring(2, 10)}`;
    
    return {
      id,
      threadId: parsed.messageId || id, 
      from: fromText,
      fromEmail: fromEmail,
      fromName: fromName,
      to,
      subject,
      bodyHtml,
      bodyText,
      internalDate: date.toISOString(),
      timestamp: Date.now(),
      snippet: bodyText.substring(0, 150).replace(/\s+/g, ' ').trim(),
      recipientAlias,
      attachments
    };
  } catch (error) {
    console.error('Error parsing email message:', error);
    // Fall back to a simple parsing approach if mailparser fails
    return {
      id: `fallback-${Date.now()}-${Math.random().toString(36).substring(2, 10)}`,
      threadId: `fallback-thread-${Date.now()}`,
      from: 'error@parsing.email',
      fromEmail: 'error@parsing.email',
      fromName: 'Error Parsing Email',
      to: recipientAlias,
      subject: 'Error Parsing Email',
      bodyHtml: '',
      bodyText: 'There was an error parsing this email. Please check your inbox directly.',
      internalDate: new Date().toISOString(),
      timestamp: Date.now(),
      snippet: 'Error parsing email',
      recipientAlias,
      attachments: []
    };
  }
}

// Get only the most recent alias for a user
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

// Rotate to a new alias for user
export async function rotateUserAlias(userId, strategy = 'dot', domain = 'gmail.com') {
  try {
    // Generate a new alias for the user (will use load balancing)
    return await generateGmailAlias(userId, strategy, domain);
  } catch (error) {
    console.error('Failed to rotate Gmail alias:', error);
    throw error;
  }
}

// Email Polling (Background Task) with improved error handling and frequency
async function pollForNewEmails(accountEmail) {
  let connection = null;
  
  try {
    // Skip if account is no longer active in our memory
    if (!activeImapAccounts.has(accountEmail)) {
      console.log(`Skipping polling for inactive account ${accountEmail}`);
      return;
    }
    
    // Get account status from database first
    const [accounts] = await pool.query(
      'SELECT * FROM gmail_accounts WHERE email = ?',
      [accountEmail]
    );
    
    if (accounts.length === 0) {
      console.log(`Account ${accountEmail} not found, skipping polling`);
      activeImapAccounts.delete(accountEmail);
      return;
    }
    
    const account = accounts[0];
    
    if (account.status !== 'active') {
      console.log(`Skipping polling for inactive account ${accountEmail} (status: ${account.status})`);
      activeImapAccounts.delete(accountEmail);
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
    
    // Get IMAP client from connection pool
    connection = await getImapConnection(accountEmail);
    const client = connection.client;
    
    // Get the two-weeks-ago date for efficient searching
    const twoWeeksAgo = new Date();
    twoWeeksAgo.setDate(twoWeeksAgo.getDate() - SEARCH_WINDOW);
    
    // Check both INBOX and Spam folders
    const folders = ['INBOX', '[Gmail]/Spam'];
    let totalNewEmails = 0;
    
    for (const folder of folders) {
      try {
        // Select the folder
        const mailbox = await client.mailboxOpen(folder);
        console.log(`Opened ${folder} for ${accountEmail}, message count: ${mailbox.exists}`);
        
        if (mailbox.exists === 0) {
          console.log(`No messages in ${folder} for ${accountEmail}`);
          continue;
        }
        
        // For each alias, search for new messages
        for (const alias of accountAliases) {
          try {
            const searchOptions = {
              to: alias, 
              since: twoWeeksAgo
            };
            
            const search = await client.search(searchOptions);
            
            if (search.length === 0) {
              console.log(`Found 0 new emails for ${alias}`);
              continue;
            }
            
            console.log(`Found ${search.length} new messages for ${alias} in ${folder} since ${twoWeeksAgo.toISOString()}`);
            
            // Process emails in reverse order (newest first) to prioritize recent emails
            const uids = search.slice().reverse();
            const batchSize = Math.min(10, uids.length); // Process 10 at a time initially
            const messagesToProcess = uids.slice(0, batchSize);
            
            // Fetch messages for this alias
            for await (const message of client.fetch(messagesToProcess, { envelope: true, source: true, uid: true })) {
              try {
                // Parse the message
                const email = await parseImapMessage(message.source.toString(), alias);
                
                // Add to cache
                const cacheKey = `${alias}:${message.uid}`;
                
                // Only add if not already in cache
                if (!emailCache.has(cacheKey)) {
                  addToEmailCache(cacheKey, email);
                  totalNewEmails++;
                  
                  // Notify connected clients about the new email in real-time
                  notifyClients(alias, email);
                }
              } catch (messageError) {
                console.error(`Error processing message ${message.uid}:`, messageError);
              }
            }
          } catch (aliasError) {
            console.error(`Error searching for emails for alias ${alias}:`, aliasError);
            // Continue to next alias
          }
        }
      } catch (folderError) {
        console.error(`Error processing folder ${folder} for ${accountEmail}:`, folderError);
        // Continue to next folder even if one fails
      }
    }
    
    if (totalNewEmails > 0) {
      console.log(`Found ${totalNewEmails} new emails total for account ${accountEmail}`);
      
      // Update account metrics in database (batch update)
      queueDatabaseUpdate(account.id, 1);
    }
  } catch (error) {
    console.error(`Error polling Gmail account ${accountEmail}:`, error);
    
    // Update account status in database with more detailed status
    let statusUpdate = 'error';
    if (error.message?.includes('Invalid credentials') || 
        error.message?.includes('authentication failed') ||
        error.message?.includes('[AUTH]')) {
      statusUpdate = 'auth-error';
      console.log(`Account ${accountEmail} has invalid credentials - marked as auth-error`);
      
      try {
        await pool.query(
          'UPDATE gmail_accounts SET status = ?, updated_at = NOW() WHERE email = ?',
          [statusUpdate, accountEmail]
        );
      } catch (dbError) {
        console.error(`Error updating status for ${accountEmail}:`, dbError);
      }
      
      // Remove from active polling immediately
      activeImapAccounts.delete(accountEmail);
      
    } else {
      // Handle other errors with exponential backoff
      handlePollingError(accountEmail, error);
    }
  } finally {
    // Always release the connection back to the pool
    if (connection) {
      try {
        connection.release();
      } catch (releaseError) {
        console.warn(`Error releasing connection for ${accountEmail}:`, releaseError);
      }
    }
  }
}

// Error handling with exponential backoff
function handlePollingError(accountEmail, error) {
  // Get current attempt count or start at 0
  const attempts = reconnectionAttempts.get(accountEmail) || 0;
  
  // Increment attempt counter
  reconnectionAttempts.set(accountEmail, attempts + 1);
  
  // Calculate delay with exponential backoff and small jitter
  // Base delay * 2^attempts + random jitter of up to 20%
  const baseDelay = Math.min(
    BASE_RECONNECTION_DELAY * Math.pow(2, attempts),
    MAX_RECONNECTION_DELAY
  );
  const jitter = Math.random() * 0.2 * baseDelay;
  const delay = Math.round(baseDelay + jitter);
  
  console.log(`Scheduling retry #${attempts + 1} for ${accountEmail} in ${Math.round(delay / 1000)} seconds`);
  
  // If we've reached max attempts, mark as error but keep trying at a much slower rate
  if (attempts >= MAX_RECONNECTION_ATTEMPTS) {
    console.log(`Max retry attempts (${MAX_RECONNECTION_ATTEMPTS}) reached for ${accountEmail}, updating status`);
    
    try {
      pool.query(
        'UPDATE gmail_accounts SET status = ?, updated_at = NOW() WHERE email = ?',
        ['error', accountEmail]
      ).catch(err => console.error(`Error updating status for ${accountEmail}:`, err));
    } catch (dbError) {
      console.error(`Error updating status for ${accountEmail}:`, dbError);
    }
  }
}

// Schedule polling for an account
function schedulePolling(accountEmail) {
  console.log(`Setting up polling schedule for ${accountEmail}`);
  
  // Self-executing async function to check account and set up polling
  (async () => {
    try {
      // Get latest account state from database
      const [accounts] = await pool.query(
        'SELECT * FROM gmail_accounts WHERE email = ?',
        [accountEmail]
      );
      
      if (accounts.length === 0) {
        console.log(`Not scheduling polling for missing account: ${accountEmail}`);
        activeImapAccounts.delete(accountEmail);
        return;
      }
      
      const accountStatus = accounts[0].status;
      if (accountStatus !== 'active') {
        console.log(`Not scheduling polling for inactive account (${accountStatus}): ${accountEmail}`);
        activeImapAccounts.delete(accountEmail);
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
      
      // Add to active accounts set
      activeImapAccounts.add(accountEmail);
      
      // Start immediate polling
      pollForNewEmails(accountEmail).catch(error => {
        console.error(`Initial poll for ${accountEmail} failed:`, error);
      });
      
      // Schedule next poll
      setTimeout(() => {
        // Only schedule next poll if account is still in active set
        if (activeImapAccounts.has(accountEmail)) {
          schedulePolling(accountEmail);
        } else {
          console.log(`Stopped polling for ${accountEmail} as it's no longer in active set`);
        }
      }, interval);
      
    } catch (error) {
      console.error(`Error setting up polling for ${accountEmail}:`, error);
    }
  })();
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
  
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    
    for (const [alias, data] of aliasCache.entries()) {
      if (now - data.lastAccessed > ALIAS_TTL) {
        // Also keep track of account aliases to update count in DB
        if (data.parentAccountId) {
          try {
            // Decrement alias count in the database
            await connection.query(
              'UPDATE gmail_accounts SET alias_count = GREATEST(0, alias_count - 1), updated_at = NOW() WHERE id = ?',
              [data.parentAccountId]
            );
          } catch (error) {
            console.error(`Error updating alias count for account ${data.parentAccountId}:`, error);
          }
        }
        
        aliasCache.delete(alias);
        aliasToAccountMap.delete(alias);
        inMemoryCleanupCount++;
      }
    }
    
    await connection.commit();
    
    if (inMemoryCleanupCount > 0) {
      console.log(`Cleaned up ${inMemoryCleanupCount} inactive aliases from memory cache`);
    }
    
    // Also clean up email cache if it's getting too big
    if (emailCache.size > MAX_CACHE_SIZE * 0.9) {
      const oldestKeys = [...emailCache.keys()]
        .map(k => ({ key: k, timestamp: emailCache.get(k).timestamp }))
        .sort((a, b) => a.timestamp - b.timestamp)
        .slice(0, Math.ceil(MAX_CACHE_SIZE * 0.3)) // Remove oldest 30% when we're getting full
        .map(item => item.key);
      
      oldestKeys.forEach(key => emailCache.delete(key));
      console.log(`Cleaned up ${oldestKeys.length} older emails from cache`);
    }
    
  } catch (error) {
    await connection.rollback();
    console.error('Error during alias cleanup:', error);
  } finally {
    connection.release();
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
      if (!activeImapAccounts.has(account.email)) {
        console.log(`Restarting polling for active account: ${account.email}`);
        schedulePolling(account.email);
        activeImapAccounts.add(account.email);
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
        if (!activeImapAccounts.has(account.email)) {
          console.log(`Restarting polling for auto-recovered account: ${account.email}`);
          schedulePolling(account.email);
          activeImapAccounts.add(account.email);
        }
      }
    }
  } catch (error) {
    console.error('Error in auto-recovery process:', error);
  }
}, 15 * 60 * 1000); // Run every 15 minutes

// Load Balancing with improved account selection and rotation
async function getNextAvailableAccount() {
  try {
    // Get available accounts with balancing strategy
    const [accounts] = await pool.query(`
      SELECT * 
      FROM gmail_accounts a
      WHERE a.status = 'active' 
      ORDER BY 
        a.alias_count ASC,
        a.quota_used ASC,
        a.last_used ASC
      LIMIT 15  -- Get top 15 accounts so we can rotate through them
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

    // Instead of always selecting the first account, select a random account from top 5
    // to provide better load distribution
    const randomIndex = accounts.length <= 5 ? 
      Math.floor(Math.random() * accounts.length) : 
      Math.floor(Math.random() * 5); // Only choose from top 5
    
    const selectedAccount = accounts[randomIndex];
    console.log(`Selected account for new alias: ${selectedAccount.email} (aliases: ${selectedAccount.alias_count}, quota: ${selectedAccount.quota_used})`);
    
    return selectedAccount;
  } catch (error) {
    console.error('Error selecting next available account:', error);
    return null;
  }
}

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

// Initialize the service by loading accounts from the database
export async function initializeImapService() {
  try {
    console.log('Initializing IMAP service...');
    
    // Ensure we start the DB flush interval
    startDbFlushInterval();
    
    // Get all active accounts from the database
    const [accounts] = await pool.query(`
      SELECT * FROM gmail_accounts WHERE status = 'active'
    `);
    
    console.log(`Found ${accounts.length} active Gmail accounts`);
    
    // Start polling for each active account
    for (const account of accounts) {
      if (!activeImapAccounts.has(account.email)) {
        console.log(`Starting polling for account: ${account.email}`);
        schedulePolling(account.email);
        activeImapAccounts.add(account.email);
      }
    }
    
    // Set up an interval to check for account health
    setInterval(monitorAccountHealth, 5 * 60 * 1000); // Every 5 minutes
    
    console.log('IMAP service initialized successfully');
    return true;
  } catch (error) {
    console.error('Failed to initialize IMAP service:', error);
    return false;
  }
}

// Monitor account health and clean up resources
async function monitorAccountHealth() {
  console.log('Running account health check...');
  
  try {
    // Check for any stale connections and clean them up
    for (const [accountEmail, pool] of connectionPools.entries()) {
      // Count active vs. available connections
      const active = pool.filter(conn => !conn.available).length;
      const available = pool.filter(conn => conn.available).length;
      
      console.log(`Account ${accountEmail}: ${active} active, ${available} available connections`);
      
      // Check if account is still active in our system
      if (!activeImapAccounts.has(accountEmail)) {
        console.log(`Account ${accountEmail} is no longer active, cleaning up connections`);
        
        // Close all connections
        for (const conn of pool) {
          try {
            await conn.client.logout();
          } catch (err) {
            // Ignore errors during logout
          }
        }
        
        // Remove from connection pools
        connectionPools.delete(accountEmail);
      }
    }
    
    // Force a DB flush if there are pending updates
    if (pendingDatabaseUpdates.size > 0) {
      await flushPendingDatabaseUpdates();
    }
  } catch (error) {
    console.error('Error during account health check:', error);
  }
}
