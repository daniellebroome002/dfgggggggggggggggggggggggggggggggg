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
const MAX_CACHE_SIZE = 100; // Maximum number of emails to cache
const ALIAS_TTL = 1 * 24 * 60 * 60 * 1000; // 1 day in milliseconds for in-memory cache
const MAX_RECONNECTION_ATTEMPTS = 5; // Maximum number of reconnection attempts
const BASE_RECONNECTION_DELAY = 2000; // Base delay for reconnection (2 seconds)
const MAX_RECONNECTION_DELAY = 10 * 60 * 1000; // Maximum delay (10 minutes)
const POLLING_INTERVALS = {
  high: 5000,      // 5 seconds for high priority accounts (reduced from 10s)
  medium: 10000,   // 10 seconds for medium priority (reduced from 20s)
  low: 15000       // 15 seconds for low priority accounts (reduced from 30s)
};
const CONNECTION_TIMEOUT = 15000; // 15 second timeout for IMAP connections (reduced from 30s)
const IDLE_TIMEOUT = 300000; // 5 minutes idle timeout (Gmail drops at ~10 min)
const MAX_EMAILS_PER_FETCH = 50; // Maximum number of emails to fetch at once
const DB_FLUSH_INTERVAL = 120000; // Flush DB updates every 2 minutes
const CONNECTION_POOL_SIZE = 10; // Maximum concurrent connections per account (increased from 5)
const SEARCH_WINDOW = 14; // Number of days to search for emails

// Global polling settings - NEW SYNCHRONIZED APPROACH
const SYNC_POLLING = {
  enabled: true,                    // Master switch for synchronized polling
  pollingInterval: 5000,            // Poll every 5 seconds (super aggressive)
  maxConcurrentFetches: 5,          // REDUCED from 20 to 5 to avoid "Too many connections" errors
  fetchTimeout: 10000,              // Timeout each fetch after 10 seconds
  checkAllFolders: true,            // Check both inbox and spam folders
  retryFailedFetches: true,         // Retry failed fetches immediately
  fetchAllAliasesAtOnce: true,      // Fetch for all aliases of an account at once
  usePromiseAllSettled: true,       // Use Promise.allSettled to continue even if some fail
  inProgress: false,                // Flag to prevent overlapping polling cycles
  lastRunTime: 0                    // Track when polling was last run
};

// New optimized IDLE settings for MUCH faster delivery
const IDLE_SETTINGS = {
  enabled: false,                     // Disable IDLE mode - using synchronized polling instead
  maxIdleAccounts: 30,                // Increased from 10 to accommodate all accounts
  idleRestartInterval: 3 * 60 * 1000, // Restart IDLE every 3 minutes (even more aggressive)
  idleTimeout: 10 * 60 * 1000,        // Gmail typically times out after ~29 minutes
  useAggressiveIdle: true,            // New setting to use more aggressive IDLE
  reconnectDelay: 500                 // Even faster reconnect on errors (500ms)
};

// Track IDLE accounts and connections
const idleAccounts = new Set(); // Accounts with active IDLE connections
const idleTimers = new Map();   // Timers for IDLE expiration

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

// Utility function to notify all connected clients for an alias
function notifyClients(alias, email) {
  // Skip if the alias is no longer active
  if (!isAliasActive(alias)) {
    return;
  }
  
  // Prepare notification payload once
  const notification = {
    type: 'new_email',
    email,
    alias,
    timestamp: new Date().toISOString()
  };
  
  const payload = JSON.stringify(notification);
  let notifiedCount = 0;
  
  // ULTRA-FAST: Direct delivery with no extra checks
  const clients = [];
  
  // Direct lookup by alias only - faster
  for (const [clientKey, clientSet] of connectedClients.entries()) {
    if (clientKey.includes(`:${alias}`)) {
      clientSet.forEach(client => clients.push(client));
    }
  }
  
  // Fast batch delivery to all clients
  if (clients.length > 0) {
    // Use Promise.all for concurrent delivery
    Promise.all(
      clients
        .filter(client => client.readyState === 1) // Only OPEN connections
        .map(client => {
          return new Promise(resolve => {
            try {
              // SPEED: Use highest priority and compression
              client.send(payload, { 
                binary: false, 
                compress: true, 
                priority: true,
                fin: true // Ensure packet is sent immediately
              }, () => resolve());
              notifiedCount++;
            } catch (err) {
              // Silent error handling
              resolve();
            }
          });
        })
    ).catch(() => {}); // Ignore errors in notification
  }
}

// Setup WebSocket Server with reduced logging
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
      // Reduced log message
      ws.close();
      return;
    }
    
    const clientKey = `${userId}:${alias}`;
    // Only log when debugging is needed
    // console.log(`WebSocket client connected: ${clientKey}`);
    
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
    
    // Trigger immediate fetch to get latest emails right away
    if (aliasToAccountMap.has(alias)) {
      const accountEmail = aliasToAccountMap.get(alias);
      
      // First, check if we have a direct IDLE connection for this account
      if (idleAccounts.has(accountEmail)) {
        // Reduced logging
        sendCachedEmailsToClient(clientKey, alias, ws);
      } else {
        // Trigger a specific fetch for this alias (in background)
        fetchEmailsForAlias(accountEmail, alias)
          .catch(err => console.error(`Error in immediate fetch for ${alias}:`, err));
      }
    }
    
    // Handle client disconnect
    ws.on('close', () => {
      // Reduced logging - no need to log every disconnect
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
        
        // Handle explicit request to refresh emails
        if (data.type === 'refresh_emails') {
          if (aliasToAccountMap.has(alias)) {
            const accountEmail = aliasToAccountMap.get(alias);
            
            // Send any cached emails immediately
            sendCachedEmailsToClient(clientKey, alias, ws);
            
            // Also trigger a fresh fetch
            fetchEmailsForAlias(accountEmail, alias)
              .catch(err => console.error(`Error in refresh fetch for ${alias}:`, err));
          }
        }
      } catch (err) {
        // Only log if it's not a connection terminated error
        if (!err.message.includes('connection') && !err.message.includes('closed')) {
          console.error('Invalid WebSocket message:', err);
        }
      }
    });
    
    // Handle errors
    ws.on('error', (err) => {
      // Only log serious errors, not disconnection errors
      if (!err.message.includes('connection') && !err.message.includes('closed')) {
        console.error(`WebSocket error for client ${clientKey}:`, err);
      }
      
      try {
        ws.close();
      } catch (closeErr) {
        // Don't log close errors
      }
    });
  });
  
  // Handle server errors
  wss.on('error', (err) => {
    console.error('WebSocket server error:', err);
  });
  
  // Log websocket stats less frequently - changed from 5 minutes to 30 minutes
  setInterval(() => {
    console.log(`WebSocket stats: ${connectedClients.size} unique aliases connected`);
    let totalConnections = 0;
    for (const clients of connectedClients.values()) {
      totalConnections += clients.size;
    }
    console.log(`Total WebSocket connections: ${totalConnections}`);
  }, 30 * 60 * 1000);
  
  return wss;
}

// Send cached emails to a specific client
function sendCachedEmailsToClient(clientKey, alias, ws) {
  // Check if alias is still active
  if (!isAliasActive(alias)) {
    return;
  }
  
  // Get cached emails for this alias
  const cachedEmails = [];
  for (const [key, email] of emailCache.entries()) {
    if (key.startsWith(`${alias}:`)) {
      cachedEmails.push(email);
    }
  }
  
  // Sort by date (newest first)
  const sortedEmails = cachedEmails.sort((a, b) => 
    new Date(b.internalDate) - new Date(a.internalDate)
  );
  
  // Only send if we have emails and the connection is open
  if (sortedEmails.length > 0 && ws.readyState === 1) {
    // Send a batch notification with all emails
    ws.send(JSON.stringify({
      type: 'cached_emails',
      emails: sortedEmails.slice(0, 20), // Limit to 20 most recent
      count: sortedEmails.length,
      alias,
      timestamp: new Date().toISOString()
    }));
  }
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
      
      // IMPROVED: Immediately set up IDLE for the new account
      if (!activeImapAccounts.has(email)) {
        console.log(`⚡⚡ ULTRA-FAST: Setting up IMMEDIATE IDLE connection for new account: ${email}`);
        
        // Add to active accounts set
        activeImapAccounts.add(email);
        
        // Get account data with fresh copy
        const accountData = {
          id,
          email,
          app_password: encryptedPassword,
          status: 'active'
        };
        
        // Import the connection pool to set priority
        const imapConnectionPool = (await import('./imapConnectionPool.js')).default;
        imapConnectionPool.setAccountPriority(email, 'high');
        
        // Try to set up IDLE mode immediately
        setupIdleMode(email, accountData)
          .then(success => {
            if (!success) {
              // Fall back to polling only if IDLE fails
              startTraditionalPolling(email, 'high');
            }
          })
          .catch(error => {
            console.error(`Error setting up IDLE for new account ${email}:`, error.message);
            // Fall back to polling
            startTraditionalPolling(email, 'high');
          });
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
      
      // Try to cleanup this client
      try {
        if (connectionPools.has(accountEmail)) {
          const pool = connectionPools.get(accountEmail);
          const index = pool.findIndex(conn => conn.client === client);
          if (index >= 0) {
            console.log(`Removing errored client from pool for ${accountEmail}`);
            pool.splice(index, 1);
          }
        }
      } catch (cleanupErr) {
        // Ignore cleanup errors
      }
    });
    
    client.on('close', () => {
      console.log(`IMAP connection closed for ${accountEmail}`);
      
      // Cleanup from connection pool if closed
      try {
        if (connectionPools.has(accountEmail)) {
          const pool = connectionPools.get(accountEmail);
          const index = pool.findIndex(conn => conn.client === client);
          if (index >= 0) {
            console.log(`Removing closed client from pool for ${accountEmail}`);
            pool.splice(index, 1);
          }
        }
      } catch (cleanupErr) {
        // Ignore cleanup errors
      }
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
      // Check for stale connections OR problematic connections
      if ((conn.available && (now - conn.lastUsed > staleTimeout)) || 
          (conn.client && (conn.client._destroyed || !conn.client.usable))) {
        console.log(`Closing problematic or stale connection for ${accountEmail}`);
        
        try {
          // Remove from pool first to prevent reuse
          pool.splice(i, 1);
          
          // Close connection with better error handling
          if (conn.client && typeof conn.client.logout === 'function') {
            conn.client.logout().catch(err => {
              // Just log the error but don't escalate it
              if (err.code !== 'NoConnection') {
                console.warn(`Error closing connection for ${accountEmail}:`, err.message);
              }
            });
          }
        } catch (err) {
          console.warn(`Error during connection cleanup for ${accountEmail}:`, err.message);
        }
      }
    }
    
    // If pool is empty, remove it
    if (pool.length === 0) {
      connectionPools.delete(accountEmail);
    }
  }
}

// Run connection pool cleanup more frequently - every 2 minutes
setInterval(cleanupConnectionPools, 2 * 60 * 1000);

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
    // Skip if the alias is no longer in the cache
    if (!aliasCache.has(alias)) {
      return [];
    }
    
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
        // Reduced logging
        
        // Select the mailbox
        const mailbox = await client.mailboxOpen(folder);
        
        // If no messages, skip to next folder
        if (mailbox.exists === 0) {
          continue;
        }
        
        // Search for emails specifically for this alias first
        // Use the since parameter to limit search to recent emails
        const specificSearch = await client.search({
          to: alias,
          since: searchDate
        });
        
        if (specificSearch.length > 0) {
          console.log(`Found ${specificSearch.length} new messages for ${alias} in ${folder}`);
        }
        
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
            // Skip if the alias has been removed during processing
            if (!aliasCache.has(alias)) {
              break;
            }
            
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
              // Reduced logging
            }
          } catch (messageError) {
            console.error(`Error processing message: ${messageError.message}`);
          }
        }
      } catch (folderError) {
        console.error(`Error processing folder ${folder}:`, folderError.message);
        // Continue to next folder even if one fails
      }
    }
    
    if (totalEmails > 0) {
      // Update account usage metrics in database (batch update)
      queueDatabaseUpdate(aliasCache.get(alias).parentAccountId, totalEmails);
    }
    
    return newEmails;
  } catch (error) {
    console.error(`Error fetching emails for alias ${alias}:`, error.message);
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
    // Get the user's current alias before generating a new one
    const oldAliases = await getUserAliases(userId);
    
    // Generate a new alias for the user (will use load balancing)
    const result = await generateGmailAlias(userId, strategy, domain);
    
    // Immediately cleanup old aliases - this is the key improvement
    if (oldAliases && oldAliases.length > 0) {
      for (const oldAlias of oldAliases) {
        // Skip if it's the same as the new alias (shouldn't happen)
        if (oldAlias === result.alias) continue;
        
        // Get parent account ID before removing from cache
        let parentAccountId = null;
        if (aliasCache.has(oldAlias)) {
          parentAccountId = aliasCache.get(oldAlias).parentAccountId;
        }
        
        // Remove old alias from caches immediately
        aliasCache.delete(oldAlias);
        aliasToAccountMap.delete(oldAlias);
        
        // Update alias count in database if we have parent account info
        if (parentAccountId) {
          try {
            await pool.query(
              'UPDATE gmail_accounts SET alias_count = GREATEST(0, alias_count - 1), updated_at = NOW() WHERE id = ?',
              [parentAccountId]
            );
          } catch (error) {
            // Don't log this error to reduce noise
          }
        }
      }
    }
    
    return result;
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
        console.error(`Error processing folder ${folder} for ${accountEmail}:`, folderError.message);
        // Continue to next folder even if one fails
      }
    }
    
    if (totalNewEmails > 0) {
      console.log(`Found ${totalNewEmails} new emails total for account ${accountEmail}`);
      
      // Update account metrics in database (batch update)
      queueDatabaseUpdate(account.id, totalNewEmails);
    }
  } catch (error) {
    console.error(`Error polling Gmail account ${accountEmail}:`, error.message);
    
    // Update account status in database with more detailed status
    let statusUpdate = 'error';
    if (error.message?.includes('Invalid credentials') || 
        error.message?.includes('authentication failed') ||
        error.message?.includes('[AUTH]')) {
      statusUpdate = 'auth-error';
      
      try {
        await pool.query(
          'UPDATE gmail_accounts SET status = ?, updated_at = NOW() WHERE email = ?',
          [statusUpdate, accountEmail]
        );
      } catch (dbError) {
        console.error(`Error updating status for ${accountEmail}:`, dbError.message);
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
        // Ignore NoConnection errors during release
        if (releaseError.code !== 'NoConnection') {
          console.warn(`Error releasing connection for ${accountEmail}:`, releaseError.message);
        }
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
  (async () => {
    try {
      // Get account data
      const [accounts] = await pool.query(
        'SELECT * FROM gmail_accounts WHERE email = ?',
        [accountEmail]
      );
      
      if (accounts.length === 0 || accounts[0].status !== 'active') {
        activeImapAccounts.delete(accountEmail);
        return;
      }
      
      // Add to active accounts
      activeImapAccounts.add(accountEmail);
      
      // ALWAYS try IDLE first - it's much faster than polling
      // Force IDLE mode for all accounts to maximize email delivery speed
      setupIdleMode(accountEmail, accounts[0])
        .then(success => {
          if (!success) {
            // Only if IDLE fails, use traditional polling
            startTraditionalPolling(accountEmail, 'high');
          }
        })
        .catch(error => {
          console.error(`IDLE setup error: ${error.message}`);
          startTraditionalPolling(accountEmail, 'high');
        });
    } catch (error) {
      console.error(`Polling schedule error: ${error.message}`);
    }
  })();
}

// Start traditional polling interval
function startTraditionalPolling(accountEmail, priority = 'medium') {
  console.log(`Using traditional polling for ${accountEmail} with priority ${priority}`);
  
  // Get polling interval based on priority
  const interval = POLLING_INTERVALS[priority] || POLLING_INTERVALS.medium;
  
  console.log(`Using polling interval of ${interval}ms for ${accountEmail}`);
  
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
}

// Setup IDLE mode for an account for real-time updates
async function setupIdleMode(accountEmail, accountData) {
  if (idleAccounts.has(accountEmail)) {
    return true;
  }
  
  try {
    // Import connection pool
    const imapConnectionPool = (await import('./imapConnectionPool.js')).default;
    
    // Get app password
    const appPassword = decrypt(accountData.app_password);
    
    // Set to highest priority and get dedicated connection
    imapConnectionPool.setAccountPriority(accountEmail, 'high'); // Always use high priority
    const client = await imapConnectionPool.getConnection(accountEmail, appPassword, true);
    
    // Select INBOX with readOnly to reduce overhead
    await client.mailboxOpen('INBOX', {readOnly: true});
    
    // Set up aggressive IDLE restart timer - shorter interval for faster recovery
    const idleTimer = setTimeout(() => {
      restartIdleMode(accountEmail, accountData);
    }, IDLE_SETTINGS.idleRestartInterval);
    
    idleTimers.set(accountEmail, idleTimer);
    
    // Set up enhanced IDLE notification handler
    client.on('exists', async (data) => {
      // Process new emails IMMEDIATELY
      console.log(`⚡ IDLE notification received from Gmail - processing instantly`);
      processNewIdleEmails(accountEmail, client, data.count).catch(err => {
        console.error(`IDLE notification processing error: ${err.message}`);
      });
    });
    
    // Better error recovery
    client.on('close', () => {
      cleanupIdleMode(accountEmail);
      
      // IMMEDIATE restart on close for fastest possible recovery
      setTimeout(() => {
        restartIdleMode(accountEmail, accountData);
      }, 100); // Reduced from 500ms to 100ms for even faster recovery
    });
    
    client.on('error', (err) => {
      cleanupIdleMode(accountEmail);
      
      // Fast error recovery - we want emails ASAP
      setTimeout(() => {
        restartIdleMode(accountEmail, accountData);
      }, IDLE_SETTINGS.reconnectDelay); 
    });
    
    // Add to active IDLE accounts
    idleAccounts.add(accountEmail);
    imapClients.set(accountEmail, client);
    
    // Immediate process of any existing emails
    processNewIdleEmails(accountEmail, client, 10).catch(err => {
      console.error(`Initial IDLE email error: ${err.message}`);
    });
    
    return true;
    } catch (error) {
    cleanupIdleMode(accountEmail);
    console.error(`IDLE setup error: ${error.message}`);
    
    // Fast retry on failure - don't give up
    setTimeout(() => {
      console.log(`Retrying IDLE setup after failure`);
      setupIdleMode(accountEmail, accountData).catch(() => {});
    }, 1000); // Reduced from 2000ms to 1000ms
    
    return false;
  }
}

// Restart IDLE mode for an account
async function restartIdleMode(accountEmail, accountData) {
  console.log(`Restarting IDLE mode for ${accountEmail}`);
  
  // Clean up existing IDLE resources
  cleanupIdleMode(accountEmail);
  
  // Wait a short time before restarting
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  // Setup IDLE again
  return setupIdleMode(accountEmail, accountData);
}

// Clean up IDLE resources
function cleanupIdleMode(accountEmail) {
  console.log(`Cleaning up IDLE resources for ${accountEmail}`);
  
  // Clear expiration timer
  if (idleTimers.has(accountEmail)) {
    clearTimeout(idleTimers.get(accountEmail));
    idleTimers.delete(accountEmail);
  }
  
  // Remove from active IDLE accounts
  idleAccounts.delete(accountEmail);
}

// Much faster email processing - optimized for speed
async function processNewIdleEmails(accountEmail, client, count) {
  try {
    // Get all aliases associated with this account
    const accountAliases = [];
    for (const [alias, data] of aliasCache.entries()) {
      if (data.parentAccount === accountEmail) {
        accountAliases.push(alias);
      }
    }
    
    if (accountAliases.length === 0) return;
    
    // Get latest message count
    const mailbox = client.mailbox;
    if (!mailbox || mailbox.exists === 0) return;
    
    // ULTRA-FAST: Always fetch more messages for better reliability
    const fetchCount = Math.max(count || 10, 15); // Increased from 5/10 to 10/15
    
    // Calculate range (newest emails)
    const from = Math.max(mailbox.exists - fetchCount + 1, 1);
    const to = mailbox.exists;
    
    console.log(`⚡⚡ ULTRA-FAST: Fetching newest ${fetchCount} messages (${from}-${to}) via IDLE notification`);
    
    // CRITICAL CHANGE: Fetch ALL newest messages first, then process them
    // This gets the messages from Gmail as fast as possible
    const messages = [];
    
    try {
      // PERFORMANCE: Fetch messages in parallel with a Promise-based approach
      const fetchPromise = new Promise((resolve, reject) => {
        const fetchedMessages = [];
        
        const fetchStream = client.fetch(`${from}:${to}`, 
          { envelope: true, source: true, uid: true, flags: true });
        
        fetchStream.on('error', (err) => reject(err));
        
        fetchStream.on('end', () => resolve(fetchedMessages));
        
        fetchStream.on('message', (message) => {
          fetchedMessages.push(message);
        });
      });
      
      // Set a timeout to ensure we don't wait too long (3 seconds max)
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('Fetch timeout')), 3000);
      });
      
      // Use Promise.race to either get messages or timeout
      const result = await Promise.race([fetchPromise, timeoutPromise]);
      messages.push(...result);
      
    } catch (fetchError) {
      console.error(`Error fetching messages: ${fetchError.message}`);
      return;
    }
    
    if (messages.length === 0) return;
    console.log(`⚡⚡ ULTRA-FAST: Retrieved ${messages.length} new messages to process`);
    
    // Now process the fetched messages for each alias
    let deliveredCount = 0;
    
    // Process in parallel for maximum speed
    await Promise.all(messages.map(async (message) => {
      try {
        // Check recipient addresses to match our aliases
        const recipients = [];
        
        // Check all possible recipient fields
        if (message.envelope && message.envelope.to) {
          recipients.push(...message.envelope.to.map(r => r.address));
        }
        if (message.envelope && message.envelope.cc) {
          recipients.push(...message.envelope.cc.map(r => r.address));
        }
        if (message.envelope && message.envelope.bcc) {
          recipients.push(...message.envelope.bcc.map(r => r.address));
        }
        
        // Find matching aliases
        const matchingAliases = accountAliases.filter(alias => 
          recipients.some(recipient => 
            recipient.toLowerCase() === alias.toLowerCase())
        );
        
        // Process for each matching alias in parallel
        await Promise.all(matchingAliases.map(async (alias) => {
          // Skip if alias no longer active
          if (!isAliasActive(alias)) return;
          
          // Parse the message
          const email = await parseImapMessage(message.source.toString(), alias);
          
          // Add to cache
          const cacheKey = `${alias}:${message.uid}`;
          
          // Only add if not already in cache
          if (!emailCache.has(cacheKey)) {
            addToEmailCache(cacheKey, email);
            deliveredCount++;
            
            // IMMEDIATE notification to connected clients
            notifyClients(alias, email);
            
            // Update account metrics
            if (aliasCache.has(alias)) {
              queueDatabaseUpdate(aliasCache.get(alias).parentAccountId, 1);
            }
          }
        }));
      } catch (messageError) {
        console.error(`Error processing message: ${messageError.message}`);
      }
    }));
    
    if (deliveredCount > 0) {
      console.log(`⚡⚡ ULTRA-FAST: Delivered ${deliveredCount} new emails to clients`);
    }
  } catch (error) {
    console.error(`IDLE processing error: ${error.message}`);
    throw error; // Rethrow for proper handling
  }
}

// Monitor account health
async function monitorAccountHealth() {
  try {
    console.log("Monitoring account health...");
    
    // Get status values from database first to determine the active status name
    const [statusCheck] = await pool.query(`
      SELECT DISTINCT status FROM gmail_accounts LIMIT 10
    `);
    
    // Build query based on available status values
    let activeStatusValue = "active"; // Default
    if (statusCheck && statusCheck.length > 0) {
      // Look for something that might represent "active" status
      const statuses = statusCheck.map(row => row.status);
      
      // Try to find active status from options
      if (statuses.includes("active")) {
        activeStatusValue = "active";
      } else if (statuses.includes("ACTIVE")) {
        activeStatusValue = "ACTIVE";
      } else if (statuses.includes("1")) {
        activeStatusValue = "1";
      } else if (statuses.includes("enabled")) {
        activeStatusValue = "enabled";
      } else if (statuses.length > 0) {
        // Use first non-error status if no obvious active status
        const nonErrorStatus = statuses.find(s => 
          !s.toLowerCase().includes("error") && 
          !s.toLowerCase().includes("disabled") &&
          !s.toLowerCase().includes("inactive")
        );
        if (nonErrorStatus) {
          activeStatusValue = nonErrorStatus;
        }
      }
    }
    
    // IMPROVED: Get all active accounts from database to check against
    const [allActiveAccounts] = await pool.query(
      `SELECT * FROM gmail_accounts WHERE status = ?`,
      [activeStatusValue]
    );
    
    // Create a set of all active account emails for fast lookups
    const activeAccountEmails = new Set(allActiveAccounts.map(account => account.email));
    
    // First part: Check active account status and remove inactive ones
    for (const accountEmail of activeImapAccounts) {
      // If account is no longer in active accounts list, remove it
      if (!activeAccountEmails.has(accountEmail)) {
        console.log(`Account ${accountEmail} no longer active in database, removing from active accounts`);
        activeImapAccounts.delete(accountEmail);
        cleanupIdleMode(accountEmail);
        continue;
      }
      
      // Check if account is not in IDLE mode but should be
      if (IDLE_SETTINGS.enabled && !idleAccounts.has(accountEmail)) {
        // Get account data
        const accountData = allActiveAccounts.find(a => a.email === accountEmail);
        
        if (accountData) {
          console.log(`⚡⚡ Account ${accountEmail} not in IDLE mode - setting up IDLE immediately!`);
          setupIdleMode(accountEmail, accountData)
            .catch(err => console.error(`Error setting up IDLE: ${err.message}`));
        }
      }
    }
    
    // Second part: Start IDLE for any active accounts that aren't being handled
    const setupPromises = [];
    
    for (const account of allActiveAccounts) {
      if (!activeImapAccounts.has(account.email)) {
        console.log(`⚡⚡ Found active account ${account.email} not being monitored - setting up now!`);
        
        // Add to active accounts
        activeImapAccounts.add(account.email);
        
        // Create a promise to set up IDLE
        const setupPromise = new Promise(async (resolve) => {
          try {
            // Try IDLE mode first (preferred for speed)
            const success = await setupIdleMode(account.email, account);
            
            if (!success) {
              // Fall back to polling if IDLE fails
              startTraditionalPolling(account.email, 'high');
            }
          } catch (error) {
            console.error(`Error setting up account ${account.email}:`, error.message);
            
            // Fall back to polling
            startTraditionalPolling(account.email, 'high');
          } finally {
            resolve(); // Always resolve to allow other setup to continue
          }
        });
        
        setupPromises.push(setupPromise);
      }
    }
    
    // Wait for all setup operations to complete (if any)
    if (setupPromises.length > 0) {
      console.log(`Setting up ${setupPromises.length} missing accounts in parallel`);
      await Promise.all(setupPromises);
    }
    
    // Log IDLE status
    console.log(`IDLE mode status: ${idleAccounts.size}/${activeImapAccounts.size} accounts using IDLE`);
    
  } catch (error) {
    console.error("Error monitoring account health:", error.message);
  }
}

// Initialize the service by loading accounts from the database
export async function initializeImapService() {
  try {
    console.log('Initializing IMAP service with SYNCHRONIZED POLLING - connecting to ALL accounts...');
    
    // Ensure we start the DB flush interval
    startDbFlushInterval();
    
    // First check what status values exist in the database
    const [statusCheck] = await pool.query(`
      SELECT DISTINCT status FROM gmail_accounts LIMIT 10
    `);
    
    // Determine which status value means "active"
    let activeStatusValue = "active"; // Default
    if (statusCheck && statusCheck.length > 0) {
      // Look for something that might represent "active" status
      const statuses = statusCheck.map(row => row.status);
      console.log("Available account status values:", statuses);
      
      if (statuses.includes("active")) {
        activeStatusValue = "active";
      } else if (statuses.includes("ACTIVE")) {
        activeStatusValue = "ACTIVE";
      } else if (statuses.includes("1")) {
        activeStatusValue = "1";
      } else if (statuses.includes("enabled")) {
        activeStatusValue = "enabled";
      } else if (statuses.length > 0) {
        // Try to find a non-error status
        const nonErrorStatus = statuses.find(s => 
          !s.toLowerCase().includes("error") && 
          !s.toLowerCase().includes("disabled") &&
          !s.toLowerCase().includes("inactive")
        );
        if (nonErrorStatus) {
          activeStatusValue = nonErrorStatus;
        }
      }
    }
    
    // Get all active accounts from the database
    const [accounts] = await pool.query(`
      SELECT * FROM gmail_accounts WHERE status = ?
    `, [activeStatusValue]);
    
    console.log(`Found ${accounts.length} active Gmail accounts - adding ALL to active accounts list`);
    
    // Simply add all accounts to the active set
    for (const account of accounts) {
      activeImapAccounts.add(account.email);
    }
    
    // Start synchronized polling immediately
    if (SYNC_POLLING.enabled) {
      console.log('Starting synchronized polling system...');
      startSynchronizedPolling();
    }
    
    // Set up an interval to check for account health
    setInterval(monitorAccountHealth, 5 * 60 * 1000); // Every 5 minutes
    
    console.log('IMAP service initialized successfully with SYNCHRONIZED POLLING');
    return true;
  } catch (error) {
    console.error('Failed to initialize IMAP service:', error.message);
    return false;
  }
}

// Improved: Schedule polling has been replaced with synchronized polling for ALL accounts

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

// Run optimization checks at regular intervals
setInterval(optimizeResources, 10 * 60 * 1000); // Run every 10 minutes

// Resource optimization function to manage IMAP resources efficiently
async function optimizeResources() {
  try {
    // Count active aliases per account to determine needed resources
    const accountAliasCount = new Map();
    const activeAliases = new Set();
    
    // Build a map of account -> active alias count
    for (const [alias, data] of aliasCache.entries()) {
      // Skip aliases with no parent account
      if (!data.parentAccount) continue;
      
      // Add to active aliases set for quick lookup
      activeAliases.add(alias);
      
      // Count for the parent account
      const parentAccount = data.parentAccount;
      accountAliasCount.set(
        parentAccount, 
        (accountAliasCount.get(parentAccount) || 0) + 1
      );
    }
    
    // For accounts with no active aliases, remove from active accounts set
    const accountsToRemove = [];
    for (const accountEmail of activeImapAccounts) {
      if (!accountAliasCount.has(accountEmail) || accountAliasCount.get(accountEmail) === 0) {
        accountsToRemove.push(accountEmail);
      }
    }
    
    // Remove inactive accounts from polling/IDLE
    for (const accountEmail of accountsToRemove) {
      console.log(`Removing inactive account from polling: ${accountEmail}`);
      activeImapAccounts.delete(accountEmail);
      cleanupIdleMode(accountEmail);
    }
    
    // Remove alias associations that are orphaned in the lookup map
    for (const [alias, accountEmail] of aliasToAccountMap.entries()) {
      if (!activeAliases.has(alias)) {
        aliasToAccountMap.delete(alias);
      }
    }
    
    // Prune email cache of items for inactive aliases
    const emailsToRemove = [];
    for (const key of emailCache.keys()) {
      const [alias] = key.split(':');
      if (!activeAliases.has(alias)) {
        emailsToRemove.push(key);
      }
    }
    
    if (emailsToRemove.length > 0) {
      console.log(`Removing ${emailsToRemove.length} emails for inactive aliases`);
      for (const key of emailsToRemove) {
        emailCache.delete(key);
      }
    }
    
    // Clean up connection pools for accounts with no active aliases
    for (const [accountEmail, pool] of connectionPools.entries()) {
      if (!accountAliasCount.has(accountEmail) || accountAliasCount.get(accountEmail) === 0) {
        console.log(`Closing connections for inactive account: ${accountEmail}`);
        
        // Close all connections in the pool
        for (const conn of pool) {
          try {
            await conn.client.logout();
          } catch (err) {
            // Ignore errors during cleanup
          }
        }
        
        // Remove the pool
        connectionPools.delete(accountEmail);
      }
    }
    
    // Optimize IDLE connections - ensure only accounts with active aliases use IDLE
    for (const idleAccount of idleAccounts) {
      if (!accountAliasCount.has(idleAccount) || accountAliasCount.get(idleAccount) === 0) {
        console.log(`Removing IDLE for account with no active aliases: ${idleAccount}`);
        cleanupIdleMode(idleAccount);
      }
    }
    
    // Log optimization stats
    console.log(`Resource optimization: ${activeAliases.size} active aliases across ${accountAliasCount.size} accounts`);
    
  } catch (error) {
    console.error(`Error during resource optimization: ${error.message}`);
  }
}

// Enhanced cleanup for inactive aliases - now more aggressive
export async function cleanupInactiveAliases() {
  // Clean up in-memory cache
  const now = Date.now();
  let inMemoryCleanupCount = 0;
  
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    
    // Identify aliases to remove
    const aliasesToRemove = [];
    
    for (const [alias, data] of aliasCache.entries()) {
      if (now - data.lastAccessed > ALIAS_TTL) {
        aliasesToRemove.push({
          alias,
          parentAccountId: data.parentAccountId
        });
      }
    }
    
    // Process all alias removals
    for (const { alias, parentAccountId } of aliasesToRemove) {
      // Update database if we have parent account info
      if (parentAccountId) {
        try {
          // Decrement alias count in the database
          await connection.query(
            'UPDATE gmail_accounts SET alias_count = GREATEST(0, alias_count - 1), updated_at = NOW() WHERE id = ?',
            [parentAccountId]
          );
        } catch (error) {
          // Continue cleanup even if DB update fails
        }
      }
      
      // Remove from caches
      aliasCache.delete(alias);
      aliasToAccountMap.delete(alias);
      inMemoryCleanupCount++;
    }
    
    await connection.commit();
    
    if (inMemoryCleanupCount > 0) {
      console.log(`Cleaned up ${inMemoryCleanupCount} inactive aliases`);
      
      // Run immediate resource optimization after cleaning up aliases
      optimizeResources().catch(err => console.error(`Failed to optimize resources: ${err.message}`));
    }
    
    // Clean email cache if it's getting too big
    if (emailCache.size > MAX_CACHE_SIZE * 0.8) {
      const oldestKeys = [...emailCache.keys()]
        .map(k => ({ key: k, timestamp: emailCache.get(k).timestamp }))
        .sort((a, b) => a.timestamp - b.timestamp)
        .slice(0, Math.ceil(MAX_CACHE_SIZE * 0.3))
        .map(item => item.key);
      
      oldestKeys.forEach(key => emailCache.delete(key));
      console.log(`Cleaned up ${oldestKeys.length} old emails from cache`);
    }
    
  } catch (error) {
    await connection.rollback();
    console.error(`Error during alias cleanup: ${error.message}`);
  } finally {
    connection.release();
  }
}

// Run alias cleanup every 30 minutes instead of every hour
setInterval(cleanupInactiveAliases, 30 * 60 * 1000);

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
    // First get available status values
    const [statusCheck] = await pool.query(`
      SELECT DISTINCT status FROM gmail_accounts LIMIT 10
    `);
    
    // Determine active status value
    let activeStatusValue = "active"; // Default
    if (statusCheck && statusCheck.length > 0) {
      const statuses = statusCheck.map(row => row.status);
      
      if (statuses.includes("active")) {
        activeStatusValue = "active";
      } else if (statuses.includes("ACTIVE")) {
        activeStatusValue = "ACTIVE";
      } else if (statuses.includes("1")) {
        activeStatusValue = "1";
      } else if (statuses.includes("enabled")) {
        activeStatusValue = "enabled";
      } else if (statuses.length > 0) {
        const nonErrorStatus = statuses.find(s => 
          !s.toLowerCase().includes("error") && 
          !s.toLowerCase().includes("disabled") &&
          !s.toLowerCase().includes("inactive")
        );
        if (nonErrorStatus) {
          activeStatusValue = nonErrorStatus;
        }
      }
    }
    
    // Get active accounts with balancing strategy
    const [accounts] = await pool.query(`
      SELECT * 
      FROM gmail_accounts a
      WHERE a.status = ? 
      ORDER BY 
        a.alias_count ASC,
        a.quota_used ASC,
        a.last_used ASC
      LIMIT 10  -- Reduced from 15 to focus on most available accounts
    `, [activeStatusValue]);
    
    if (accounts.length === 0) {
      console.error('No available Gmail accounts');
      return null;
    }

    // Prioritize accounts with existing IDLE setup to conserve resources
    const idleAccount = accounts.find(a => idleAccounts.has(a.email));
    if (idleAccount) {
      console.log(`Reusing IDLE account for new alias: ${idleAccount.email}`);
      return idleAccount;
    }
    
    // Otherwise select a random account from top 3 to balance load
    const randomIndex = accounts.length <= 3 ? 
      Math.floor(Math.random() * accounts.length) : 
      Math.floor(Math.random() * 3); // Only choose from top 3
    
    const selectedAccount = accounts[randomIndex];
    return selectedAccount;
  } catch (error) {
    console.error('Error selecting next available account:', error.message);
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

// Check if an alias is still active (centralize this logic)
function isAliasActive(alias) {
  return aliasCache.has(alias) && aliasToAccountMap.has(alias);
}

// Add keepalive ping function to keep IMAP connections alive
setInterval(sendKeepAlive, 60000); // Run every minute

// Keep IMAP connections alive to prevent disconnections
async function sendKeepAlive() {
  // Send NOOP to all IDLE clients to keep them alive
  for (const accountEmail of idleAccounts) {
    const client = imapClients.get(accountEmail);
    if (client && client.usable) {
      try {
        await client.noop();
      } catch (error) {
        if (error.code === 'NoConnection') {
          // Connection lost - restart IDLE
          cleanupIdleMode(accountEmail);
          
          // Get account info
          try {
            const [accounts] = await pool.query(
              'SELECT * FROM gmail_accounts WHERE email = ?',
              [accountEmail]
            );
            
            if (accounts.length > 0) {
              setTimeout(() => {
                setupIdleMode(accountEmail, accounts[0]).catch(() => {});
              }, 1000);
            }
          } catch (dbError) {
            console.error(`Error getting account: ${dbError.message}`);
          }
        }
      }
    }
  }
}

// Start synchronized polling system
let syncPollingTimer = null;

function startSynchronizedPolling() {
  if (syncPollingTimer) {
    clearInterval(syncPollingTimer);
  }
  
  console.log(`🔄 Starting SYNCHRONIZED polling every ${SYNC_POLLING.pollingInterval}ms`);
  
  // Initial immediate poll
  runSynchronizedPolling();
  
  // Set up regular interval
  syncPollingTimer = setInterval(runSynchronizedPolling, SYNC_POLLING.pollingInterval);
}

// Stop synchronized polling
function stopSynchronizedPolling() {
  if (syncPollingTimer) {
    clearInterval(syncPollingTimer);
    syncPollingTimer = null;
    console.log('Stopped synchronized polling');
  }
}

// Run a synchronized polling cycle
async function runSynchronizedPolling() {
  // Prevent overlapping runs
  if (SYNC_POLLING.inProgress) {
    return;
  }
  
  // Check if we have active accounts
  if (activeImapAccounts.size === 0) {
    return;
  }
  
  try {
    SYNC_POLLING.inProgress = true;
    SYNC_POLLING.lastRunTime = Date.now();
    
    // Get all active accounts with their aliases
    const accountAliasMap = new Map();
    
    // Build the map of account -> aliases
    for (const [alias, data] of aliasCache.entries()) {
      if (data.parentAccount && activeImapAccounts.has(data.parentAccount)) {
        if (!accountAliasMap.has(data.parentAccount)) {
          accountAliasMap.set(data.parentAccount, []);
        }
        accountAliasMap.get(data.parentAccount).push(alias);
      }
    }
    
    // Skip if no accounts with aliases
    if (accountAliasMap.size === 0) {
      SYNC_POLLING.inProgress = false;
      return;
    }
    
    // Reduced logging to prevent log spam
    if (accountAliasMap.size > 5) {
      console.log(`🔄 Running synchronized polling for ${accountAliasMap.size} accounts with aliases`);
    }
    
    // Create fetch tasks with limit on concurrent execution
    const fetchTasks = [];
    const accounts = Array.from(accountAliasMap.keys());
    
    // Limit concurrent fetches to avoid overwhelming server
    // CRITICAL: Reduced concurrent fetches to avoid "Too many connections" errors
    const batchSize = Math.min(accounts.length, SYNC_POLLING.maxConcurrentFetches);
    const accountsToProcess = accounts.slice(0, batchSize);
    
    // Create tasks for each account
    for (const accountEmail of accountsToProcess) {
      const aliases = accountAliasMap.get(accountEmail);
      
      // Skip accounts with no aliases
      if (!aliases || aliases.length === 0) continue;
      
      // Create a fetch task with timeout
      const fetchTask = new Promise(async (resolve) => {
        try {
          // Set up timeout
          const timeoutId = setTimeout(() => {
            console.log(`⚠️ Fetch timeout for account ${accountEmail}`);
            resolve({ accountEmail, success: false, error: 'Timeout' });
          }, SYNC_POLLING.fetchTimeout);
          
          // Run fetches for this account
          const result = await fetchAllAliasesForAccount(accountEmail, aliases);
          
          // Clear timeout
          clearTimeout(timeoutId);
          
          resolve({ accountEmail, success: true, result });
          
        } catch (error) {
          console.error(`Error in fetch task for ${accountEmail}:`, error.message);
          resolve({ accountEmail, success: false, error: error.message });
        }
      });
      
      fetchTasks.push(fetchTask);
    }
    
    // Execute all fetch tasks in parallel with Promise.allSettled
    const results = await Promise.allSettled(fetchTasks);
    
    // Process results
    const successCount = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
    const failedCount = fetchTasks.length - successCount;
    
    if (failedCount > 0) {
      console.log(`⚠️ ${failedCount} account fetches failed - will retry on next poll`);
    }
    
    // Reduced logging for normal operation to prevent log spam
    if (successCount > 5 || failedCount > 0) {
      console.log(`🔄 Completed synchronized polling cycle: ${successCount}/${fetchTasks.length} successful`);
    }
    
  } catch (error) {
    console.error('Error in synchronized polling:', error.message);
  } finally {
    SYNC_POLLING.inProgress = false;
  }
}

// Fetch all aliases for a single account
async function fetchAllAliasesForAccount(accountEmail, aliases) {
  let connection = null;
  
  try {
    // Get IMAP client from the connection pool
    connection = await getImapConnection(accountEmail);
    const client = connection.client;
    
    // Folders to check
    const folders = SYNC_POLLING.checkAllFolders ? 
      ['INBOX', '[Gmail]/Spam'] : ['INBOX'];
    
    let totalEmails = 0;
    const fetchResults = [];
    
    // Process each folder
    for (const folder of folders) {
      try {
        // Select the mailbox
        const mailbox = await client.mailboxOpen(folder);
        
        // Skip empty folders
        if (mailbox.exists === 0) continue;
        
        // Get search date (two weeks ago)
        const searchDate = new Date();
        searchDate.setDate(searchDate.getDate() - SEARCH_WINDOW);
        
        // Process all aliases at once for this folder
        for (const alias of aliases) {
          try {
            // Search for emails for this alias
            const searchResults = await client.search({
              to: alias,
              since: searchDate
            });
            
            if (searchResults.length === 0) continue;
            
            // Get newest messages first (limit to most recent)
            const uids = searchResults.slice().sort((a, b) => b - a);
            const messagesToFetch = uids.slice(0, 30); // Fetch 30 most recent (increased from 10)
            
            // FIXED: Don't use stream events - use simple for-await loop
            try {
              // Fetch messages using simple for-await loop (this is safer)
              for await (const message of client.fetch(messagesToFetch, { 
                envelope: true, source: true, uid: true 
              })) {
                try {
                  // Skip if the alias has been removed during processing
                  if (!isAliasActive(alias)) continue;
                  
                  // Parse message
                  const email = await parseImapMessage(message.source.toString(), alias);
                  
                  // Add to cache if not already there
                  const cacheKey = `${alias}:${message.uid}`;
                  
                  if (!emailCache.has(cacheKey)) {
                    addToEmailCache(cacheKey, email);
                    totalEmails++;
                    
                    // Notify connected clients
                    notifyClients(alias, email);
                    
                    fetchResults.push({
                      alias,
                      messageId: message.uid,
                      folder
                    });
                  }
                } catch (err) {
                  console.error(`Error processing message: ${err.message}`);
                }
              }
            } catch (fetchError) {
              // Handle fetch errors more gracefully
              console.error(`Error fetching messages for ${alias} in ${folder}: ${fetchError.message}`);
              // Continue with next alias
            }
          } catch (aliasError) {
            console.error(`Error processing alias ${alias}:`, aliasError.message);
          }
        }
      } catch (folderError) {
        console.error(`Error processing folder ${folder}:`, folderError.message);
      }
    }
    
    // Update account metrics if we found new emails
    if (totalEmails > 0) {
      console.log(`🔄 Found ${totalEmails} new emails for account ${accountEmail}`);
      
      // Update metrics for the account
      const accountId = aliases
        .filter(alias => aliasCache.has(alias))
        .map(alias => aliasCache.get(alias).parentAccountId)
        .find(id => id); // Get first valid ID
        
      if (accountId) {
        queueDatabaseUpdate(accountId, totalEmails);
      }
    }
    
    return { totalEmails, fetchResults };
    
  } catch (error) {
    console.error(`Error fetching emails for account ${accountEmail}:`, error.message);
    throw error;
  } finally {
    // Always release the connection back to the pool
    if (connection) {
      connection.release();
    }
  }
}
