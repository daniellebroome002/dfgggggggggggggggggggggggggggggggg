import WebSocket from 'ws';
import { ImapFlow } from 'imapflow';
import { simpleParser } from 'mailparser';
import { v4 as uuidv4 } from 'uuid';
import { pool } from '../db/init.js';
import imapConnectionPool from './imapConnectionPool.js';

// In-memory storage for Gmail connections, aliases, and emails
const inMemoryStore = {
  // Email cache - map of alias:messageId -> email data
  emailCache: new Map(),
  
  // Active aliases - map of alias -> { userId, parentAccount, created, lastAccessed }
  aliases: new Map(),
  
  // Active polling - set of email addresses currently being polled
  activePolling: new Set(),
  
  // WebSocket connections - map of userId -> Set of WebSocket connections
  wsConnections: new Map(),
  
  // Max cache sizes and TTL settings
  maxEmailCacheSize: 10000,
  maxCacheEntryAge: 24 * 60 * 60 * 1000, // 24 hours
  aliasExpiryTime: 7 * 24 * 60 * 60 * 1000, // 7 days
  
  // Fetch settings
  maxMailboxSize: 1000, // Maximum emails to fetch from a mailbox
  maxEmailsPerFetch: 50, // Maximum emails to fetch in one operation
  
  // Connection management
  connectionBackoffTime: new Map() // Email -> timestamp when to retry
};

// Cleanup expired emails from cache periodically
setInterval(() => {
  const now = Date.now();
  let expiredCount = 0;
  
  // Remove expired email cache entries
  for (const [key, data] of inMemoryStore.emailCache.entries()) {
    if (now - data.timestamp > inMemoryStore.maxCacheEntryAge) {
      inMemoryStore.emailCache.delete(key);
      expiredCount++;
    }
  }
  
  if (expiredCount > 0) {
    console.log(`Cleaned up ${expiredCount} expired emails from cache`);
  }
  
  // Cleanup expired aliases
  let expiredAliases = 0;
  for (const [alias, data] of inMemoryStore.aliases.entries()) {
    if (now - data.created > inMemoryStore.aliasExpiryTime) {
      inMemoryStore.aliases.delete(alias);
      expiredAliases++;
    }
  }
  
  if (expiredAliases > 0) {
    console.log(`Cleaned up ${expiredAliases} expired aliases`);
  }
}, 30 * 60 * 1000); // Run every 30 minutes

/**
 * Set up WebSocket server for real-time email notifications
 * @param {http.Server} server - HTTP server instance
 */
export function setupWebSocketServer(server) {
  const wss = new WebSocket.Server({ noServer: true });
  
  // Handle WebSocket upgrade
  server.on('upgrade', (request, socket, head) => {
    // Only handle /email-ws path
    if (request.url.startsWith('/email-ws')) {
      wss.handleUpgrade(request, socket, head, ws => {
        wss.emit('connection', ws, request);
      });
    }
  });
  
  // Handle WebSocket connections
  wss.on('connection', (ws, req) => {
    // Extract user ID and alias from URL parameters
    const url = new URL(req.url, `http://${req.headers.host}`);
    const userId = url.searchParams.get('userId');
    const alias = url.searchParams.get('alias');
    
    if (!userId) {
      ws.close(4000, 'Missing userId parameter');
      return;
    }
    
    // Store the connection
    if (!inMemoryStore.wsConnections.has(userId)) {
      inMemoryStore.wsConnections.set(userId, new Set());
    }
    
    const connections = inMemoryStore.wsConnections.get(userId);
    connections.add(ws);
    
    console.log(`WebSocket client connected for user ${userId}${alias ? ` and alias ${alias}` : ''}`);
    
    // Send initial email data if available
    if (alias) {
      const emails = getEmailsForAlias(alias);
      if (emails.length > 0) {
        ws.send(JSON.stringify({
          type: 'initial',
          alias,
          emails
        }));
      }
    }
    
    // Handle connection close
    ws.on('close', () => {
      if (inMemoryStore.wsConnections.has(userId)) {
        inMemoryStore.wsConnections.get(userId).delete(ws);
        if (inMemoryStore.wsConnections.get(userId).size === 0) {
          inMemoryStore.wsConnections.delete(userId);
        }
      }
    });
    
    // Handle WebSocket messages
    ws.on('message', (message) => {
      try {
        const data = JSON.parse(message);
        
        // Handle user commands
        if (data.type === 'subscribe' && data.alias) {
          // Subscribe to a specific alias
          ws.alias = data.alias;
          console.log(`Client subscribed to alias ${data.alias}`);
          
          // Send current data for this alias
          const emails = getEmailsForAlias(data.alias);
          ws.send(JSON.stringify({
            type: 'initial',
            alias: data.alias,
            emails
          }));
        }
      } catch (error) {
        console.error('Error processing WebSocket message:', error);
      }
    });
    
    // Send heartbeat to keep connection alive
    const heartbeatInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'heartbeat', timestamp: Date.now() }));
      } else {
        clearInterval(heartbeatInterval);
      }
    }, 30000); // Every 30 seconds
    
    // Set a timeout to clean up very long-lived connections
    setTimeout(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.close(1000, 'Connection lifetime exceeded');
      }
      clearInterval(heartbeatInterval);
    }, 6 * 60 * 60 * 1000); // 6 hours max lifetime
  });
  
  console.log('WebSocket server for email notifications initialized');
}

/**
 * Get emails for a specific alias
 * @param {string} alias - Email alias
 * @returns {Array} - Array of emails
 */
function getEmailsForAlias(alias) {
  const emails = [];
  
  // Collect all emails for this alias from cache
  for (const [key, email] of inMemoryStore.emailCache.entries()) {
    if (key.startsWith(`${alias}:`)) {
      emails.push(email);
    }
  }
  
  // Sort by internalDate, newest first
  return emails.sort((a, b) => 
    new Date(b.internalDate) - new Date(a.internalDate)
  );
}

/**
 * Broadcast new email to connected WebSocket clients
 * @param {string} alias - Email alias that received the email
 * @param {Object} email - Email data
 */
function broadcastNewEmail(alias, email) {
  // Find the user ID for this alias
  let targetUserId = null;
  
  if (inMemoryStore.aliases.has(alias)) {
    targetUserId = inMemoryStore.aliases.get(alias).userId;
  }
  
  if (!targetUserId) {
    console.log(`No user found for alias ${alias}, can't broadcast`);
    return;
  }
  
  // Get WebSocket connections for this user
  const connections = inMemoryStore.wsConnections.get(targetUserId);
  if (!connections || connections.size === 0) {
    return;
  }
  
  // Prepare message
  const message = JSON.stringify({
    type: 'new-email',
    alias,
    email
  });
  
  // Send to all connections for this user
  let sentCount = 0;
  for (const ws of connections) {
    if (ws.readyState === WebSocket.OPEN && (!ws.alias || ws.alias === alias)) {
      ws.send(message);
      sentCount++;
    }
  }
  
  if (sentCount > 0) {
    console.log(`Broadcast new email to ${sentCount} connections for user ${targetUserId}`);
  }
}

/**
 * Add Gmail account with app password for IMAP access
 * @param {string} email - Gmail address
 * @param {string} appPassword - App password for IMAP
 * @returns {Promise<Object>} - Result of operation
 */
export async function addGmailAccount(email, appPassword) {
  try {
    // Validate that we can connect to the account
    const client = new ImapFlow({
      host: 'imap.gmail.com',
      port: 993,
      secure: true,
      auth: {
        user: email,
        pass: appPassword
      },
      logger: false,
      timeoutConnection: 15000
    });
    
    // Test connection
    try {
      await client.connect();
      
      // Check for INBOX access
      const mailbox = await client.mailboxOpen('INBOX');
      console.log(`Successfully connected to ${email} mailbox:`, {
        exists: mailbox.exists,
        uidValidity: mailbox.uidValidity
      });
      
      await client.logout();
    } catch (error) {
      console.error(`Failed to test connection to ${email}:`, error);
      throw new Error(`Connection test failed: ${error.message}`);
    }
    
    // Connection succeeded, store account in database
    const id = uuidv4();
    const encryptedPassword = appPassword; // In production, you'd encrypt this
    
    // Check if account already exists
    const [existingAccounts] = await pool.query(
      'SELECT id FROM gmail_accounts WHERE email = ?',
      [email]
    );
    
    if (existingAccounts.length > 0) {
      // Update existing account
      await pool.query(
        `UPDATE gmail_accounts SET 
         app_password = ?,
         status = 'active',
         last_used = NOW(),
         updated_at = NOW()
         WHERE email = ?`,
        [encryptedPassword, email]
      );
      
      console.log(`Updated existing Gmail account: ${email}`);
      
      // Start polling if not already active
      if (!inMemoryStore.activePolling.has(email)) {
        console.log(`Starting polling for account: ${email}`);
        schedulePolling(email, appPassword);
        inMemoryStore.activePolling.add(email);
      }
      
      return { 
        success: true, 
        id: existingAccounts[0].id,
        email,
        message: 'Gmail account updated successfully'
      };
    } else {
      // Insert new account
      await pool.query(
        `INSERT INTO gmail_accounts (
          id, email, app_password, quota_used, alias_count, status, last_used
        ) VALUES (?, ?, ?, 0, 0, 'active', NOW())`,
        [id, email, encryptedPassword]
      );
      
      console.log(`Added new Gmail account: ${email}`);
      
      // Start polling
      console.log(`Starting polling for new account: ${email}`);
      schedulePolling(email, appPassword);
      inMemoryStore.activePolling.add(email);
      
      return { 
        success: true, 
        id,
        email,
        message: 'Gmail account added successfully' 
      };
    }
  } catch (error) {
    console.error('Failed to add Gmail account:', error);
    return { 
      success: false, 
      error: error.message 
    };
  }
}

/**
 * Generate a new Gmail alias
 * @param {string} userId - User ID requesting the alias
 * @param {string} strategy - 'dot' or 'plus' for alias generation
 * @param {string} domain - 'gmail.com' or 'googlemail.com'
 * @returns {Promise<Object>} - Generated alias
 */
export async function generateGmailAlias(userId, strategy = 'dot', domain = 'gmail.com') {
  try {
    // Get next available account
    const [accounts] = await pool.query(`
      SELECT id, email, app_password 
      FROM gmail_accounts 
      WHERE status = 'active' 
      ORDER BY alias_count, last_used 
      LIMIT 1
    `);
    
    if (accounts.length === 0) {
      throw new Error('No active Gmail accounts available');
    }
    
    const account = accounts[0];
    
    // Generate alias based on strategy
    let alias;
    if (strategy === 'dot') {
      alias = generateDotAlias(account.email, domain);
    } else {
      alias = generatePlusAlias(account.email, domain);
    }
    
    console.log(`Generated ${strategy} alias ${alias} using account ${account.email}`);
    
    // Update alias count in database
    await pool.query(
      `UPDATE gmail_accounts 
       SET alias_count = alias_count + 1, last_used = NOW(), updated_at = NOW() 
       WHERE id = ?`,
      [account.id]
    );
    
    // Store in-memory alias
    inMemoryStore.aliases.set(alias, {
      userId,
      parentAccount: account.email,
      parentAccountId: account.id,
      created: Date.now(),
      lastAccessed: Date.now(),
      expiresAt: Date.now() + inMemoryStore.aliasExpiryTime
    });
    
    // Ensure account is being polled
    if (!inMemoryStore.activePolling.has(account.email)) {
      schedulePolling(account.email, account.app_password);
      inMemoryStore.activePolling.add(account.email);
    }
    
    return { 
      success: true,
      alias,
      expiresAt: new Date(Date.now() + inMemoryStore.aliasExpiryTime).toISOString()
    };
  } catch (error) {
    console.error('Failed to generate Gmail alias:', error);
    throw error;
  }
}

/**
 * Generate a Gmail dot alias
 * @param {string} email - Base Gmail address
 * @param {string} domain - Domain to use (gmail.com or googlemail.com)
 * @returns {string} - Generated alias
 */
function generateDotAlias(email, domain = 'gmail.com') {
  // Extract username from email
  const [username] = email.split('@');
  
  // Randomly insert dots in the username
  let aliasUsername = '';
  for (let i = 0; i < username.length; i++) {
    aliasUsername += username[i];
    // Randomly add dots between characters
    if (i < username.length - 1 && Math.random() > 0.6) {
      aliasUsername += '.';
    }
  }
  
  // Replace multiple consecutive dots with a single dot
  aliasUsername = aliasUsername.replace(/\.+/g, '.');
  
  // Make sure it's different from original
  if (aliasUsername === username) {
    // Force add a dot somewhere
    const position = Math.floor(Math.random() * (username.length - 1));
    aliasUsername = username.substring(0, position) + '.' + username.substring(position);
  }
  
  return `${aliasUsername}@${domain}`;
}

/**
 * Generate a Gmail plus alias
 * @param {string} email - Base Gmail address
 * @param {string} domain - Domain to use (gmail.com or googlemail.com)
 * @returns {string} - Generated alias
 */
function generatePlusAlias(email, domain = 'gmail.com') {
  // Extract username from email
  const [username] = email.split('@');
  
  // Generate random tag
  const tag = Math.random().toString(36).substring(2, 8);
  
  return `${username}+${tag}@${domain}`;
}

/**
 * Get Gmail aliases for a user
 * @param {string} userId - User ID
 * @returns {Promise<Array>} - Array of aliases
 */
export async function getUserAliases(userId) {
  try {
    // Get aliases from in-memory store
    const userAliases = [];
    
    for (const [alias, data] of inMemoryStore.aliases.entries()) {
      if (data.userId === userId) {
        userAliases.push({
          alias,
          createdAt: new Date(data.created).toISOString(),
          expiresAt: new Date(data.expiresAt).toISOString()
        });
      }
    }
    
    // Sort by creation time, newest first
    userAliases.sort((a, b) => 
      new Date(b.createdAt) - new Date(a.createdAt)
    );
    
    return userAliases;
  } catch (error) {
    console.error('Failed to get user aliases:', error);
    return [];
  }
}

/**
 * Fetch emails for a specific Gmail alias
 * @param {string} userId - User ID
 * @param {string} alias - Email alias
 * @returns {Promise<Array>} - Array of emails
 */
export async function fetchGmailEmails(userId, alias) {
  try {
    // Update last accessed time for alias
    if (inMemoryStore.aliases.has(alias)) {
      const aliasData = inMemoryStore.aliases.get(alias);
      aliasData.lastAccessed = Date.now();
      
      // Verify user ID matches (security check)
      if (aliasData.userId !== userId) {
        console.warn(`User ${userId} tried to access alias ${alias} owned by ${aliasData.userId}`);
        return [];
      }
      
      inMemoryStore.aliases.set(alias, aliasData);
    } else {
      console.warn(`Alias ${alias} not found in memory cache`);
      return [];
    }
    
    // Get emails from cache
    const emails = getEmailsForAlias(alias);
    
    // Trigger a background refresh if needed
    triggerBackgroundRefresh(alias);
    
    return emails;
  } catch (error) {
    console.error(`Error fetching emails for alias ${alias}:`, error);
    return [];
  }
}

/**
 * Trigger background refresh of emails for an alias
 * @param {string} alias - Email alias
 */
function triggerBackgroundRefresh(alias) {
  // Only refresh if alias exists in memory
  if (!inMemoryStore.aliases.has(alias)) return;
  
  const aliasData = inMemoryStore.aliases.get(alias);
  const { parentAccount } = aliasData;
  
  // Skip if parent account is not being actively polled
  if (!inMemoryStore.activePolling.has(parentAccount)) return;
  
  // Check if a refresh was done recently (throttle)
  if (aliasData.lastRefresh && Date.now() - aliasData.lastRefresh < 60000) {
    return;
  }
  
  // Mark as being refreshed
  aliasData.lastRefresh = Date.now();
  inMemoryStore.aliases.set(alias, aliasData);
  
  // Get account password and perform refresh
  (async () => {
    try {
      const [accounts] = await pool.query(
        'SELECT app_password FROM gmail_accounts WHERE email = ?',
        [parentAccount]
      );
      
      if (accounts.length > 0) {
        // Force immediate check for new emails
        await fetchNewEmails(parentAccount, accounts[0].app_password, [alias]);
      }
    } catch (error) {
      console.error(`Error during background refresh for ${alias}:`, error);
    }
  })();
}

/**
 * Rotate to a new Gmail alias for a user
 * @param {string} userId - User ID
 * @param {string} strategy - 'dot' or 'plus'
 * @param {string} domain - 'gmail.com' or 'googlemail.com'
 * @returns {Promise<Object>} - New alias data
 */
export async function rotateUserAlias(userId, strategy = 'dot', domain = 'gmail.com') {
  // Generate a new alias
  return await generateGmailAlias(userId, strategy, domain);
}

/**
 * Schedule polling for a Gmail account
 * @param {string} accountEmail - Gmail account email
 * @param {string} appPassword - App password
 */
function schedulePolling(accountEmail, appPassword) {
  console.log(`Setting up polling schedule for ${accountEmail}`);
  
  // Add to active polling set
  inMemoryStore.activePolling.add(accountEmail);
  
  // Determine polling interval based on account activity
  const getPollingInterval = async () => {
    try {
      // Get alias count to determine activity level
      const [accountData] = await pool.query(
        'SELECT alias_count FROM gmail_accounts WHERE email = ?',
        [accountEmail]
      );
      
      if (accountData.length === 0) return 30000; // Default 30 seconds
      
      const { alias_count } = accountData[0];
      
      // Adjust interval based on activity
      if (alias_count > 10) return 5000;     // 10 seconds for high activity
      else if (alias_count > 5) return 10000; // 20 seconds for medium activity
      else return 15000;                     // 30 seconds for low activity
    } catch (error) {
      console.error(`Error determining polling interval for ${accountEmail}:`, error);
      return 30000; // Default 30 seconds
    }
  };
  
  // Check for backoff time
  const checkBackoff = () => {
    if (inMemoryStore.connectionBackoffTime.has(accountEmail)) {
      const backoffUntil = inMemoryStore.connectionBackoffTime.get(accountEmail);
      if (Date.now() < backoffUntil) {
        const remainingBackoff = Math.ceil((backoffUntil - Date.now()) / 1000);
        console.log(`Account ${accountEmail} in backoff for ${remainingBackoff}s`);
        return true;
      } else {
        // Backoff expired
        inMemoryStore.connectionBackoffTime.delete(accountEmail);
      }
    }
    return false;
  };
  
  // Start polling loop
  const poll = async () => {
    // Skip if backoff is active
    if (checkBackoff()) {
      setTimeout(poll, 5000); // Check again in 5 seconds
      return;
    }
    
    // Get aliases associated with this account
    const aliases = [];
    for (const [alias, data] of inMemoryStore.aliases.entries()) {
      if (data.parentAccount === accountEmail) {
        aliases.push(alias);
      }
    }
    
    // Skip polling if no aliases exist
    if (aliases.length === 0) {
      console.log(`No aliases for ${accountEmail}, skipping poll`);
      setTimeout(poll, 30000); // Check again in 30 seconds
      return;
    }
    
    try {
      // Fetch new emails
      await fetchNewEmails(accountEmail, appPassword, aliases);
      
      // Schedule next poll with dynamic interval
      const interval = await getPollingInterval();
      setTimeout(poll, interval);
    } catch (error) {
      console.error(`Error during polling for ${accountEmail}:`, error);
      
      // Implement backoff strategy for errors
      if (error.message.includes('rate limit') || 
          error.message.includes('Too many simultaneous connections') ||
          error.message.includes('OVERQUOTA')) {
        
        // Set backoff for 5 minutes
        inMemoryStore.connectionBackoffTime.set(accountEmail, Date.now() + 5 * 60 * 1000);
        console.log(`Setting backoff for ${accountEmail} due to rate limiting`);
      }
      
      // Try again after a delay (longer after errors)
      setTimeout(poll, 60000); // 1 minute retry after error
    }
  };
  
  // Start the polling loop
  setTimeout(poll, 2000); // Start first poll after 2 seconds
}

/**
 * Fetch new emails using IMAP
 * @param {string} accountEmail - Gmail account email
 * @param {string} appPassword - App password
 * @param {Array<string>} aliases - Array of aliases to check
 */
async function fetchNewEmails(accountEmail, appPassword, aliases) {
  if (!aliases || aliases.length === 0) {
    console.log(`No aliases to check for ${accountEmail}`);
    return;
  }
  
  console.log(`Fetching new emails for ${accountEmail} (${aliases.length} aliases)`);
  
  let client;
  try {
    // Get connection from pool
    client = await imapConnectionPool.getConnection(accountEmail, appPassword, false, {
      testConnection: true
    });
    
    // Open INBOX with readOnly flag
    const mailbox = await client.mailboxOpen('INBOX', { readOnly: true });
    console.log(`Opened mailbox with ${mailbox.exists} messages`);
    
    // Limit to recent emails (last 3 days)
    const since = new Date();
    since.setDate(since.getDate() - 3);
    
    // Process all aliases for this account
    for (const alias of aliases) {
      try {
        // Create a search for emails to this alias
        const searchOptions = {
          since,
          to: alias
        };
        
        // Search for messages
        const results = await client.search(searchOptions);
        
        if (results.length === 0) {
          continue; // No messages for this alias
        }
        
        console.log(`Found ${results.length} messages for alias ${alias}`);
        
        // Get newest messages first, limit to most recent ones
        const messageIds = results.sort((a, b) => b - a).slice(0, inMemoryStore.maxEmailsPerFetch);
        
        // Fetch messages in bulk
        for (const seq of messageIds) {
          // Check if we've already cached this message
          const message = await client.fetchOne(seq, { source: true });
          
          if (!message || !message.source) {
            continue;
          }
          
          // Use unique ID based on message ID and alias
          const cacheKey = `${alias}:${message.uid}`;
          
          // Skip if already in cache
          if (inMemoryStore.emailCache.has(cacheKey)) {
            continue;
          }
          
          // Parse email
          const parsed = await simpleParser(message.source);
          
          // Process into standard format
          const email = {
            id: message.uid.toString(),
            messageId: parsed.messageId,
            threadId: parsed.messageId, // Use message ID as thread ID
            from: parsed.from?.text || '',
            to: parsed.to?.text || '',
            subject: parsed.subject || '(No Subject)',
            bodyHtml: parsed.html || '',
            bodyText: parsed.text || '',
            snippet: parsed.text?.substring(0, 150) || '',
            internalDate: parsed.date?.toISOString() || new Date().toISOString(),
            timestamp: Date.now(),
            hasAttachments: parsed.attachments?.length > 0,
            labels: message.flags || [],
            recipientAlias: alias
          };
          
          // Cache the email
          inMemoryStore.emailCache.set(cacheKey, email);
          
          // Broadcast to WebSocket clients
          broadcastNewEmail(alias, email);
        }
      } catch (error) {
        console.error(`Error processing alias ${alias}:`, error);
      }
    }
  } catch (error) {
    console.error(`Error fetching emails for ${accountEmail}:`, error);
    
    // Update account status if there's a serious error
    if (error.message.includes('Invalid credentials') || 
        error.message.includes('Authentication failed')) {
      
      try {
        await pool.query(
          'UPDATE gmail_accounts SET status = \'auth-error\', updated_at = NOW() WHERE email = ?',
          [accountEmail]
        );
        
        // Set long backoff for authentication errors
        inMemoryStore.connectionBackoffTime.set(accountEmail, Date.now() + 30 * 60 * 1000); // 30 minutes
        console.log(`Marked account ${accountEmail} as auth-error`);
      } catch (dbError) {
        console.error('Error updating account status:', dbError);
      }
    }
    
    throw error; // Rethrow for polling handler
  } finally {
    // Release connection back to pool
    if (client) {
      try {
        await imapConnectionPool.releaseConnection(accountEmail, client);
      } catch (error) {
        console.error(`Error releasing connection for ${accountEmail}:`, error);
      }
    }
  }
}

/**
 * Set up IDLE connections for real-time email monitoring
 * Used for high-priority accounts that need instant notifications
 * @param {string} accountEmail - Gmail account email
 * @param {string} appPassword - App password
 * @param {Array<string>} aliases - Array of aliases to watch
 */
async function setupIdleConnection(accountEmail, appPassword, aliases) {
  if (!aliases || aliases.length === 0) return;
  
  let idleClient;
  try {
    console.log(`Setting up IDLE connection for ${accountEmail}`);
    
    // Get dedicated IDLE connection
    idleClient = await imapConnectionPool.getConnection(accountEmail, appPassword, true);
    
    // Open INBOX
    await idleClient.mailboxOpen('INBOX');
    
    // Set up event handler for new emails
    idleClient.on('exists', async () => {
      console.log(`New message arrived for ${accountEmail}, fetching`);
      
      // Fetch new messages immediately
      try {
        await fetchNewEmails(accountEmail, appPassword, aliases);
      } catch (error) {
        console.error(`Error fetching new emails after IDLE notification:`, error);
      }
    });
    
    // Start IDLE
    await idleClient.idle();
    console.log(`IDLE connection established for ${accountEmail}`);
    
    // Setup refresh of IDLE every 25 minutes (before Gmail's 30 minute limit)
    const idleRefreshInterval = setInterval(async () => {
      try {
        // End current IDLE
        await idleClient.idle.stop();
        
        // Perform NOOP to keep connection alive
        await idleClient.noop();
        
        // Restart IDLE
        await idleClient.idle();
        console.log(`Refreshed IDLE connection for ${accountEmail}`);
      } catch (error) {
        console.error(`Error refreshing IDLE:`, error);
        
        // Clean up
        clearInterval(idleRefreshInterval);
        
        // Release the connection
        try {
          await imapConnectionPool.releaseConnection(accountEmail, idleClient);
        } catch (releaseError) {
          console.error(`Error releasing IDLE connection:`, releaseError);
        }
        
        // Try to re-establish IDLE after a delay
        setTimeout(() => {
          setupIdleConnection(accountEmail, appPassword, aliases);
        }, 60000); // Retry after 1 minute
      }
    }, 25 * 60 * 1000); // 25 minutes
    
    // Handle connection errors
    idleClient.on('error', async (err) => {
      console.error(`IDLE connection error for ${accountEmail}:`, err);
      
      // Clean up
      clearInterval(idleRefreshInterval);
      
      // Try to release the connection
      try {
        await imapConnectionPool.releaseConnection(accountEmail, idleClient);
      } catch (releaseError) {
        console.error(`Error releasing IDLE connection after error:`, releaseError);
      }
      
      // Try to re-establish after a delay
      setTimeout(() => {
        setupIdleConnection(accountEmail, appPassword, aliases);
      }, 60000); // Retry after 1 minute
    });
    
  } catch (error) {
    console.error(`Error setting up IDLE connection for ${accountEmail}:`, error);
    
    // Release the connection if it was created
    if (idleClient) {
      try {
        await imapConnectionPool.releaseConnection(accountEmail, idleClient);
      } catch (releaseError) {
        console.error(`Error releasing IDLE connection:`, releaseError);
      }
    }
    
    // Try again later
    setTimeout(() => {
      setupIdleConnection(accountEmail, appPassword, aliases);
    }, 5 * 60 * 1000); // Retry after 5 minutes
  }
}

/**
 * Initialize the IMAP service
 * Loads accounts from database and starts polling
 */
export async function initializeImapService() {
  try {
    console.log('Initializing Gmail IMAP service...');
    
    // Get all active accounts from database
    const [accounts] = await pool.query(`
      SELECT id, email, app_password, alias_count, status
      FROM gmail_accounts
      WHERE status = 'active'
    `);
    
    console.log(`Found ${accounts.length} active Gmail accounts`);
    
    // Setup polling for each account
    for (const account of accounts) {
      if (!inMemoryStore.activePolling.has(account.email)) {
        console.log(`Starting polling for account: ${account.email}`);
        schedulePolling(account.email, account.app_password);
        inMemoryStore.activePolling.add(account.email);
      }
    }
    
    // Setup maintenance task
    setInterval(async () => {
      await maintainImapConnections();
    }, 15 * 60 * 1000); // Every 15 minutes
    
    console.log('Gmail IMAP service initialized successfully');
  } catch (error) {
    console.error('Failed to initialize IMAP service:', error);
    throw error;
  }
}

/**
 * Maintain IMAP connections - restart polling for accounts that should be active
 */
async function maintainImapConnections() {
  try {
    console.log('Running IMAP connection maintenance...');
    
    // Get all active accounts from database
    const [accounts] = await pool.query(`
      SELECT id, email, app_password, alias_count, status
      FROM gmail_accounts
      WHERE status = 'active'
    `);
    
    // Check for accounts that should be polling but aren't
    for (const account of accounts) {
      if (!inMemoryStore.activePolling.has(account.email)) {
        console.log(`Restarting polling for active account: ${account.email}`);
        schedulePolling(account.email, account.app_password);
        inMemoryStore.activePolling.add(account.email);
      }
    }
    
    // Check for accounts that are polling but should not be
    for (const email of inMemoryStore.activePolling) {
      const matchingAccount = accounts.find(acc => acc.email === email);
      
      if (!matchingAccount || matchingAccount.status !== 'active') {
        console.log(`Stopping polling for inactive account: ${email}`);
        inMemoryStore.activePolling.delete(email);
      }
    }
    
    console.log('IMAP connection maintenance completed');
  } catch (error) {
    console.error('Error during IMAP connection maintenance:', error);
  }
}

/**
 * Get statistics about Gmail accounts and cache
 * @returns {Promise<Object>} - Statistics object
 */
export async function getGmailAccountStats() {
  try {
    // Get all accounts from database
    const [accounts] = await pool.query(`
      SELECT id, email, status, quota_used, alias_count, last_used, updated_at
      FROM gmail_accounts
    `);
    
    return {
      totalAccounts: accounts.length,
      activeAccounts: accounts.filter(a => a.status === 'active').length,
      activePolling: inMemoryStore.activePolling.size,
      totalAliases: inMemoryStore.aliases.size,
      accounts: accounts.map(account => ({
        id: account.id,
        email: account.email,
        status: account.status,
        aliasCount: account.alias_count,
        inMemoryAliasCount: Array.from(inMemoryStore.aliases.values())
          .filter(data => data.parentAccount === account.email).length,
        polling: inMemoryStore.activePolling.has(account.email),
        quotaUsed: account.quota_used,
        lastUsed: account.last_used
      }))
    };
  } catch (error) {
    console.error('Failed to get Gmail account stats:', error);
    return {
      error: error.message
    };
  }
}

/**
 * Get statistics about the email cache
 * @returns {Object} - Cache statistics
 */
export function getEmailCacheStats() {
  // Count emails per alias
  const emailsByAlias = new Map();
  for (const [key] of inMemoryStore.emailCache.entries()) {
    const alias = key.split(':')[0];
    emailsByAlias.set(alias, (emailsByAlias.get(alias) || 0) + 1);
  }
  
  // Get top aliases by email count
  const topAliases = Array.from(emailsByAlias.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([alias, count]) => ({ alias, count }));
  
  return {
    size: inMemoryStore.emailCache.size,
    maxSize: inMemoryStore.maxEmailCacheSize,
    aliasesWithEmails: emailsByAlias.size,
    topAliases
  };
}

// Export in-memory store for debugging and testing
export const debugStores = {
  emailCache: inMemoryStore.emailCache,
  aliases: inMemoryStore.aliases,
  activePolling: inMemoryStore.activePolling,
  wsConnections: inMemoryStore.wsConnections
};
