import { ImapFlow } from 'imapflow';

/**
 * Enhanced IMAP Connection Pool Manager
 * 
 * Optimized for performance, reliability and efficient connection reuse
 * with intelligent connection management and error recovery.
 */
class ImapConnectionPool {
  constructor() {
    // Connection pool - key: email, value: { client, lastUsed, inUse, created, health }
    this.connections = new Map();
    
    // Connection limits - balanced for performance and compliance
    this.maxConnectionsPerAccount = 3;  // Reduced from 5 to be more conservative
    this.maxTotalConnections = 30;      // Set overall connection limit
    
    // Connection health tracking
    this.accountHealth = new Map();    // Track error rates and health
    
    // Shorter timeouts for better responsiveness
    this.connectionTimeout = 90 * 1000;  // 90 seconds (reduced from 120)
    this.operationTimeout = 30 * 1000;   // 30 seconds for operations
    
    // Statistics
    this.stats = {
      totalCreated: 0,
      totalReused: 0,
      totalErrors: 0,
      totalReleased: 0
    };
    
    // More frequent cleanup interval for better resource management
    this.cleanupInterval = setInterval(() => this.cleanupConnections(), 15 * 1000); // every 15 seconds
    
    // Regular health check interval
    this.healthCheckInterval = setInterval(() => this.performHealthCheck(), 60 * 1000); // every minute
    
    console.log('⚡ Enhanced IMAP Connection Pool initialized with optimized settings');
  }
  
  /**
   * Track account health and error rates
   * @param {string} email - Gmail account email
   * @param {string} status - 'success' or 'error'
   * @param {Error} [error] - Optional error object
   */
  trackHealth(email, status, error = null) {
    if (!this.accountHealth.has(email)) {
      this.accountHealth.set(email, {
        successCount: 0,
        errorCount: 0,
        consecutiveErrors: 0,
        lastError: null,
        lastStatus: null,
        lastUpdated: Date.now(),
        backoffUntil: 0
      });
    }
    
    const health = this.accountHealth.get(email);
    health.lastUpdated = Date.now();
    health.lastStatus = status;
    
    if (status === 'success') {
      health.successCount++;
      health.consecutiveErrors = 0;
    } else if (status === 'error') {
      health.errorCount++;
      health.consecutiveErrors++;
      health.lastError = error ? error.message : 'Unknown error';
      
      // Implement exponential backoff for accounts with consecutive errors
      if (health.consecutiveErrors > 3) {
        const backoffTime = Math.min(
          2 ** (health.consecutiveErrors - 3) * 5000, // Exponential backoff starting at 5s
          15 * 60 * 1000 // Max 15 minutes
        );
        health.backoffUntil = Date.now() + backoffTime;
      }
    }
  }
  
  /**
   * Check if account is in backoff period
   * @param {string} email - Gmail account email
   * @returns {boolean} - Whether the account is in backoff
   */
  isInBackoff(email) {
    if (!this.accountHealth.has(email)) return false;
    return this.accountHealth.get(email).backoffUntil > Date.now();
  }
  
  /**
   * Get maximum connections allowed for an account
   * @param {string} email - Gmail account email
   * @returns {number} - Maximum connections
   */
  getMaxConnectionsForAccount(email) {
    // If in backoff, only allow one connection
    if (this.isInBackoff(email)) return 1;
    
    // Otherwise use standard limit
    return this.maxConnectionsPerAccount;
  }
  
  /**
   * Get an IMAP connection for a Gmail account with improved connection sharing
   * @param {string} email - Gmail account email
   * @param {string} password - App password
   * @param {boolean} forIdle - Whether this connection will be used for IDLE
   * @param {Object} options - Additional options
   * @returns {Promise<ImapFlow>} - IMAP client
   */
  async getConnection(email, password, forIdle = false, options = {}) {
    // Check if account is in backoff period
    if (this.isInBackoff(email) && !options.bypassBackoff) {
      const health = this.accountHealth.get(email);
      const remainingBackoff = Math.ceil((health.backoffUntil - Date.now()) / 1000);
      
      console.log(`Account ${email} is in backoff period (${remainingBackoff}s remaining). Using conservative connection approach.`);
    }
    
    // IDLE connections are prioritized
    if (forIdle) {
      try {
        return await this.createOrReuseConnection(email, password, true);
      } catch (error) {
        this.trackHealth(email, 'error', error);
        this.stats.totalErrors++;
        throw error;
      }
    }
    
    // Try to find an available non-idle connection
    try {
      const existingConnections = Array.from(this.connections.entries())
        .filter(([key, data]) => 
          key === email && 
          !data.inUse && 
          !data.forIdle &&
          data.client && 
          data.client.usable);
      
      if (existingConnections.length > 0) {
        // Reuse existing connection
        const [, connectionData] = existingConnections[0];
        connectionData.inUse = true;
        connectionData.lastUsed = Date.now();
        this.stats.totalReused++;
        
        // Perform a quick check to ensure connection is really alive
        if (options.testConnection) {
          try {
            await this.testConnectionAliveness(connectionData.client);
          } catch (error) {
            // If test fails, close and create a new connection
            console.log(`Connection for ${email} failed aliveness test, creating new one`);
            await this.closeConnection(email, connectionData.client);
            return await this.createOrReuseConnection(email, password, forIdle);
          }
        }
        
        return connectionData.client;
      }
      
      // If no available connection, create a new one
      return await this.createOrReuseConnection(email, password, forIdle);
    } catch (error) {
      this.trackHealth(email, 'error', error);
      this.stats.totalErrors++;
      throw error;
    }
  }
  
  /**
   * Create a new or reuse an existing connection
   * @param {string} email - Gmail account email
   * @param {string} password - App password
   * @param {boolean} forIdle - Whether this connection will be used for IDLE
   * @returns {Promise<ImapFlow>} - IMAP client
   */
  async createOrReuseConnection(email, password, forIdle) {
    // Check total connection count
    if (this.connections.size >= this.maxTotalConnections) {
      // Find and close least recently used connection
      const connectionEntries = Array.from(this.connections.entries())
        .filter(([, data]) => !data.inUse)
        .sort((a, b) => a[1].lastUsed - b[1].lastUsed);
      
      if (connectionEntries.length > 0) {
        const [oldestKey, oldestData] = connectionEntries[0];
        try {
          await this.closeConnection(oldestKey, oldestData.client);
          console.log(`Closed oldest connection for ${oldestKey} to make room for new connection`);
        } catch (error) {
          console.error(`Error closing oldest connection: ${error.message}`);
        }
      }
    }
    
    // Count existing connections for this account
    const accountConnections = Array.from(this.connections.entries())
      .filter(([key]) => key === email);
    
    const maxConnections = this.getMaxConnectionsForAccount(email);
    
    if (accountConnections.length >= maxConnections) {
      // If we already have max connections, try to reuse one
      const availableConnections = accountConnections
        .filter(([, data]) => !data.inUse)
        .sort((a, b) => {
          // Prefer non-IDLE over IDLE if we need a non-IDLE connection
          if (!forIdle) {
            if (!a[1].forIdle && b[1].forIdle) return -1;
            if (a[1].forIdle && !b[1].forIdle) return 1;
          }
          // Otherwise sort by last used
          return a[1].lastUsed - b[1].lastUsed;
        });
      
      if (availableConnections.length > 0) {
        const [, connectionData] = availableConnections[0];
        connectionData.inUse = true;
        connectionData.lastUsed = Date.now();
        connectionData.forIdle = forIdle; // Update IDLE status
        this.stats.totalReused++;
        return connectionData.client;
      }
      
      // If all connections are in use, wait for one or close the oldest
      if (forIdle) {
        // For IDLE, prioritize closing a non-IDLE connection
        const nonIdleConnections = accountConnections
          .filter(([, data]) => !data.forIdle)
          .sort((a, b) => a[1].lastUsed - b[1].lastUsed);
        
        if (nonIdleConnections.length > 0) {
          const [oldestKey, oldestData] = nonIdleConnections[0];
          try {
            await this.closeConnection(oldestKey, oldestData.client);
          } catch (error) {
            console.error(`Error closing connection: ${error.message}`);
          }
        } else {
          // If all are IDLE, close the oldest
          const [oldestKey, oldestData] = accountConnections
            .sort((a, b) => a[1].lastUsed - b[1].lastUsed)[0];
          
          try {
            await this.closeConnection(oldestKey, oldestData.client);
          } catch (error) {
            console.error(`Error closing oldest connection: ${error.message}`);
          }
        }
      } else {
        // For non-IDLE, try to wait briefly for a connection
        try {
          return await this.waitForAvailableConnection(email, password, forIdle);
        } catch (error) {
          // If waiting fails, close the oldest connection
          const [oldestKey, oldestData] = accountConnections
            .sort((a, b) => a[1].lastUsed - b[1].lastUsed)[0];
          
          try {
            await this.closeConnection(oldestKey, oldestData.client);
          } catch (error) {
            console.error(`Error closing oldest connection: ${error.message}`);
          }
        }
      }
    }
    
    // Create new connection
    return await this.createConnection(email, password, forIdle);
  }
  
  /**
   * Test if a connection is still alive and responsive
   * @param {ImapFlow} client - The IMAP client to test
   * @returns {Promise<boolean>} - Whether the connection is alive
   */
  async testConnectionAliveness(client) {
    try {
      // Use noop command with a short timeout to test connection
      await Promise.race([
        client.noop(),
        new Promise((_, reject) => setTimeout(() => 
          reject(new Error('Connection test timeout')), 3000))
      ]);
      return true;
    } catch (error) {
      return false;
    }
  }
  
  /**
   * Create a new IMAP connection with optimized settings
   * @param {string} email - Gmail account email
   * @param {string} password - App password
   * @param {boolean} forIdle - Whether this connection will be used for IDLE
   * @returns {Promise<ImapFlow>} - IMAP client
   */
  async createConnection(email, password, forIdle = false) {
    // Determine if this account has had recent errors
    const hasRecentErrors = this.accountHealth.has(email) && 
                          this.accountHealth.get(email).consecutiveErrors > 0;
    
    try {
      // Create new connection with optimized settings
      const client = new ImapFlow({
        host: 'imap.gmail.com',
        port: 993,
        secure: true,
        auth: {
          user: email,
          pass: password
        },
        logger: false,
        emitLogs: false,
        
        // Connection settings - more conservative if account has had errors
        disableAutoIdle: !forIdle,
        timeoutConnection: hasRecentErrors ? 30000 : 15000, // 30s for problematic accounts
        timeoutAuth: hasRecentErrors ? 30000 : 15000,
        timeoutIdle: forIdle ? 25 * 60 * 1000 : 5 * 60 * 1000, // 25 min for IDLE, 5 min otherwise
        
        // TLS settings
        tls: {
          rejectUnauthorized: true,
          enableTrace: false,
          minVersion: 'TLSv1.2',
          maxVersion: 'TLSv1.3',
          servername: 'imap.gmail.com', // Explicitly set SNI
        },
        
        // Use CONDSTORE for change detection if available
        allowCondStore: true,
        
        // IMAP extensions to use
        enableCompression: true // Enable COMPRESS=DEFLATE if server supports it
      });
      
      // Add custom event handlers for better error detection
      client.on('error', (err) => {
        console.error(`IMAP error on ${email} connection:`, err.message);
        this.trackHealth(email, 'error', err);
        
        // If this is a fatal connection error, mark as unusable
        if (err.code === 'ECONNRESET' || 
            err.code === 'ECONNREFUSED' || 
            err.code === 'ETIMEDOUT') {
          const connectionData = Array.from(this.connections.entries())
            .find(([key, data]) => key === email && data.client === client);
            
          if (connectionData) {
            const [, data] = connectionData;
            data.unusable = true;
          }
        }
      });
      
      // Listen for successful actions to track health
      client.on('mailboxOpen', () => {
        this.trackHealth(email, 'success');
      });
      
      // Connect with timeout
      await Promise.race([
        client.connect(),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('Connection timeout')), hasRecentErrors ? 30000 : 20000)
        )
      ]);
      
      // Store the connection in the pool with health info
      this.connections.set(`${email}:${Date.now()}`, {
        client,
        lastUsed: Date.now(),
        inUse: true,
        created: Date.now(),
        forIdle,
        health: {
          errorCount: 0,
          successCount: 0
        },
        unusable: false
      });
      
      this.stats.totalCreated++;
      this.trackHealth(email, 'success');
      
      return client;
    } catch (error) {
      // Track connection errors by type
      this.stats.totalErrors++;
      this.trackHealth(email, 'error', error);
      
      // Enhance the error with diagnostic info
      const enhancedError = new Error(`IMAP connection error for ${email}: ${error.message}`);
      enhancedError.originalError = error;
      enhancedError.email = email;
      
      throw enhancedError;
    }
  }
  
  /**
   * Release a connection back to the pool with health verification
   * @param {string} email - Gmail account email
   * @param {ImapFlow} client - The client to release
   */
  async releaseConnection(email, client) {
    // Find the specific connection entry
    const connectionEntries = Array.from(this.connections.entries())
      .filter(([key, data]) => 
        key.startsWith(`${email}:`) && 
        data.client === client);
      
    if (connectionEntries.length === 0) {
      console.warn(`Attempted to release unknown connection for ${email}`);
      return;
    }
    
    const [connectionKey, connectionData] = connectionEntries[0];
    
    // If connection is marked as unusable, close it
    if (connectionData.unusable) {
      console.log(`Closing unusable connection for ${email}`);
      await this.closeConnection(connectionKey, client);
      return;
    }
    
    // Test if connection is still usable (for non-IDLE connections)
    if (!connectionData.forIdle && connectionData.inUse) {
      try {
        // Quick check if connection is still responsive
        const isAlive = await this.testConnectionAliveness(client);
        
        if (!isAlive) {
          console.log(`Connection for ${email} failed health check, closing`);
          await this.closeConnection(connectionKey, client);
          return;
        }
      } catch (error) {
        console.warn(`Error testing connection health: ${error.message}`);
        await this.closeConnection(connectionKey, client);
        return;
      }
    }
    
    // Mark as not in use and update lastUsed timestamp
    connectionData.inUse = false;
    connectionData.lastUsed = Date.now();
    this.stats.totalReleased++;
    
    // Keep track of connection in the pool
    this.connections.set(connectionKey, connectionData);
  }
  
  /**
   * Wait for an available connection with improved timeout
   * @param {string} email - Gmail account email
   * @param {string} password - App password
   * @param {boolean} forIdle - Whether this connection will be used for IDLE
   * @returns {Promise<ImapFlow>} - IMAP client
   */
  async waitForAvailableConnection(email, password, forIdle = false) {
    return new Promise((resolve, reject) => {
      let resolved = false;
      let timer;
      
      // Check every 50ms for an available connection (faster than before)
      const checkInterval = setInterval(() => {
        const availableConnections = Array.from(this.connections.entries())
          .filter(([key, data]) => 
            key.startsWith(`${email}:`) && 
            !data.inUse && 
            !data.unusable &&
            data.client && 
            data.client.usable);
        
        if (availableConnections.length > 0) {
          clearInterval(checkInterval);
          clearTimeout(timer);
          
          if (resolved) return; // Prevent double resolution
          resolved = true;
          
          const [connectionKey, connectionData] = availableConnections[0];
          connectionData.inUse = true;
          connectionData.lastUsed = Date.now();
          connectionData.forIdle = forIdle;
          this.connections.set(connectionKey, connectionData);
          this.stats.totalReused++;
          
          resolve(connectionData.client);
        }
      }, 50); // Faster check interval
      
      // Timeout after 2 seconds (reduced from 3s)
      timer = setTimeout(() => {
        clearInterval(checkInterval);
        
        if (resolved) return; // Prevent double resolution
        resolved = true;
        
        // If waiting fails, try to create a new connection
        this.createConnection(email, password, forIdle)
          .then(client => resolve(client))
          .catch(error => reject(error));
      }, 2000);
    });
  }
  
  /**
   * Close a specific connection with improved error handling
   * @param {string} connectionKey - Connection key in the pool
   * @param {ImapFlow} client - The client to close
   */
  async closeConnection(connectionKey, client) {
    if (!this.connections.has(connectionKey)) {
      // If we have a client but no key, try to find it
      if (client) {
        const entries = Array.from(this.connections.entries())
          .filter(([, data]) => data.client === client);
          
        if (entries.length > 0) {
          connectionKey = entries[0][0];
        } else {
          // If we can't find the connection, try to close the client directly
          try {
            if (client.usable) await client.logout();
            else if (client._socket && client._socket.writable) await client.close();
          } catch (error) {
            // Ignore errors, just log them
            if (error.code !== 'NoConnection') {
              console.warn(`Error directly closing IMAP client: ${error.message}`);
            }
          }
          return;
        }
      } else {
        // No key and no client, nothing to do
        return;
      }
    }
    
    const connectionData = this.connections.get(connectionKey);
    if (!connectionData || !connectionData.client) {
      this.connections.delete(connectionKey);
      return;
    }
    
    try {
      const closePromise = new Promise(async (resolve) => {
        try {
          // Try graceful logout first if connection is usable
          if (connectionData.client.usable) {
            await connectionData.client.logout();
          } 
          // Fallback to force close if needed
          else if (connectionData.client._socket && connectionData.client._socket.writable) {
            await connectionData.client.close();
          }
        } catch (error) {
          // Ignore expected errors
          if (error.code !== 'NoConnection') {
            console.warn(`Error during connection close: ${error.message}`);
          }
        }
        resolve();
      });
      
      // Add timeout to prevent hanging on close
      await Promise.race([
        closePromise,
        new Promise(resolve => setTimeout(resolve, 3000))
      ]);
    } catch (error) {
      // Catch any unexpected errors during close
      console.warn(`Unexpected error closing connection: ${error.message}`);
    } finally {
      // Always remove from pool
      this.connections.delete(connectionKey);
    }
  }
  
  /**
   * Clean up idle and old connections with improved monitoring
   */
  async cleanupConnections() {
    const now = Date.now();
    const totalBefore = this.connections.size;
    
    // Create a list of connections to close
    const toClose = [];
    
    for (const [key, data] of this.connections.entries()) {
      // Skip connections that are in use
      if (data.inUse) continue;
      
      // Close connections marked as unusable
      if (data.unusable) {
        toClose.push([key, data.client]);
        continue;
      }
      
      // Close idle connections after timeout
      const idleTime = now - data.lastUsed;
      if (idleTime > this.connectionTimeout) {
        toClose.push([key, data.client]);
        continue;
      }
      
      // Close connections that are too old (approaching Gmail's timeouts)
      const age = now - data.created;
      const maxAge = data.forIdle ? 25 * 60 * 1000 : 15 * 60 * 1000; // 25 min for IDLE, 15 min otherwise
      
      if (age > maxAge) {
        toClose.push([key, data.client]);
        continue;
      }
    }
    
    // Close all identified connections
    for (const [key, client] of toClose) {
      try {
        await this.closeConnection(key, client);
      } catch (error) {
        console.error(`Error during connection cleanup: ${error.message}`);
      }
    }
    
    const closedCount = totalBefore - this.connections.size;
    if (closedCount > 0) {
      console.log(`Cleaned up ${closedCount} idle or old IMAP connections`);
    }
  }
  
  /**
   * Perform health check on all connections
   */
  async performHealthCheck() {
    const accountEmails = new Set();
    
    // Collect all unique account emails
    for (const key of this.connections.keys()) {
      const email = key.split(':')[0];
      accountEmails.add(email);
    }
    
    // Check health of each account
    for (const email of accountEmails) {
      const connectionEntries = Array.from(this.connections.entries())
        .filter(([key]) => key.startsWith(`${email}:`));
      
      const activeCount = connectionEntries.filter(([, data]) => data.inUse).length;
      const idleCount = connectionEntries.filter(([, data]) => !data.inUse).length;
      const idleConnectionEntries = connectionEntries.filter(([, data]) => !data.inUse);
      
      // Test one idle connection to ensure the account is still working properly
      if (idleConnectionEntries.length > 0) {
        const [key, data] = idleConnectionEntries[0];
        
        try {
          const isAlive = await this.testConnectionAliveness(data.client);
          if (!isAlive) {
            console.log(`Health check: Connection for ${email} failed, closing`);
            await this.closeConnection(key, data.client);
          }
        } catch (error) {
          console.warn(`Health check error for ${email}: ${error.message}`);
        }
      }
      
      // Log account status
      if (this.accountHealth.has(email)) {
        const health = this.accountHealth.get(email);
        if (health.consecutiveErrors > 0) {
          console.log(`Account ${email} health: ${health.consecutiveErrors} consecutive errors, ${activeCount} active, ${idleCount} idle connections`);
        }
      }
    }
  }
  
  /**
   * Get detailed connection stats for monitoring
   * @returns {Object} - Connection statistics
   */
  getStats() {
    const now = Date.now();
    const stats = {
      totalConnections: this.connections.size,
      accountsWithConnections: new Set(
        Array.from(this.connections.keys()).map(key => key.split(':')[0])
      ).size,
      inUseConnections: 0,
      idleConnections: 0,
      idleConnections: Array.from(this.connections.values())
        .filter(data => !data.inUse).length,
      connectionsByAge: {
        lessThan1Min: 0,
        lessThan5Min: 0,
        lessThan15Min: 0,
        moreThan15Min: 0
      },
      idleConnectionsByTime: {
        lessThan30s: 0,
        lessThan1Min: 0,
        lessThan2Min: 0,
        moreThan2Min: 0
      },
      connectionsPerAccount: {},
      errorRates: {},
      performance: this.stats
    };
    
    // Collect detailed stats
    for (const [key, data] of this.connections.entries()) {
      const email = key.split(':')[0];
      
      // Count in-use connections
      if (data.inUse) {
        stats.inUseConnections++;
      }
      
      // Track age distribution
      const age = now - data.created;
      if (age < 60 * 1000) stats.connectionsByAge.lessThan1Min++;
      else if (age < 5 * 60 * 1000) stats.connectionsByAge.lessThan5Min++;
      else if (age < 15 * 60 * 1000) stats.connectionsByAge.lessThan15Min++;
      else stats.connectionsByAge.moreThan15Min++;
      
      // Track idle time distribution
      if (!data.inUse) {
        const idleTime = now - data.lastUsed;
        if (idleTime < 30 * 1000) stats.idleConnectionsByTime.lessThan30s++;
        else if (idleTime < 60 * 1000) stats.idleConnectionsByTime.lessThan1Min++;
        else if (idleTime < 2 * 60 * 1000) stats.idleConnectionsByTime.lessThan2Min++;
        else stats.idleConnectionsByTime.moreThan2Min++;
      }
      
      // Count connections per account
      if (!stats.connectionsPerAccount[email]) {
        stats.connectionsPerAccount[email] = {
          total: 0,
          idle: 0,
          inUse: 0,
          forIdle: 0
        };
      }
      
      stats.connectionsPerAccount[email].total++;
      
      if (data.inUse) stats.connectionsPerAccount[email].inUse++;
      else stats.connectionsPerAccount[email].idle++;
      
      if (data.forIdle) stats.connectionsPerAccount[email].forIdle++;
    }
    
    // Add account health stats
    for (const [email, health] of this.accountHealth.entries()) {
      stats.errorRates[email] = {
        successCount: health.successCount,
        errorCount: health.errorCount,
        errorRate: health.successCount + health.errorCount > 0
          ? (health.errorCount / (health.successCount + health.errorCount)).toFixed(2)
          : 0,
        consecutiveErrors: health.consecutiveErrors,
        lastStatus: health.lastStatus,
        lastError: health.lastError,
        inBackoff: this.isInBackoff(email),
        backoffRemaining: Math.max(0, (health.backoffUntil - now) / 1000).toFixed(0) + 's'
      };
    }
    
    return stats;
  }
  
  /**
   * Force close all connections and reset the pool
   * Useful for server shutdown or restart
   */
  async shutdown() {
    console.log(`Shutting down IMAP connection pool (${this.connections.size} connections)`);
    
    // Clear intervals
    clearInterval(this.cleanupInterval);
    clearInterval(this.healthCheckInterval);
    
    // Close all connections
    const closePromises = [];
    for (const [key, data] of this.connections.entries()) {
      closePromises.push(this.closeConnection(key, data.client));
    }
    
    // Wait for all connections to close (with timeout)
    await Promise.race([
      Promise.allSettled(closePromises),
      new Promise(resolve => setTimeout(resolve, 5000))
    ]);
    
    this.connections.clear();
    console.log('IMAP connection pool shutdown complete');
  }
}

// Create singleton instance
const imapConnectionPool = new ImapConnectionPool();

export default imapConnectionPool;
