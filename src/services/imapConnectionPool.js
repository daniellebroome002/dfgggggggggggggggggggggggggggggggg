import { ImapFlow } from 'imapflow';

/**
 * IMAP Connection Pool Manager - OPTIMIZED FOR MAXIMUM SPEED
 * 
 * Handles creating, tracking, and reusing IMAP connections to Gmail accounts
 * to avoid "Too many simultaneous connections" errors.
 */
class ImapConnectionPool {
  constructor() {
    // Connection pool - key: email, value: { client, lastUsed, inUse, created }
    this.connections = new Map();
    
    // Ultra-fast settings - more connections for speed
    this.maxConnectionsPerAccount = 5;
    this.maxHighPriorityConnections = 5;
    
    // Account priority cache (email -> priority)
    this.accountPriorities = new Map();
    
    // Shorter connection timeout for faster recovery
    this.connectionTimeout = 2 * 60 * 1000; // 2 minutes (reduced from 3 minutes)
    
    // Fast cleanup interval
    this.cleanupInterval = setInterval(() => this.cleanupConnections(), 20 * 1000); // every 20 seconds (faster cleanup)
    
    console.log('⚡ IMAP Connection Pool initialized with connection limits to avoid Gmail errors');
  }
  
  /**
   * Set account priority level
   * @param {string} email - Gmail account email
   * @param {string} priority - Priority level (high, medium, low)
   */
  setAccountPriority(email, priority) {
    this.accountPriorities.set(email, priority);
  }
  
  /**
   * Get maximum connections allowed for an account based on priority
   * @param {string} email - Gmail account email
   * @returns {number} - Maximum connections
   */
  getMaxConnectionsForAccount(email) {
    // Always return max priority connections for all accounts to eliminate inconsistency
    return this.maxHighPriorityConnections;
  }
  
  /**
   * Get an IMAP connection for a Gmail account
   * @param {string} email - Gmail account email
   * @param {string} password - App password
   * @param {boolean} forIdle - Whether this connection will be used for IDLE
   * @returns {Promise<ImapFlow>} - IMAP client
   */
  async getConnection(email, password, forIdle = false) {
    // IDLE connections get priority - create new if needed
    if (forIdle) {
      return await this.createConnection(email, password, true);
    }
    
    // Check if we have an available connection for this account
    if (this.connections.has(email)) {
      const connectionData = this.connections.get(email);
      
      // If the connection exists and is not in use, return it
      if (connectionData && !connectionData.inUse && connectionData.client) {
        try {
          // Check if connection is still alive
          if (!connectionData.client.usable) {
            await this.closeConnection(email);
            return await this.createConnection(email, password, forIdle);
          }
          
          // Mark as in use and update lastUsed timestamp
          connectionData.inUse = true;
          connectionData.lastUsed = Date.now();
          connectionData.forIdle = forIdle;
          this.connections.set(email, connectionData);
          
          return connectionData.client;
        } catch (error) {
          await this.closeConnection(email);
          return await this.createConnection(email, password, forIdle);
        }
      }
      
      // If all connections are in use but we haven't reached max, create a new one
      const accountConnections = Array.from(this.connections.entries())
        .filter(([key]) => key === email);
      
      const maxConnections = this.getMaxConnectionsForAccount(email);
      
      if (accountConnections.length < maxConnections) {
        return await this.createConnection(email, password, forIdle);
      }
      
      // If we've reached max connections and this is for IDLE, force a connection
      if (forIdle) {
        // Close any non-IDLE connection to make room
        const nonIdleConnections = accountConnections.filter(([_, data]) => !data.forIdle);
        if (nonIdleConnections.length > 0) {
          const [oldestKey, oldestData] = nonIdleConnections
            .sort(([_, a], [__, b]) => a.lastUsed - b.lastUsed)[0];
          
          try {
            await oldestData.client.logout();
          } catch (error) {
            // Ignore connection errors
          }
          
          this.connections.delete(oldestKey);
          return await this.createConnection(email, password, true);
        }
        
        // If all are IDLE, create a new one anyway - IDLE is critical
        return await this.createConnection(email, password, true);
      }
      
      // If we've reached max connections, wait for one to become available
      return await this.waitForAvailableConnection(email, password, forIdle);
    }
    
    // If no connection exists for this account, create a new one
    return await this.createConnection(email, password, forIdle);
  }
  
  /**
   * Create a new IMAP connection - optimized settings
   * @param {string} email - Gmail account email
   * @param {string} password - App password
   * @param {boolean} forIdle - Whether this connection will be used for IDLE
   * @returns {Promise<ImapFlow>} - IMAP client
   */
  async createConnection(email, password, forIdle = false) {
    try {
      // Create new connection with retry logic
      const client = new ImapFlow({
        host: 'imap.gmail.com',
        port: 993,
        secure: true,
        auth: {
          user: email,
          pass: password
        },
        logger: false,
        // Performance optimized settings
        emitLogs: false,
        disableAutoIdle: !forIdle, // Enable auto-IDLE for IDLE connections
        timeoutConnection: 15000, // 15 second connection timeout (reduced)
        timeoutAuth: 15000,      // 15 second auth timeout (reduced)
        timeoutIdle: forIdle ? 20 * 60 * 1000 : 10 * 60 * 1000, // IDLE gets longer timeout
        tls: {
          rejectUnauthorized: true,
          enableTrace: false,
          minVersion: 'TLSv1.2',
          maxVersion: 'TLSv1.3'
        }
      });
      
      // Connect to the server
      await client.connect();
      
      // Store the connection in the pool
      this.connections.set(email, {
        client,
        lastUsed: Date.now(),
        inUse: true,
        created: Date.now(),
        forIdle
      });
      
      return client;
    } catch (error) {
      // Throw with clearer message
      throw error;
    }
  }
  
  /**
   * Release a connection back to the pool
   * @param {string} email - Gmail account email
   * @param {ImapFlow} client - The client to release
   */
  releaseConnection(email, client) {
    // Find the specific connection
    if (this.connections.has(email)) {
      const connectionData = this.connections.get(email);
      
      // Only release if it's the same client object
      if (connectionData && connectionData.client === client) {
        // Mark as not in use and update lastUsed timestamp
        connectionData.inUse = false;
        connectionData.lastUsed = Date.now();
        this.connections.set(email, connectionData);
      }
    }
  }
  
  /**
   * Wait for an available connection - faster timeouts
   * @param {string} email - Gmail account email
   * @param {string} password - App password
   * @param {boolean} forIdle - Whether this connection will be used for IDLE
   * @returns {Promise<ImapFlow>} - IMAP client
   */
  async waitForAvailableConnection(email, password, forIdle = false) {
    return new Promise((resolve) => {
      // Check every 100ms for an available connection (ultra-fast)
      const checkInterval = setInterval(async () => {
        const connections = Array.from(this.connections.entries())
          .filter(([key, data]) => key === email && !data.inUse);
        
        if (connections.length > 0) {
          clearInterval(checkInterval);
          
          const [, connectionData] = connections[0];
          connectionData.inUse = true;
          connectionData.lastUsed = Date.now();
          connectionData.forIdle = forIdle;
          this.connections.set(email, connectionData);
          
          resolve(connectionData.client);
        }
      }, 100); // Ultra-fast check interval
      
      // Timeout after 3 seconds (down from 6) for faster response
      setTimeout(async () => {
        clearInterval(checkInterval);
        
        // Force create a new connection
        try {
          // Try to close any old connections first
          const accountConnections = Array.from(this.connections.entries())
            .filter(([key]) => key === email)
            .sort((a, b) => a[1].lastUsed - b[1].lastUsed);
          
          if (accountConnections.length > 0) {
            // Prefer closing non-IDLE connections if this is for IDLE
            if (forIdle && accountConnections.some(([_, data]) => !data.forIdle)) {
              const oldestNonIdle = accountConnections
                .filter(([_, data]) => !data.forIdle)
                .sort((a, b) => a[1].lastUsed - b[1].lastUsed)[0];
                
              await this.closeConnection(oldestNonIdle[0]);
            } else {
              // Otherwise close the oldest connection
              await this.closeConnection(accountConnections[0][0]);
            }
          }
          
          // Create a new connection
          const client = await this.createConnection(email, password, forIdle);
          resolve(client);
        } catch (error) {
          throw error;
        }
      }, 3000); // Reduced timeout
    });
  }
  
  /**
   * Close a specific connection
   * @param {string} email - Gmail account email
   */
  async closeConnection(email) {
    if (this.connections.has(email)) {
      const connectionData = this.connections.get(email);
      
      if (connectionData && connectionData.client) {
        try {
          // Better connection closing based on state with error handling
          try {
            if (connectionData.client.usable) {
              await connectionData.client.logout();
            } else if (connectionData.client._socket && connectionData.client._socket.writable) {
              await connectionData.client.close();
            }
          } catch (error) {
            // Only log meaningful errors, not NoConnection which is expected sometimes
            if (error.code !== 'NoConnection') {
              console.error(`Error closing IMAP connection:`, error.message);
            }
          }
        } catch (outerError) {
          // Catch-all error handler
          if (outerError.code !== 'NoConnection') {
            console.error(`Outer error in connection closing:`, outerError.message);
          }
        }
      }
      
      // Always remove from pool regardless of errors
      this.connections.delete(email);
    }
  }
  
  /**
   * Clean up idle connections - more aggressive
   */
  async cleanupConnections() {
    const now = Date.now();
    const totalConnections = this.connections.size;
    
    // Skip if no connections
    if (totalConnections === 0) return;
    
    for (const [email, connectionData] of this.connections.entries()) {
      // Skip connections that are in use
      if (connectionData.inUse) continue;
      
      // Skip IDLE connections that are newer than 15 minutes (reduced from 20)
      if (connectionData.forIdle && now - connectionData.created < 15 * 60 * 1000) continue;
      
      // If connection is idle for too long, close it
      if (now - connectionData.lastUsed > this.connectionTimeout) {
        await this.closeConnection(email);
      }
      // If connection is too old (approaching Gmail's timeouts), close it
      else if (now - connectionData.created > 20 * 60 * 1000) { // 20 minutes (reduced from 25)
        await this.closeConnection(email);
      }
    }
  }
  
  /**
   * Get connection stats for monitoring
   */
  getStats() {
    const stats = {
      totalConnections: this.connections.size,
      accountsWithConnections: 0,
      connectionsPerAccount: {},
      idleConnections: 0
    };
    
    const accountCounts = {};
    
    for (const [email, data] of this.connections.entries()) {
      if (!accountCounts[email]) {
        accountCounts[email] = { total: 0, idle: 0, inUse: 0 };
        stats.accountsWithConnections++;
      }
      
      accountCounts[email].total++;
      
      if (data.forIdle) {
        accountCounts[email].idle++;
        stats.idleConnections++;
      }
      
      if (data.inUse) {
        accountCounts[email].inUse++;
      }
    }
    
    stats.connectionsPerAccount = accountCounts;
    return stats;
  }
}

// Create singleton instance
const imapConnectionPool = new ImapConnectionPool();

export default imapConnectionPool;
