import { ImapFlow } from 'imapflow';

/**
 * IMAP Connection Pool Manager
 * 
 * Handles creating, tracking, and reusing IMAP connections to Gmail accounts
 * to avoid "Too many simultaneous connections" errors.
 */
class ImapConnectionPool {
  constructor() {
    // Connection pool - key: email, value: { client, lastUsed, inUse, created }
    this.connections = new Map();
    
    // Maximum connections per account
    this.maxConnectionsPerAccount = 3;
    
    // Connection timeout in milliseconds (5 minutes)
    this.connectionTimeout = 5 * 60 * 1000;
    
    // Start the connection cleanup interval
    this.cleanupInterval = setInterval(() => this.cleanupConnections(), 60 * 1000);
    
    console.log('IMAP Connection Pool initialized');
  }
  
  /**
   * Get an IMAP connection for a Gmail account
   * @param {string} email - Gmail account email
   * @param {string} password - App password
   * @returns {Promise<ImapFlow>} - IMAP client
   */
  async getConnection(email, password) {
    console.log(`Requesting IMAP connection for ${email}`);
    
    // Check if we have an available connection for this account
    if (this.connections.has(email)) {
      const connectionData = this.connections.get(email);
      
      // If the connection exists and is not in use, return it
      if (connectionData && !connectionData.inUse && connectionData.client) {
        console.log(`Reusing existing IMAP connection for ${email}`);
        
        try {
          // Check if connection is still alive
          if (!connectionData.client.usable) {
            console.log(`Connection for ${email} is no longer usable, creating new one`);
            await this.closeConnection(email);
            return await this.createConnection(email, password);
          }
          
          // Mark as in use and update lastUsed timestamp
          connectionData.inUse = true;
          connectionData.lastUsed = Date.now();
          this.connections.set(email, connectionData);
          
          return connectionData.client;
        } catch (error) {
          console.error(`Error checking existing connection for ${email}:`, error);
          await this.closeConnection(email);
          return await this.createConnection(email, password);
        }
      }
      
      // If all connections are in use but we haven't reached max, create a new one
      const accountConnections = Array.from(this.connections.entries())
        .filter(([key]) => key === email);
      
      if (accountConnections.length < this.maxConnectionsPerAccount) {
        console.log(`All connections for ${email} in use, creating new one (${accountConnections.length + 1}/${this.maxConnectionsPerAccount})`);
        return await this.createConnection(email, password);
      }
      
      // If we've reached max connections, wait for one to become available
      console.log(`Reached max connections for ${email}, waiting for one to become available`);
      return await this.waitForAvailableConnection(email, password);
    }
    
    // If no connection exists for this account, create a new one
    return await this.createConnection(email, password);
  }
  
  /**
   * Create a new IMAP connection
   * @param {string} email - Gmail account email
   * @param {string} password - App password
   * @returns {Promise<ImapFlow>} - IMAP client
   */
  async createConnection(email, password) {
    console.log(`Creating new IMAP connection for ${email}`);
    
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
        // Increase timeouts for better reliability
        emitLogs: false,
        disableAutoIdle: true,
        timeoutConnection: 30000,
        timeoutAuth: 30000,
        timeoutIdle: 20 * 60 * 1000, // 20 minutes
      });
      
      // Connect to the server
      console.log(`Connecting to IMAP server for ${email}...`);
      await client.connect();
      
      // Store the connection in the pool
      this.connections.set(email, {
        client,
        lastUsed: Date.now(),
        inUse: true,
        created: Date.now()
      });
      
      console.log(`IMAP connection established for ${email}`);
      return client;
      
    } catch (error) {
      console.error(`Failed to create IMAP connection for ${email}:`, error);
      throw error;
    }
  }
  
  /**
   * Release a connection back to the pool
   * @param {string} email - Gmail account email
   */
  releaseConnection(email) {
    if (this.connections.has(email)) {
      const connectionData = this.connections.get(email);
      
      if (connectionData) {
        console.log(`Releasing IMAP connection for ${email} back to pool`);
        
        // Mark as not in use and update lastUsed timestamp
        connectionData.inUse = false;
        connectionData.lastUsed = Date.now();
        this.connections.set(email, connectionData);
      }
    }
  }
  
  /**
   * Wait for an available connection
   * @param {string} email - Gmail account email
   * @param {string} password - App password
   * @returns {Promise<ImapFlow>} - IMAP client
   */
  async waitForAvailableConnection(email, password) {
    return new Promise((resolve) => {
      // Check every 500ms for an available connection
      const checkInterval = setInterval(async () => {
        const connections = Array.from(this.connections.entries())
          .filter(([key, data]) => key === email && !data.inUse);
        
        if (connections.length > 0) {
          clearInterval(checkInterval);
          
          const [, connectionData] = connections[0];
          connectionData.inUse = true;
          connectionData.lastUsed = Date.now();
          this.connections.set(email, connectionData);
          
          resolve(connectionData.client);
        }
      }, 500);
      
      // Timeout after 10 seconds and create a new connection if needed
      setTimeout(async () => {
        clearInterval(checkInterval);
        
        console.log(`Timed out waiting for available connection for ${email}, force creating new one`);
        
        // Force close the oldest connection for this account
        const accountConnections = Array.from(this.connections.entries())
          .filter(([key]) => key === email)
          .sort((a, b) => a[1].lastUsed - b[1].lastUsed);
        
        if (accountConnections.length > 0) {
          const [oldestEmail] = accountConnections[0];
          await this.closeConnection(oldestEmail);
        }
        
        // Create a new connection
        try {
          const client = await this.createConnection(email, password);
          resolve(client);
        } catch (error) {
          console.error(`Failed to create new connection for ${email} after timeout:`, error);
          throw error;
        }
      }, 10000);
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
          console.log(`Closing IMAP connection for ${email}`);
          await connectionData.client.logout();
        } catch (error) {
          console.error(`Error closing IMAP connection for ${email}:`, error);
        }
      }
      
      // Remove from pool
      this.connections.delete(email);
    }
  }
  
  /**
   * Clean up idle connections
   */
  async cleanupConnections() {
    const now = Date.now();
    console.log(`Running IMAP connection cleanup, current pool size: ${this.connections.size}`);
    
    for (const [email, connectionData] of this.connections.entries()) {
      // Skip connections that are in use
      if (connectionData.inUse) {
        continue;
      }
      
      // If connection is idle for too long, close it
      if (now - connectionData.lastUsed > this.connectionTimeout) {
        console.log(`Closing idle IMAP connection for ${email} (idle for ${Math.floor((now - connectionData.lastUsed) / 1000)}s)`);
        await this.closeConnection(email);
      }
      // If connection is too old (approaching Gmail's 29-minute timeout), close it
      else if (now - connectionData.created > 25 * 60 * 1000) { // 25 minutes
        console.log(`Closing old IMAP connection for ${email} (age: ${Math.floor((now - connectionData.created) / 1000 / 60)}m)`);
        await this.closeConnection(email);
      }
    }
  }
  
  /**
   * Clean up all connections
   */
  async cleanup() {
    console.log('Cleaning up all IMAP connections');
    
    // Clear the cleanup interval
    clearInterval(this.cleanupInterval);
    
    // Close all connections
    for (const [email] of this.connections.entries()) {
      await this.closeConnection(email);
    }
  }
}

// Create singleton instance
const imapConnectionPool = new ImapConnectionPool();

export default imapConnectionPool;
