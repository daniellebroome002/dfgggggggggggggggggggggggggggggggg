import { v4 as uuidv4 } from 'uuid';
import WebSocket from 'ws';

// In-memory storage for active users and their activities
const activeUsers = {
  // Store active connections (WebSocket clients)
  clients: new Map(),
  // Store active users by IP address
  byIp: new Map(),
  // Store request counts by endpoint
  endpointStats: new Map(),
  // Store recent requests for display
  recentRequests: [],
  // Maximum number of recent requests to keep
  maxRecentRequests: 100,
  // Last update timestamp
  lastUpdate: Date.now()
};

// Time buckets for tracking activity (in milliseconds)
const TIME_BUCKETS = {
  FIFTEEN_MIN: 15 * 60 * 1000,
  THIRTY_MIN: 30 * 60 * 1000,
  ONE_HOUR: 60 * 60 * 1000,
  TWO_HOURS: 2 * 60 * 60 * 1000
};

// Data retention period - delete data older than this
const DATA_RETENTION_PERIOD = 2 * 60 * 60 * 1000; // 2 hours

// Track which URLs we've processed for WebSocket upgrades to avoid double processing
const processedSocketUrls = new Set();

// Create WebSocket server without attaching to HTTP server (noServer mode)
let wss;

// Initialize WebSocket Server
export function setupActivityTracker(server) {
  // Create WebSocket server without attaching to server
  wss = new WebSocket.Server({ noServer: true });

  console.log('Activity tracking WebSocket server initialized in noServer mode');

  // Handle WebSocket connection
  wss.on('connection', (ws, req) => {
    const clientId = uuidv4();
    const ip = req.headers['x-forwarded-for']?.split(',')[0].trim() || 
               req.connection.remoteAddress;

    console.log(`Activity monitor connected: ${clientId} from ${ip}`);
    
    // Store client connection
    activeUsers.clients.set(clientId, {
      ws,
      ip,
      isAdmin: true, // Assuming only admins connect to activity-ws
      connectedAt: Date.now()
    });

    // Send initial data
    sendSnapshot(ws);

    // Handle client disconnection
    ws.on('close', () => {
      console.log(`Activity monitor disconnected: ${clientId}`);
      activeUsers.clients.delete(clientId);
    });

    // Handle client messages (if any)
    ws.on('message', (message) => {
      try {
        const data = JSON.parse(message);
        
        // Handle admin commands like clearing stats
        if (data.command === 'clear-stats') {
          clearActivityStats();
          broadcastUpdate();
        }
      } catch (error) {
        console.error('Error processing WebSocket message:', error);
      }
    });
  });

  // Manage WebSocket upgrade - use a unique handler identifier
  const activityTrackerHandler = (request, socket, head) => {
    // Create a unique ID for this socket to prevent double processing
    const socketId = `${request.url}:${socket.remoteAddress}:${socket.remotePort}`;
    
    // Only handle WebSocket upgrade for our specific path and only if we haven't processed this socket
    if (request.url === '/activity-ws' && !processedSocketUrls.has(socketId)) {
      // Mark as processed to prevent double handling
      processedSocketUrls.add(socketId);
      
      // Clean up the set periodically to prevent memory leaks
      setTimeout(() => {
        processedSocketUrls.delete(socketId);
      }, 10000); // Remove after 10 seconds
      
      try {
        wss.handleUpgrade(request, socket, head, (ws) => {
          wss.emit('connection', ws, request);
        });
      } catch (error) {
        console.error('Error handling WebSocket upgrade for activity tracker:', error);
        
        // If we can't handle the upgrade, make sure we don't leave the socket hanging
        if (socket.readyState === socket.OPEN) {
          socket.destroy();
        }
      }
      
      // Return true to indicate we handled this upgrade
      return true;
    }
    
    // Return false to indicate we didn't handle this upgrade
    return false;
  };

  // Register our handler on the server
  server.on('upgrade', (request, socket, head) => {
    // Try to handle with activity tracker
    const handled = activityTrackerHandler(request, socket, head);
    
    // If our handler didn't process this, it will fall through to other handlers
    // No need to do anything here, as other handlers will check their own paths
  });

  // Start periodic broadcasting of updates to all clients
  setInterval(() => {
    // Only broadcast if there are connected clients
    if (activeUsers.clients.size > 0) {
      broadcastUpdate();
    }
    
    // Cleanup old data but less frequently
    const now = Date.now();
    if (now % (60 * 1000) < 5000) { // Run cleanup only once per minute
      cleanupOldData();
    }
  }, 5000); // Every 5 seconds
}

// Track user activity (called from middleware)
export function trackActivity(req, res) {
  const ip = req.headers['x-forwarded-for']?.split(',')[0].trim() || 
             req.connection.remoteAddress;
  const userId = req.user?.id || 'guest';
  const userAgent = req.headers['user-agent'] || '';
  const path = req.originalUrl || req.url;
  const method = req.method;
  const timestamp = Date.now();
  
  // Get or create user entry
  if (!activeUsers.byIp.has(ip)) {
    activeUsers.byIp.set(ip, {
      ip,
      firstSeen: timestamp,
      lastSeen: timestamp,
      requestCount: 0,
      paths: new Map(),
      userIds: new Set(),
      userAgents: new Set(),
      recentRequests: []
    });
  }
  
  const userInfo = activeUsers.byIp.get(ip);
  userInfo.lastSeen = timestamp;
  userInfo.requestCount++;
  
  // Track unique user IDs associated with this IP
  userInfo.userIds.add(userId);
  
  // Track user agents
  userInfo.userAgents.add(userAgent);
  
  // Track path usage
  if (!userInfo.paths.has(path)) {
    userInfo.paths.set(path, { count: 0 });
  }
  userInfo.paths.get(path).count++;
  
  // Add to global recent requests (limited to maxRecentRequests)
  const request = {
    id: uuidv4(),
    ip,
    userId,
    path,
    method,
    timestamp,
    userAgent
  };
  
  activeUsers.recentRequests.unshift(request);
  if (activeUsers.recentRequests.length > activeUsers.maxRecentRequests) {
    activeUsers.recentRequests.pop();
  }
  
  // Add to IP-specific recent requests
  userInfo.recentRequests.unshift(request);
  if (userInfo.recentRequests.length > 10) {
    userInfo.recentRequests.pop();
  }
  
  // Track endpoint statistics
  if (!activeUsers.endpointStats.has(path)) {
    activeUsers.endpointStats.set(path, {
      count: 0,
      methods: new Map()
    });
  }
  
  const pathStats = activeUsers.endpointStats.get(path);
  pathStats.count++;
  
  if (!pathStats.methods.has(method)) {
    pathStats.methods.set(method, 0);
  }
  pathStats.methods.set(method, pathStats.methods.get(method) + 1);
  
  // Update the last update timestamp
  activeUsers.lastUpdate = timestamp;

  // When a response is finished, capture status code
  res.on('finish', () => {
    request.statusCode = res.statusCode;
    
    // For any 4xx or 5xx responses, mark as error
    if (res.statusCode >= 400) {
      request.isError = true;
    }
  });
}

// Send snapshot of current data to a client
function sendSnapshot(ws) {
  try {
    const data = prepareActivityData();
    ws.send(JSON.stringify({
      type: 'snapshot',
      data
    }));
  } catch (error) {
    console.error('Error sending snapshot:', error);
  }
}

// Broadcast updates to all connected clients
function broadcastUpdate() {
  try {
    const data = prepareActivityData();
    const message = JSON.stringify({
      type: 'update',
      data
    });
    
    for (const client of activeUsers.clients.values()) {
      if (client.ws.readyState === WebSocket.OPEN) {
        client.ws.send(message);
      }
    }
  } catch (error) {
    console.error('Error broadcasting update:', error);
  }
}

// Prepare activity data for transmission, with time-based tracking
function prepareActivityData(page = 0, limit = 25) {
  const now = Date.now();
  
  // Get active IPs for different time frames
  const activeIps15m = getActiveIpsByTimeframe(TIME_BUCKETS.FIFTEEN_MIN);
  const activeIps30m = getActiveIpsByTimeframe(TIME_BUCKETS.THIRTY_MIN);
  const activeIps1h = getActiveIpsByTimeframe(TIME_BUCKETS.ONE_HOUR);
  const activeIps2h = getActiveIpsByTimeframe(TIME_BUCKETS.TWO_HOURS);
  
  // Get all IPs active in the last 15 minutes
  const allActiveIps = Array.from(activeUsers.byIp.values())
    .filter(user => (now - user.lastSeen) < TIME_BUCKETS.FIFTEEN_MIN)
    .map(user => ({
      ip: user.ip,
      requestCount: user.requestCount,
      lastSeen: user.lastSeen,
      uniquePathCount: user.paths.size,
      userIds: Array.from(user.userIds)
    }))
    .sort((a, b) => b.lastSeen - a.lastSeen);
  
  // Apply pagination to limit the number of IPs sent to the frontend
  const startIndex = page * limit;
  const activeIpsDetailed = allActiveIps.slice(startIndex, startIndex + limit);
  
  // Get top endpoints
  const topEndpoints = Array.from(activeUsers.endpointStats.entries())
    .map(([path, stats]) => ({
      path,
      count: stats.count,
      methods: Object.fromEntries(stats.methods)
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);
  
  // Get recent requests with formatted data
  const recentRequests = activeUsers.recentRequests
    .slice(0, 20)
    .map(req => ({
      id: req.id,
      ip: req.ip,
      path: req.path,
      method: req.method,
      timestamp: req.timestamp,
      statusCode: req.statusCode,
      isError: req.isError
    }));
  
  return {
    // Active users = unique IPs, per user request
    activeUsers15m: activeIps15m.length,
    activeUsers30m: activeIps30m.length,
    activeUsers1h: activeIps1h.length,
    activeUsers2h: activeIps2h.length,
    uniqueIps: allActiveIps.length,
    activeIps: activeIpsDetailed,
    totalPages: Math.ceil(allActiveIps.length / limit),
    currentPage: page,
    topEndpoints,
    recentRequests,
    lastUpdate: activeUsers.lastUpdate
  };
}

// Helper function to get active IPs by timeframe
function getActiveIpsByTimeframe(timeframe) {
  const now = Date.now();
  return Array.from(activeUsers.byIp.values())
    .filter(user => (now - user.lastSeen) < timeframe);
}

// Cleanup old data that exceeds the retention period
function cleanupOldData() {
  const now = Date.now();
  
  // 1. Clean up old IP entries
  for (const [ip, data] of activeUsers.byIp.entries()) {
    if (now - data.lastSeen > DATA_RETENTION_PERIOD) {
      activeUsers.byIp.delete(ip);
    }
  }
  
  // 2. Clean up old recent requests
  activeUsers.recentRequests = activeUsers.recentRequests.filter(
    req => (now - req.timestamp) < DATA_RETENTION_PERIOD
  );
  
  // 3. Check for unused endpoint stats and clean them up too
  for (const [path, stats] of activeUsers.endpointStats.entries()) {
    // Endpoint is considered unused if none of the recent requests used it
    if (!activeUsers.recentRequests.some(req => req.path === path)) {
      activeUsers.endpointStats.delete(path);
    }
  }
  
  console.log(`Cleaned up old data. Active IPs: ${activeUsers.byIp.size}, Recent requests: ${activeUsers.recentRequests.length}, Endpoint stats: ${activeUsers.endpointStats.size}`);
}

// Clear all activity statistics (keep active connections)
function clearActivityStats() {
  // Keep the clients connected but reset all stats
  activeUsers.byIp.clear();
  activeUsers.endpointStats.clear();
  activeUsers.recentRequests = [];
  activeUsers.lastUpdate = Date.now();
}

// Get a summary of current activity (for API endpoint)
export function getActivitySummary(page = 0, limit = 25, filters = {}) {
  // Extract filter parameters
  const { ipFilter = '', minRequests = 0, userType = 'all' } = filters;
  
  // Get the current timestamp
  const now = Date.now();
  
  // Get all IPs active in the last 15 minutes
  let allActiveIps = Array.from(activeUsers.byIp.values())
    .filter(user => (now - user.lastSeen) < TIME_BUCKETS.FIFTEEN_MIN);

  // Apply filters
  if (ipFilter) {
    allActiveIps = allActiveIps.filter(user => user.ip.includes(ipFilter));
  }
  
  if (minRequests && !isNaN(minRequests)) {
    allActiveIps = allActiveIps.filter(user => user.requestCount >= Number(minRequests));
  }
  
  if (userType === 'guest') {
    allActiveIps = allActiveIps.filter(user => 
      user.userIds.size === 1 && user.userIds.has('guest'));
  } else if (userType === 'registered') {
    allActiveIps = allActiveIps.filter(user => 
      !user.userIds.has('guest') || user.userIds.size > 1);
  }
  
  // Process filtered IPs to create the summary data
  const processedActiveIps = allActiveIps.map(user => ({
    ip: user.ip,
    requestCount: user.requestCount,
    lastSeen: user.lastSeen,
    uniquePathCount: user.paths.size,
    userIds: Array.from(user.userIds)
  }))
  .sort((a, b) => b.lastSeen - a.lastSeen);
  
  // Apply pagination to limit the number of IPs sent to the frontend
  const startIndex = page * limit;
  const totalPages = Math.ceil(processedActiveIps.length / limit);
  const activeIpsDetailed = processedActiveIps.slice(startIndex, startIndex + limit);
  
  // Get active IPs for different time frames - these don't use the filters
  const activeIps15m = getActiveIpsByTimeframe(TIME_BUCKETS.FIFTEEN_MIN);
  const activeIps30m = getActiveIpsByTimeframe(TIME_BUCKETS.THIRTY_MIN);
  const activeIps1h = getActiveIpsByTimeframe(TIME_BUCKETS.ONE_HOUR);
  const activeIps2h = getActiveIpsByTimeframe(TIME_BUCKETS.TWO_HOURS);
  
  // Get top endpoints
  const topEndpoints = Array.from(activeUsers.endpointStats.entries())
    .map(([path, stats]) => ({
      path,
      count: stats.count,
      methods: Object.fromEntries(stats.methods)
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);
  
  // Get recent requests with formatted data
  const recentRequests = activeUsers.recentRequests
    .slice(0, 20)
    .map(req => ({
      id: req.id,
      ip: req.ip,
      path: req.path,
      method: req.method,
      timestamp: req.timestamp,
      statusCode: req.statusCode,
      isError: req.isError
    }));
  
  return {
    // Active users = unique IPs, per user request
    activeUsers15m: activeIps15m.length,
    activeUsers30m: activeIps30m.length,
    activeUsers1h: activeIps1h.length,
    activeUsers2h: activeIps2h.length,
    uniqueIps: processedActiveIps.length,
    activeIps: activeIpsDetailed,
    totalPages,
    currentPage: page,
    topEndpoints,
    recentRequests,
    lastUpdate: activeUsers.lastUpdate
  };
} 