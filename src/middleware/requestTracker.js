import { v4 as uuidv4 } from 'uuid';
import { pool } from '../db/init.js';
import axios from 'axios';

// Store recent requests in memory for quick access
const recentRequests = {
  // Structure: { [requestId]: requestData }
  byId: new Map(),
  // Structure: { [ip]: [requestIds] }
  byIp: new Map(),
  // Maximum number of requests to keep in memory per IP
  maxPerIp: 100,
  // Maximum total entries in the byId map
  maxTotal: 10000,
  // Time-to-live for cached entries (1 hour)
  ttl: 60 * 60 * 1000,
};

// Add geo location cache with 6-hour TTL
const geoCache = {
  // Structure: { [ip]: { data: geoData, timestamp: Date.now() } }
  byIp: new Map(),
  // 6 hours in milliseconds
  ttl: 6 * 60 * 60 * 1000
};

// Add request buffer for batch processing
const requestBuffer = {
  logs: [],                  // Pending logs waiting to be written to database
  maxSize: 20000,            // Max buffer size before forced flush (only flush when reaching exactly this number)
  lastFlush: Date.now(),     // Timestamp of last flush
  flushInterval: null,       // No time-based flushing
  chunkSize: 5000            // Process in chunks of 5000 to avoid transaction issues
};

// Function to periodically clean up old entries
const cleanupInterval = setInterval(() => {
  const now = Date.now();
  let count = 0;
  
  // Clean up old entries
  for (const [requestId, data] of recentRequests.byId.entries()) {
    if (now - data.timestamp > recentRequests.ttl) {
      recentRequests.byId.delete(requestId);
      count++;
    }
  }
  
  // Clean up IP references to non-existent requests
  for (const [ip, requestIds] of recentRequests.byIp.entries()) {
    recentRequests.byIp.set(ip, requestIds.filter(id => recentRequests.byId.has(id)));
    if (recentRequests.byIp.get(ip).length === 0) {
      recentRequests.byIp.delete(ip);
    }
  }
  
  // Clean up expired geo cache entries
  for (const [ip, cacheEntry] of geoCache.byIp.entries()) {
    if (now - cacheEntry.timestamp > geoCache.ttl) {
      geoCache.byIp.delete(ip);
    }
  }
  
  if (count > 0) {
    console.log(`Cleaned up ${count} expired request entries from memory cache`);
  }
}, 15 * 60 * 1000); // Run every 15 minutes

// Set up scheduled flushing of request logs
const flushInterval = setInterval(() => {
  // Only flush when buffer reaches exactly maxSize (20000 logs)
  if (requestBuffer.logs.length >= requestBuffer.maxSize) {
    console.log(`Buffer reached ${requestBuffer.logs.length} logs, flushing to database`);
    flushRequestLogs();
  }
}, 30000); // Check every 30 seconds

// Ensure cleanup on process exit
process.on('exit', () => {
  clearInterval(cleanupInterval);
  clearInterval(flushInterval);
  
  // Note: This might not work reliably for async operations during 'exit'
  if (requestBuffer.logs.length > 0) {
    console.log(`Server shutting down with ${requestBuffer.logs.length} unflushed logs (will be lost)`);
    // Cannot reliably flush asynchronously during 'exit' event
  }
});

// Better handlers for graceful shutdown
['SIGINT', 'SIGTERM'].forEach(signal => {
  process.on(signal, async () => {
    console.log(`Received ${signal}, gracefully shutting down...`);
    clearInterval(cleanupInterval);
    clearInterval(flushInterval);
    
    // On shutdown, we should flush any remaining logs regardless of buffer size
    // to prevent data loss, even though during normal operation we only flush at maxSize
    if (requestBuffer.logs.length > 0) {
      console.log(`Flushing ${requestBuffer.logs.length} remaining logs before shutdown (emergency flush)`);
      try {
        await flushRequestLogs();
        console.log('Final log flush completed successfully');
      } catch (error) {
        console.error('Error during final log flush:', error);
      }
    }
    
    // Allow time for flush to complete
    setTimeout(() => {
      console.log('Exiting process');
      process.exit(0);
    }, 1000);
  });
});

// Function to flush request logs in batch
async function flushRequestLogs() {
  if (requestBuffer.logs.length === 0) return;
  
  const batchToProcess = [...requestBuffer.logs];
  requestBuffer.logs = [];
  requestBuffer.lastFlush = Date.now();
  
  console.log(`Flushing ${batchToProcess.length} request logs to database`);
  
  try {
    // Get a connection from the pool
    const connection = await pool.getConnection();
    
    // Split the batch into chunks to avoid transaction timeouts
    const chunks = [];
    for (let i = 0; i < batchToProcess.length; i += requestBuffer.chunkSize) {
      chunks.push(batchToProcess.slice(i, i + requestBuffer.chunkSize));
    }
    
    console.log(`Processing in ${chunks.length} chunks of up to ${requestBuffer.chunkSize} logs each`);
    
    // Process each chunk in its own transaction
    let successCount = 0;
    let failedChunks = [];
    
    for (let chunkIndex = 0; chunkIndex < chunks.length; chunkIndex++) {
      const chunk = chunks[chunkIndex];
      
      try {
        await connection.beginTransaction();
        
        // Optimize by grouping logs by whether they include geo info
        const geoLogs = chunk.filter(log => log.includeGeo);
        const nonGeoLogs = chunk.filter(log => !log.includeGeo);
        
        // Process logs with geo info
        if (geoLogs.length > 0) {
          // Prepare values for bulk insert
          const values = geoLogs.map(log => [
            log.id, log.requestId, log.clientIp, log.userId, log.userAgent, 
            log.requestPath, log.method, log.statusCode, log.responseTime,
            log.geoCountry, log.geoCity, log.geoRegion, 
            log.referer, log.isBot ? 1 : 0
          ]);
          
          // Use bulk insert syntax
          await connection.query(
            `INSERT INTO request_logs 
             (id, request_id, client_ip, user_id, user_agent, request_path, request_method, 
              status_code, response_time, geo_country, geo_city, geo_region, referer, is_bot) 
             VALUES ?`,
            [values]
          );
        }
        
        // Process logs without geo info
        if (nonGeoLogs.length > 0) {
          // Prepare values for bulk insert
          const values = nonGeoLogs.map(log => [
            log.id, log.requestId, log.clientIp, log.userId, log.userAgent, 
            log.requestPath, log.method, log.statusCode, log.responseTime,
            log.referer, log.isBot ? 1 : 0
          ]);
          
          // Use bulk insert syntax
          await connection.query(
            `INSERT INTO request_logs 
             (id, request_id, client_ip, user_id, user_agent, request_path, request_method, 
              status_code, response_time, referer, is_bot) 
             VALUES ?`,
            [values]
          );
        }
        
        await connection.commit();
        successCount += chunk.length;
        console.log(`Successfully processed chunk ${chunkIndex + 1}/${chunks.length} (${chunk.length} logs)`);
      } catch (error) {
        await connection.rollback();
        console.error(`Error processing chunk ${chunkIndex + 1}/${chunks.length}:`, error);
        failedChunks.push(chunk);
      }
    }
    
    // Handle any failed chunks - retry logic
    if (failedChunks.length > 0) {
      console.warn(`${failedChunks.length} chunks failed, retrying failed logs individually`);
      
      // Collect all logs from failed chunks
      const failedLogs = failedChunks.flat();
      
      // Add retry count and put back in buffer
      failedLogs.forEach(log => {
        // Add retry count property if not exists
        if (!log.retryCount) log.retryCount = 0;
        
        // Only retry up to 3 times
        if (log.retryCount < 3) {
          log.retryCount++;
          requestBuffer.logs.push(log);
        } else {
          console.error('Dropped log after 3 retry attempts:', log.id);
        }
      });
      
      console.log(`${successCount} logs succeeded, ${failedLogs.length} logs returned to buffer for retry`);
    } else {
      console.log(`Successfully processed all ${successCount} logs in batch`);
    }
    
    // Release connection
    connection.release();
  } catch (error) {
    console.error('Failed to get database connection for batch processing:', error);
    
    // Return logs to buffer if we couldn't get a connection
    requestBuffer.logs.unshift(...batchToProcess);
    
    // Limit buffer size if it grows too large during connection problems
    if (requestBuffer.logs.length > requestBuffer.maxSize * 1.5) {
      console.warn(`Request buffer exceeded limit (${requestBuffer.logs.length}), trimming oldest entries`);
      requestBuffer.logs = requestBuffer.logs.slice(-requestBuffer.maxSize);
    }
  }
}

// Get geo information for an IP address
async function getGeoInfo(ip) {
  try {
    // Skip for localhost and private IPs
    if (ip === '127.0.0.1' || ip === 'localhost' || ip.startsWith('192.168.') || ip.startsWith('10.')) {
      return { country: 'Local', city: 'Development', region: 'Internal' };
    }
    
    // Check if we have this IP in cache and return it regardless of age
    // (once we have geo data for an IP, we'll always use it)
    if (geoCache.byIp.has(ip)) {
      return geoCache.byIp.get(ip).data;
    }
    
    // Check database first to see if we've stored this IP's geo info before
    try {
      const [existingGeoInfo] = await pool.query(
        `SELECT geo_country as country, geo_city as city, geo_region as region 
         FROM request_logs 
         WHERE client_ip = ? 
         AND geo_country IS NOT NULL 
         AND geo_country != '' 
         LIMIT 1`,
        [ip]
      );
      
      if (existingGeoInfo && existingGeoInfo.length > 0 && existingGeoInfo[0].country) {
        const geoData = {
          country: existingGeoInfo[0].country,
          city: existingGeoInfo[0].city,
          region: existingGeoInfo[0].region
        };
        
        // Cache the result from database
        geoCache.byIp.set(ip, {
          data: geoData,
          timestamp: Date.now()
        });
        
        return geoData;
      }
    } catch (dbError) {
      console.error('Error checking database for existing geo info:', dbError.message);
      // Continue to external API if DB lookup fails
    }
    
    // If not in cache or database, fetch from external API with increased timeout
    const response = await axios.get(`http://ip-api.com/json/${ip}?fields=status,country,regionName,city`, {
      timeout: 10000 // 10 second timeout (increased from 5 seconds)
    });
    
    let geoData;
    if (response.data && response.data.status === 'success') {
      geoData = {
        country: response.data.country || '',
        city: response.data.city || '',
        region: response.data.regionName || ''
      };
    } else {
      geoData = { country: '', city: '', region: '' };
    }
    
    // Cache the result permanently (we'll reuse it for this IP)
    geoCache.byIp.set(ip, {
      data: geoData,
      timestamp: Date.now()
    });
    
    return geoData;
  } catch (error) {
    console.error('Error fetching geo info:', error.message);
    
    // If we have cached data, return it even if expired
    if (geoCache.byIp.has(ip)) {
      return geoCache.byIp.get(ip).data;
    }
    
    return { country: '', city: '', region: '' };
  }
}

// Detect if request is from a bot
function detectBot(userAgent = '') {
  if (!userAgent) return false;
  
  const userAgentLower = userAgent.toLowerCase();
  const botPatterns = [
    'bot', 'spider', 'crawler', 'googlebot', 'bingbot', 'yandex', 'baidu', 
    'semrush', 'ahrefs', 'screaming frog', 'httrack', 'wget', 'curl', 'puppeteer',
    'headless', 'scraper', 'lighthouse', 'pagespeed', 'google-structured-data'
  ];
  
  return botPatterns.some(pattern => userAgentLower.includes(pattern));
}

// Request tracking middleware
export async function requestTrackerMiddleware(req, res, next) {
  // Start timer for response time
  const start = Date.now();
  
  // Generate unique request ID if not already present
  const requestId = req.headers['x-request-id'] || uuidv4();
  req.requestId = requestId;
  
  // Set request ID header for response
  res.setHeader('X-Request-ID', requestId);
  
  // Get client IP
  const clientIp = 
    req.headers['x-forwarded-for']?.split(',')[0].trim() || 
    req.headers['x-real-ip'] || 
    req.connection.remoteAddress || 
    req.socket.remoteAddress || 
    'unknown';
  
  // Extract user ID if authenticated
  const userId = req.user?.id || null;

  // Store basic request data for immediate access
  const requestData = {
    requestId,
    clientIp,
    userId,
    requestPath: req.originalUrl || req.url,
    requestMethod: req.method,
    userAgent: req.headers['user-agent'] || '',
    referer: req.headers['referer'] || '',
    timestamp: Date.now(),
    isBot: detectBot(req.headers['user-agent'])
  };
  
  // Cache request data in memory
  recentRequests.byId.set(requestId, requestData);
  
  // Add to IP-indexed map
  if (!recentRequests.byIp.has(clientIp)) {
    recentRequests.byIp.set(clientIp, []);
  }
  const ipRequests = recentRequests.byIp.get(clientIp);
  ipRequests.push(requestId);
  
  // Limit requests stored per IP
  if (ipRequests.length > recentRequests.maxPerIp) {
    const removed = ipRequests.shift();
    recentRequests.byId.delete(removed);
  }
  
  // Limit total cached requests
  if (recentRequests.byId.size > recentRequests.maxTotal) {
    // Remove oldest entries
    const entries = Array.from(recentRequests.byId.entries());
    entries.sort((a, b) => a[1].timestamp - b[1].timestamp);
    
    const toRemove = entries.slice(0, Math.floor(recentRequests.maxTotal * 0.2)); // Remove oldest 20%
    for (const [id, _] of toRemove) {
      recentRequests.byId.delete(id);
    }
  }

  // Capture response data on finish
  res.on('finish', async () => {
    try {
      const responseTime = Date.now() - start;
      const statusCode = res.statusCode;
      
      // Update cached data with response info
      if (recentRequests.byId.has(requestId)) {
        const data = recentRequests.byId.get(requestId);
        data.statusCode = statusCode;
        data.responseTime = responseTime;
      }
      
      // Get geo information (async, don't block response)
      getGeoInfo(clientIp).then(async (geoInfo) => {
        // Always include geo info when we have it - we're now only fetching it once per IP
        const includeGeo = !!(geoInfo.country || geoInfo.city || geoInfo.region);
        
        // Add to batch buffer instead of directly inserting to DB
        requestBuffer.logs.push({
          id: uuidv4(),
          requestId,
          clientIp,
          userId,
          userAgent: req.headers['user-agent'] || '',
          requestPath: req.originalUrl || req.url,
          method: req.method,
          statusCode,
          responseTime,
          geoCountry: geoInfo.country || '',
          geoCity: geoInfo.city || '',
          geoRegion: geoInfo.region || '',
          referer: req.headers['referer'] || '',
          isBot: detectBot(req.headers['user-agent']),
          includeGeo: includeGeo
        });
        
        // Only flush when buffer reaches exactly maxSize
        if (requestBuffer.logs.length === requestBuffer.maxSize) {
          console.log(`Buffer reached exactly ${requestBuffer.maxSize} logs, flushing to database`);
          flushRequestLogs();
        }
        
        // Update cached data with geo info
        if (recentRequests.byId.has(requestId)) {
          const data = recentRequests.byId.get(requestId);
          data.geoCountry = geoInfo.country;
          data.geoCity = geoInfo.city;
          data.geoRegion = geoInfo.region;
        }
      }).catch(err => {
        console.error('Error logging request:', err);
      });
    } catch (error) {
      console.error('Error in request tracking:', error);
    }
  });
  
  next();
}

// Function to lookup requests by ID
export async function lookupRequestById(requestId) {
  // Check in-memory cache first
  if (recentRequests.byId.has(requestId)) {
    return recentRequests.byId.get(requestId);
  }
  
  // If not in cache, look up in database
  try {
    const [rows] = await pool.query(
      `SELECT * FROM request_logs WHERE request_id = ? ORDER BY created_at DESC LIMIT 1`,
      [requestId]
    );
    
    if (rows.length > 0) {
      return rows[0];
    }
    
    return null;
  } catch (error) {
    console.error('Error looking up request by ID:', error);
    throw error;
  }
}

// Function to lookup requests by IP
export async function lookupRequestsByIp(ip, limit = 50) {
  try {
    const [rows] = await pool.query(
      `SELECT * FROM request_logs WHERE client_ip = ? ORDER BY created_at DESC LIMIT ?`,
      [ip, limit]
    );
    
    return rows;
  } catch (error) {
    console.error('Error looking up requests by IP:', error);
    throw error;
  }
}

// Get stats for an IP
export async function getIpStats(ip) {
  try {
    // Get request count
    const [countResult] = await pool.query(
      `SELECT COUNT(*) as total FROM request_logs WHERE client_ip = ?`,
      [ip]
    );
    
    // Get unique paths
    const [pathsResult] = await pool.query(
      `SELECT request_path, COUNT(*) as count FROM request_logs 
       WHERE client_ip = ? GROUP BY request_path 
       ORDER BY count DESC LIMIT 10`,
      [ip]
    );
    
    // Get first seen date
    const [firstSeenResult] = await pool.query(
      `SELECT MIN(created_at) as first_seen FROM request_logs WHERE client_ip = ?`,
      [ip]
    );
    
    // Get last seen date
    const [lastSeenResult] = await pool.query(
      `SELECT MAX(created_at) as last_seen FROM request_logs WHERE client_ip = ?`,
      [ip]
    );
    
    // Get average response time
    const [avgTimeResult] = await pool.query(
      `SELECT AVG(response_time) as avg_time FROM request_logs WHERE client_ip = ?`,
      [ip]
    );
    
    // Get user IDs if any
    const [userIdsResult] = await pool.query(
      `SELECT DISTINCT user_id FROM request_logs WHERE client_ip = ? AND user_id IS NOT NULL`,
      [ip]
    );
    
    // Get error rate
    const [errorRateResult] = await pool.query(
      `SELECT 
        COUNT(*) as total_requests,
        SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as error_count
       FROM request_logs WHERE client_ip = ?`,
      [ip]
    );
    
    // Get geo information (last known)
    const [geoResult] = await pool.query(
      `SELECT geo_country, geo_city, geo_region FROM request_logs 
       WHERE client_ip = ? AND geo_country != '' 
       ORDER BY created_at DESC LIMIT 1`,
      [ip]
    );
    
    // Calculate error rate
    const errorRate = errorRateResult[0].total_requests > 0 
      ? (errorRateResult[0].error_count / errorRateResult[0].total_requests) * 100 
      : 0;
    
    return {
      totalRequests: countResult[0].total,
      topPaths: pathsResult,
      firstSeen: firstSeenResult[0].first_seen,
      lastSeen: lastSeenResult[0].last_seen,
      avgResponseTime: avgTimeResult[0].avg_time,
      associatedUsers: userIdsResult.map(row => row.user_id),
      errorRate: errorRate.toFixed(2) + '%',
      geoInfo: geoResult.length > 0 ? {
        country: geoResult[0].geo_country,
        city: geoResult[0].geo_city,
        region: geoResult[0].geo_region
      } : null
    };
  } catch (error) {
    console.error('Error getting IP stats:', error);
    throw error;
  }
}

// Get recent unique IPs
export async function getRecentIps(limit = 30) {
  try {
    const [rows] = await pool.query(
      `SELECT client_ip, MAX(created_at) as last_seen, 
       COUNT(*) as request_count, geo_country, geo_city
       FROM request_logs
       GROUP BY client_ip
       ORDER BY last_seen DESC
       LIMIT ?`,
      [limit]
    );
    
    return rows;
  } catch (error) {
    console.error('Error getting recent IPs:', error);
    throw error;
  }
}
