import { pool } from '../db/init.js';

// Enhanced in-memory cache with batch operations
const customDomainUsageCache = {
  // Structure: { [userId]: { dailyCount: number, totalCount: number, lastReset: Date, dirty: boolean } }
  users: new Map(),
  
  // Track pending database operations for batch processing
  pendingOperations: new Map(), // { [userId]: { dailyDelta: number, totalDelta: number, domainId: string } }
  
  // Clean up expired cache entries (older than 25 hours)
  cleanup() {
    const now = new Date();
    const cutoff = new Date(now.getTime() - 25 * 60 * 60 * 1000); // 25 hours ago
    
    for (const [userId, data] of this.users.entries()) {
      if (data.lastReset < cutoff) {
        this.users.delete(userId);
        this.pendingOperations.delete(userId);
      }
    }
  },
  
  // Reset daily counters if needed
  resetDailyIfNeeded(userId) {
    const now = new Date();
    const today = now.toDateString();
    const data = this.users.get(userId);
    
    if (!data || data.lastReset.toDateString() !== today) {
      // Reset daily counter
      const newData = {
        dailyCount: 0,
        totalCount: data?.totalCount || 0,
        lastReset: now,
        dirty: true
      };
      this.users.set(userId, newData);
      
      // Mark for database reset
      this.pendingOperations.set(userId, {
        dailyDelta: -data?.dailyCount || 0, // Reset daily to 0
        totalDelta: 0,
        domainId: null,
        resetDaily: true
      });
      
      return newData;
    }
    
    return data;
  },
  
  // Add pending operation for batch processing
  addPendingOperation(userId, dailyDelta, totalDelta, domainId = null) {
    const existing = this.pendingOperations.get(userId) || { dailyDelta: 0, totalDelta: 0, domainId: null };
    this.pendingOperations.set(userId, {
      dailyDelta: existing.dailyDelta + dailyDelta,
      totalDelta: existing.totalDelta + totalDelta,
      domainId: domainId || existing.domainId,
      resetDaily: existing.resetDaily || false
    });
  },
  
  // Process all pending operations in batch
  async flushPendingOperations() {
    if (this.pendingOperations.size === 0) return;
    
    console.log(`🔄 Flushing ${this.pendingOperations.size} pending custom domain operations to database`);
    
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();
      
      for (const [userId, operation] of this.pendingOperations.entries()) {
        try {
          // Get user's custom domains
          const [domains] = await connection.query(
            'SELECT id FROM custom_domains WHERE user_id = ? AND status = ?',
            [userId, 'verified']
          );
          
          if (domains.length > 0) {
            // Use the specific domain if provided, otherwise use the first one
            const targetDomainId = operation.domainId || domains[0].id;
            
            if (operation.resetDaily) {
              // Reset daily count for all user's domains
              await connection.query(
                'UPDATE custom_domains SET daily_count = 0, last_reset_date = NOW() WHERE user_id = ? AND status = ?',
                [userId, 'verified']
              );
            }
            
            if (operation.dailyDelta !== 0 || operation.totalDelta !== 0) {
              // Update the specific domain with deltas
              await connection.query(
                'UPDATE custom_domains SET daily_count = GREATEST(daily_count + ?, 0), total_count = GREATEST(total_count + ?, 0) WHERE id = ?',
                [operation.dailyDelta, operation.totalDelta, targetDomainId]
              );
            }
          }
        } catch (error) {
          console.error(`Error processing operation for user ${userId}:`, error);
        }
      }
      
      await connection.commit();
      console.log('✅ Successfully flushed all pending operations to database');
      
      // Clear pending operations
      this.pendingOperations.clear();
      
    } catch (error) {
      await connection.rollback();
      console.error('❌ Error flushing pending operations:', error);
    } finally {
      connection.release();
    }
  }
};

// Custom domain rate limits
const CUSTOM_DOMAIN_LIMITS = {
  DAILY_LIMIT: 20,
  TOTAL_LIMIT: 100
};

// Load usage from database for a user (sum across all their custom domains)
async function loadUserUsageFromDB(userId) {
  try {
    const [results] = await pool.query(
      'SELECT SUM(daily_count) as total_daily, SUM(total_count) as total_overall, MAX(last_reset_date) as last_reset FROM custom_domains WHERE user_id = ? AND status = ?',
      [userId, 'verified']
    );
    
    if (results.length > 0 && results[0].total_daily !== null) {
      const { total_daily, total_overall, last_reset } = results[0];
      const now = new Date();
      const lastReset = last_reset ? new Date(last_reset) : new Date(0);
      
      // Check if we need to reset daily counter
      const shouldResetDaily = lastReset.toDateString() !== now.toDateString();
      
      const usage = {
        dailyCount: shouldResetDaily ? 0 : (total_daily || 0),
        totalCount: total_overall || 0,
        lastReset: shouldResetDaily ? now : lastReset,
        dirty: false
      };
      
      // Update cache
      customDomainUsageCache.users.set(userId, usage);
      
      // Schedule daily reset if needed
      if (shouldResetDaily) {
        customDomainUsageCache.addPendingOperation(userId, -total_daily, 0);
        customDomainUsageCache.pendingOperations.get(userId).resetDaily = true;
      }
      
      return usage;
    }
    
    // Return default usage if no domains found
    const defaultUsage = {
      dailyCount: 0,
      totalCount: 0,
      lastReset: new Date(),
      dirty: false
    };
    customDomainUsageCache.users.set(userId, defaultUsage);
    return defaultUsage;
  } catch (error) {
    console.error('Error loading user usage from DB:', error);
    return {
      dailyCount: 0,
      totalCount: 0,
      lastReset: new Date(),
      dirty: false
    };
  }
}

// Get user usage across all custom domains (cache-first, then DB)
async function getUserUsage(userId) {
  // Clean up old cache entries periodically
  if (Math.random() < 0.1) { // 10% chance
    customDomainUsageCache.cleanup();
  }
  
  // Try cache first
  let usage = customDomainUsageCache.resetDailyIfNeeded(userId);
  
  if (!usage) {
    // Load from database
    usage = await loadUserUsageFromDB(userId);
  }
  
  return usage;
}

// Increment usage counters for a user (INSTANT cache update, batch DB write)
async function incrementUserUsage(userId, domainId) {
  const usage = await getUserUsage(userId);
  
  // Update cache INSTANTLY
  usage.dailyCount++;
  usage.totalCount++;
  usage.dirty = true;
  
  // Update cache
  customDomainUsageCache.users.set(userId, usage);
  
  // Add to pending operations for batch processing
  customDomainUsageCache.addPendingOperation(userId, 1, 1, domainId);
  
  console.log(`📈 Incremented usage for user ${userId}: daily=${usage.dailyCount}, total=${usage.totalCount} (cached)`);
  
  return usage;
}

// Decrement usage counters for a user (INSTANT cache update, batch DB write)
async function decrementUserUsage(userId, customDomainId) {
  const usage = await getUserUsage(userId);
  
  // Update cache INSTANTLY (only decrement total, not daily)
  if (usage.totalCount > 0) {
    usage.totalCount--;
    usage.dirty = true;
    
    // Update cache
    customDomainUsageCache.users.set(userId, usage);
    
    // Add to pending operations for batch processing
    customDomainUsageCache.addPendingOperation(userId, 0, -1, customDomainId);
    
    console.log(`📉 Decremented usage for user ${userId}: daily=${usage.dailyCount}, total=${usage.totalCount} (cached)`);
    
    return usage;
  }
  
  return usage;
}

// Check if user has reached limits across all their custom domains
export async function checkCustomDomainLimits(userId) {
  const usage = await getUserUsage(userId);
  
  const dailyLimitReached = usage.dailyCount >= CUSTOM_DOMAIN_LIMITS.DAILY_LIMIT;
  const totalLimitReached = usage.totalCount >= CUSTOM_DOMAIN_LIMITS.TOTAL_LIMIT;
  
  return {
    canCreate: !dailyLimitReached && !totalLimitReached,
    dailyLimitReached,
    totalLimitReached,
    dailyCount: usage.dailyCount,
    totalCount: usage.totalCount,
    dailyLimit: CUSTOM_DOMAIN_LIMITS.DAILY_LIMIT,
    totalLimit: CUSTOM_DOMAIN_LIMITS.TOTAL_LIMIT,
    resetTime: new Date(usage.lastReset.getTime() + 24 * 60 * 60 * 1000), // Next day
    cached: true // Indicate this is from cache for instant response
  };
}

// Middleware to check custom domain rate limits
export async function customDomainRateLimitMiddleware(req, res, next) {
  try {
    const { domainId } = req.body;
    
    if (!domainId) {
      return next(); // No domain specified, let other middleware handle
    }
    
    // Check if this is a custom domain and get user info
    const [customDomains] = await pool.query(
      'SELECT id, domain, user_id FROM custom_domains WHERE id = ? AND status = ?',
      [domainId, 'verified']
    );
    
    if (customDomains.length === 0) {
      return next(); // Not a custom domain, no limits apply
    }
    
    const customDomain = customDomains[0];
    
    // Check limits for the user (across all their custom domains)
    const limitCheck = await checkCustomDomainLimits(customDomain.user_id);
    
    if (!limitCheck.canCreate) {
      return res.status(429).json({
        error: 'CUSTOM_DOMAIN_LIMIT_EXCEEDED',
        message: 'Custom domain email creation limit reached across all your domains',
        limits: {
          daily: {
            current: limitCheck.dailyCount,
            limit: limitCheck.dailyLimit,
            reached: limitCheck.dailyLimitReached
          },
          total: {
            current: limitCheck.totalCount,
            limit: limitCheck.totalLimit,
            reached: limitCheck.totalLimitReached
          },
          resetTime: limitCheck.resetTime,
          domain: customDomain.domain,
          cached: limitCheck.cached
        }
      });
    }
    
    // Store domain info for later use
    req.customDomainInfo = {
      id: domainId,
      domain: customDomain.domain,
      userId: customDomain.user_id,
      limits: limitCheck
    };
    
    next();
  } catch (error) {
    console.error('Custom domain rate limit check error:', error);
    next(); // Continue on error to avoid breaking the flow
  }
}

// Increment usage after successful email creation (INSTANT)
export async function incrementCustomDomainUsage(domainId) {
  try {
    // Get the user ID for this domain
    const [customDomains] = await pool.query(
      'SELECT user_id FROM custom_domains WHERE id = ? AND status = ?',
      [domainId, 'verified']
    );
    
    if (customDomains.length > 0) {
      return await incrementUserUsage(customDomains[0].user_id, domainId);
    }
    
    return null;
  } catch (error) {
    console.error('Error incrementing custom domain usage:', error);
    return null;
  }
}

// Decrement total count when custom domain email is deleted (INSTANT)
export async function decrementCustomDomainUsage(customDomainId, userId) {
  try {
    // Verify domain belongs to user and is verified
    const [customDomains] = await pool.query(
      'SELECT user_id FROM custom_domains WHERE id = ? AND user_id = ? AND status = ?',
      [customDomainId, userId, 'verified']
    );
    
    if (customDomains.length === 0) {
      console.log(`Domain ${customDomainId} not found or not verified for user ${userId}`);
      return null; // Domain not found or not verified
    }
    
    return await decrementUserUsage(userId, customDomainId);
  } catch (error) {
    console.error('Error decrementing custom domain usage:', error);
    return null;
  }
}

// Batch flush pending operations every 30 seconds
setInterval(async () => {
  try {
    await customDomainUsageCache.flushPendingOperations();
  } catch (error) {
    console.error('Error in periodic flush:', error);
  }
}, 30 * 1000); // Every 30 seconds

// Clean up cache periodically
setInterval(() => {
  customDomainUsageCache.cleanup();
}, 60 * 60 * 1000); // Every hour

// Graceful shutdown - flush pending operations
process.on('SIGTERM', async () => {
  console.log('🔄 Graceful shutdown: flushing pending custom domain operations...');
  try {
    await customDomainUsageCache.flushPendingOperations();
    console.log('✅ Pending operations flushed successfully');
  } catch (error) {
    console.error('❌ Error flushing operations during shutdown:', error);
  }
});

process.on('SIGINT', async () => {
  console.log('🔄 Graceful shutdown: flushing pending custom domain operations...');
  try {
    await customDomainUsageCache.flushPendingOperations();
    console.log('✅ Pending operations flushed successfully');
  } catch (error) {
    console.error('❌ Error flushing operations during shutdown:', error);
  }
});

export { CUSTOM_DOMAIN_LIMITS }; 