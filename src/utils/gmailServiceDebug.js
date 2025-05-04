import { pool } from '../db/init.js';
import { stores } from '../services/gmailService.js';

/**
 * Utility functions for debugging the Gmail service
 */

// Function to generate a debug report of all Gmail service memory stores
export async function generateGmailDebugReport() {
  const { 
    emailCache,
    aliasCache
  } = stores;
  
  // Fetch accounts from database
  const [accounts] = await pool.query(`
    SELECT 
      id, email, status, quota_used, alias_count, last_used, created_at, updated_at
    FROM gmail_accounts
    ORDER BY last_used DESC
  `);
  
  // Fetch credentials from database
  const [credentials] = await pool.query(`
    SELECT 
      id, client_id, redirect_uri, active, usage_count, last_used, created_at, updated_at
    FROM gmail_credentials
    ORDER BY last_used DESC
  `);
  
  const report = {
    timestamp: new Date().toISOString(),
    accounts: {
      count: accounts.length,
      active: accounts.filter(acc => acc.status === 'active').length,
      auth_error: accounts.filter(acc => acc.status === 'auth-error').length,
      rate_limited: accounts.filter(acc => acc.status === 'rate-limited').length,
      details: accounts.map(account => ({
        id: account.id,
        email: account.email,
        status: account.status,
        aliasCount: account.alias_count,
        lastUsed: account.last_used,
        quotaUsed: account.quota_used,
        created: account.created_at,
        updated: account.updated_at
      }))
    },
    aliases: {
      count: aliasCache.size,
      databaseCount: 0,
      memoryCacheCount: aliasCache.size,
      details: [...aliasCache.entries()].slice(0, 20).map(([alias, data]) => ({
        alias,
        parentAccount: data.parentAccount,
        userId: data.userId,
        created: new Date(data.created).toISOString(),
        lastAccessed: new Date(data.lastAccessed).toISOString(),
        expires: data.expires ? new Date(data.expires).toISOString() : null
      }))
    },
    credentials: {
      count: credentials.length,
      active: credentials.filter(c => c.active).length,
      details: credentials.map(cred => ({
        id: cred.id,
        active: cred.active,
        usageCount: cred.usage_count,
        lastUsed: cred.last_used,
        redirectUri: cred.redirect_uri,
        created: cred.created_at,
        updated: cred.updated_at
      }))
    },
    emailCache: {
      size: emailCache.size
    }
  };
  
  return report;
}

// Function to dump the Gmail service status to the console
export async function dumpGmailServiceStatus() {
  const report = await generateGmailDebugReport();
  
  console.log('======= GMAIL SERVICE DEBUG REPORT =======');
  console.log(`Generated at: ${report.timestamp}`);
  console.log('\n--- ACCOUNTS ---');
  console.log(`Total accounts: ${report.accounts.count} (Active: ${report.accounts.active}, Auth Error: ${report.accounts.auth_error}, Rate Limited: ${report.accounts.rate_limited})`);
  console.table(report.accounts.details.map(acc => ({
    email: acc.email,
    status: acc.status,
    aliases: acc.aliasCount,
    quota: acc.quotaUsed,
    lastUsed: new Date(acc.lastUsed).toLocaleString(),
    updated: new Date(acc.updated).toLocaleString()
  })));
  
  console.log('\n--- ALIASES ---');
  console.log(`Total aliases: ${report.aliases.count} (Memory: ${report.aliases.memoryCacheCount})`);
  if (report.aliases.details.length > 0) {
    console.table(report.aliases.details.map(a => ({
      alias: a.alias,
      parent: a.parentAccount,
      user: a.userId,
      created: new Date(a.created).toLocaleString(),
      expires: a.expires ? new Date(a.expires).toLocaleString() : 'never'
    })));
  } else {
    console.log('No aliases found');
  }
  
  console.log('\n--- CREDENTIALS ---');
  console.log(`Total credentials: ${report.credentials.count} (Active: ${report.credentials.active})`);
  console.table(report.credentials.details.map(c => ({
    id: c.id.substring(0, 8),
    active: c.active ? 'Yes' : 'No',
    usage: c.usageCount,
    lastUsed: new Date(c.lastUsed).toLocaleString()
  })));
  
  console.log('\n--- EMAIL CACHE ---');
  console.log(`Cache size: ${report.emailCache.size}`);
  console.log('=========================================');
  
  return report;
}

// Function to check the health of specific accounts with enhanced diagnostics
export async function checkAccountHealth(accountEmail) {
  try {
    // Get account details from database
    const [accounts] = await pool.query(
      'SELECT * FROM gmail_accounts WHERE email = ?',
      [accountEmail]
    );
    
    if (accounts.length === 0) {
      console.error(`Account ${accountEmail} not found`);
      return {
        found: false,
        email: accountEmail
      };
    }
    
    const account = accounts[0];
    
    // Check if account has refresh token
    const hasRefreshToken = !!account.refresh_token;
    
    // Count aliases from in-memory cache
    let aliasCount = 0;
    const aliases = [];
    for (const [alias, data] of stores.aliasCache.entries()) {
      if (data.parentAccount === accountEmail) {
        aliasCount++;
        aliases.push({
          alias,
          created: new Date(data.created).toISOString(),
          lastAccessed: new Date(data.lastAccessed).toISOString(),
          userId: data.userId
        });
      }
    }
    
    // Calculate last error time if in auth-error status
    let lastErrorTime = null;
    let errorDuration = null;
    if (account.status === 'auth-error') {
      lastErrorTime = new Date(account.updated_at).toISOString();
      const durationMs = Date.now() - new Date(account.updated_at).getTime();
      errorDuration = {
        ms: durationMs,
        minutes: Math.floor(durationMs / 60000),
        hours: Math.floor(durationMs / 3600000)
      };
    }
    
    return {
      found: true,
      id: account.id,
      email: account.email,
      status: account.status,
      aliasCount: aliasCount,
      inMemoryAliases: aliasCount,
      databaseAliasCount: account.alias_count,
      tokenExpiry: account.expires_at ? new Date(parseInt(account.expires_at)).toISOString() : 'unknown',
      hasRefreshToken,
      lastUsed: new Date(account.last_used).toISOString(),
      lastUpdated: new Date(account.updated_at).toISOString(),
      quotaUsed: account.quota_used,
      errorStatus: account.status === 'auth-error' ? {
        since: lastErrorTime,
        duration: errorDuration
      } : null,
      aliases: aliases.slice(0, 10) // Limit to 10 aliases for readability
    };
  } catch (error) {
    console.error(`Error checking account health: ${error.message}`);
    throw error;
  }
}

// Function to check all alias mappings for a specific user with improved details
export async function checkUserAliases(userId) {
  try {
    // Count aliases from in-memory cache
    const userAliases = [];
    for (const [alias, data] of stores.aliasCache.entries()) {
      if (data.userId === userId) {
        userAliases.push({
          alias,
          parentAccount: data.parentAccount,
          created: new Date(data.created).toISOString(),
          lastAccessed: new Date(data.lastAccessed).toISOString(),
          age: {
            ms: Date.now() - data.created,
            minutes: Math.floor((Date.now() - data.created) / 60000),
            hours: Math.floor((Date.now() - data.created) / 3600000),
            days: Math.floor((Date.now() - data.created) / 86400000)
          },
          expires: data.expires ? new Date(data.expires).toISOString() : null,
          timeUntilExpiry: data.expires ? {
            ms: data.expires - Date.now(),
            hours: Math.floor((data.expires - Date.now()) / 3600000),
            days: Math.floor((data.expires - Date.now()) / 86400000)
          } : null
        });
      }
    }
    
    // Sort by creation time, newest first
    userAliases.sort((a, b) => new Date(b.created) - new Date(a.created));
    
    return {
      found: userAliases.length > 0,
      userId,
      aliasCount: userAliases.length,
      details: userAliases
    };
  } catch (error) {
    console.error(`Error checking user aliases: ${error.message}`);
    throw error;
  }
}

// Function to find all aliases for an account with detailed stats
export async function findAccountAliases(accountEmail) {
  try {
    // Count aliases from in-memory cache
    const aliases = [];
    const userMap = new Map();
    
    for (const [alias, data] of stores.aliasCache.entries()) {
      if (data.parentAccount === accountEmail) {
        aliases.push({
          alias,
          userId: data.userId,
          created: new Date(data.created).toISOString(),
          lastAccessed: new Date(data.lastAccessed).toISOString(),
          age: {
            hours: Math.floor((Date.now() - data.created) / 3600000),
            days: Math.floor((Date.now() - data.created) / 86400000)
          }
        });
        
        // Track unique users
        if (data.userId) {
          if (userMap.has(data.userId)) {
            userMap.set(data.userId, userMap.get(data.userId) + 1);
          } else {
            userMap.set(data.userId, 1);
          }
        }
      }
    }
    
    // Sort by last accessed time, newest first
    aliases.sort((a, b) => new Date(b.lastAccessed) - new Date(a.lastAccessed));
    
    // Extract user stats
    const userStats = Array.from(userMap.entries()).map(([userId, count]) => ({
      userId, aliasCount: count
    }));
    
    return {
      email: accountEmail,
      found: aliases.length > 0,
      aliasCount: aliases.length,
      uniqueUsers: userMap.size,
      userStats: userStats.sort((a, b) => b.aliasCount - a.aliasCount),
      aliases: aliases
    };
  } catch (error) {
    console.error(`Error finding account aliases: ${error.message}`);
    throw error;
  }
}

// Recovery functions with improved reliability
export async function recoverFailedAccount(accountEmail) {
  try {
    // Get account details
    const [accounts] = await pool.query(
      'SELECT * FROM gmail_accounts WHERE email = ?',
      [accountEmail]
    );
    
    if (accounts.length === 0) {
      return {
        success: false,
        message: `Account ${accountEmail} not found`
      };
    }
    
    const account = accounts[0];
    const oldStatus = account.status;
    
    // Update status to active
    await pool.query(
      'UPDATE gmail_accounts SET status = \'active\', updated_at = NOW() WHERE id = ?',
      [account.id]
    );
    
    // If this account was in auth error, attempt to check its tokens
    if (oldStatus === 'auth-error') {
      try {
        // Start polling for this account to force token refresh
        if (!activePollingAccounts.has(accountEmail)) {
          schedulePolling(accountEmail);
          activePollingAccounts.add(accountEmail);
          console.log(`Started polling for recovered account ${accountEmail}`);
        }
      } catch (pollingError) {
        console.warn(`Could not start polling for recovered account ${accountEmail}:`, pollingError);
      }
    }
    
    return {
      success: true,
      email: accountEmail,
      oldStatus,
      newStatus: 'active',
      message: `Account ${accountEmail} recovered from ${oldStatus} status`
    };
  } catch (error) {
    console.error(`Error recovering account: ${error.message}`);
    throw error;
  }
}

// Manually reassign an alias to a specific account with confirmation
export async function reassignAlias(aliasEmail, targetAccountEmail) {
  try {
    // Verify target account exists
    const [targetAccounts] = await pool.query(
      'SELECT id, email FROM gmail_accounts WHERE email = ?',
      [targetAccountEmail]
    );
    
    if (targetAccounts.length === 0) {
      return {
        success: false,
        message: `Target account ${targetAccountEmail} not found`
      };
    }
    
    const targetAccount = targetAccounts[0];
    
    // Get the alias mapping from memory
    if (!stores.aliasCache.has(aliasEmail)) {
      return {
        success: false,
        message: `Alias ${aliasEmail} not found in memory cache`
      };
    }
    
    const aliasData = stores.aliasCache.get(aliasEmail);
    const oldParentAccount = aliasData.parentAccount;
    
    // Update in-memory cache
    aliasData.parentAccount = targetAccountEmail;
    aliasData.parentAccountId = targetAccount.id;
    aliasData.lastAccessed = Date.now();
    stores.aliasCache.set(aliasEmail, aliasData);
    
    // Update account alias counts in database
    // Decrement old parent account's alias count
    if (aliasData.parentAccountId) {
      await pool.query(
        'UPDATE gmail_accounts SET alias_count = GREATEST(0, alias_count - 1), updated_at = NOW() WHERE id = ?',
        [aliasData.parentAccountId]
      );
    }
    
    // Increment new parent account's alias count
    await pool.query(
      'UPDATE gmail_accounts SET alias_count = alias_count + 1, last_used = NOW(), updated_at = NOW() WHERE id = ?',
      [targetAccount.id]
    );
    
    return {
      success: true,
      alias: aliasEmail,
      oldParent: oldParentAccount,
      newParent: targetAccountEmail,
      message: `Alias ${aliasEmail} reassigned from ${oldParentAccount} to ${targetAccountEmail}`
    };
  } catch (error) {
    console.error(`Error reassigning alias: ${error.message}`);
    throw error;
  }
}
