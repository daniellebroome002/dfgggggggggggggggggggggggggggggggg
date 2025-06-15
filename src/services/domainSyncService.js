import axios from 'axios';
import { pool } from '../db/init.js';

// Push domain to mailserver function (reused from domains.js)
const pushDomainToMailserver = async (domain, action) => {
  try {
    const mailserverUrl = process.env.MAILSERVER_URL;
    const mailserverToken = process.env.MAILSERVER_TOKEN;
    
    if (!mailserverUrl || !mailserverToken) {
      console.warn('⚠️ Mailserver URL or token not configured, skipping domain sync');
      return { success: false, error: 'Mailserver not configured' };
    }

    const endpoint = action === 'add' ? '/domain/add' : '/domain/delete';
    const response = await axios.post(`${mailserverUrl}${endpoint}`, 
      { domain },
      {
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${mailserverToken}`
        },
        timeout: 10000 // 10 second timeout
      }
    );

    return { success: true, result: response.data };
  } catch (error) {
    if (error.response) {
      return { success: false, error: error.response.data };
    } else {
      return { success: false, error: error.message };
    }
  }
};

// Sync all domains to mailserver
export const syncAllDomainsToMailserver = async () => {
  try {
    console.log('🔄 Starting domain sync to mailserver...');
    
    // Get all regular domains
    const [regularDomains] = await pool.query('SELECT domain FROM domains ORDER BY domain');
    
    // Get all verified custom domains
    const [customDomains] = await pool.query(
      'SELECT domain FROM custom_domains WHERE status = ? ORDER BY domain',
      ['verified']
    );
    
    // Combine all domains
    const allDomains = [
      ...regularDomains.map(d => ({ domain: d.domain, type: 'regular' })),
      ...customDomains.map(d => ({ domain: d.domain, type: 'custom' }))
    ];
    
    if (allDomains.length === 0) {
      console.log('📭 No domains found to sync');
      return { success: true, synced: 0, failed: 0 };
    }
    
    console.log(`📡 Found ${allDomains.length} domains to sync:`, 
      allDomains.map(d => `${d.domain} (${d.type})`).join(', '));
    
    let successCount = 0;
    let failureCount = 0;
    const failedDomains = [];
    
    // Sync each domain to mailserver
    for (const domainObj of allDomains) {
      const { domain, type } = domainObj;
      
      try {
        const result = await pushDomainToMailserver(domain, 'add');
        
        if (result.success) {
          console.log(`✅ Synced ${type} domain: ${domain}`);
          successCount++;
        } else {
          console.error(`❌ Failed to sync ${type} domain ${domain}:`, result.error);
          failureCount++;
          failedDomains.push({ domain, type, error: result.error });
        }
        
        // Small delay to avoid overwhelming the mailserver
        await new Promise(resolve => setTimeout(resolve, 100));
        
      } catch (error) {
        console.error(`❌ Error syncing ${type} domain ${domain}:`, error.message);
        failureCount++;
        failedDomains.push({ domain, type, error: error.message });
      }
    }
    
    // Summary
    console.log(`🎯 Domain sync completed: ${successCount} synced, ${failureCount} failed`);
    
    if (failedDomains.length > 0) {
      console.warn('⚠️ Failed domains:', failedDomains);
    }
    
    return {
      success: true,
      synced: successCount,
      failed: failureCount,
      failedDomains: failedDomains
    };
    
  } catch (error) {
    console.error('💥 Critical error during domain sync:', error);
    return {
      success: false,
      error: error.message,
      synced: 0,
      failed: 0
    };
  }
};

// Health check function to verify mailserver connectivity
export const checkMailserverHealth = async () => {
  try {
    const mailserverUrl = process.env.MAILSERVER_URL;
    
    if (!mailserverUrl) {
      return { healthy: false, error: 'Mailserver URL not configured' };
    }
    
    const response = await axios.get(`${mailserverUrl}/health`, { timeout: 5000 });
    
    return { 
      healthy: true, 
      status: response.data,
      url: mailserverUrl 
    };
    
  } catch (error) {
    return { 
      healthy: false, 
      error: error.message,
      url: process.env.MAILSERVER_URL || 'not configured'
    };
  }
}; 