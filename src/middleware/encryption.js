import crypto from 'crypto';

class EncryptionService {
  constructor() {
    this.key = null;
    this.initKey();
  }

  initKey() {
    const keyString = process.env.ENCRYPTION_KEY;
    if (!keyString) {
      console.warn('ENCRYPTION_KEY not found in environment variables');
      return;
    }
    
    // Use the key string directly
    this.keyString = keyString;
  }

  encrypt(data) {
    if (!this.keyString) {
      return data; // Return original if no key available
    }

    try {
      const jsonString = JSON.stringify(data);
      const keyBytes = Buffer.from(this.keyString, 'utf8');
      const dataBytes = Buffer.from(jsonString, 'utf8');
      
      let encrypted = '';
      for (let i = 0; i < dataBytes.length; i++) {
        const encryptedByte = dataBytes[i] ^ keyBytes[i % keyBytes.length];
        encrypted += encryptedByte.toString(16).padStart(2, '0');
      }
      
      return { encrypted };
    } catch (error) {
      console.error('Encryption error:', error);
      return data; // Return original on error
    }
  }

  decrypt(encryptedData) {
    if (!this.keyString || !encryptedData.encrypted) {
      return encryptedData;
    }

    try {
      const encrypted = encryptedData.encrypted;
      const keyBytes = Buffer.from(this.keyString, 'utf8');
      const encryptedBytes = [];
      
      for (let i = 0; i < encrypted.length; i += 2) {
        encryptedBytes.push(parseInt(encrypted.substr(i, 2), 16));
      }
      
      let decrypted = '';
      for (let i = 0; i < encryptedBytes.length; i++) {
        decrypted += String.fromCharCode(encryptedBytes[i] ^ keyBytes[i % keyBytes.length]);
      }
      
      return JSON.parse(decrypted);
    } catch (error) {
      console.error('Decryption error:', error);
      return encryptedData;
    }
  }
}

const encryptionService = new EncryptionService();

// List of routes that should be encrypted (14 core scraping targets)
const ROUTES_TO_ENCRYPT = [
  // Domain discovery
  '/domains/public',
  
  // Public email creation (no auth)
  '/emails/public/create',
  '/emails/public/', // covers /emails/public/:email
  
  // Guest email system
  '/guest/init',
  '/guest/emails/create',
  '/guest/emails/', // covers both /guest/emails and /guest/emails/:id
  
  // Authenticated email system  
  '/emails/create',
  '/emails/:id',
  '/emails/', // when used as GET /emails/
  
  // Gmail alternative system
  '/gmail/public/create',
  '/gmail/public/aliases',
  '/gmail/public/rotate',
  '/gmail/public/version',
  '/gmail/', // covers /gmail/:alias/emails
];

// Function to check if a route should be encrypted
const shouldEncryptRoute = (req) => {
  const path = req.path;
  const method = req.method;
  
  // Skip OPTIONS requests (CORS preflight)
  if (method === 'OPTIONS') {
    return false;
  }
  
  // Skip admin, monitoring, auth, and webhook routes
  if (path.includes('/admin/') || 
      path.includes('/health') || 
      path.includes('/monitor/') ||
      path.includes('/auth/') ||
      path.includes('/webhooks/') ||
      path.includes('/debug/')) {
    return false;
  }
  
  // Skip email content routes (international character issues)
  if (path.includes('/received')) {
    return false;
  }
  
  // Check if route matches our encryption targets
  return ROUTES_TO_ENCRYPT.some(route => {
    if (route.endsWith('/')) {
      // For routes ending with /, check if path starts with the route
      return path.startsWith(route);
    } else {
      // For exact routes, check exact match or with ID parameter
      return path === route || path.startsWith(route + '/');
    }
  });
};

// Middleware to encrypt responses
export const encryptResponse = (req, res, next) => {
  // Check if this route should be encrypted
  if (!shouldEncryptRoute(req)) {
    return next();
  }
  
  const originalJson = res.json;
  
  res.json = function(data) {
    try {
      const encryptedData = encryptionService.encrypt(data);
      return originalJson.call(this, encryptedData);
    } catch (error) {
      console.error('Encryption error:', error);
      // Fallback to unencrypted response
      return originalJson.call(this, data);
    }
  };
  
  next();
};

export default encryptionService; 