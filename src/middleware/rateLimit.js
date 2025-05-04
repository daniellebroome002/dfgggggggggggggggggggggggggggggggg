import { v4 as uuidv4 } from 'uuid';
import axios from 'axios';

// In-memory store for rate limiting
const rateLimitStore = {
  // Structure: { [ip]: { count: number, resetAt: timestamp, captchaRequired: boolean } }
  limits: {},
  // Structure: { [userId]: { count: number, resetAt: timestamp, captchaRequired: boolean } }
  userLimits: {},
  
  // Rotating CAPTCHA keys
  captchaKeys: [
    { 
      siteKey: process.env.CAPTCHA_SITE_KEY_1, 
      secretKey: process.env.CAPTCHA_SECRET_KEY_1,
      useCount: 0,
      active: true
    },
    { 
      siteKey: process.env.CAPTCHA_SITE_KEY_2, 
      secretKey: process.env.CAPTCHA_SECRET_KEY_2,
      useCount: 0,
      active: false
    },
    { 
      siteKey: process.env.CAPTCHA_SITE_KEY_3, 
      secretKey: process.env.CAPTCHA_SECRET_KEY_3,
      useCount: 0,
      active: false
    }
  ].filter(key => key.siteKey && key.secretKey), // Filter out undefined keys
  
  // Get current active CAPTCHA keys
  getActiveCaptchaKey() {
    const activeKey = this.captchaKeys.find(key => key.active);
    if (activeKey) {
      activeKey.useCount++;
      
      // Rotate keys if current key exceeds 9,500 uses (close to Google's 10k limit)
      if (activeKey.useCount > 9500) {
        this.rotateCaptchaKeys();
      }
      
      return { siteKey: activeKey.siteKey, secretKey: activeKey.secretKey };
    }
    
    // Fallback if no active key (shouldn't happen normally)
    if (this.captchaKeys.length > 0) {
      this.captchaKeys[0].active = true;
      return { siteKey: this.captchaKeys[0].siteKey, secretKey: this.captchaKeys[0].secretKey };
    }
    
    // Emergency fallback to default key
    return { 
      siteKey: '6LeJJ-UgAAAAAPGWWrhpHGCwwV-1ogC2kjOa_NKm', 
      secretKey: '6LeJJ-UgAAAAAHPkW-3XK2qv2HTCHn-q6lbOt-gL' 
    };
  },
  
  // Rotate to next CAPTCHA key
  rotateCaptchaKeys() {
    if (this.captchaKeys.length <= 1) return;
    
    const currentActiveIndex = this.captchaKeys.findIndex(key => key.active);
    if (currentActiveIndex >= 0) {
      this.captchaKeys[currentActiveIndex].active = false;
      
      // Move to next key
      const nextIndex = (currentActiveIndex + 1) % this.captchaKeys.length;
      this.captchaKeys[nextIndex].active = true;
      this.captchaKeys[nextIndex].useCount = 0;
      
      console.log(`Rotated CAPTCHA key to index ${nextIndex}`);
    }
  },
  
  // Clean up old rate limits periodically
  cleanup() {
    const now = Date.now();
    Object.keys(this.limits).forEach(ip => {
      if (this.limits[ip].resetAt < now) {
        delete this.limits[ip];
      }
    });
    
    Object.keys(this.userLimits).forEach(userId => {
      if (this.userLimits[userId].resetAt < now) {
        delete this.userLimits[userId];
      }
    });
  }
};

// Clean up every hour
setInterval(() => rateLimitStore.cleanup(), 60 * 60 * 1000);

// Configuration
const RATE_LIMIT = {
  MAX_EMAILS_PER_HOUR: 15,
  WINDOW_MS: 60 * 60 * 1000, // 1 hour in milliseconds
  // Additional limits for authenticated users
  AUTH_MAX_EMAILS_PER_HOUR: 15, // Higher limit for authenticated users
};

// Rate limit middleware
export function rateLimitMiddleware(req, res, next) {
  // Get client IP
  const clientIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
  
  const now = Date.now();
  
  // Handle authenticated users with separate limits
  if (req.user) {
    const userId = req.user.id;
    
    // Initialize or reset expired limit for this user
    if (!rateLimitStore.userLimits[userId] || rateLimitStore.userLimits[userId].resetAt < now) {
      rateLimitStore.userLimits[userId] = {
        count: 0,
        resetAt: now + RATE_LIMIT.WINDOW_MS,
        captchaRequired: false
      };
    }
    
    // Increment count for this user
    rateLimitStore.userLimits[userId].count++;
    
    // Check if rate limit is exceeded for authenticated user
    if (rateLimitStore.userLimits[userId].count > RATE_LIMIT.AUTH_MAX_EMAILS_PER_HOUR) {
      rateLimitStore.userLimits[userId].captchaRequired = true;
    }
    
    // Add rateLimitInfo to request for use in route handlers
    req.rateLimitInfo = {
      current: rateLimitStore.userLimits[userId].count,
      limit: RATE_LIMIT.AUTH_MAX_EMAILS_PER_HOUR,
      captchaRequired: rateLimitStore.userLimits[userId].captchaRequired,
      resetAt: rateLimitStore.userLimits[userId].resetAt
    };
    
    return next();
  }
  
  // Handle anonymous users (original logic)
  // Initialize or reset expired limit for this IP
  if (!rateLimitStore.limits[clientIp] || rateLimitStore.limits[clientIp].resetAt < now) {
    rateLimitStore.limits[clientIp] = {
      count: 0,
      resetAt: now + RATE_LIMIT.WINDOW_MS,
      captchaRequired: false
    };
  }
  
  // Increment count for this IP
  rateLimitStore.limits[clientIp].count++;
  
  // Check if rate limit is exceeded
  if (rateLimitStore.limits[clientIp].count > RATE_LIMIT.MAX_EMAILS_PER_HOUR) {
    rateLimitStore.limits[clientIp].captchaRequired = true;
    
    // Don't block the request - we'll check for CAPTCHA in the route handler
  }
  
  // Add rateLimitInfo to request for use in route handlers
  req.rateLimitInfo = {
    current: rateLimitStore.limits[clientIp].count,
    limit: RATE_LIMIT.MAX_EMAILS_PER_HOUR,
    captchaRequired: rateLimitStore.limits[clientIp].captchaRequired,
    resetAt: rateLimitStore.limits[clientIp].resetAt
  };
  
  next();
}

// Verify captcha middleware
export async function verifyCaptcha(req, res, next) {
  const clientIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
  
  // Check if captcha is required
  let isCaptchaRequired = false;
  
  if (req.user) {
    // For authenticated users
    const userId = req.user.id;
    isCaptchaRequired = rateLimitStore.userLimits[userId]?.captchaRequired || false;
  } else {
    // For anonymous users
    isCaptchaRequired = rateLimitStore.limits[clientIp]?.captchaRequired || false;
  }
  
  if (!isCaptchaRequired) {
    return next();
  }
  
  // Get captcha response from request
  const captchaResponse = req.body.captchaResponse;
  
  if (!captchaResponse) {
    return res.status(400).json({ error: 'CAPTCHA_REQUIRED', message: 'CAPTCHA verification required' });
  }
  
  try {
    // Get active CAPTCHA key for verification
    const { secretKey } = rateLimitStore.getActiveCaptchaKey();
    
    // Verify with Google reCAPTCHA
    const verificationURL = `https://www.google.com/recaptcha/api/siteverify?secret=${secretKey}&response=${captchaResponse}`;
    
    const response = await axios.post(verificationURL);
    const data = response.data;
    
    if (data.success) {
      // CAPTCHA verification successful, reset rate limit counter
      if (req.user) {
        // For authenticated users
        const userId = req.user.id;
        if (rateLimitStore.userLimits[userId]) {
          rateLimitStore.userLimits[userId].count = 0; // Reset counter
          rateLimitStore.userLimits[userId].captchaRequired = false; // No longer require CAPTCHA
        }
      } else {
        // For anonymous users
        if (rateLimitStore.limits[clientIp]) {
          rateLimitStore.limits[clientIp].count = 0; // Reset counter
          rateLimitStore.limits[clientIp].captchaRequired = false; // No longer require CAPTCHA
        }
      }
      
      // Proceed with request
      next();
    } else {
      return res.status(400).json({ error: 'INVALID_CAPTCHA', message: 'CAPTCHA verification failed' });
    }
  } catch (error) {
    console.error('CAPTCHA verification error:', error);
    return res.status(500).json({ error: 'CAPTCHA_ERROR', message: 'Error verifying CAPTCHA' });
  }
}

// Utility function to get the current CAPTCHA site key
export function getCurrentCaptchaSiteKey() {
  const { siteKey } = rateLimitStore.getActiveCaptchaKey();
  return siteKey;
}

// Middleware to check if CAPTCHA is required and provide site key
export function checkCaptchaRequired(req, res, next) {
  const clientIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
  
  // Get rate limit info for the appropriate entity (user or IP)
  let isCaptchaRequired = false;
  
  if (req.user) {
    // For authenticated users
    const userId = req.user.id;
    isCaptchaRequired = rateLimitStore.userLimits[userId]?.captchaRequired || false;
  } else {
    // For anonymous users
    isCaptchaRequired = rateLimitStore.limits[clientIp]?.captchaRequired || false;
  }
  
  // Add CAPTCHA info to the response
  res.locals.captchaRequired = isCaptchaRequired;
  if (isCaptchaRequired) {
    res.locals.captchaSiteKey = getCurrentCaptchaSiteKey();
  }
  
  next();
}

// Export the rate limit store for testing/monitoring
export { rateLimitStore };
