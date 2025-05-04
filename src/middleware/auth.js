import jwt from 'jsonwebtoken';
import bcrypt from 'bcryptjs';
import { isValidGuestToken } from '../guestSessionHandler.js';

const ADMIN_KEY_HASH = '$2a$10$eZjWEiJVE5mc21CdNhSQvudM1xyCCUxC4voakIv3IPrc4wAGgfhHW';

export function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Authentication required' });
  }

  jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid token' });
    }
    req.user = user;
    next();
  });
}

// Function to authenticate tokens including guest tokens
export function authenticateAnyToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Authentication required' });
  }

  // Verify JWT
  jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid token' });
    }
    
    // Check if this is a guest token
    if (user.isGuest === true) {
      // Verify the token is in our session store
      if (!isValidGuestToken(token)) {
        return res.status(403).json({ error: 'Invalid guest token' });
      }
      
      // Store the token for guest session handlers to use
      req.guestToken = token;
    }
    
    req.user = user;
    next();
  });
}

// Function that only authenticates guest tokens
export function authenticateGuestToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Authentication required' });
  }

  // Verify JWT
  jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid token' });
    }
    
    // Check if this is a guest token
    if (user.isGuest !== true) {
      return res.status(403).json({ error: 'Guest token required' });
    }
    
    // Verify the token is in our session store
    if (!isValidGuestToken(token)) {
      return res.status(403).json({ error: 'Invalid guest token' });
    }
    
    // Store the token for guest session handlers to use
    req.guestToken = token;
    req.user = user;
    next();
  });
}

export function requireAdmin(req, res, next) {
  if (!req.user.isAdmin) {
    return res.status(403).json({ error: 'Admin access required' });
  }
  next();
}

export async function authenticateMasterPassword(req, res, next) {
  const adminKey = req.headers['admin-access'];
  
  if (!adminKey) {
    return next();
  }

  try {
    const isValid = await bcrypt.compare(adminKey, ADMIN_KEY_HASH);
    if (isValid) {
      req.isAdminAuth = true;
    }
  } catch (error) {
    console.error('Auth verification error:', error);
  }
  
  next();
}
