import DOMPurify from 'isomorphic-dompurify';
import validator from 'validator';

// **COMPREHENSIVE INPUT VALIDATION & SANITIZATION UTILITY**
// This module provides centralized security functions to prevent:
// - XSS attacks
// - SQL injection
// - NoSQL injection  
// - Path traversal
// - Command injection
// - Email injection
// - Header injection

// **HTML Sanitization**
export const sanitizeHTML = (html) => {
  if (!html || typeof html !== 'string') return '';
  
  return DOMPurify.sanitize(html, {
    ALLOWED_TAGS: ['b', 'i', 'em', 'strong', 'a', 'br', 'span', 'p', 'div'],
    ALLOWED_ATTR: ['href', 'target', 'rel'],
    FORBID_TAGS: ['script', 'iframe', 'object', 'embed', 'form', 'input', 'style', 'link'],
    FORBID_ATTR: ['onerror', 'onload', 'onclick', 'onmouseover', 'style', 'javascript:']
  });
};

// **Text Sanitization (removes all HTML)**
export const sanitizeText = (text) => {
  if (!text || typeof text !== 'string') return '';
  
  // Remove all HTML tags and decode entities
  return DOMPurify.sanitize(text, { 
    ALLOWED_TAGS: [], 
    ALLOWED_ATTR: [] 
  }).trim();
};

// **Email Validation & Sanitization**
export const validateEmail = (email) => {
  if (!email || typeof email !== 'string') return { isValid: false, sanitized: '' };
  
  const sanitized = sanitizeText(email).toLowerCase().trim();
  
  // Basic format check
  if (!validator.isEmail(sanitized)) {
    return { isValid: false, sanitized };
  }
  
  // Additional security checks
  if (sanitized.includes('<') || sanitized.includes('>') || 
      sanitized.includes('"') || sanitized.includes("'") ||
      sanitized.includes('\n') || sanitized.includes('\r')) {
    return { isValid: false, sanitized };
  }
  
  return { isValid: true, sanitized };
};

// **Domain Validation (Fixed for subdomains)**
export const validateDomain = (domain) => {
  if (!domain || typeof domain !== 'string') return { isValid: false, sanitized: '' };
  
  const sanitized = sanitizeText(domain).toLowerCase().trim();
  
  // Updated regex to support subdomains (multiple dots)
  const domainRegex = /^[a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]?(?:\.[a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]?)*\.[a-zA-Z]{2,}$/;
  
  if (!domainRegex.test(sanitized)) {
    return { isValid: false, sanitized };
  }
  
  // Additional security checks
  if (sanitized.includes('<') || sanitized.includes('>') || 
      sanitized.includes('"') || sanitized.includes("'") ||
      sanitized.includes(' ') || sanitized.includes('\n') || sanitized.includes('\r')) {
    return { isValid: false, sanitized };
  }
  
  return { isValid: true, sanitized };
};

// **Password Validation**
export const validatePassword = (password) => {
  if (!password || typeof password !== 'string') return { isValid: false, errors: ['Password is required'] };
  
  const errors = [];
  
  if (password.length < 8) {
    errors.push('Password must be at least 8 characters long');
  }
  
  if (password.length > 128) {
    errors.push('Password must be less than 128 characters');
  }
  
  // Check for null bytes and control characters
  if (/[\x00-\x1f\x7f]/.test(password)) {
    errors.push('Password contains invalid characters');
  }
  
  return { isValid: errors.length === 0, errors };
};

// **SQL Injection Prevention**
export const sanitizeForSQL = (input) => {
  if (!input || typeof input !== 'string') return '';
  
  // Remove dangerous SQL keywords and characters
  return input
    .replace(/['"\\;]/g, '') // Remove quotes and semicolons
    .replace(/\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC|UNION|SCRIPT)\b/gi, '') // Remove SQL keywords
    .trim();
};

// **NoSQL Injection Prevention**
export const sanitizeForNoSQL = (input) => {
  if (typeof input === 'object' && input !== null) {
    // Prevent NoSQL injection through objects
    return JSON.parse(JSON.stringify(input).replace(/\$[a-zA-Z_][a-zA-Z0-9_]*/g, ''));
  }
  return sanitizeText(input);
};

// **Path Traversal Prevention**
export const sanitizePath = (path) => {
  if (!path || typeof path !== 'string') return '';
  
  return path
    .replace(/\.\./g, '') // Remove directory traversal
    .replace(/[<>:"|?*]/g, '') // Remove invalid filename characters
    .replace(/^\/+/, '') // Remove leading slashes
    .trim();
};

// **Command Injection Prevention**
export const sanitizeCommand = (input) => {
  if (!input || typeof input !== 'string') return '';
  
  // Remove dangerous command characters
  return input
    .replace(/[;&|`$(){}[\]<>]/g, '')
    .replace(/\n|\r/g, '')
    .trim();
};

// **Header Injection Prevention**
export const sanitizeHeader = (header) => {
  if (!header || typeof header !== 'string') return '';
  
  // Remove CRLF characters that could be used for header injection
  return header
    .replace(/[\r\n]/g, '')
    .replace(/[<>]/g, '')
    .trim();
};

// **URL Validation & Sanitization**
export const validateURL = (url) => {
  if (!url || typeof url !== 'string') return { isValid: false, sanitized: '' };
  
  const sanitized = sanitizeText(url).trim();
  
  try {
    const urlObj = new URL(sanitized);
    
    // Only allow http and https protocols
    if (!['http:', 'https:'].includes(urlObj.protocol)) {
      return { isValid: false, sanitized };
    }
    
    return { isValid: true, sanitized: urlObj.toString() };
  } catch {
    return { isValid: false, sanitized };
  }
};

// **Integer Validation**
export const validateInteger = (value, min = null, max = null) => {
  const num = parseInt(value);
  
  if (isNaN(num)) {
    return { isValid: false, value: 0 };
  }
  
  if (min !== null && num < min) {
    return { isValid: false, value: min };
  }
  
  if (max !== null && num > max) {
    return { isValid: false, value: max };
  }
  
  return { isValid: true, value: num };
};

// **String Length Validation**
export const validateStringLength = (str, minLength = 0, maxLength = 1000) => {
  if (!str || typeof str !== 'string') {
    return { isValid: false, sanitized: '' };
  }
  
  const sanitized = sanitizeText(str);
  
  if (sanitized.length < minLength || sanitized.length > maxLength) {
    return { isValid: false, sanitized };
  }
  
  return { isValid: true, sanitized };
};

// **UUID Validation**
export const validateUUID = (uuid) => {
  if (!uuid || typeof uuid !== 'string') return false;
  
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  return uuidRegex.test(uuid);
};

// **IP Address Validation**
export const validateIP = (ip) => {
  if (!ip || typeof ip !== 'string') return false;
  
  return validator.isIP(ip);
};

// **Comprehensive Request Body Sanitizer**
export const sanitizeRequestBody = (body) => {
  if (!body || typeof body !== 'object') return {};
  
  const sanitized = {};
  
  for (const [key, value] of Object.entries(body)) {
    const sanitizedKey = sanitizeText(key);
    
    if (typeof value === 'string') {
      sanitized[sanitizedKey] = sanitizeText(value);
    } else if (typeof value === 'number') {
      sanitized[sanitizedKey] = value;
    } else if (typeof value === 'boolean') {
      sanitized[sanitizedKey] = value;
    } else if (Array.isArray(value)) {
      sanitized[sanitizedKey] = value.map(item => 
        typeof item === 'string' ? sanitizeText(item) : item
      );
    } else if (typeof value === 'object' && value !== null) {
      sanitized[sanitizedKey] = sanitizeRequestBody(value);
    }
  }
  
  return sanitized;
};

// **Rate Limiting Helper**
export const generateRateLimitKey = (ip, userId = null) => {
  const sanitizedIP = sanitizeText(ip);
  const sanitizedUserId = userId ? sanitizeText(userId.toString()) : 'anonymous';
  return `${sanitizedIP}:${sanitizedUserId}`;
};

// **Validation Middleware Factory**
export const createValidationMiddleware = (validationRules) => {
  return (req, res, next) => {
    const errors = [];
    
    for (const [field, rules] of Object.entries(validationRules)) {
      const value = req.body[field];
      
      if (rules.required && (!value || value.toString().trim() === '')) {
        errors.push(`${field} is required`);
        continue;
      }
      
      if (value && rules.type === 'email') {
        const { isValid } = validateEmail(value);
        if (!isValid) {
          errors.push(`${field} must be a valid email address`);
        }
      }
      
      if (value && rules.type === 'domain') {
        const { isValid } = validateDomain(value);
        if (!isValid) {
          errors.push(`${field} must be a valid domain`);
        }
      }
      
      if (value && rules.type === 'password') {
        const { isValid, errors: passwordErrors } = validatePassword(value);
        if (!isValid) {
          errors.push(...passwordErrors);
        }
      }
      
      if (value && rules.minLength && value.toString().length < rules.minLength) {
        errors.push(`${field} must be at least ${rules.minLength} characters`);
      }
      
      if (value && rules.maxLength && value.toString().length > rules.maxLength) {
        errors.push(`${field} must be less than ${rules.maxLength} characters`);
      }
    }
    
    if (errors.length > 0) {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: errors 
      });
    }
    
    // Sanitize the request body
    req.body = sanitizeRequestBody(req.body);
    next();
  };
};

export default {
  sanitizeHTML,
  sanitizeText,
  validateEmail,
  validateDomain,
  validatePassword,
  sanitizeForSQL,
  sanitizeForNoSQL,
  sanitizePath,
  sanitizeCommand,
  sanitizeHeader,
  validateURL,
  validateInteger,
  validateStringLength,
  validateUUID,
  validateIP,
  sanitizeRequestBody,
  generateRateLimitKey,
  createValidationMiddleware
}; 