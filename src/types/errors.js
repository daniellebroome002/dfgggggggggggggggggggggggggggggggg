// Error types for consistent error handling across the application
export const ErrorTypes = {
  EMAIL: {
    EXISTS: 'EMAIL_EXISTS',
    INVALID: 'INVALID_EMAIL',
    CREATION_FAILED: 'EMAIL_CREATION_FAILED',
    NOT_FOUND: 'EMAIL_NOT_FOUND',
    EXPIRED: 'EMAIL_EXPIRED'
  },
  DOMAIN: {
    INVALID: 'INVALID_DOMAIN',
    NOT_FOUND: 'DOMAIN_NOT_FOUND',
    UNAVAILABLE: 'DOMAIN_UNAVAILABLE'
  },
  RATE_LIMIT: {
    EXCEEDED: 'RATE_LIMIT_EXCEEDED',
    CAPTCHA_REQUIRED: 'CAPTCHA_REQUIRED',
    INVALID_CAPTCHA: 'INVALID_CAPTCHA'
  },
  AUTH: {
    UNAUTHORIZED: 'UNAUTHORIZED',
    INVALID_TOKEN: 'INVALID_TOKEN',
    TOKEN_EXPIRED: 'TOKEN_EXPIRED'
  },
  VALIDATION: {
    FAILED: 'VALIDATION_FAILED',
    MISSING_FIELDS: 'MISSING_REQUIRED_FIELDS'
  },
  SERVER: {
    ERROR: 'SERVER_ERROR',
    DATABASE_ERROR: 'DATABASE_ERROR',
    MAINTENANCE: 'SERVER_MAINTENANCE'
  }
};

// Error messages with user-friendly descriptions and recovery suggestions
export const ErrorMessages = {
  [ErrorTypes.EMAIL.EXISTS]: {
    message: 'This email address already exists',
    suggestion: 'Please try a different email address or modify the existing one'
  },
  [ErrorTypes.EMAIL.INVALID]: {
    message: 'Invalid email address format',
    suggestion: 'Please check the email format and try again'
  },
  [ErrorTypes.EMAIL.CREATION_FAILED]: {
    message: 'Failed to create email address',
    suggestion: 'Please try again in a few moments'
  },
  [ErrorTypes.EMAIL.NOT_FOUND]: {
    message: 'Email address not found',
    suggestion: 'The email may have expired or been deleted'
  },
  [ErrorTypes.EMAIL.EXPIRED]: {
    message: 'This email address has expired',
    suggestion: 'Please create a new temporary email'
  },
  [ErrorTypes.DOMAIN.INVALID]: {
    message: 'Invalid domain selected',
    suggestion: 'Please select a different domain'
  },
  [ErrorTypes.DOMAIN.NOT_FOUND]: {
    message: 'Domain not found',
    suggestion: 'Please select an available domain'
  },
  [ErrorTypes.DOMAIN.UNAVAILABLE]: {
    message: 'Domain temporarily unavailable',
    suggestion: 'Please try a different domain or try again later'
  },
  [ErrorTypes.RATE_LIMIT.EXCEEDED]: {
    message: 'Rate limit exceeded',
    suggestion: 'Please wait a few minutes before trying again'
  },
  [ErrorTypes.RATE_LIMIT.CAPTCHA_REQUIRED]: {
    message: 'Please complete the CAPTCHA verification',
    suggestion: 'This helps us prevent abuse of our service'
  },
  [ErrorTypes.RATE_LIMIT.INVALID_CAPTCHA]: {
    message: 'Invalid CAPTCHA response',
    suggestion: 'Please complete the CAPTCHA verification again'
  },
  [ErrorTypes.AUTH.UNAUTHORIZED]: {
    message: 'Unauthorized access',
    suggestion: 'Please log in to continue'
  },
  [ErrorTypes.AUTH.INVALID_TOKEN]: {
    message: 'Invalid authentication token',
    suggestion: 'Please log in again'
  },
  [ErrorTypes.AUTH.TOKEN_EXPIRED]: {
    message: 'Your session has expired',
    suggestion: 'Please log in again to continue'
  },
  [ErrorTypes.VALIDATION.FAILED]: {
    message: 'Validation failed',
    suggestion: 'Please check your input and try again'
  },
  [ErrorTypes.VALIDATION.MISSING_FIELDS]: {
    message: 'Required fields are missing',
    suggestion: 'Please fill in all required fields'
  },
  [ErrorTypes.SERVER.ERROR]: {
    message: 'Server error occurred',
    suggestion: 'Please try again later'
  },
  [ErrorTypes.SERVER.DATABASE_ERROR]: {
    message: 'Database error occurred',
    suggestion: 'Please try again later'
  },
  [ErrorTypes.SERVER.MAINTENANCE]: {
    message: 'Server is under maintenance',
    suggestion: 'Please try again in a few minutes'
  }
};

// Custom error class for application errors
export class AppError extends Error {
  constructor(type, details = {}) {
    const errorInfo = ErrorMessages[type] || { 
      message: 'An unknown error occurred',
      suggestion: 'Please try again'
    };
    
    super(errorInfo.message);
    
    this.name = 'AppError';
    this.type = type;
    this.details = details;
    this.suggestion = errorInfo.suggestion;
    this.timestamp = new Date().toISOString();
  }

  toJSON() {
    return {
      error: this.type,
      message: this.message,
      suggestion: this.suggestion,
      details: this.details,
      timestamp: this.timestamp
    };
  }
}