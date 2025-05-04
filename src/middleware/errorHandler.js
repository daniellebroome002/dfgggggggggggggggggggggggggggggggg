import { ErrorTypes, AppError } from '../types/errors.js';

// Error handler middleware
export const errorHandler = (err, req, res, next) => {
  console.error('Error:', err);

  // If the error is already an AppError, use its properties
  if (err instanceof AppError) {
    return res.status(getStatusCode(err.type)).json(err.toJSON());
  }

  // Handle specific error types
  if (err.name === 'ValidationError') {
    const appError = new AppError(ErrorTypes.VALIDATION.FAILED, {
      fields: Object.keys(err.errors),
      details: Object.values(err.errors).map(e => e.message)
    });
    return res.status(400).json(appError.toJSON());
  }

  if (err.name === 'UnauthorizedError') {
    const appError = new AppError(ErrorTypes.AUTH.UNAUTHORIZED);
    return res.status(401).json(appError.toJSON());
  }

  // Default error response
  const appError = new AppError(ErrorTypes.SERVER.ERROR, {
    originalError: process.env.NODE_ENV === 'development' ? err.message : undefined
  });
  return res.status(500).json(appError.toJSON());
};

// Get HTTP status code based on error type
function getStatusCode(errorType) {
  const statusCodes = {
    [ErrorTypes.EMAIL.EXISTS]: 409,
    [ErrorTypes.EMAIL.INVALID]: 400,
    [ErrorTypes.EMAIL.NOT_FOUND]: 404,
    [ErrorTypes.EMAIL.EXPIRED]: 410,
    [ErrorTypes.DOMAIN.INVALID]: 400,
    [ErrorTypes.DOMAIN.NOT_FOUND]: 404,
    [ErrorTypes.DOMAIN.UNAVAILABLE]: 503,
    [ErrorTypes.RATE_LIMIT.EXCEEDED]: 429,
    [ErrorTypes.RATE_LIMIT.CAPTCHA_REQUIRED]: 429,
    [ErrorTypes.RATE_LIMIT.INVALID_CAPTCHA]: 400,
    [ErrorTypes.AUTH.UNAUTHORIZED]: 401,
    [ErrorTypes.AUTH.INVALID_TOKEN]: 401,
    [ErrorTypes.AUTH.TOKEN_EXPIRED]: 401,
    [ErrorTypes.VALIDATION.FAILED]: 400,
    [ErrorTypes.VALIDATION.MISSING_FIELDS]: 400,
    [ErrorTypes.SERVER.ERROR]: 500,
    [ErrorTypes.SERVER.DATABASE_ERROR]: 503,
    [ErrorTypes.SERVER.MAINTENANCE]: 503
  };

  return statusCodes[errorType] || 500;
}

// Request validation middleware
export const validateRequest = (schema) => {
  return (req, res, next) => {
    try {
      const { error } = schema.validate(req.body);
      if (error) {
        throw new AppError(ErrorTypes.VALIDATION.FAILED, {
          details: error.details.map(detail => detail.message)
        });
      }
      next();
    } catch (err) {
      next(err);
    }
  };
};

// Rate limit error handler
export const handleRateLimit = (req, res, next) => {
  if (req.rateLimit && req.rateLimit.remaining === 0) {
    const error = new AppError(ErrorTypes.RATE_LIMIT.EXCEEDED, {
      retryAfter: req.rateLimit.resetTime - Date.now(),
      limit: req.rateLimit.limit,
      windowMs: req.rateLimit.windowMs
    });
    return res.status(429).json(error.toJSON());
  }
  next();
};

// Not found error handler
export const notFoundHandler = (req, res) => {
  const error = new AppError(ErrorTypes.SERVER.ERROR, {
    path: req.originalUrl,
    method: req.method
  });
  res.status(404).json(error.toJSON());
};
