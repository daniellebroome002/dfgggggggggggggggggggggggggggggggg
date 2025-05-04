export const trackError = (error, req, res, next) => {
  // Log error details
  console.error('Blog Error:', {
    error: error.message,
    stack: error.stack,
    path: req.path,
    method: req.method,
    body: req.body,
    params: req.params,
    query: req.query,
    timestamp: new Date().toISOString()
  });

  next(error);
};

export const errorHandler = (error, req, res, next) => {
  res.status(error.status || 500).json({
    error: error.message || 'Internal server error',
    code: error.code,
    timestamp: new Date().toISOString()
  });
};
