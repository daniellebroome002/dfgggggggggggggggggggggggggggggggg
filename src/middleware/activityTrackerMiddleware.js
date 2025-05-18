import { trackActivity } from '../services/activityTracker.js';

export function activityTrackerMiddleware(req, res, next) {
  // Track this request for real-time monitoring
  trackActivity(req, res);
  
  // Continue to the next middleware
  next();
} 