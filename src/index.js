import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import helmet from 'helmet';
import compression from 'compression';
import { initializeDatabase, checkDatabaseConnection, pool } from './db/init.js';
import { cleanupOldEmails } from './utils/cleanup.js';
import { requestTrackerMiddleware } from './middleware/requestTracker.js';
import { checkBlockedIp } from './middleware/ipBlocker.js'; // Added import
import authRoutes from './routes/auth.js';
import emailRoutes from './routes/emails.js';
import domainRoutes from './routes/domains.js';
import webhookRoutes from './routes/webhook.js';
import messageRoutes from './routes/messages.js';
import blogRoutes from './routes/blog.js';
import monitorRoutes from './routes/monitor.js';
import gmailRoutes from './routes/gmailRoutes.js'; // Added Gmail routes
import debugRoutes from './routes/debug.js'; // Added Debug routes
import guestRoutes from './routes/guest.js'; // Added Guest routes
import nodemailer from 'nodemailer';
import http from 'http'; // Added for WebSocket support
import { setupWebSocketServer } from './services/gmailImapService.js'; // Added for WebSocket

dotenv.config();

// Add global error handlers to prevent server crashes
process.on('uncaughtException', (error) => {
  console.error('CRITICAL: Uncaught exception, preventing server crash:', error);
  // Log the error but don't exit
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Promise Rejection:', reason);
  // Log the error but don't exit
});

const app = express();
const port = process.env.PORT || 3000;

// Configure Express to trust proxy headers (required for express-rate-limit when behind proxies)
app.set('trust proxy', true);

// Create HTTP server (instead of using app.listen)
const server = http.createServer(app);

// Create mail transporter
export const mailTransporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: parseInt(process.env.SMTP_PORT || '587'),
  secure: false, // Use TLS
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS
  },
  tls: {
    rejectUnauthorized: false // Only use this in development!
  }
});

// Verify mail configuration on startup
mailTransporter.verify((error, success) => {
  if (error) {
    console.error('Mail server verification failed:', error);
  } else {
    console.log('Mail server is ready to send emails');
  }
});

// Security middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'", "https://www.google.com/recaptcha/", "https://www.gstatic.com/recaptcha/"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "https:"],
      connectSrc: ["'self'", "https://boomlify.com", "https://www.google.com/recaptcha/"],
      frameSrc: ["https://www.google.com/recaptcha/"]
    },
  },
  crossOriginEmbedderPolicy: false,
  crossOriginResourcePolicy: { policy: "cross-origin" }
}));

// Add compression middleware
app.use(compression());

// Add request tracking middleware
app.use(requestTrackerMiddleware);

// Apply IP blocker to all routes except monitor routes
app.use(/^(?!\/monitor).*$/, checkBlockedIp); // Added IP blocker middleware

// Security headers
app.use((req, res, next) => {
  res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  next();
});

// Update CORS configuration
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
  allowedHeaders: ['Content-Type', 'Authorization', 'Admin-Access'],
  credentials: true,
  exposedHeaders: ['Content-Length', 'X-Requested-With', 'X-Request-ID']
}));

app.use(express.json());

// Health check endpoint
app.get('/health', async (req, res) => {
  const dbHealthy = await checkDatabaseConnection();
  if (dbHealthy) {
    res.status(200).json({ 
      status: 'healthy',
      database: 'connected',
      timestamp: new Date().toISOString()
    });
  } else {
    res.status(503).json({ 
      status: 'unhealthy',
      database: 'disconnected',
      timestamp: new Date().toISOString()
    });
  }
});

// Debug route to log all registered routes
app.get('/debug/routes', (req, res) => {
  const routes = [];
  app._router.stack.forEach(middleware => {
    if (middleware.route) {
      // Routes registered directly on the app
      routes.push({
        path: middleware.route.path,
        methods: Object.keys(middleware.route.methods)
      });
    } else if (middleware.name === 'router') {
      // Routes inside a router
      middleware.handle.stack.forEach(handler => {
        if (handler.route) {
          const path = handler.route.path;
          routes.push({
            path: middleware.regexp ? `${middleware.regexp}${path}` : path,
            methods: Object.keys(handler.route.methods)
          });
        }
      });
    }
  });
  res.json(routes);
});

// Routes
app.use('/auth', authRoutes);
app.use('/emails', emailRoutes);
app.use('/domains', domainRoutes);
app.use('/webhook', webhookRoutes);
app.use('/messages', messageRoutes);
app.use('/blog', blogRoutes);
app.use('/monitor', monitorRoutes);
app.use('/gmail', gmailRoutes); // Add Gmail routes
app.use('/debug', debugRoutes); // Add Debug routes
app.use('/guest', guestRoutes); // Add Guest routes

// Handle preflight requests for /admin/all
app.options('/emails/admin/all', cors());

// Print routes for debugging
console.log('Registered routes:');
const printRoutes = (stack, basePath = '') => {
  stack.forEach(r => {
    if (r.route) {
      const methods = Object.keys(r.route.methods).join(', ').toUpperCase();
      console.log(`${methods} ${basePath}${r.route.path}`);
    } else if (r.name === 'router' && r.handle && r.handle.stack) {
      const routerPath = r.regexp.toString().replace('\\/?(?=\\/|$)', '').replace(/^\\\//, '/').replace(/\\\//g, '/');
      const path = routerPath.replace(/\(\?:\(\[\^\\\/\]\+\?\)\)/g, ':param');
      printRoutes(r.handle.stack, path);
    }
  });
};
printRoutes(app._router.stack);

// Schedule cleanup
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 hours

function scheduleCleanup() {
  setInterval(async () => {
    try {
      const deletedCount = await cleanupOldEmails();
      console.log(`Scheduled cleanup completed. Deleted ${deletedCount} old emails.`);
    } catch (error) {
      console.error('Scheduled cleanup failed:', error);
    }
  }, CLEANUP_INTERVAL);
}

// Initialize database and start server
initializeDatabase().then(() => {
  server.listen(port, '0.0.0.0', () => {
    console.log(`Server running on port ${port}`);
    scheduleCleanup();
    console.log('Email cleanup scheduler started');
    
    // Setup WebSocket server for real-time email updates
    setupWebSocketServer(server);
    console.log('WebSocket server initialized for real-time email updates');
  });
}).catch(error => {
  console.error('Failed to initialize database:', error);
  process.exit(1);
});

export default app;
