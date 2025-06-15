import jwt from 'jsonwebtoken';
import { v4 as uuidv4 } from 'uuid';
import bcrypt from 'bcryptjs';
import { pool } from './db/init.js';

// In-memory storage for guest sessions
// Using Map for better performance
interface TempEmailData {
  id: string;
  email: string;
  domain_id: string;
  expires_at: string;
  created_at: string;
}

interface ReceivedEmailData {
  id: string;
  temp_email_id: string;
  from_email: string;
  from_name: string;
  subject: string;
  body_html: string;
  body_text: string;
  received_at: string;
}

interface GuestSession {
  id: string;
  emails: Map<string, TempEmailData>; // Map of email.id -> email data
  inbox: Map<string, ReceivedEmailData[]>; // Map of temp_email_id -> received emails
  created_at: Date;
}

// Session storage - token -> session data
const guestSessions = new Map<string, GuestSession>();

// Clean up expired sessions every hour
setInterval(() => {
  const now = new Date();
  for (const [token, session] of guestSessions.entries()) {
    // Check if the token is expired by decoding it
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET || 'default_secret') as { exp: number };
      if (decoded.exp * 1000 < now.getTime()) {
        guestSessions.delete(token);
      }
    } catch (err) {
      // If verification fails, token is invalid or expired
      guestSessions.delete(token);
    }
  }
}, 60 * 60 * 1000); // Run every hour

/**
 * Generates a guest JWT token with 24h expiration
 * @returns {string} JWT token for guest session
 */
export function generateGuestJWT(): string {
  const id = uuidv4();
  const token = jwt.sign(
    { 
      id, 
      isGuest: true,
      isAdmin: false 
    },
    process.env.JWT_SECRET || 'default_secret',
    { expiresIn: '24h' }
  );

  // Create a new guest session
  guestSessions.set(token, {
    id,
    emails: new Map(),
    inbox: new Map(),
    created_at: new Date()
  });

  return token;
}

/**
 * Stores temporary email data in memory for a guest session
 * @param {string} token - Guest JWT token
 * @param {object} emailData - Email data to store
 * @returns {string|null} - ID of stored email or null if failed
 */
export function storeTempEmail(token: string, emailData: TempEmailData): string | null {
  try {
    const session = guestSessions.get(token);
    if (!session) return null;

    const emailId = emailData.id || uuidv4();
    emailData.id = emailId;
    
    // Add created_at timestamp if not provided
    if (!emailData.created_at) {
      emailData.created_at = new Date().toISOString();
    }

    // Store email data in session
    session.emails.set(emailId, emailData);
    
    // Initialize empty inbox for this email
    if (!session.inbox.has(emailId)) {
      session.inbox.set(emailId, []);
    }

    return emailId;
  } catch (error) {
    console.error('Error storing temp email:', error);
    return null;
  }
}

/**
 * Retrieves all temporary emails for a guest session
 * @param {string} token - Guest JWT token
 * @returns {TempEmailData[]} - Array of temp emails or empty array if none found
 */
export function getTempEmails(token: string): TempEmailData[] {
  try {
    const session = guestSessions.get(token);
    if (!session) return [];
    
    return Array.from(session.emails.values());
  } catch (error) {
    console.error('Error retrieving temp emails:', error);
    return [];
  }
}

/**
 * Gets a single temporary email by ID
 * @param {string} token - Guest JWT token
 * @param {string} emailId - ID of the temporary email
 * @returns {TempEmailData|null} - Temp email data or null if not found
 */
export function getTempEmailById(token: string, emailId: string): TempEmailData | null {
  try {
    const session = guestSessions.get(token);
    if (!session) return null;

    return session.emails.get(emailId) || null;
  } catch (error) {
    console.error('Error retrieving temp email by ID:', error);
    return null;
  }
}

/**
 * Stores a received email in the guest's inbox
 * @param {string} token - Guest JWT token
 * @param {string} tempEmailId - ID of the temporary email
 * @param {object} emailData - Received email data
 * @returns {boolean} - Success status
 */
export function storeReceivedEmail(
  token: string,
  tempEmailId: string,
  emailData: ReceivedEmailData
): boolean {
  try {
    const session = guestSessions.get(token);
    if (!session) return false;

    if (!session.emails.has(tempEmailId)) return false;

    // Ensure inbox exists for this email
    if (!session.inbox.has(tempEmailId)) {
      session.inbox.set(tempEmailId, []);
    }

    // Add ID if not provided
    const emailId = emailData.id || uuidv4();
    emailData.id = emailId;
    emailData.temp_email_id = tempEmailId;

    // Add timestamp if not provided
    if (!emailData.received_at) {
      emailData.received_at = new Date().toISOString();
    }

    // Add email to inbox
    const inbox = session.inbox.get(tempEmailId)!;
    inbox.push(emailData);

    return true;
  } catch (error) {
    console.error('Error storing received email:', error);
    return false;
  }
}

/**
 * Gets inbox content for a temporary email
 * @param {string} token - Guest JWT token
 * @param {string} tempEmailId - ID of the temporary email
 * @returns {ReceivedEmailData[]} - Array of received emails
 */
export function getInbox(token: string, tempEmailId: string): ReceivedEmailData[] {
  try {
    const session = guestSessions.get(token);
    if (!session) return [];

    return session.inbox.get(tempEmailId) || [];
  } catch (error) {
    console.error('Error retrieving inbox:', error);
    return [];
  }
}

/**
 * Migrates guest session data to a registered user
 * @param {string} token - Guest JWT token
 * @param {string} userId - ID of the registered user
 * @param {string} realEmail - User's real email
 * @param {string} realPassword - User's real password
 * @returns {Promise<boolean>} - Success status
 */
export async function migrateGuestSessionToUser(
  token: string,
  userId: string,
  realEmail: string,
  realPassword: string
): Promise<boolean> {
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    const session = guestSessions.get(token);
    if (!session) return false;

    // First check if the user already exists
    const [existingUsers] = await connection.query(
      'SELECT id FROM users WHERE email = ?',
      [realEmail]
    ) as any[];

    if (existingUsers.length > 0) {
      // User already exists, we can't migrate
      throw new Error('User already exists');
    }

    // Hash the password
    const hashedPassword = await bcrypt.hash(realPassword, 10);

    // Insert the new user
    await connection.query(
      'INSERT INTO users (id, email, password) VALUES (?, ?, ?)',
      [userId, realEmail, hashedPassword]
    );

    // Migrate all temporary emails
    for (const [emailId, emailData] of session.emails) {
      // Insert temp_email with the real user_id (guest migration only uses regular domains)
      await connection.query(
        'INSERT INTO temp_emails (id, user_id, email, domain_id, custom_domain_id, expires_at, created_at) VALUES (?, ?, ?, ?, NULL, ?, ?)',
        [
          emailId,
          userId,
          emailData.email,
          emailData.domain_id,
          emailData.expires_at,
          emailData.created_at
        ]
      );

      // Migrate all received emails for this temp email
      const receivedEmails = session.inbox.get(emailId) || [];
      for (const receivedEmail of receivedEmails) {
        await connection.query(
          `INSERT INTO received_emails 
          (id, temp_email_id, from_email, from_name, subject, body_html, body_text, received_at) 
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            receivedEmail.id,
            emailId,
            receivedEmail.from_email,
            receivedEmail.from_name,
            receivedEmail.subject,
            receivedEmail.body_html,
            receivedEmail.body_text,
            receivedEmail.received_at
          ]
        );
      }
    }

    await connection.commit();
    
    // Delete the guest session after successful migration
    guestSessions.delete(token);
    
    return true;
  } catch (error) {
    await connection.rollback();
    console.error('Error migrating guest session:', error);
    return false;
  } finally {
    connection.release();
  }
}

/**
 * Checks if a token is a valid guest token
 * @param {string} token - JWT token to verify
 * @returns {boolean} - Whether the token is a valid guest token
 */
export function isValidGuestToken(token: string): boolean {
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET || 'default_secret') as { isGuest?: boolean };
    return !!decoded.isGuest && guestSessions.has(token);
  } catch (error) {
    return false;
  }
}

/**
 * Deletes a guest session
 * @param {string} token - Guest JWT token
 * @returns {boolean} - Success status
 */
export function deleteGuestSession(token: string): boolean {
  return guestSessions.delete(token);
} 