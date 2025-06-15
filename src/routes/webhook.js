import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { pool } from '../db/init.js';
import { simpleParser } from 'mailparser';
import iconv from 'iconv-lite';
import { 
  findGuestByEmail, 
  storeReceivedEmail, 
  isEmailAddressInUse,
  findRegisteredUserByEmail, 
  cacheReceivedEmail
} from '../guestSessionHandler.js';

// Email parsing helper functions
function extractSenderEmail(emailFrom) {
  // If no email provided, return empty string
  if (!emailFrom) return '';

  // Try to extract email from format "Name <email@domain.com>"
  const angleEmailMatch = emailFrom.match(/<(.+?)>/);
  if (angleEmailMatch) {
    return angleEmailMatch[1];
  }

  // Try to extract email from format "email@domain.com"
  const simpleEmailMatch = emailFrom.match(/([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9._-]+)/);
  if (simpleEmailMatch) {
    return simpleEmailMatch[1];
  }

  // Handle bounce/system emails
  if (emailFrom.includes('bounce') || emailFrom.includes('mailer-daemon')) {
    // Try to extract original sender from common bounce formats
    const bounceMatch = emailFrom.match(/original-sender:\s*([^\s]+@[^\s]+)/i);
    if (bounceMatch) {
      return bounceMatch[1];
    }
    
    // If it's a bounce but we can't find original sender, mark it clearly
    return 'system@bounced.mail';
  }

  // Return original if no pattern matches
  return emailFrom;
}

function extractSenderName(emailFrom) {
  if (!emailFrom) return 'Unknown Sender';

  // Try to extract name from "Name <email@domain.com>"
  const nameMatch = emailFrom.match(/^"?([^"<]+)"?\s*</);
  if (nameMatch) {
    return nameMatch[1].trim();
  }

  // For bounce messages, return clear system name
  if (emailFrom.includes('bounce') || emailFrom.includes('mailer-daemon')) {
    return 'System Notification';
  }

  // If no name found, use email local part
  const email = extractSenderEmail(emailFrom);
  return email.split('@')[0] || 'Unknown Sender';
}

function cleanSubject(subject) {
  if (!subject) return 'No Subject';

  // Remove common prefixes
  const prefixesToRemove = [
    /^re:\s*/i,
    /^fwd:\s*/i,
    /^fw:\s*/i,
    /^\[SPAM\]\s*/i,
    /^bounce:/i,
    /^auto.*reply:\s*/i,
    /^automatic\s+reply:\s*/i
  ];

  let cleanedSubject = subject;
  prefixesToRemove.forEach(prefix => {
    cleanedSubject = cleanedSubject.replace(prefix, '');
  });

  // Decode HTML entities
  cleanedSubject = cleanedSubject
    .replace(/&quot;/g, '"')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#(\d+);/g, (match, dec) => String.fromCharCode(dec));

  // Remove excess whitespace
  cleanedSubject = cleanedSubject.replace(/\s+/g, ' ').trim();

  // Limit length
  if (cleanedSubject.length > 100) {
    cleanedSubject = cleanedSubject.substring(0, 97) + '...';
  }

  return cleanedSubject || 'No Subject';
}

async function parseEmailContent(rawContent) {
  try {
    // Decode content if needed
    let decodedContent = rawContent;
    if (typeof rawContent === 'string') {
      try {
        // Try UTF-8 first
        decodedContent = iconv.decode(Buffer.from(rawContent), 'utf8');
      } catch (err) {
        // Fallback to latin1
        decodedContent = iconv.decode(Buffer.from(rawContent), 'latin1');
      }
    }

    // Parse email using mailparser
    const parsed = await simpleParser(decodedContent);

    return {
      headers: parsed.headers,
      subject: parsed.subject,
      from: parsed.from?.text || '',
      to: parsed.to?.text || '',
      text: parsed.text,
      html: parsed.html,
      attachments: parsed.attachments.map(attachment => ({
        filename: attachment.filename,
        contentType: attachment.contentType,
        size: attachment.size,
        content: attachment.content.toString('base64')
      }))
    };
  } catch (error) {
    console.error('Error parsing email:', error);
    return {
      headers: {},
      subject: 'Unable to parse subject',
      from: '',
      to: '',
      text: rawContent,
      html: '',
      attachments: []
    };
  }
}

const router = express.Router();

/**
 * Webhook endpoint for receiving emails
 * Priority:
 * 1. Check if recipient is a guest user (in-memory)
 * 2. If not, check if it's a registered user with cached data (in-memory)
 * 3. If not, check if it's a registered user (database)
 * This prevents database load for both guest users and frequent registered users
 */
router.post('/email/incoming', express.urlencoded({ extended: true }), async (req, res) => {
  console.log('Received webhook request');
  console.log('Content-Type:', req.headers['content-type']);
  
  try {
    const rawContent = req.body.body;
    const parsedEmail = await parseEmailContent(rawContent);
    
    // Extract and clean email data
    const senderEmail = extractSenderEmail(req.body.sender || parsedEmail.from);
    const senderName = extractSenderName(req.body.sender || parsedEmail.from);
    const cleanedSubject = cleanSubject(parsedEmail.subject);
    
    // Clean the recipient email address
    const cleanRecipient = (req.body.recipient || parsedEmail.to).includes('<') ? 
      (req.body.recipient || parsedEmail.to).match(/<(.+)>/)[1] : 
      (req.body.recipient || parsedEmail.to).trim();
    
    const emailData = {
      id: uuidv4(),
      from_email: senderEmail,
      from_name: senderName,
      subject: cleanedSubject,
      body_html: parsedEmail.html || '',
      body_text: parsedEmail.text || '',
      received_at: new Date().toISOString(),
      is_spam: false // You could add spam detection logic here
    };
    
    // 1. FIRST: Check if the recipient belongs to a guest user
    const guestInfo = findGuestByEmail(cleanRecipient);
    
    // 2. SECOND: Check if it belongs to a registered user in cache
    const registeredInfo = findRegisteredUserByEmail(cleanRecipient);
    
    // 3. THIRD: Check if it exists in the database (regular domains)
    const [tempEmails] = await pool.query(
      'SELECT id, user_id FROM temp_emails WHERE email = ? AND expires_at > NOW()',
      [cleanRecipient]
    );
    
    // 4. FOURTH: Check if it's from a custom domain
    const recipientDomain = cleanRecipient.split('@')[1];
    const [customDomainEmails] = await pool.query(
      `SELECT te.id, te.user_id, cd.domain 
       FROM temp_emails te 
       JOIN custom_domains cd ON cd.user_id = te.user_id 
       WHERE te.email = ? AND cd.domain = ? AND cd.status = 'verified' AND te.expires_at > NOW()`,
      [cleanRecipient, recipientDomain]
    );
    
    // Process in priority order: guest, registered cache, database
    
    // Handle potential conflicts between memory caches and database
    if ((guestInfo || registeredInfo) && tempEmails.length > 0) {
      console.warn(`CONFLICT: Email ${cleanRecipient} exists in multiple stores!`);
      
      // Prioritize registered user in database
      const tempEmailId = tempEmails[0].id;
      const userId = tempEmails[0].user_id;
      
      // Insert into the database
      await pool.query(
        'INSERT INTO received_emails (id, temp_email_id, from_email, from_name, subject, body_html, body_text, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())',
        [
          emailData.id,
          tempEmailId,
          emailData.from_email,
          emailData.from_name,
          emailData.subject,
          emailData.body_html,
          emailData.body_text
        ]
      );
      
      // Also update cache if it exists
      if (registeredInfo && registeredInfo.userId === userId) {
        cacheReceivedEmail(userId, tempEmailId, emailData);
      }
      
      return res.status(200).json({ 
        success: true, 
        message: 'Email stored in database (conflict resolved)',
        emailId: emailData.id,
        conflict: true
      });
    }
    
    // If it's a guest user, store in memory
    if (guestInfo) {
      console.log(`Received email for guest user: ${cleanRecipient}`);
      const success = storeReceivedEmail(guestInfo.token, guestInfo.emailId, emailData);
      
      if (success) {
        return res.status(200).json({ 
          success: true, 
          message: 'Email stored in guest session',
          emailId: emailData.id
        });
      }
    }
    
    // If it's a registered user with active cache, update both DB and cache
    if (registeredInfo) {
      console.log(`Received email for cached registered user: ${cleanRecipient}`);
      
      // Store in database
      await pool.query(
        'INSERT INTO received_emails (id, temp_email_id, from_email, from_name, subject, body_html, body_text, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())',
        [
          emailData.id,
          registeredInfo.emailId,
          emailData.from_email,
          emailData.from_name,
          emailData.subject,
          emailData.body_html,
          emailData.body_text
        ]
      );
      
      // Also update the cache
      cacheReceivedEmail(registeredInfo.userId, registeredInfo.emailId, emailData);
      
      return res.status(200).json({ 
        success: true, 
        message: 'Email stored in database and cache',
        emailId: emailData.id
      });
    }
    
    // If not a guest or cached registered user, check database (regular domains)
    if (tempEmails.length > 0) {
      // Store in database for registered user
      await pool.query(
        'INSERT INTO received_emails (id, temp_email_id, from_email, from_name, subject, body_html, body_text, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())',
        [
          emailData.id,
          tempEmails[0].id,
          emailData.from_email,
          emailData.from_name,
          emailData.subject,
          emailData.body_html,
          emailData.body_text
        ]
      );
      
      return res.status(200).json({ 
        success: true, 
        message: 'Email stored in database',
        emailId: emailData.id
      });
    }
    
    // Check for custom domain emails
    if (customDomainEmails.length > 0) {
      // Store in database for custom domain user
      await pool.query(
        'INSERT INTO received_emails (id, temp_email_id, from_email, from_name, subject, body_html, body_text, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())',
        [
          emailData.id,
          customDomainEmails[0].id,
          emailData.from_email,
          emailData.from_name,
          emailData.subject,
          emailData.body_html,
          emailData.body_text
        ]
      );
      
      return res.status(200).json({ 
        success: true, 
        message: 'Email stored in database (custom domain)',
        emailId: emailData.id,
        customDomain: customDomainEmails[0].domain
      });
    }
    
    // Check if this is from a deleted custom domain (orphaned email)
    const [deletedDomainCheck] = await pool.query(
      'SELECT domain FROM custom_domains WHERE domain = ? AND status != ?',
      [recipientDomain, 'verified']
    );
    
    if (deletedDomainCheck.length > 0) {
      console.warn(`Received email for deleted/unverified custom domain: ${recipientDomain}`);
      
      return res.status(404).json({ 
        success: false, 
        message: 'Custom domain not verified or deleted',
        recipient: cleanRecipient,
        domain: recipientDomain,
        reason: 'orphaned_custom_domain'
      });
    }
    
    // If the email doesn't exist in any system, return a 404
    return res.status(404).json({ 
      success: false, 
      message: 'Recipient not found in any system',
      recipient: cleanRecipient
    });
  } catch (error) {
    console.error('Webhook error:', error);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to process incoming email',
      error: process.env.NODE_ENV === 'development' ? error.message : 'Internal server error'
    });
  }
});

export default router;
