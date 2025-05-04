import nodemailer from 'nodemailer';
import dotenv from 'dotenv';

dotenv.config();

// Create test email configuration
const testConfig = {
  host: process.env.SMTP_HOST,
  port: parseInt(process.env.SMTP_PORT || '587'),
  secure: false,
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS
  },
  tls: {
    rejectUnauthorized: false
  }
};

// Create transporter
const transporter = nodemailer.createTransport(testConfig);

// Test function
async function testMailServer() {
  console.log('Testing mail server configuration...\n');
  console.log('Mail Server Settings:');
  console.log('Host:', testConfig.host);
  console.log('Port:', testConfig.port);
  console.log('Username:', testConfig.auth.user);
  console.log('TLS Enabled:', !testConfig.secure);
  console.log('\nAttempting to verify connection...');

  try {
    const verify = await transporter.verify();
    console.log('\n✅ Mail server connection successful!');
    
    // Try sending a test email
    console.log('\nAttempting to send test email...');
    
    const info = await transporter.sendMail({
      from: process.env.SMTP_USER,
      to: process.env.SMTP_USER, // Send to self for testing
      subject: 'Mail Server Test',
      text: 'If you receive this email, your mail server is working correctly.',
      html: '<p>If you receive this email, your mail server is working correctly.</p>'
    });

    console.log('\n✅ Test email sent successfully!');
    console.log('Message ID:', info.messageId);
    
  } catch (error) {
    console.error('\n❌ Mail server test failed!');
    console.error('Error details:', error.message);
    
    // Additional error information
    if (error.code === 'ECONNREFUSED') {
      console.error('\nPossible issues:');
      console.error('- Mail server is not running');
      console.error('- Incorrect host or port');
      console.error('- Firewall blocking connection');
    } else if (error.code === 'EAUTH') {
      console.error('\nPossible issues:');
      console.error('- Incorrect username or password');
      console.error('- Authentication method not supported');
    } else if (error.code === 'ESOCKET') {
      console.error('\nPossible issues:');
      console.error('- SSL/TLS configuration problem');
      console.error('- Port configuration mismatch');
    }
  }
}

// Run the test
testMailServer();
