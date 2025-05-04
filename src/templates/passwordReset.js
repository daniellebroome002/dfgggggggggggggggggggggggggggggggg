export const getPasswordResetEmailTemplate = (resetLink) => ({
    subject: 'Reset Your Password - Boomlify',
    html: `
      <!DOCTYPE html>
      <html>
        <head>
          <style>
            .email-container {
              max-width: 600px;
              margin: 0 auto;
              font-family: Arial, sans-serif;
              color: #333333;
            }
            .header {
              background-color: #4A90E2;
              color: white;
              padding: 20px;
              text-align: center;
            }
            .content {
              padding: 20px;
              line-height: 1.5;
            }
            .button {
              background-color: #4A90E2;
              color: white;
              padding: 12px 24px;
              text-decoration: none;
              border-radius: 4px;
              display: inline-block;
              margin: 20px 0;
            }
            .footer {
              text-align: center;
              padding: 20px;
              font-size: 12px;
              color: #666666;
            }
          </style>
        </head>
        <body>
          <div class="email-container">
            <div class="header">
              <h1>Reset Your Password</h1>
            </div>
            <div class="content">
              <p>Hello,</p>
              <p>We received a request to reset your password. Click the button below to create a new password:</p>
              <p style="text-align: center;">
                <a href="${resetLink}" class="button">Reset Password</a>
              </p>
              <p>This link will expire in 1 hour for security reasons.</p>
              <p>If you didn't request this password reset, please ignore this email or contact support if you have concerns.</p>
              <p>Best regards,<br>The Boomlify Team</p>
            </div>
            <div class="footer">
              <p>This is an automated message, please do not reply to this email.</p>
              <p>Boomlify - Secure Temporary Email Service</p>
            </div>
          </div>
        </body>
      </html>
    `
  });
