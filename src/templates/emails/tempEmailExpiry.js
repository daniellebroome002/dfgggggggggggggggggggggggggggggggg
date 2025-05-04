export const tempEmailExpiryTemplate = {
  subject: "Your temporary email {{temp_email}} is expiring soon",
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
          .expiry-notice {
            background-color: #fff3cd;
            border: 1px solid #ffeeba;
            color: #856404;
            padding: 15px;
            border-radius: 4px;
            margin: 15px 0;
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
            <h1>Email Expiring Soon</h1>
          </div>
          <div class="content">
            <p>Hello {{name}},</p>
            
            <div class="expiry-notice">
              <p>Your temporary email <strong>{{temp_email}}</strong> will expire in {{days_left}} days.</p>
            </div>

            <p>To ensure you don't miss any important emails, please:</p>
            <ul>
              <li>Check your inbox for any pending messages</li>
              <li>Save any important information</li>
              <li>Consider extending the email validity if needed</li>
            </ul>

            <p style="text-align: center;">
              <a href="{{dashboard_url}}" class="button">Manage My Emails</a>
            </p>
            
            <p>Best regards,<br>The Boomlify Team</p>
          </div>
          <div class="footer">
            <p>
              You received this email because you're a Boomlify user. 
              <a href="{{unsubscribe_url}}">Unsubscribe</a>
            </p>
          </div>
        </div>
      </body>
    </html>
  `
};