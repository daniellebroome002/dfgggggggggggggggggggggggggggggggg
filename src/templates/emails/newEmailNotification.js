export const newEmailNotificationTemplate = {
  subject: "New Email Received at {{temp_email}}",
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
          .email-details {
            background-color: #f5f5f5;
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
            <h1>New Email Received!</h1>
          </div>
          <div class="content">
            <p>Hello {{name}},</p>
            <p>You've received a new email at <strong>{{temp_email}}</strong></p>
            
            <div class="email-details">
              <p><strong>From:</strong> {{sender}}</p>
              <p><strong>Subject:</strong> {{subject}}</p>
            </div>

            <p style="text-align: center;">
              <a href="{{email_url}}" class="button">Read Email</a>
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