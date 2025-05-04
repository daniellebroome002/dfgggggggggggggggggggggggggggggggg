export const inactivityReminderTemplate = {
  subject: "We miss you at Boomlify!",
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
            <h1>We Miss You!</h1>
          </div>
          <div class="content">
            <p>Hello {{name}},</p>
            <p>We noticed you haven't checked your temporary emails recently. You have {{email_count}} active email addresses.</p>
            <p>Don't miss any important messages - check your inbox now!</p>
            <p style="text-align: center;">
              <a href="{{dashboard_url}}" class="button">View My Inbox</a>
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