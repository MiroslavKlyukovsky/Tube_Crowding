import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class EmailInformant:
    def __init__(self, smtp_server, smtp_port, smtp_email, email_password, recipient_email):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_email = smtp_email
        self.email_password = email_password
        self.recipient_email = recipient_email

    def send_email(self, subject, body_text):
        message = MIMEMultipart()
        message['From'] = self.smtp_email
        message['To'] = self.recipient_email
        message['Subject'] = subject
        message.attach(MIMEText(body_text, 'plain'))

        try:
            smtp_server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            smtp_server.login(self.smtp_email, self.email_password)
            smtp_server.sendmail(self.smtp_email, self.recipient_email, message.as_string())
        except Exception as error:
            raise Exception(f"Error sending email: {error}")
        finally:
            if 'smtp_server' in locals():
                smtp_server.quit()
