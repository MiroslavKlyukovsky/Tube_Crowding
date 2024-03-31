'''import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import email_password

def send_email(body_text):
    # SMTP server settings
    smtp_server = 'smtp.ukr.net'
    smtp_port = 465
    smtp_email = 'myroslav875@ukr.net'

    # Create message
    subject = 'Email Notification'
    message = MIMEMultipart()
    message['From'] = smtp_email
    message['To'] = 'my.kvant2222@gmail.com'
    message['Subject'] = subject
    message.attach(MIMEText(body_text, 'plain'))

    # Connect to SMTP server
    try:
        smtp_server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        smtp_server.login(smtp_email, email_password)
        smtp_server.sendmail(smtp_email, 'my.kvant2222@gmail.com', message.as_string())
        print('Email sent successfully!')
    except Exception as e:
        print(f'Error sending email: {e}')
    finally:
        if 'smtp_server' in locals():
            smtp_server.quit()
'''
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
            raise Exception(f"Error creating table: {error}")
        finally:
            if 'smtp_server' in locals():
                smtp_server.quit()
