import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class EmailInformant:
    """
        A class to send emails using SMTP.

        Attributes:
        - smtp_server_name (str): SMTP server hostname.
        - smtp_server (smtplib.SMTP_SSL or None): SMTP server connection.
        - smtp_port (int): SMTP server port number.
        - smtp_email (str): Sender's email address.
        - email_password (str): Sender's email password.
        - recipient_email (str): Recipient's email address.
    """
    def __init__(self, smtp_server_name, smtp_port, smtp_email, email_password, recipient_email):
        """
            Initializes the EmailInformant instance.

            Args:
            - smtp_server_name (str): SMTP server hostname.
            - smtp_port (int): SMTP server port number.
            - smtp_email (str): Sender's email address.
            - email_password (str): Sender's email password.
            - recipient_email (str): Recipient's email address.
        """
        self.smtp_server_name = smtp_server_name
        self.smtp_server = None
        self.smtp_port = smtp_port
        self.smtp_email = smtp_email
        self.email_password = email_password
        self.recipient_email = recipient_email

    def create_message(self, subject, body_text):
        """
            Creates an email message.

            Args:
            - subject (str): Email subject.
            - body_text (str): Email body text.

            Returns:
            - message (email.mime.multipart.MIMEMultipart): Email message.

            Raises:
            - Exception: If an error occurs while creating the email message.
        """
        try:
            message = MIMEMultipart()
            message['From'] = self.smtp_email
            message['To'] = self.recipient_email
            message['Subject'] = subject
            message.attach(MIMEText(body_text, 'plain'))
            return message
        except Exception as err:
            raise Exception(f"[create_message] {err}")

    def send_email(self, subject, body_text):
        """
            Sends an email.

            Args:
            - subject (str): Email subject.
            - body_text (str): Email body text.

            Raises:
            - Exception: If an error occurs while sending the email.
        """
        try:
            message = self.create_message(subject, body_text)
            self.smtp_server = smtplib.SMTP_SSL(self.smtp_server_name, self.smtp_port)
            self.smtp_server.login(self.smtp_email, self.email_password)
            self.smtp_server.sendmail(self.smtp_email, self.recipient_email, message.as_string())
        except Exception as err:
            raise Exception(f"[send_email] {err}")
        finally:
            if self.smtp_server:
                self.smtp_server.quit()
