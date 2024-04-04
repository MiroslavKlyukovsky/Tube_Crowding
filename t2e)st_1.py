from datetime import datetime, timedelta
import time
from config import email_password, smtp_server, smtp_port, smtp_email, recipient_email
from email_informant import EmailInformant

email_informant = EmailInformant(smtp_server, smtp_port, smtp_email, email_password, recipient_email)
try:
    email_informant.send_email("Srachka bolychka","pizdec")
except Exception as error:
    print(error)