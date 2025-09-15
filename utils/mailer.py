import smtplib
from email.header import Header
from smtplib import SMTPException
from email.utils import formataddr
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


import pandas as pd


def send_email(header:str, body: str, receiver_list:list, sender_email:str, sender_pwd:str) -> str:
    """
    Sends email based on status message after the upload function is run.

    Arguments:
        header (str): Subject of the email to be sent
        body (str): Email content with the info about the error
        receiver_list (list): List of email recipients of the log
        sender_email (str): Email of the person who is sending it
        sender_pwd (str): Password of the person who is sending the email for OAUTH purposes. 
    Return:
        str: A message with the header of the email and its status 
    """
    try:
        # Setup the MIME
        message = MIMEMultipart()
        message['From'] = formataddr((str(Header('BOT NOTIFICATION', 'utf-8')), sender_email))
        message['To'] = ','.join(receiver_list)
        message['Subject'] = header

        # The body and the attachments for the mail
        message.attach(MIMEText(body, 'html'))

        # Create SMTP session for sending the mail
        session = smtplib.SMTP('smtp.gmail.com', 587)
        session.starttls()
        session.login(sender_email, sender_pwd)
        text = message.as_string()
        session.sendmail(sender_pwd, receiver_list, text)
        session.quit()
        
        return f'E-mail:\'{header}\' sent to {receiver_list}'
    except SMTPException:
        print("Error: unable to send email")


if __name__ == '__main__':
    send_email()