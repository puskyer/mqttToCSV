# smtplib module send mail

import smtplib

TO = 'recipient@mailservice.com'
SUBJECT = 'TEST MAIL'
TEXT = 'Here is a message from python.'

# Gmail Sign In
gmail_sender = 'sender@gmail.com'
gmail_passwd = 'password'

server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
server.login(gmail_sender, gmail_passwd)

BODY = '\r\n'.join(['To: %s' % TO,
                    'From: %s' % gmail_sender,
                    'Subject: %s' % SUBJECT,
                    '', TEXT])

try:
    server.sendmail(gmail_sender, [TO], BODY)
    print ('email sent')
except:
    print ('error sending mail')

server.quit()