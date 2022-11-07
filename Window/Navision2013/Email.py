
import smtplib,sys
from email.mime.text import MIMEText
from email.header import Header
server = smtplib.SMTP("smtp.gmail.com", 587)#port
server.ehlo()
server.starttls()
server.login('notification@kockpit.in', 'tcpl@team')
body = 'Hello there'

msg = MIMEText(body,'plain','utf-8')
subject = 'Email test'
msg["Subject"] = Header(subject, 'utf-8')
from1='notification@kockpit.in'
to='sukriti.saluja@kockpit.in'
to2='amit.kumar@kockpit.in'
msg["From"] = Header(from1, 'utf-8')
msg["To"] = Header(to, 'utf-8')
txt = msg.as_string()

msg = "Kockpit Product - Kockpit reloads failed for Servername- "+sys.argv[3]+" , Entity "+sys.argv[4]+" - "+sys.argv[2]\
            + "encountered error at Line No.- " +sys.argv[5]+" " 


server.sendmail(from1, to, msg)
server.sendmail(from1, to2, msg)
