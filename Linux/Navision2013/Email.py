import smtplib,sys,os
from email.mime.text import MIMEText
from email.header import Header
from os.path import dirname, join, abspath
root_directory =abspath(join(join(dirname(__file__))))
root_directory=root_directory+"/"
DBList=[]
for folders in os.listdir(root_directory):
    if os.path.isdir(os.path.join(root_directory,folders)):
        if 'DB' in folders:
            if 'DB0' in folders:
                pass
            else:
                DBList.insert(0,folders )
Connection =abspath(join(join(dirname(__file__),DBList[0])))
sys.path.insert(0, Connection)
from Configuration.Constant import *
server = smtplib.SMTP("smtp.gmail.com", 587)
server.ehlo()
server.starttls()
server.login('alert@kockpit.in', 'alert@123')
body = 'Hello there'
PostgresDbInfo.PostgresUrl
msg = MIMEText(body,'plain','utf-8')
subject = 'Email test'
msg["Subject"] = Header(subject, 'utf-8')
from1='alert@kockpit.in'
to='sukriti.saluja@kockpit.in'
to2='abhishek@kockpit.in'
msg["From"] = Header(from1, 'utf-8')
msg["To"] = Header(to, 'utf-8')
txt = msg.as_string()
msg = "Kockpit Product - Kockpit Product reloads failed for Server "+PostgresDbInfo.Host+" - "+sys.argv[3]+" , Stage  "+sys.argv[4]+" - "+sys.argv[2]\
            + " encountered error at Line No.- " +sys.argv[5]+" " 
server.sendmail(from1, to, msg)
server.sendmail(from1, to2, msg)
