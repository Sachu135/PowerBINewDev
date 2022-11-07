
import smtplib,sys
import keyring
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login("alert@kockpit.in",eval(chr(34)+keyring.get_password("kockpit","email")+chr(34)))

Batcherror=sys.argv[1]

if Batcherror=='1':
    msg = "kockpit_new  script failed for Servername- "+sys.argv[3]+" , Entity "+sys.argv[4]+" - "+sys.argv[2]
    server.sendmail("alert@kockpit.in", "amit.kumar@kockpit.in", msg)
    server.sendmail("alert@kockpit.in", "sukriti.saluja@kockpit.in", msg)
    print(msg)
    server.quit()

'''
if Batcherror=='0':
    msg = "Reloads completed Power Kockpit "+sys.argv[2]
    server.sendmail("alert@kockpit.in", "abhishek.p@kockpit.in", msg)
    #server.sendmail("alert@kockpit.in", "sanjay.rana@kockpit.in", msg)
    #server.sendmail("alert@kockpit.in", "bhanuday.birla@kockpit.in", msg)
    server.quit()
'''