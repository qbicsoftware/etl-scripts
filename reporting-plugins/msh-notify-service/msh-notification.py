import smtplib
from email.mime.text import MIMEText

def process(tr, parameters, tableBuilder):
  """ Notify MSH users that sample status has changed """ 

  fromState = str(parameters.get("fromState"))
  toState = str(parameters.get("toState"))
  sampleCode = str(parameters.get("sampleCode"))
 
  server = "smtpserv.uni-tuebingen.de"
  fromA = "notification_service@qbis.qbic.uni-tuebingen.de"

  # TODO get emails of space users
  # Get it via liferay and pass it to this service ?
  toA = "erhan.kenar@uni-tuebingen.de"
  subject = "Status of QBiC sample %s has changed" % sampleCode
  text = '%s\n\n%s%s%s\n\'%s\'%s\n\'%s\'%s\n\n%s\n\n%s' % ("Dear customer,","Sample ",sampleCode," moved from state", fromState, " to",toState,".","Have a nice day,","Your QBiC team")

  msg = MIMEText(text)
  msg['From'] = fromA
  msg['To'] = toA
  msg['Subject'] = subject
  # check for info@qbic.uni-tuebingen.de
  msg['reply-to'] = "mohr@informatik.uni-tuebingen.de"

  smtpServer = smtplib.SMTP(server)
  smtpServer.sendmail(fromA, toA, msg.as_string())
  smtpServer.close()
  
