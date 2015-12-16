import smtplib
from email.mime.text import MIMEText

def process(tr, parameters, tableBuilder):
  """
	Service to notify users if something has changed in their projects
	Parameters should contain a list of mail addresses as "users"
	and some subject as "subject" and the ID of the project as "project"
	and the change which has been made as "change"
  """

  server = "smtpserv.uni-tuebingen.de"
  fromA = "notification_service@qbis.qbic.uni-tuebingen.de"

  toA = ''
  for mail in parameters.get("users"):
	toA += '%s,' % mail

  text = 'There has been an update in your project %s. %s \n Please go to https://portal.qbic.uni-tuebingen.de/portal/ to view the changes which have been made.' % (parameters.get("project"), parameters.get("change"))

  msg = MIMEText(text)
  msg['From'] = fromA
  msg['To'] = toA
  msg['Subject'] = parameters.get("subject")
  # check for info@qbic.uni-tuebingen.de
  msg['reply-to'] = "info@qbic.uni-tuebingen.de"

  smtpServer = smtplib.SMTP(server)
  smtpServer.sendmail(fromA, toA, msg.as_string())
  smtpServer.close()
  
