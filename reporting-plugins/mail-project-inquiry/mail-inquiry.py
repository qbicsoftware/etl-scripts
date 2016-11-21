import smtplib
from email.mime.text import MIMEText

def process(tr, parameters, tableBuilder):
  """
	Service to notify qbic and send project tsv if a user chose
	not to register a project but to inquire about costs in the
	wizard.
  """

  server = "smtpserv.uni-tuebingen.de"
  fromA = "notification_service@qbis.qbic.uni-tuebingen.de"

  project = parameters.get("project")
  space = parameters.get("space")
  user = parameters.get("user")
  toA = ''
  for mail in ["andreas.friedrich@uni-tuebingen.de"]:#test
    toA += '%s,' % mail

  text = "Hi,\n\n%s would like to register the Project %s in Space %s.\nI've attached the project TSV for you.\n\nHave a nice day,\nYour friendly mail service." % (user,project,space)
  msg = MIMEText(text)
  msg['From'] = fromA
  msg['To'] = toA
  msg['Subject'] = parameters.get("subject")
  # check for info@qbic.uni-tuebingen.de
  msg['reply-to'] = "info@qbic.uni-tuebingen.de"

  smtpServer = smtplib.SMTP(server)
  smtpServer.sendmail(fromA, toA, msg.as_string())
  smtpServer.close()