import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.mime.text import MIMEText
from email import Encoders

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
  tsv = parameters.get("project-tsv")

  subject = user+" would like to register the new project "+project
  toA = ''
  for mail in ["andreas.friedrich@uni-tuebingen.de"]:#test
    toA += '%s,' % mail

  text = "Hi,\n\n%s would like to register the Project %s in Space %s.\nI've attached the project TSV for you.\n\nHave a nice day,\nYour friendly mail service plugin.\n\n" % (user,project,space)
  msg = MIMEMultipart()
  msg['From'] = fromA
  msg['To'] = toA
  msg['Subject'] = subject
  #msg['reply-to'] = "info@qbic.uni-tuebingen.de"

  msg.attach(MIMEText(text))
  part = MIMEBase('application', "octet-stream")
  part.set_payload(tsv)
  Encoders.encode_base64(part)

  part.add_header('Content-Disposition', 'attachment; filename="%s"' % project+"_plan.tsv")
  msg.attach(part)

  smtpServer = smtplib.SMTP(server)
  smtpServer.sendmail(fromA, toA, msg.as_string())
  smtpServer.close()