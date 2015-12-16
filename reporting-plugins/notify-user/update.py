import smtplib
from email.mime.text import MIMEText

def process(tr, parameters, tableBuilder):
  """Change properties of experiment

  """

  expId = parameters.get("identifier")
  exp = tr.getExperimentForUpdate(expId)

  properties = parameters.get("properties")
  
  for prop in properties.keySet():
  	exp.setPropertyValue(prop, properties.get(prop))
 
  #server = "smtpserv.uni-tuebingen.de"
  #fromA = "notification_service@qbis.qbic.uni-tuebingen.de"

  # TODO get emails of space users
  # Get it via liferay and pass it to this service ?
  #toA = "mohr@informatik.uni-tuebingen.de"
  #subject = "Update information for Experiment %s" % expId
  #text = "Status of Experiment %s has been updated" % expId #, properties.get("Q_CURRENT_STATUS"))

  #msg = MIMEText(text)
  #msg['From'] = fromA
  #msg['To'] = toA
  #msg['Subject'] = subject
  # check for info@qbic.uni-tuebingen.de
  #msg['reply-to'] = "mohr@informatik.uni-tuebingen.de"

  #smtpServer = smtplib.SMTP(server)
  #smtpServer.sendmail(fromA, toA, msg.as_string())
  #smtpServer.close()
  
