def wrap(element, input):
  return "<"+element+">"+input+"</"+element+">\n"

def process(tr, parameters, tableBuilder):
  id = parameters.get("id")
  sample = tr.getSampleForUpdate(id)
  user = parameters.get("user")
  comment = parameters.get("comment")
  time = str(parameters.get("time"))

  xml = sample.getPropertyValue("Q_NOTES")

  all = ""
  try:
    for line in xml.split("\n"):
      if not "</notes>" in line:
        all += line
  except:
    all = "<notes>"
  note = "\n<note>\n"
  note += wrap("comment",comment)+wrap("time",time)+wrap("username",user)
  note += "</note>\n"
  all += note
  all += "</notes>"
  sample.setPropertyValue("Q_NOTES",all)
