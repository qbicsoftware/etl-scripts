import datetime
import sys

def isDate(value):
  if not isinstance(value, str if sys.version_info[0] >= 3 else basestring):
    return False
  try: 
    datetime.datetime.strptime(value, "%d-%m-%Y")
    return True
  except ValueError:
    return False

def setProperties(tr, exp, props):
  for prop in props.keySet():
    if prop == "ENZYMES":
      m = 0
      matType = "Q_PROTEASE_PROTOCOL"
      matCode = project+"_Proteases"
      while tr.getMaterial(matCode, matType):
        m += 1
        matCode = project+"_Proteases"+str(m)
      material = tr.createNewMaterial(matCode, matType)
      enzymes = props.get("ENZYMES")
      i = 0
      for e in enzymes:
        i+=1
        material.setPropertyValue("Q_PROTEASE_"+str(i),e)
      exp.setPropertyValue("Q_PROTEASE_DIGESTION", matCode)
    else:
      if isDate(props.get(prop)):
        time = props.get(prop)
        date = datetime.datetime.strptime(time, "%d-%m-%Y").strftime('%Y-%m-%d %H:%M:%S')
        exp.setPropertyValue(prop, date)
      else:
        if props.get(prop):
          try:
            val = props.get(prop)
            val = str(val)
          except:
            val = unicode(val,"utf-8")
            val = val.encode("utf-8")
          exp.setPropertyValue(prop, val)

def process(tr, parameters, tableBuilder):
  """Create a new experiment with the code specified in the parameters

  """
  user = parameters.get("user")
  if user:
    tr.setUserId(user)
  codes = parameters.get("codes") # unique codes so no error is thrown
  types = parameters.get("types")
  props = parameters.get("properties")
  #only one experiment
  if not codes:
    props = [props]
    codes = [parameters.get("code")]
    types = [parameters.get("type")]
  project = parameters.get("project")
  space = parameters.get("space")

  existing = [] #duplicates
  for data in zip(codes, types, props):
    if not data[0] in existing:
      existing.append(data[0])
      expId = "/" + space + "/" + project + "/" + data[0]
      exp = tr.createNewExperiment(expId, data[1])
      if not data[2] == None:
        setProperties(tr, exp, data[2])