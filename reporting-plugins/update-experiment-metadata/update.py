
def process(tr, parameters, tableBuilder):
  """Change properties of experiment

  """
  user = parameters.get("user")
  if not user == None:
       tr.setUserId(user)
  expId = parameters.get("identifier")
  exp = tr.getExperimentForUpdate(str(expId))

  properties = parameters.get("properties")
  
  for prop in properties.keySet():
       exp.setPropertyValue(prop, properties.get(prop))
