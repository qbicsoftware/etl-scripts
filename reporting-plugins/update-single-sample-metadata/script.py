
def process(tr, parameters, tableBuilder):
  """Change properties of a single sample
  """
  user = parameters.get("user")
  if not user == None:
    tr.setUserId(user)

  sampleID = parameters.get("identifier")
  sample = tr.getSampleForUpdate(sampleID)

  properties = parameters.get("properties")
  
  for prop in properties.keySet():
  	sample.setPropertyValue(prop, properties.get(prop))
