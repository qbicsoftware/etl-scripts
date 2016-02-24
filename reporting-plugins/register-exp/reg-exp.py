def process(tr, parameters, tableBuilder):
  """Create a new experiment with the code specified in the parameters

  """
  user = parameters.get("user")
  if not user == None:
    tr.setUserId(user)

  expCode = parameters.get("code")
  expType = parameters.get("type")
  project = parameters.get("project")
  space = parameters.get("space")
  expId = "/" + space + "/" + project + "/" + expCode
  exp = tr.createNewExperiment(expId, expType)

  # additional properties
  properties = parameters.get("properties")
  if not properties == None:
    for prop in properties.keySet():
      if prop == "ENZYMES":
        m = 0
        matType = "Q_PROTEASE_PROTOCOL"
        matCode = project+"_Proteases"
        while tr.getMaterial(matCode, matType):
          m += 1
          matCode = project+"_Proteases"+str(m)

        material = tr.createNewMaterial(matCode, matType)
        enzymes = properties.get("ENZYMES")
        i = 0
        for e in enzymes:
          i+=1
          material.setPropertyValue("Q_PROTEASE_"+str(i),e)
        exp.setPropertyValue("Q_PROTEASE_DIGESTION", matCode)
      else:
        print prop
        print properties.get(prop)
        exp.setPropertyValue(prop, properties.get(prop))
