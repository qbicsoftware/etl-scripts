def process(tr, parameters, tableBuilder):
  """Create a new project with the code specified in the parameters
      Code: *
      Space: *
      Description: *
  """

  if "user" in parameters:
    tr.setUserId(parameters.get("user"))

  projCode = parameters.get("code")
  projSpace = parameters.get("space")
  projDesc = parameters.get("desc")
  projId = "/" + projSpace + "/" + projCode + "/"
  proj = tr.createNewProject(projId)

  proj.setDescription(projDesc)

  #exp = tr.getExperiment("/TEST/TEST-PROJECT/DEMO-EXP-HCS")
  #sample.setExperiment(exp)
  
  #tableBuilder.addHeader("CODE")  
  #tableBuilder.addHeader("IDENTIFIER")
  #row = tableBuilder.addRow()
  #row.setCell("CODE", sampleCode)
  #row.setCell("IDENTIFIER", sampleId)
