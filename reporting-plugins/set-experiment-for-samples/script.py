
def process(tr, parameters, tableBuilder):
  """Change experiment of a samples
  """
  ids = parameters.get("identifiers")
  expID = parameters.get("experiment")
  exp = tr.getExperiment(expID)
  for sampID in ids:
    sample = tr.getSampleForUpdate(sampID)
    sample.setExperiment(exp)