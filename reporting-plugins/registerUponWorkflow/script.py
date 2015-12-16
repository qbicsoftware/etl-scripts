def process(tr, parameters, tableBuilder):
  """Ingestion service for workflow triggering.
     This service will create a new experiment, a new sample 
     and add corresponding properties (e.g. status of experiment).
  """

  # get needed instance identifieres
  spaceCode = parameters.get("space")
  projectCode = parameters.get("project")
  experimentCode = parameters.get("experiment")
  sampleCode = parameters.get("sample")

  # get experiment and sample types
  experimentType = parameters.get("experimentType")
  sampleType = parameters.get("sampleType")

  # get number of experiments for given project to construct experiment code
  searchService = tr.getSearchService()
  numberOfExperiments = len(searchService.listExperiments("/" + spaceCode + "/" + projectCode)) + 1

  # register new experiment and sample
  expId = "/" + spaceCode + "/" + projectCode + "/" + experimentCode + str(numberOfExperiments)
  experiment = tr.createNewExperiment(expId, experimentType)

  sampId = "/" + spaceCode + "/MSQC" + sampleCode
  sample = tr.createNewSample(sampId, sampleType)

  # set experiment of sample
  sample.setExperiment(experiment)

  # set user who registered new instances
  tr.setUserId(parameters.get("userID"))

  # get experiment and sample properties and set them accordingly
  experimentProperties = parameters.get("experimentProperties")
  if experimentProperties:
  	for prop in experimentProperties.keySet():
    		experiment.setPropertyValue(prop, experimentProperties.get(prop))

  sampleProperties = parameters.get("sampleProperties")
  if sampleProperties:
  	for prop in sampleProperties.keySet():
    		sample.setPropertyValue(prop, sampleProperties.get(prop))


  sampId2 = "/" + spaceCode + "/MSQC" + sampleCode + "T"
  sample2 = tr.createNewSample(sampId2, sampleType)

  # set experiment of sample
  sample2.setExperiment(experiment)
  sample.setParentSampleIdentifiers(["/" + spaceCode + "/" + sampleCode + "T"])


  sampleProperties = parameters.get("sampleProperties")
  if sampleProperties:
        for prop in sampleProperties.keySet():
                sample2.setPropertyValue(prop, sampleProperties.get(prop))
