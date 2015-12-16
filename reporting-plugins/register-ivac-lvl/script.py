##
# ingestion service script to register multiple levels of an ivac patient
# (entity level, biological sample level, test sample level, NGS sample level)
##

def process(tr, parameters, tableBuilder):
  """Create different levels for ivac patient, entity level to NGS sequencing level
  """

  level = parameters.get("lvl")
  tr.setUserId(parameters.get("user"))

  if level == "1":
    projectDetailsID = parameters.get("projectDetails")
    expDesignID = parameters.get("experimentalDesign")
    secondaryName = parameters.get("secondaryName")
    biologicalEntityID = parameters.get("biologicalEntity")

    projDetails = tr.createNewExperiment(projectDetailsID, "Q_PROJECT_DETAILS")
    projDetails.setPropertyValue("Q_SECONDARY_NAME", secondaryName)

    expDesign = tr.createNewExperiment(expDesignID, "Q_EXPERIMENTAL_DESIGN")
    expDesign.setPropertyValue("Q_SECONDARY_NAME", secondaryName)
    expDesign.setPropertyValue("Q_CURRENT_STATUS", "FINISHED")

    biologicalEntity = tr.createNewSample(biologicalEntityID, "Q_BIOLOGICAL_ENTITY")
    biologicalEntity.setExperiment(expDesign)
    biologicalEntity.setPropertyValue("Q_NCBI_ORGANISM", "9606")
    biologicalEntity.setPropertyValue("Q_SECONDARY_NAME", secondaryName)

  elif level == "2":
    sampleExtractions = parameters.get("sampleExtraction")
    biologicalSamples = parameters.get("biologicalSamples")
    secondaryName = parameters.get("secondaryNames")
    parent = parameters.get("parent")
    primaryTissues = parameters.get("primaryTissue")
    detailedTissues = parameters.get("detailedTissue")

    for i in range(0,len(sampleExtractions)):
      sampleExtraction = tr.createNewExperiment(sampleExtractions[i],"Q_SAMPLE_EXTRACTION")
      sampleExtraction.setPropertyValue("Q_CURRENT_STATUS", "FINISHED")

      biologicalSample = tr.createNewSample(biologicalSamples[i], "Q_BIOLOGICAL_SAMPLE")
      biologicalSample.setPropertyValue("Q_PRIMARY_TISSUE", primaryTissues[i])
      biologicalSample.setPropertyValue("Q_TISSUE_DETAILED", detailedTissues[i])
      biologicalSample.setParentSampleIdentifiers([parent])
      biologicalSample.setPropertyValue("Q_SECONDARY_NAME", secondaryName)
      biologicalSample.setExperiment(sampleExtraction)

  elif level == "3":
      parent = parameters.get("parent")
      samplePreparations = parameters.get("experiments")
      testSamples = parameters.get("samples")
      testTypes = parameters.get("types")

      for i in range(0,len(samplePreparations)):
        samplePreparation = tr.createNewExperiment(samplePreparations[i],"Q_SAMPLE_PREPARATION")
        samplePreparation.setPropertyValue("Q_CURRENT_STATUS", "FINISHED")

        testSample = tr.createNewSample(testSamples[i], "Q_TEST_SAMPLE")
        testSample.setPropertyValue("Q_SAMPLE_TYPE", testTypes[i])
        testSample.setParentSampleIdentifiers([parent])
        testSample.setExperiment(samplePreparation)

  elif level == "4":
      parents = parameters.get("parents")
      ngsMeasurements = parameters.get("experiments")
      ngsRuns = parameters.get("samples")
      types = parameters.get("types")
      additionalInfo = parameters.get("info")
      devices = parameters.get("device")

      for i in range(0,len(ngsMeasurements)):
        ngsMeasurement = tr.createNewExperiment(ngsMeasurements[i],"Q_NGS_MEASUREMENT")
        ngsMeasurement.setPropertyValue("Q_CURRENT_STATUS", "STARTED")
        ngsMeasurement.setPropertyValue("Q_SEQUENCING_TYPE", types[i])
        ngsMeasurement.setPropertyValue("Q_DEEP_SEQUENCING", str(additionalInfo[i]))
	ngsMeasurement.setPropertyValue("Q_SEQUENCER_DEVICE", devices[i])
	
        ngsRun = tr.createNewSample(ngsRuns[i], "Q_NGS_SINGLE_SAMPLE_RUN")
        ngsRun.setParentSampleIdentifiers([parents[i]])
        ngsRun.setExperiment(ngsMeasurement)

  elif level == "5":
      experimentIDs = parameters.get("experiments")
      sampleIDs = parameters.get("samples")
      typings = parameters.get("typings")
      hlaClasses = parameters.get("classes")
      typingMethods = parameters.get("methods")
      parent = parameters.get("parent")      

      for i in range(0,len(experimentIDs)):
        hlaTyping = tr.createNewExperiment(experimentIDs[i], "Q_NGS_HLATYPING")
        hlaTyping.setPropertyValue("Q_CURRENT_STATUS", "FINISHED")
        hlaTyping.setPropertyValue("Q_HLA_CLASS", hlaClasses[i])
	hlaTyping.setPropertyValue("Q_HLA_TYPING_METHOD", typingMethods[i])

	hlaTypingSample = tr.createNewSample(sampleIDs[i], "Q_NGS_HLATYPING")
	hlaTypingSample.setPropertyValue("Q_HLA_CLASS", hlaClasses[i])
	hlaTypingSample.setPropertyValue("Q_HLA_TYPING", typings[i])
	hlaTypingSample.setExperiment(hlaTyping)
        hlaTypingSample.setParentSampleIdentifiers([parent])
