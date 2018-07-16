import sys

sys.path.append('/abi-projects/QBiC/scripts')

# registers a list of samples and their metadata with openBIS

from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria


class SampleAlreadyExistsError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

def process(tr, params, tableBuilder):
  ignore_existing = "IGNORE EXISTING" in params
  if "user" in params:
    tr.setUserId(params.get("user"))
  for sample in params.keySet():
    parameters = params.get(sample)
    sampleCode = parameters.get("code")
    search_service = tr.getSearchService() 
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, sampleCode))
    foundSamples = search_service.searchForSamples(sc)
    if(foundSamples.size() < 1):
      proj = parameters.get("project")
      space = parameters.get("space")
      sampleType = parameters.get("type")
      species = parameters.get("species")
      sampleId = "/" + space + "/" + sampleCode
      sample = tr.createNewSample(sampleId, sampleType)
      exp = "/"+space+"/"+proj+"/"+parameters.get("experiment")
      exp = tr.getExperiment(exp)
      sample.setExperiment(exp)
      if parameters.get("Q_SECONDARY_NAME"):
        sample.setPropertyValue("Q_SECONDARY_NAME",parameters.get("Q_SECONDARY_NAME"))
      if parameters.get("parents"):
        sample.setParentSampleIdentifiers(parameters.get("parents"))
      if parameters.get("metadata"):
        properties = parameters.get("metadata")
        for prop in properties.keySet():
          try:
            val = properties.get(prop)
            val = str(val)
          except:
            val = unicode(val,"utf-8")
            val = val.encode("utf-8")
          sample.setPropertyValue(prop, val)
    else:
      if not ignore_existing:
        raise SampleAlreadyExistsError("Sample "+sampleCode+" already exists in openBIS!")