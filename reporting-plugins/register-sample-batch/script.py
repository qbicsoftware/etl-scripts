import sys

sys.path.append('/abi-projects/QBiC/scripts')

# registers a list of samples and their metadata with openBIS

from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria

def process(tr, params, tableBuilder):
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
      sample.setPropertyValue("Q_SECONDARY_NAME",parameters.get("sample_class"))
      if parameters.get("parents"):
        sample.setParentSampleIdentifiers(parameters.get("parents"))
      if parameters.get("metadata"):
        properties = parameters.get("metadata")
        for prop in properties.keySet():
          sample.setPropertyValue(prop, properties.get(prop))
