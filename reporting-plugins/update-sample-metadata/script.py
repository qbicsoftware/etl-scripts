from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria

def process(tr, parameters, tableBuilder):
  ids = parameters.get("identifiers")
  search_service = tr.getSearchService()
  expCodes = []
  if "Experiment" in parameters:
    print "preparing experiment update"
    for exp in search_service.listExperiments(parameters.get("Project")):
      expCodes.append(exp.getExperimentIdentifier().split("/")[-1])
  for id in ids:
    print "searching id "+id
    entity = None
    if "Experiment" in parameters and id in expCodes:
      entity = tr.getExperimentForUpdate(parameters.get("Project")+"/"+id)
    else:
      sc = SearchCriteria()
      sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, id))
      found = search_service.searchForSamples(sc)
      print "found: "+str(found)
      if found.size() > 0:
        entity = tr.getSampleForUpdate(id)
    if entity:
      for type in parameters.get("types"):
        print "handling type "+type
        typeMap = parameters.get(type)
        print typeMap
        try:
          value = typeMap.get(id)
          print "value "+value
          entity.setPropertyValue(type,value)
        except:
          print "exception when trying to set property value!"
          pass
