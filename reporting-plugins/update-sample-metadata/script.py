from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria

def process(tr, parameters, tableBuilder):
  ids = parameters.get("identifiers")
  search_service = tr.getSearchService()
  expCodes = []
  if "Experiment" in parameters:
    print "preparing experiment update"
    for exp in search_service.listExperiments(parameters.get("Project")):
      expCodes.append(exp.getExperimentIdentifier().split("/")[-1])
  else:
    print "sample update"
  print "updating types:"
  types = parameters.get("types")
  print types
  for id in ids:
    print "searching id "+id
    entity = None
    if "Experiment" in parameters and id in expCodes:
      entity = tr.getExperimentForUpdate(parameters.get("Project")+"/"+id)
    else:
      sc = SearchCriteria()
      sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, id))
      found = search_service.searchForSamples(sc)
      if len(found) > 0:
        print "found sample"
        entity = tr.getSampleForUpdate(found[0].getSampleIdentifier())
      else:
        print "could not find sample"
    if entity:
      for prop_type in types:
        print "handling type "+prop_type
        typeMap = parameters.get(prop_type)
        try:
          value = typeMap.get(id)
          print "value for this entity: "+value
          entity.setPropertyValue(prop_type,value)
        except:
          print "exception when trying to set property value!"
          pass