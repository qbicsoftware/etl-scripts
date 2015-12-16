import sys

sys.path.append('/abi-projects/QBiC/scripts')

from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from java.lang import Byte
from java.io import FileInputStream
from java.io import File
import jarray

def process(tr, parameters, tableBuilder):
  ids = parameters.get("identifiers")
  search_service = tr.getSearchService()
  expCodes = []
  if "Experiment" in parameters:
    for exp in search_service.listExperiments(parameters.get("Project")):
      expCodes.append(exp.getExperimentIdentifier().split("/")[-1])
  for id in ids:
    entity = None
    if "Experiment" in parameters and id in expCodes:
      entity = tr.getExperimentForUpdate(parameters.get("Project")+"/"+id)
    else:
      sc = SearchCriteria()
      sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, id.split("/")[-1]))
      found = search_service.searchForSamples(sc)
      if found.size() > 0:
        entity = tr.getSampleForUpdate(id)
    if entity:
      for type in parameters.get("types"):
        typeMap = parameters.get(type)
        try:
          value = typeMap.get(id)
          entity.setPropertyValue(type,value)
        except:
          pass
