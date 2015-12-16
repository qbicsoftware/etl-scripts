from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

PROJECT = "Code"
DATASETS = "Datasets"
projectMap = {}

def aggregate(parameters, tableBuilder):
	codes = parameters.get("codes")

	tableBuilder.addHeader(PROJECT)
	tableBuilder.addHeader(DATASETS)

	allCodes = ""
	for code in codes:
		allCodes += code+" "
	sc = SearchCriteria()
	pc = SearchCriteria()
	pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, allCodes))
	sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
	found = searchService.searchForDataSets(sc)
	for ds in found:
		project = ds.getExperiment().getExperimentIdentifier().split("/")[2]
		try:
			projectMap[project] = projectMap[project]+1
		except:
			projectMap[project] = 1
	for key in projectMap:
		row = tableBuilder.addRow()
		row.setCell(PROJECT, key)
		row.setCell(DATASETS, projectMap[key])
