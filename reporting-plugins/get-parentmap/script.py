from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClause
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClauseAttribute
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria

CODE = "Child"
PARENT = "Parent"

def aggregate(parameters, tableBuilder):
	codes = parameters.get("codes")

	tableBuilder.addHeader(CODE)
	tableBuilder.addHeader(PARENT)

	for code in codes:
		sc = SearchCriteria()
		sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code))

		sample  = searchService.searchForSamples(sc)[0]
		handleSample(sample, tableBuilder)

def handleSample(sample, tableBuilder):
	for parent in sample.getParentSampleIdentifiers():
		row = tableBuilder.addRow()
		row.setCell(CODE, sample.getCode())
		row.setCell(PARENT, parent.split("/")[-1])
