from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClause
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClauseAttribute
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria

# This script returns a table containing sample code, external ID, secondary name and q_properties (xml string). It is currently used for providing
# the workflows with metadata from the main portlet
CODE = "Child"
EXTERNAL_ID = "External ID"
SECONDARY_NAME = "Secondary Name"
XML = "XML"

def aggregate(parameters, tableBuilder):
	codes = parameters.get("codes")

	tableBuilder.addHeader(CODE)
	tableBuilder.addHeader(EXTERNAL_ID)
	tableBuilder.addHeader(SECONDARY_NAME)
	tableBuilder.addHeader(XML)

	for code in codes:
		sc = SearchCriteria()
		sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code))

		sample  = searchService.searchForSamples(sc)[0]
		row = tableBuilder.addRow()
		row.setCell(CODE, sample.getCode())
		row.setCell(EXTERNAL_ID, sample.getPropertyValue("Q_EXTERNALDB_ID"))
		row.setCell(SECONDARY_NAME, sample.getPropertyValue("Q_SECONDARY_NAME"))
		row.setCell(XML, sample.getPropertyValue("Q_PROPERTIES"))
