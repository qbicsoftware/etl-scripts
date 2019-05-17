from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClause
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClauseAttribute
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SampleFetchOption
from java.util import EnumSet

def process(tr, parameters, tableBuilder):
	codes = parameters.get("codes")
	infos = parameters.get("infos")
	search = tr.getSearchService()
	for code in codes:
		info = infos[code]
		sc = SearchCriteria()
		pc = SearchCriteria()
		pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code));
		sc.addSubCriteria(SearchSubCriteria.createSampleCriteria(pc))
		datasets = search.searchForDataSets(sc)
		for d in datasets:
			print d
			dataset = tr.getDataSetForUpdate(d.getCode())
			dataset.setPropertyValue("Q_ADDITIONAL_INFO", info)