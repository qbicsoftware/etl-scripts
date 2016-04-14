from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClause
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClauseAttribute
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SampleFetchOption

# This script returns a spreadsheet of humanly readable information of interest of an experiment. As requested on the QBiC retreat 10/2015 each layer in the data model
# is returned (and has to be requested) separately. Experimental factors are returned as xml and have to be parsed in the portal

CODE = "Code"
SECONDARY_NAME = "Secondary Name"
SOURCE = "Source Name"
EXTERNAL_ID = "External ID"
XML = "Attributes"
SAMPLE_TYPE = "Sample Type"
TIER = "Sample Level"

def fetchSource(samples, terms, res):
	roots = []
	for sample in samples:
		if sample.getSampleType() != "Q_BIOLOGICAL_ENTITY":
			samples = samples + sample.getParents()
		else:
			roots.append(sample)
	roots = set(roots)
	res = []
	for sample in roots:
		try:
			id = sample.getCode().split('-')[1]
		except:
			id = sample.getCode()
		organism = sample.getPropertyValue("Q_NCBI_ORGANISM")
		if organism:
			for term in terms:
				if organism == term.getCode():
					print organism
					desc = term.getDescription()
					print desc
					print term.getLabel()
					if desc == None or len(desc) < 1:
						desc = term.getLabel()
			if desc == "human":
				desc == "patient"
			res.append(sample.getPropertyValue("Q_SECONDARY_NAME")+"("+desc+' '+id+")")
		else:
			res.append("unknown source")
	return '+'.join(res)

def process(tr, parameters, tableBuilder):
	#ids = sorted(parameters.get("ids"))
	types = parameters.get("types") #sample types (tiers) that are requested for the tsv
	project = parameters.get("project")

	tableBuilder.addHeader(CODE)
	tableBuilder.addHeader(SECONDARY_NAME)
	tableBuilder.addHeader(SOURCE)
	tableBuilder.addHeader(EXTERNAL_ID)
	tableBuilder.addHeader(SAMPLE_TYPE)
	tableBuilder.addHeader(XML)
	tableBuilder.addHeader(TIER)

	#search all samples of project
	search = tr.getSearchService()
	sc = SearchCriteria()
	pc = SearchCriteria()
	pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project));
	sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
	fetchOptions = EnumSet.of(SampleFetchOption.ANCESTORS, SampleFetchOption.PROPERTIES)
	allSamples = service.searchForSamples(sc, fetchOptions)
	#filter all samples by types
	samples = []
	for s in allSamples:
		if s.getSampleType() in types:
			samples.append(s)
	#sort remaining samples-
	samples = sorted(samples)

	voc = search.getVocabulary("Q_NCBI_TAXONOMY")
	for s in samples:
		code = sample.getCode()
		row = tableBuilder.addRow()
		row.setCell(CODE, code)
		row.setCell(SECONDARY_NAME, sample.getPropertyValue("Q_SECONDARY_NAME"))
		row.setCell(SOURCE, fetchSource([sample], voc.getTerms(), []))
		row.setCell(EXTERNAL_ID, sample.getPropertyValue("Q_EXTERNALDB_ID"))
		extrType = sample.getPropertyValue("Q_PRIMARY_TISSUE")
		if not extrType:
			extrType = sample.getPropertyValue("Q_SAMPLE_TYPE")
		if not extrType:
			extrType = ""
		if extrType=="CELL_LINE":
			extrType = sample.getPropertyValue("Q_TISSUE_DETAILED")
		row.setCell(SAMPLE_TYPE, extrType)
		row.setCell(XML, sample.getPropertyValue("Q_PROPERTIES"))
		row.setCell(TIER, sample.getSampleType())
