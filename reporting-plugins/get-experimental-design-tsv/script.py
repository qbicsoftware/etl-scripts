from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClause
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClauseAttribute
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria

# This script returns a spreadsheet of humanly readable information of interest of an experiment. As requested on the QBiC retreat 10/2015 each layer in the data model
# is returned (and has to be requested) separately. Experimental factors are returned as xml and have to be parsed in the portal

CODE = "Code"
SECONDARY_NAME = "Secondary Name"
SOURCE = "Source Name"
EXTERNAL_ID = "External ID"
XML = "Attributes"
SAMPLE_TYPE = "Sample Type"
TIER = "Sample Level"

def getParents(samples, sampleMap):
	res = []
	for sample in samples:
		ids = sample.getParentSampleIdentifiers()
		for id in ids:
			res.append(sampleMap[id])
	return res

def fetchSource(id, sampleMap, terms):
	sample = sampleMap[id]
	top = [sample]
	cycle = True

	while cycle:
		roots = getParents(top, sampleMap)
		if len(roots) > 0:
			top = roots
		else:
			cycle = False
	sources = []
	top = set(top)
	for sample in top:
		id = sample.getCode().split('-')[1]
		organism = sample.getPropertyValue("Q_NCBI_ORGANISM")
		for term in terms:
			if organism == term.getCode():
				desc = term.getDescription()
				if desc == None or len(desc) < 1:
					desc = term.getLabel()
		if desc == "human":
			desc == "patient"
		sources.append(desc+' '+id)
	return '+'.join(sources)

def process(tr, parameters, tableBuilder):
	ids = sorted(parameters.get("ids"))
	types = parameters.get("types") #sample types (tiers) that are requested for the tsv

	tableBuilder.addHeader(CODE)
	tableBuilder.addHeader(SECONDARY_NAME)
	tableBuilder.addHeader(SOURCE)
	tableBuilder.addHeader(EXTERNAL_ID)
	tableBuilder.addHeader(SAMPLE_TYPE)
	tableBuilder.addHeader(XML)
	tableBuilder.addHeader(TIER)

	sampleMap = {}

	voc = searchService.getVocabulary("Q_NCBI_TAXONOMY")
	for id in ids:
		sc = SearchCriteria()
		sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, id.split('/')[-1]))
		sample = searchService.searchForSamples(sc)[0]
		sampleMap[id] = sample
	for id in ids:
		sample = sampleMap[id]
		if sample.getSampleType() in types:
			code = sample.getCode()
			row = tableBuilder.addRow()
			row.setCell(CODE, code)
			row.setCell(SECONDARY_NAME, sample.getPropertyValue("Q_SECONDARY_NAME"))
			row.setCell(SOURCE, fetchSource(id, sampleMap, voc.getTerms()))
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
