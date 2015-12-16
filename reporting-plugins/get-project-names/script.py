from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

CODE = "ProjectCode"
NAME = "ProjectName"
projectMap = {}

def process(tr, parameters, tableBuilder):
	ids = parameters.get("ids")

	tableBuilder.addHeader(CODE)
	tableBuilder.addHeader(NAME)
	for id in ids:
		code = id.split("/")[-2]
		exp = tr.getExperiment(id)
		if exp:
			row = tableBuilder.addRow()
			row.setCell(CODE, code)
			row.setCell(NAME, exp.getPropertyValue("Q_SECONDARY_NAME"))
