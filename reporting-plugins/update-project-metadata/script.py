from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria


def process(tr, parameters, tableBuilder):
        user = parameters.get("user")
        if not user == None:
                tr.setUserId(user)
        identifier = parameters.get("identifier")
        description = parameters.get("description")
        project = tr.getProjectForUpdate(identifier)

        project.setDescription(description)
