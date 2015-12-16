from ch.systemsx.cisd.openbis.generic.shared.basic.dto.RoleWithHierarchy import RoleCode

def process(tr, parameters, tableBuilder):
  """Create a new project with the code specified in the parameters
      Code: *
      Space Admin ID (or null): *
  """

  spaceCode = parameters.get("code")
  spaceAdmin = parameters.get("null")
  space = tr.createNewSpace(spaceCode,spaceAdmin)

  pu = parameters.get("powerusers")
  admins = parameters.get("admins")
  observers = parameters.get("observers")
  users = parameters.get("users")
  print users
  if(admins!=None):
 	 tr.assignRoleToSpace(RoleCode.ADMIN, space, admins, [])
  if(users!=None):
 	 tr.assignRoleToSpace(RoleCode.USER, space, users, [])
  if(observers!=None):
 	 tr.assignRoleToSpace(RoleCode.OBSERVER, space, observers, [])
  if(pu!=None):
 	 tr.assignRoleToSpace(RoleCode.POWER_USER, space, pu, [])
