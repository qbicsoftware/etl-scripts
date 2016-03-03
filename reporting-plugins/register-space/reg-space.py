from ch.systemsx.cisd.openbis.generic.shared.basic.dto.RoleWithHierarchy import RoleCode

def process(tr, parameters, tableBuilder):
  """Create a new space with the code specified in the parameters
      Code: *
      Space Admin ID (or null): *
  """

  spaceCode = parameters.get("code")
  spaceAdmin = parameters.get("null")
  space = tr.createNewSpace(spaceCode,spaceAdmin)

  pu = parameters.get("poweruser")
  admins = parameters.get("admin")
  observers = parameters.get("observer")
  users = parameters.get("user")
  print users
  if(admins!=None):
 	 tr.assignRoleToSpace(RoleCode.ADMIN, space, admins, [])
  if(users!=None):
 	 tr.assignRoleToSpace(RoleCode.USER, space, users, [])
  if(observers!=None):
 	 tr.assignRoleToSpace(RoleCode.OBSERVER, space, observers, [])
  if(pu!=None):
 	 tr.assignRoleToSpace(RoleCode.POWER_USER, space, pu, [])