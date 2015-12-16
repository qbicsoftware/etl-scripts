def process(tr, parameters, tableBuilder):
  """Copy the dataset represented by the SrcPath to DestinationPath specified in the parameters
      SourcePath: *
      DestinationPath: *
      userid
  """
  import shutil
  import subprocess
  import os
  userid = parameters.get('userid')
  dest_path = parameters.get('dest_path')
  src_path = parameters.get('src_path')
  
  if os.path.isfile(src_path):
    shutil.copy(src_path,dest_path)
  else:
    shutil.copytree(src_path,dest_path)   

  cmd_line = []
  cmd_line.append('setfacl')
  cmd_line.append('-m')
  param = 'u:'
  param = param + userid
  param = param + ':r-x'
  cmd_line.append(param)
  cmd_line.append(dest_path+os.path.basename(src_path))
  print cmd_line
  p = subprocess.Popen(cmd_line,stdout=subprocess.PIPE)
  stdout, stderr = p.communicate()
  print stdout
  print stderr
  p2 = subprocess.Popen(["getfacl",dest_path],stdout=subprocess.PIPE)
  stdout2, stderr2 = p2.communicate()
  print stdout2
  print stderr2
