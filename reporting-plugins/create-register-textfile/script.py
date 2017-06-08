import os

# creates a short file in the specified etl dropbox and starts registration with a marker file
def process(tr, parameters, tableBuilder):
  file_name = parameters.get("filename")
  lines = parameters.get("content")
  dropbox = "/mnt/DSS1/openbis_dss/"+parameters.get("dropbox")+"/"
  f_path = os.path.join(dropbox+filename)
  f = open(f_path, "w")
  for line in lines:
	f.write(line+"\n")
  f.close()
  marker_path = os.path.join(dropbox, "MARKER_is_finished_"+filename)
  print f_path
  print marker_path
  #marker = open(marker_path, 'a')
  #marker.close()
