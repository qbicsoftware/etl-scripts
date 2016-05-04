import sys

sys.path.append('/abi-projects/QBiC/scripts')

#import mk_barcodes_eps_pdf_2perpage as barcodes2D
#import mk_barcodes_eps_pdf as barcodes1D

#import ch.systemsx.cisd.etlserver.registrator.api.v2
#from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from java.lang import Byte
from java.io import FileInputStream
from java.io import File
import jarray

def getByteArray(fileUrl):
  file = File(fileUrl);
  inputStream = FileInputStream(file)
  length = file.length()
  bytes = jarray.zeros(length, 'b')
  #Read in the bytes
  offset = 0
  numRead = 0
  while offset<length:
      if numRead>= 0:
          print numRead
          numRead=inputStream.read(bytes, offset, length-offset)
          offset = offset + numRead
  return bytes

def attach(name,file,sample):
  bytes = getByteArray(file)
  sample.addAttachment(file, name,"",bytes)

def process(tr, parameters, tableBuilder):
  print parameters
  if "user" in parameters:
    tr.setUserId(parameters.get("user"))
  sampleCode = parameters.get("code")
  search_service = tr.getSearchService() 
  sc = SearchCriteria()
  sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, sampleCode))
  foundSamples = search_service.searchForSamples(sc)
  if(foundSamples.size() < 1):
    proj = parameters.get("project")
    space = parameters.get("space")
    sampleType = parameters.get("type")
    sampleId = "/" + space + "/" + sampleCode
    sample = tr.createNewSample(sampleId, sampleType)
    exp = "/"+space+"/"+proj+"/"+parameters.get("experiment")
    exp = tr.getExperiment(exp)
    sample.setExperiment(exp)
    if parameters.get("sample_class"):
      sample.setPropertyValue("Q_SECONDARY_NAME",parameters.get("sample_class"))
    if parameters.get("parents"):
      sample.setParentSampleIdentifiers(parameters.get("parents"))
    if parameters.get("properties"):
      properties = parameters.get("properties")
      for prop in properties.keySet():
        sample.setPropertyValue(prop, properties.get(prop))
