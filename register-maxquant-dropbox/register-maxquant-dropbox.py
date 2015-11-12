import re
import os
import shutil
import ch.systemsx.cisd.openbis.dss.generic.shared.api.internal.v1.IExperimentImmutable;
import ch.systemsx.cisd.openbis.dss.generic.shared.api.internal.v1.ISearchService;
import ch.systemsx.cisd.openbis.dss.generic.shared.api.internal.v1.ISampleImmutable;
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
import ch.systemsx.cisd.etlserver.registrator.api.v1.IDataSet;
import ch.systemsx.cisd.etlserver.registrator.api.v1.ISample;

def process(transaction):
    root = transaction.getIncoming().getAbsolutePath()
    name = transaction.getIncoming().getName()
    pattern = re.search("MAXQUANTQ\w{4}-[0-9]*",name)
    sampleCode = pattern.group(0)

    resultNames = ["proteinGroups", "peptides", "allPeptides", "modificationSpecificPeptides", "experimentalDesignTemplate.txt", "mqpar.xml"]
    combined = root + "/combined"
    txt = combined + "/txt"
    results = txt + "/results"

    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, sampleCode))
    foundSamples = search_service.searchForSamples(sc)

    if len(foundSamples) > 0:
      try:
        os.mkdir(results)
      except:
        pass
      rootsub = os.listdir(root)
      for f in rootsub:
        if f.endswith(".raw"):
          os.remove(os.path.realpath(os.path.join(root,f)))
        elif f in resultNames:
          curdir = os.path.join(root, f)
          dstdir =  os.path.join(results, f)
          shutil.copy(curdir, dstdir)
        elif f=="combined":
          combsub = os.listdir(combined)
          for cf in combsub:
            if cf=="txt":
              txtsub = os.listdir(txt)
              for tf in txtsub:
                for n in resultNames:
                  if n in tf:
                    curdir = os.path.join(txt, tf)
                    dstdir =  os.path.join(results, tf)
                    shutil.copy(curdir, dstdir)
            elif cf in resultNames:
              curdir = os.path.join(combined, cf)
              dstdir =  os.path.join(results, cf)
              shutil.copy(curdir, dstdir)
      tarPath = root + "/mq_archive.tar.gz"
      zip = "tar -zcvf "+tarPath+" -C  "+os.path.dirname(root)+" "+os.path.basename(os.path.normpath(root))
      os.system(zip)
      fullDataSet = transaction.createNewDataSet("Q_WF_MS_MAXQUANT_ORIGINAL_OUT")
      resultsDataSet = transaction.createNewDataSet("Q_WF_MS_MAXQUANT_RESULTS")
      sa = transaction.getSampleForUpdate(foundSamples.get(0).getSampleIdentifier())
      fullDataSet.setSample(sa)
      resultsDataSet.setSample(sa)
      transaction.moveFile(results, resultsDataSet)
      transaction.moveFile(tarPath, fullDataSet)
