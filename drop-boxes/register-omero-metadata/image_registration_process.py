import re
import checksum

import ch.systemsx.cisd.etlserver.registrator.api.v2
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
import csv
from subprocess import Popen, PIPE

barcode_pattern = re.compile('Q[a-zA-Z0-9]{4}[0-9]{3}[A-Z][a-zA-Z0-9]')

class ImageRegistrationProcess:

    def __init__(self, transaction, env_name="omero_env_0", project_code="", sample_code=""):

        self._transaction = transaction
        self._incoming_file_name = transaction.getIncoming().getName()

        self._project_code = project_code
        self._sample_code = sample_code

        self._init_cmd_list = []
        self._init_cmd_list.append('eval "$(/home/qeana10/miniconda2/bin/conda shell.bash hook)"')
        self._init_cmd_list.append('export OMERO_PREFIX=/home/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-omero-metadata/OMERO.py-5.4.10-ice36-b105')
        self._init_cmd_list.append('export PYTHONPATH=$PYTHONPATH:$OMERO_PREFIX/lib/python')
        self._init_cmd_list.append('export PATH=$PATH:/home/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-omero-metadata/OMERO.server-5.4.10-ice36-b105/bin')

        self._init_cmd_list.append('conda ' + env_name)
        self._init_cmd_list.append('cd /home/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-omero-metadata/') #move to the dir where backendinterface.py lives

    def fetchOpenBisSampleCode(self):

        found = barcode_pattern.findall(self._incoming_file_name)
        if len(found) == 0:
            raise SampleCodeError(self._incoming_file_name, "Sample code does not match the QBiC sample code schema.")
        self._sample_code = found[0]
        if self._isValidSampleCode(self._sample_code):
            self._project_code = self._sample_code[:5]
        else:
            raise SampleCodeError(self._sample_code, "The sample code seems to be invalid, the checksum could not be confirmed.")

        return self._project_code, self._sample_code
    
    def _isValidSampleCode(self, sample_code):
        try:
            id = sample_code[0:9]
            return checksum.checksum(id)==sample_code[9]
        except:
            return False

    def requestOmeroDatasetId(self, project_code=None, sample_code=None):

        if project_code == None:
            project_code = self._project_code
        if sample_code == None:
            sample_code = self._sample_code

        cmd_list = list(self._init_cmd_list)
        cmd_list.append( "python backendinterface.py -p " + str(project_code) + " -s " + str(sample_code) )

        commands = ""
        for cmd in cmd_list:
            commands = commands + cmd + "\n"
        #print "cmds: " + commands

        process = Popen( "/bin/bash", shell=False, universal_newlines=True, stdin=PIPE, stdout=PIPE, stderr=PIPE )
        out, err = process.communicate( commands )

        #print str(out)

        ds_id = str(out)

        return ds_id

    def registerImageFileInOmero(self, file_path, dataset_id):

        cmd_list = list(self._init_cmd_list)
        cmd_list.append( "python backendinterface.py -f " + file_path + " -d " + str(dataset_id) )

        commands = ""
        for cmd in cmd_list:
            commands = commands + cmd + "\n"
        #print "cmds: " + commands

        process = Popen( "/bin/bash", shell=False, universal_newlines=True, stdin=PIPE, stdout=PIPE, stderr=PIPE )
        out, err = process.communicate( commands )

        id_list = str(out).split()

        return id_list
        

    def triggerOMETiffConversion(self):
        pass

    #ToDo Check if Metadata file is provided as was suggested in test.tsv provided by LK
    def extractMetadataFromTSV(self, tsvFilePath):
        tsvFileMap = {}
        try:
            with open(tsvFilePath) as tsvfile:
                reader = csv.DictReader(tsvfile, delimiter='\t', strict=True)
                for row in reader:
                    tsvFileMap.update(row)
        except IOError:
            print "Error: No file found at provided filepath " + tsvFilePath
        except csv.Error as e:
            print 'Could not gather the Metadata from TSVfile %s, in line %d: %s' % (tsvfile, reader.line_num, e)

        return tsvFileMap

    def registerExperimentDataInOpenBIS(self):
        pass


class SampleCodeError(Exception):
    
    def __init__(self, sample_code, message):
        self.sample_code = sample_code
        self.message = message
        super().__init__(self.message)

    def test(self):
        pass


