import re
import checksum
import ch.systemsx.cisd.etlserver.registrator.api.v2
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
import csv
from subprocess import Popen, PIPE

barcode_pattern = re.compile('Q[a-zA-Z0-9]{4}[0-9]{3}[A-Z][a-zA-Z0-9]')
conda_home_path = "/home/qeana10/miniconda2/"
omero_lib_path = "/home/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-omero-metadata/OMERO.py-5.4.10-ice36-b105"
etl_home_path = "/home/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-omero-metadata/"


class ImageRegistrationProcess:

    def __init__(self, transaction, env_name="omero_env_0", project_code="", sample_code="", conda_path=None, omero_path=None, etl_path=None):

        self._transaction = transaction
        self._incoming_file_name = transaction.getIncoming().getName()
        self._search_service = transaction.getSearchService()

        self._project_code = project_code
        self._sample_code = sample_code

        ###set env
        self._conda_path = conda_home_path
        if not conda_path is None:
            self._conda_path = conda_path

        self._omero_path = omero_lib_path
        if not omero_path is None:
            self._omero_path = omero_path

        self._etl_path= etl_home_path
        if not etl_path is None:
            self._etl_path = etl_path

        self._init_cmd_list = []
        self._init_cmd_list.append('eval "$(' + self._conda_path + 'bin/conda shell.bash hook)"')
        self._init_cmd_list.append('conda activate ' + env_name)

        self._init_cmd_list.append('export OMERO_PREFIX=' + self._omero_path)
        self._init_cmd_list.append('export PYTHONPATH=$PYTHONPATH:$OMERO_PREFIX/lib/python')
        
        #now use the omero-importer app packaged in the conda env
        #self._init_cmd_list.append('export PATH=$PATH:/home/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-omero-metadata/OMERO.server-5.4.10-ice36-b105/bin')
        self._init_cmd_list.append('export PATH=$PATH:' + self._conda_path + 'envs/' + env_name + '/bin')

        #move to the dir where backendinterface.py lives for exec.
        self._init_cmd_list.append('cd ' + self._etl_path)

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

    def searchOpenBisSample(self, sample_code):
        #find specific sample
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, sample_code))
        foundSamples = self._search_service.searchForSamples(sc)
        if len(foundSamples) == 0:
            raise SampleNotFoundError(sample_code, "Sample could not be found in openBIS.")
        sample = foundSamples[0]
        return sample

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

        process = Popen( "/bin/bash", shell=False, universal_newlines=True, stdin=PIPE, stdout=PIPE, stderr=PIPE )
        out, err = process.communicate( commands )

        ds_id = str(out)

        return ds_id

    def registerImageFileInOmero(self, file_path, dataset_id):
        print "using file_path:"
        print file_path
        cmd_list = list(self._init_cmd_list)
        cmd_list.append( "python backendinterface.py -f " + file_path + " -d " + str(dataset_id) )

        commands = ""
        for cmd in cmd_list:
            commands = commands + cmd + "\n"

        process = Popen( "/bin/bash", shell=False, universal_newlines=True, stdin=PIPE, stdout=PIPE, stderr=PIPE )
        out, err = process.communicate( commands )

        id_list = str(out).split()
        for img_id in id_list:
            if not img_id.isdigit():
                return []

        return id_list
        

    def triggerOMETiffConversion(self):
        pass

    #ToDo Check if Metadata file is provided as was suggested in test.tsv provided by LK
    def extractMetadataFromTSV(self, tsv_file_path):
        tsvFileMap = {}
        try:
            with open(tsv_file_path) as tsvfile:
                reader = csv.DictReader(tsvfile, delimiter='\t', strict=True)
                for row in reader:
                    tsvFileMap.update(row)
        except IOError:
            print "Error: No file found at provided filepath " + tsv_file_path
        except csv.Error as e:
            print 'Could not gather the Metadata from TSVfile %s, in line %d: %s' % (tsvfile, reader.line_num, e)

        return tsvFileMap

    def registerExperimentDataInOpenBIS(self):
        pass

    def registerKeyValuePairs(self, image_id, property_map):
        cmd_list = list(self._init_cmd_list)

        #string format: key1::value1//key2::value2//key3::value3//...
        key_value_str = ""
        for key in property_map.keys(): 
            key_value_str = key_value_str + str(key) + "::" + str(property_map[key]) + "//"
        key_value_str = key_value_str[:len(key_value_str)-2] #remove last two chars
        #print("irp str: " + key_value_str)

        cmd_list.append( "python backendinterface.py -i " + str(image_id) + " -a " + key_value_str )

        commands = ""
        for cmd in cmd_list:
            commands = commands + cmd + "\n"

        process = Popen( "/bin/bash", shell=False, universal_newlines=True, stdin=PIPE, stdout=PIPE, stderr=PIPE )
        out, err = process.communicate( commands )

        #print(out)

        return 0


class SampleCodeError(Exception):
    
    def __init__(self, sample_code, message):
        self.sample_code = sample_code
        self.message = message
        super().__init__(self.message)

    def test(self):
        pass

class SampleNotFoundError(Exception):
    
    def __init__(self, sample_code, message):
        self.sample_code = sample_code
        self.message = message
        super().__init__(self.message)

    def test(self):
        pass


