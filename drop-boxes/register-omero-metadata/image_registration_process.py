import re
import checksum
import ch.systemsx.cisd.etlserver.registrator.api.v2
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
import csv
from subprocess import Popen, PIPE
import xml.etree.ElementTree as ET
import os

barcode_pattern = re.compile('Q[a-zA-Z0-9]{4}[0-9]{3}[A-Z][a-zA-Z0-9]')

# TODO: avoid hardcoding paths, try environment variables or parameters
conda_home_path = "/home/qeana10/miniconda3/"
omero_lib_path = "/home/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-omero-metadata/OMERO.py-5.4.10-ice36-b105"
etl_home_path = "/home/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-omero-metadata/"


class ImageRegistrationProcess:

    def __init__(self, transaction, env_name="etl-omero-bifrost", project_code="", sample_code="", conda_path=None, omero_path=None, etl_path=None):

        self._transaction = transaction
        self._incoming_file_name = transaction.getIncoming().getName()
        self._search_service = transaction.getSearchService()

        self._project_code = project_code
        self._sample_code = sample_code

        # set process exec. env.
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

        # move to the dir where ETL lives for exec.
        self._init_cmd_list.append('cd ' + self._etl_path)

    def executeCommandList(self, cmd_list):

        commands = ""
        for cmd in cmd_list:
            commands = commands + cmd + "\n"

        process = Popen( "/bin/bash", shell=False, universal_newlines=True, stdin=PIPE, stdout=PIPE, stderr=PIPE )
        out, err = process.communicate( commands )

        return out, err

    def requestOmeroDatasetId(self, project_code=None, sample_code=None):

        if project_code == None:
            project_code = self._project_code
        if sample_code == None:
            sample_code = self._sample_code

        cmd_list = list(self._init_cmd_list)
        cmd_list.append( "omero-bifrost query dataset-id " + str(project_code) + " " + str(sample_code) + " --to-xml" )

        out, err = self.executeCommandList(cmd_list)

        ds_id = -1
        output_xml = ET.fromstring(str(out))
        for output_item in output_xml:
            # print "---> " + str(output_item.tag) + " - " + str(output_item.attrib)
            ds_id = int(output_item.attrib["id"])
            break

        return ds_id

    def registerImageFileInOmero(self, file_path, dataset_id):

        cmd_list = list(self._init_cmd_list)
        cmd_list.append( "omero-bifrost push img-file " + file_path + " " + str(dataset_id) + " --to-xml" )

        out, err = self.executeCommandList(cmd_list)

        id_list = []
        output_xml = ET.fromstring(str(out))
        for output_item in output_xml:
            img_id = output_item.attrib["id"]
            if img_id.isdigit():
                id_list.append(img_id)
            else:
                return []

        return id_list

    def standardImageFolderRegistration(self, image_folder_path, dataset_id):
        """Imports folder using the standard process of omero-py
        """

        cmd_list = list(self._init_cmd_list)
        cmd_list.append( "omero-bifrost push img-folder " + image_folder_path + " " + str(dataset_id) + " --to-xml" )

        out, err = self.executeCommandList(cmd_list)

        id_list = []
        output_xml = ET.fromstring(str(out))
        for output_item in output_xml:
            img_id = output_item.attrib["id"]
            if img_id.isdigit():
                id_list.append(img_id)
            else:
                return []

        return id_list

    def registerImageFolder(self, image_folder_path, dataset_id):

        return self.standardImageFolderRegistration(image_folder_path, dataset_id)

    def attachFileToImage(self, file_path, image_id):

        cmd_list = list(self._init_cmd_list)

        cmd_list.append( "omero-bifrost push file-atch " + file_path + " " + str(image_id) )

        out, err = self.executeCommandList(cmd_list)

        return 0

    def tagOmeroImage(self, image_id, tag_value):

        cmd_list = list(self._init_cmd_list)

        cmd_list.append( "omero-bifrost push img-tag " + str(image_id) + " " + tag_value )

        out, err = self.executeCommandList(cmd_list)

        return 0

    def registerOmeroKeyValuePairs(self, image_id, property_map):
        """Registers the property map as key-value pairs in the OMERO server.
        """
        
        cmd_list = list(self._init_cmd_list)

        key_value_str = ""
        for key in property_map.keys(): 
            key_value_str = key_value_str + "--kv-pair " + str(key) + ":" + str(property_map[key]) + " "
        key_value_str = key_value_str[:len(key_value_str)-2] #remove last two chars

        cmd_list.append( "omero-bifrost push key-value " + str(image_id) + " " + key_value_str )

        out, err = self.executeCommandList(cmd_list)

        return 0

    # TODO: clean following functions

    def scanImageFolder(self, image_folder_path):
        """Scans folder for image files and returns list of file paths
        """

        for root, subFolders, files in os.walk(image_folder_path):
            for f in files:
                stem, ext = os.path.splitext(f)
                if ext.lower() =='.tsv':
                    with open(os.path.join(root, f), 'U') as fh: metadataFileContent = fh.readlines()
        return metadataFileContent

    def triggerOMETiffConversion(self):
        pass

    #TODO: Check if Metadata file is provided as defined

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
        # find specific sample

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

class DICOMVolumeRegistrationProcess(ImageRegistrationProcess):

    def __init__(self, transaction, env_name="etl-omero-bifrost", project_code="", sample_code="", conda_path=None, omero_path=None, etl_path=None):

        super().__init__(transaction, env_name, project_code, sample_code, conda_path, omero_path, etl_path)

    def registerImageFolder(self, image_folder_path, dataset_id):

        return self.standardImageFolderRegistration(image_folder_path, dataset_id)

class ImageRegistrationProcessFactory:

    def __init__(self):
        
        self._default_tag = "std-import"

    def createRegistrationProcess(self, transaction, etl_tag="std-import"):

        if etl_tag == "dicom-vol":
            reg_proc = DICOMVolumeRegistrationProcess(transaction)
        else:
            reg_proc = ImageRegistrationProcess(transaction)

        return reg_proc


####################

class SampleCodeError(Exception):
    
    def __init__(self, sample_code, message):
        self.sample_code = sample_code
        self.message = message
        super(SampleCodeError, self).__init__(message)

    def test(self):
        pass

class SampleNotFoundError(Exception):
    
    def __init__(self, sample_code, message):
        self.sample_code = sample_code
        self.message = message
        super(SampleNotFoundError, self).__init__(message)

    def test(self):
        pass

