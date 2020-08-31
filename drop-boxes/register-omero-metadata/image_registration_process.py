import re

import checksum

import ch.systemsx.cisd.etlserver.registrator.api.v2
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
import csv

class ImageRegistrationProcess:

    barcode_pattern = re.compile('Q[a-zA-Z0-9]{4}[0-9]{3}[A-Z][a-zA-Z0-9]')

    def __init__(self, transaction):
        self._transaction = transaction
        self._incoming_file_name = transaction.getIncoming().getName()
        self._sample_code = ""
        self._project_code = ""

    def fetchOpenBisSampleCode(self):
        found = barcode_pattern.findall(self._incoming_file_name)
        if len(found) == 0:
            raise SampleCodeError(self._incoming_file_name, "Sample code does not match the QBiC sample code schema.")
        self._sample_code = found[0]
        if self._isValidSampleCode(self._sample_code):
            self._project_code = self._sample_code[:5]
        else:
            raise SampleCodeError(self._sample_code, "The sample code seems to be invalid, the checksum could not be confirmed.")
    
    def _isValidSampleCode(self, sample_code):
        try:
            id = sample_code[0:9]
            return checksum.checksum(id)==sample_code[9]
        except:
            return False

    #ToDo If we want to compare on Project Level then should we transfer the project Id and sample code or just iterate over all projects?
    def requestOmeroDatasetId(self, openbis_sample_code):
        pass

    #ToDo Should the Image be provided via File or as an Array? This Code was originally written for File Input and calls the Omero Import Tool from the commandline
    def registerImageInOmero(self, omero_dataset_id):
        pass

    #ToDo Check if Metadata file is provided as was suggested in test.tsv provided by LK
    def extractMetadataFromTSV(self, tsvFilePath):
        tsvFileMap = {}

        with open(tsvFilePath) as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t')
            for row in reader:
                tsvFileMap.update(row)

        return tsvFileMap

    #ToDo Not Implemented Yet wait for Andreas?
    def registerExperimentDataInOpenBIS(self, omero_image_ids):
        pass

    #ToDo This can be removed since it's handled in the java Omero-client-lib
    def triggerOMETiffConversion(self):
        pass


class SampleCodeError(Exception):
    
    def __init__(self, sample_code, message):
        self.sample_code = sample_code
        self.message = message
        super().__init__(self.message)

    def test(self):
        pass
