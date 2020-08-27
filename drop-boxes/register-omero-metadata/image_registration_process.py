import re

import checksum

import ch.systemsx.cisd.etlserver.registrator.api.v2

from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

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

    def requestOmeroDatasetId(self, openbis_sample_code):
        pass

    def registerImageInOmero(self, omero_dataset_id):
        pass

    def extractMetadataFromTSV(self):
        pass

    def registerExperimentDataInOpenBIS(self, omero_image_ids):
        pass

    def triggerOMETiffConversion(self):
        pass


class SampleCodeError(Exception):
    
    def __init__(self, sample_code, message):
        self.sample_code = sample_code
        self.message = message
        super().__init__(self.message)

    def test(self):
        pass
