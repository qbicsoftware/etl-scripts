import re

import checksum

import ch.systemsx.cisd.etlserver.registrator.api.v2

from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
import subprocess

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

        omero_dataset_id = -1
        found_id = False

        for project in self.getObjects("Project"):

            if found_id:
                break

            #ToDo  Is this coherent between OpenBis and Omero, then we should provide the project ID?
            if project.getName() == openbis_project_id:
                for dataset in project.listChildren():

                    if dataset.getName() == openbis_sample_code:
                        omero_dataset_id = dataset.getId()

                        found_id = True
                        break

        return omero_dataset_id

    #ToDo Should the Image be provided via File or as an Array? This Code was originally written for File Input and calls the Omero Import Tool from the commandline
    def registerImageInOmero(self, omero_dataset_id):

        if omero_dataset_id != -1:


            #ToDo How should we handle the transaction process here? we need to provide host port user and pwd to connect to Omero. Can we extract that from the current transaction.

            cmd = "omero import -s " + host + " -p " + str(port) + " -u " + usr + " -w " + pwd + " -d " + str(
                int(omero_dataset_id)) + " " + file_path
            print("----cmd: " + cmd)

            proc = subprocess.Popen(cmd,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    shell=True,
                                    universal_newlines=True)

            std_out, std_err = proc.communicate()

            print("std_out: " + std_out)
            print("std_err: " + std_err)
            print("return_code: " + str(proc.returncode))

            if int(proc.returncode) == 0:

                print("-->" + std_out)

                fist_line = std_out.splitlines()[0]
                image_ids = fist_line[6:].split(',')

                print("id list: " + str(image_ids))

            else:
                print("return code fail")

        else:
            print("invalid sample_id")

        return image_ids


        pass

    #ToDo Not Implemented Yet since we don't know how the metadata will look like
    def extractMetadataFromTSV(self):
        pass

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
