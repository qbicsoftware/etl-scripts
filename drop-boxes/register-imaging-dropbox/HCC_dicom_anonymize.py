#!/usr/bin/python
# -*- coding: utf-8 -*-
# author(s): Mathew Divine <divine@informatik.uni-tuebingen.de>

"""
This is a command line tool to anonymize dicom files, which are located in the
directory hierachry of the given path. One could also just import it and use the
individual functions and thereby achieving the same aims. The functions of high
interst are called 'anonymize_to_qbic' and 'multiple_anonymize_to_qbic'.
"""
#%%
try:
    import pydicom as dicom
except ImportError:
    import dicom
import os
import numpy as np
from argparse import ArgumentParser
import pickle
from glob import glob
import tarfile
import re





#TODO: Replce anonymization field names with Tags (hexadecimal)
ANONYMIZATION_FIELDS = ['StudyDate','SeriesDate','AcquisitionDate','ContentDate','OverlayDate',\
                    'CurveDate','AcquisitionDatetime','StudyTime','SeriesTime','AcquisitionTime',\
                    'ContentTime','OverlayTime','CurveTime','AccessionNumber','InstitutionName',\
                    'InstitutionAddress','ReferringPhysiciansName','ReferringPhysiciansAddress',\
                    'ReferringPhysiciansTelephoneNumber','ReferringPhysicianIDSequence',\
                    'InstitutionalDepartmentName','PhysicianOfRecord','PhysicianOfRecordIDSequence',\
                    'PerformingPhysiciansName','PerformingPhysicianIDSequence','NameOfPhysicianReadingStudy',\
                    'PhysicianReadingStudyIDSequence','OperatorsName','PatientsName','PatientID',\
                    'IssuerOfPatientID','PatientsBirthDate','PatientsBirthTime','PatientsSex',\
                    'OtherPatientIDs','OtherPatientNames','PatientsBirthName','PatientsAge',\
                    'PatientsAddress','PatientsMothersBirthName','CountryOfResidence',\
                    'RegionOfResidence','PatientsTelephoneNumbers','StudyID','CurrentPatientLocation',\
                    'PatientsInstitutionResidence','DateTime','Date','Time','PersonName',\
                    'ProtocolName']

#SAVE_DIR = '/home/divine/HCC_MultiScale/PatientImageData/Thaiss_v1/RadiologyData/test2'


FNAME_COMPONENTS = ['patient_name','time_point','modality','pet_tracer','organ','date']

def parse_args():
    """
    Parses input and returns arguments as a dictionary.

    Returns
    -------
    out: Dictionary of command-line arguments.
    """
    parser = ArgumentParser(description='Anonymize Directory of Dicom files using\
                            the 50 attributes outlined in "Free DICOM de-identification\
                            tools in clinical research: functioning and safety of patient\
                            privacy." Eur J Radiology. 2015 25:3685–3695.\
                            DOI: 10.1007/s00330-015-3794-0')

    parser.add_argument('--root-directory',dest='root_dir',
                            help=('Root directory containing study directories\
                            which contain folders of DICOM files to anonymize.'))

    parser.add_argument('--study-directory',dest="study_dir",
                        help=('Name of directory containing folders of DICOM files to Anonymize'))

    parser.add_argument("--clean-up",dest="clean_up",
                        help=("The name of the directory for Organizing DICOM files \
                            into folder strucutre reflecting the SeriesDescription \
                            and SeriesNumber. This is a utility to use before \
                            using the --study-directory or --root-directory options \
                            !! Your DICOM files can't just be laying around they \
                             need to be in a folder structure !!"))
    parser.add_argument("--clean-up-multiple",dest="clean_up_multiple",
                         help="Analogous to the --clean-up option, but this will\
                            parse through multiple directories, cleaning up \
                            sub-directories.")

    parser.add_argument("--save-directory",dest='save_dir',
                        help=("The name of the directory where the qbic ready \
                        tarballs should be saved. If not given, a directory called \
                        'anonymize_to_qbic' will be created in the current working \
                        directory"))

    args = parser.parse_args()


    return args

def sequence_name_to_folder(dir_to_clean='',dicom_suffix='dcm'):
    """
    This function to be used to clean up loose dicoms. No arguements are necessray
    because it is assumed that you are in the directory.
    """

    if dir_to_clean:
        abs_dir_to_clean = os.path.abspath(dir_to_clean)
        os.chdir(abs_dir_to_clean)

    cwd = os.getcwd()
    print("this is the cwd: {}".format(cwd))
    for loose_dicom in glob(cwd+'/*'+dicom_suffix):

            try:
                dcm_img = dicom.read_file(loose_dicom,force=True)
            except:
                #print dcm
                print("exception occured at {}".format(loose_dicom))
                continue
            #grab the attributes which define folder name
            try:
                series_descr = getattr(dcm_img,'SeriesDescription')
                series_num   = getattr(dcm_img,'SeriesNumber')
            except AttributeError:
                continue
            # create the folder name
            folder_name = "{}_{}".format(series_descr,series_num).replace(' ','_')
            print("new folder name: {}".format(folder_name))
            if not os.path.isdir(os.path.join(cwd,folder_name)):
                # if the folder is not there, make it
                os.mkdir(os.path.join(cwd,folder_name))
            # move the dcm into the specified folder
            old_name = loose_dicom
            new_name = os.path.join(cwd,folder_name,os.path.basename(loose_dicom))
            print("Old Name: {}\t New Name: {}".format(old_name,new_name))
            os.rename(old_name,new_name)

def multi_sequence_name_to_folder(root_dir='', dirs_to_clean=[]):
    """
    Look through all sub-directories and clean up the loose dicoms. This is
    basically 'sequence_name_to_folder' with a for loop around it.
    """
    abs_root_dir = os.path.abspath(root_dir)
    dir_list = glob(abs_root_dir+"/*")
    for item in dir_list:
        if os.path.isdir(item):
            sequence_name_to_folder(dir_to_clean=item)



def anonymize_dicom(dicom_file,patient_name='anonymous',
                    fields_to_anonymize=ANONYMIZATION_FIELDS,
                    fields_to_return=None,path_to_save='.',
                    new_dicom_name='anonymous.dcm'):
    """ Given a dicom file, alter the given fields, anonymizing the
    patient name seperatley. Save a new dicom in the given directory with
    the given name
    """
    #having lots of issues with the character encoding
    # changed to python 3, now having more fun
    try:
        #im = dicom.read_file(unicode(dicom_file,'utf-8'))
        im = dicom.read_file(dicom_file)
    except UnicodeDecodeError:
        print("utf-8 codec can't decode byte...filename {}".format(dicom_file))
    except dicom.errors.InvalidDicomError:
        #im = dicom.read_file(unicode(dicom_file,'utf-8'),force=True)
        im = dicom.read_file(dicom_file,force=True)
    if fields_to_return:
    # create dictionary to hold returned fields
        returned_fields ={}.fromkeys(fields_to_return)
    # collect fields to retrieve
        for attr in returned_fields:
            try:
            # expect the field not to exist
                returned_fields[attr]=getattr(im,attr)
            except AttributeError:
                continue
    # now replace fields to anonymize with ''
    for attr in fields_to_anonymize:
        if attr=='PatientsName':
            set_attr = patient_name
        else:
            set_attr=''
        try:
            setattr(im,attr,set_attr)
            #print "{} has been set to {}".format(attr, set_attr)
        except AttributeError:
            print("The following attribute not found: {}".format(set_attr))
        except UnboundLocalError:
            print("Can't set attribute: utf-8 codec can't decode byte...filename {}".format(dicom_file))


    # now save the new dicom
    new_name = os.path.join(path_to_save,new_dicom_name)
    im.save_as(new_name)
    if fields_to_return:
        return returned_fields

def meta_data_from_dir_name(dir_name):
    """ Meta-Data will be inferred from the name of a directory.
        Here the patient_name, time_point, modality, pet_tracer, and
        organ will be inferred from the directories whose names are the
        patient meta information:
        !! WORKS BEST WITH ABSOLUTE PATHS !!
        Example: S01TP1_MRPET_FDG

        patient_name    = 'S01'
        time_point      = 'TP1'
        modality        = 'MRPET'
        pet_tracer      = 'FDG'
        organ           = 'None'
        url             = '/absolute/path/of/dir_name'
        However, CT images won't have a pet_tracer, and therefore it will be None'
    """
    # initialize dictionary to save meta data
    patient_meta_info = {}.fromkeys(['patient_name','time_point','modality',
                                    'pet_tracer','url','organ','date'],'None')

    name_split = os.path.basename(dir_name).split('_')
    patient_meta_info['patient_name']   = name_split[0][:3]
    patient_meta_info['time_point']     = name_split[0][3:6]
    patient_meta_info['url']            = dir_name


    if name_split[1] == 'MRPET':

        patient_meta_info['modality']   = name_split[1]
        patient_meta_info['pet_tracer'] = name_split[2]

    elif name_split[1] =='Punktion':
        patient_meta_info['modality']   = name_split[1]

    elif name_split[1].lower() in ['liver','tumor']:

        patient_meta_info['modality']   = 'CTPerfusion'
        patient_meta_info['organ']      = name_split[1]

    # look for a dicom and grab the date information -- !! should !!be the same
    # for all dicoms under this directory

    try:
        dicom_file = glob(dir_name+'/*dcm')[0]
    except IndexError:
        dicom_file = glob(dir_name+'/*/*dcm')[0]
        #print dicom_file
    #let's try moving into the directory and only feeding the basefile name to
    # the dicom.read_file function
    base_dir = os.getcwd()
    try:
        os.chdir(os.path.dirname(dicom_file))
        #im = dicom.read_file(unicode(os.path.basename(dicom_file),'utf-8'))
        im = dicom.read_file(os.path.basename(dicom_file),force=True)
        patient_meta_info['date'] = getattr(im,'StudyDate')
        os.chdir(base_dir)
    except UnicodeDecodeError:
        print('error')

    except:
        print("InvalidDicomError")
        patient_meta_info['date'] = "unkown_date"
        os.chdir(base_dir)
        #df = unicode(dicom_file)
        #df = dicom_file.decode('utf-16').encode('utf-8')
        #im = dicom.read_file(df)


    return patient_meta_info

def make_QBiC_readable(meta_data_info, FNAME_COMPONENTS=FNAME_COMPONENTS):
    """ make a "QBiC readable" identifier from the "patient-time-point" folders
    one layer below modality
    """
    patient_name = meta_data_info['patient_name']

    if patient_name[0]=='S':
        return 'QMSHSENTITY-'+patient_name.lstrip('S0')+'_'+'_'.join(\
                        [meta_data_info[keys] for keys in FNAME_COMPONENTS])
    else:
        return 'QMSHTENTITY-'+patient_name.lstrip('T0')+'_'+'_'.join(\
                        [meta_data_info[keys] for keys in FNAME_COMPONENTS])

def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

def anonymize_to_qbic(study_folder='',save_dir=SAVE_DIR):
    """
    This is quite possibly the function for which you are looking. Given a list of
    well behaved folder names -- we are talking absolute paths here -- go through
    these folders and anonymize all dicoms contained within.
    Now, there are some caveats. If the dicoms are just laying around inside of
    your root_folder, you will have to pack them up into folders named after the
    series description and series number. After which, this strucutre will then
    be placed into the agreed upon qbic format for posting to the portal.

    :param str: a single folder. Please use absolute paths. The folder should have
    a similar name to the following:

    S01TP2_Punktion  S02TP2_Punktion  S03TP2_Punktion  S05TP1_Punktion
    S01TP1_MRPET_Cholin  S03TP2_MRPET_Cholin  S05TP2_MRPET_FDG
    S01TP1_liver  S02TP2_tumor  S04TP1_liver  S05TP3_tumor
    S01TP2_Punktion  S03TP1_Punktion  S05TP1_Punktion

    :param str save_dir: There is a big, fat, glaringly ugly global variable
    defined at the beginning of this file. This is were everything will be saved.
    If you don't like it, it can be changed.

    :return: Nothing, just anonymizes, renames, and packs the intersting files
    and folders into qbic likeable tarballs.

    """

    # need to change to directory where the work will be performed in order
    # not give complete path names to the dicom file reader, which has
    # problems with some character sets which are encountered. cwd is used for
    # reference. Otherwise, the program ends up in the last folder where
    # dicom files are anonymized.
    cwd = os.getcwd()



    # set up a save directory
    if not os.path.isdir(SAVE_DIR):
        os.mkdir(SAVE_DIR)


    patient_timepoint_modality = os.path.basename(study_folder)
    print("printing patient_timepoint_modality: {}".format(patient_timepoint_modality))
    meta_data = meta_data_from_dir_name(study_folder)
    # first path component
    qbic_identifier = make_QBiC_readable(meta_data)
    print("current QBiC Id: {}".format(qbic_identifier))
    #if QBiC tarball dir doesn't exist, make it happen
    if not os.path.isdir(os.path.join(SAVE_DIR,qbic_identifier)):
        os.mkdir(os.path.join(SAVE_DIR,qbic_identifier))
        print("making directory {}...".format(os.path.join(SAVE_DIR,qbic_identifier)))




    sequence_dirs = glob(study_folder+'/*')
    for sequence_dir in sequence_dirs:
        if not os.path.isdir(sequence_dir):
            continue
        sequence_name = os.path.basename(sequence_dir)
        path_to_save = os.path.join(SAVE_DIR,qbic_identifier,sequence_name)
        #
        if not os.path.isdir(path_to_save):
            os.mkdir(path_to_save)

        #dicom_dir = os.path.join(patient_timepoint_modality,sequence_name)

        os.chdir(sequence_dir)

        print("Sequence Directory: {}...".format(sequence_dir))
        files = os.listdir(sequence_dir)
        for f in files:
            if f[0]=='.':
            #skip hidden files if present
                continue

            # changing each time to directory because of issues with the
            # encoding of folder names -- let's see how this works.
            #dicom_file = os.path.join(root,patient_timepoint_modality,sequence_name,f)
            print("file name: {}".format(f))
            dicom_file = f
            rand_num =  np.random.randint(10000000000000,size=1)[0]
            new_dicom_name = str(rand_num)+'.dcm'
            anonymize_dicom(dicom_file,patient_name=meta_data['patient_name'],
                             fields_to_anonymize=ANONYMIZATION_FIELDS,
                             fields_to_return=None,path_to_save=path_to_save,
                             new_dicom_name=new_dicom_name)


    output_filename = os.path.join(SAVE_DIR,qbic_identifier+'.tar.gz')
    print("writing output_filename...".format(output_filename))
    source_dir = os.path.join(SAVE_DIR,qbic_identifier)
    make_tarfile(output_filename, source_dir)

    os.chdir(cwd)
    print("Done.")

def multiple_anonymize_to_qbic(root_folder=[],save_dir=SAVE_DIR,regex=r"S\d+TP\d_\S+"):
    """
    This is just a wrapper around anonymize_to_qbic, which adds a for loop
    so multiple study folder can be processed at one go, given that they are
    all in the root_folder and pass the regex test. Also, this function will
    act poorly if given relative path names.

    In the past we have gottent the following folder strucutre from Wolfgang in the
    Radiology Department:

    Punktion/
        S01TP2_Punktion/
            Planung_KM_2/
            Planung_KM__20__I30f__VIA_5/
            Probe_1_20__B30f_6/
            Probe_1_20__B30f_7
            .
            .
            .
        S03TP1_Punktion/
            .
            .
            .
        S05TP1_Punktion/
            .
            .
            .
    I think you get the point. This is the function, for which you are looking.
    Just point it at the root folder, in this case 'Punktion', and let it happen.

    :param str root_folder: This is the top most directory like shown in the
    diagram above. Please use absolute paths!

    :param str save_dir: This is where the output will be saved. Please check the
    global variable at the beginning of this file to see if that is the default
    you want. Otherwise, you know what to do.

    :param str regex: This regex checks that the stud folders somehow behave correctly
    for what we have seen in the past. This assumes the Sorafenib identifier "S". For
    the TACE arm, one would need to change this.
    """

    study_folders = glob(root_folder+'/*')
    for study_folder in study_folders:
        study = os.path.basename(study_folder)
        study_name = re.findall(regex,study)
        print(study_name)
        if study_name and len(os.listdir(study_folder))>4:
            print("found a study: {}".format(study_name))
            anonymize_to_qbic(study_folder=study_folder,save_dir=SAVE_DIR)
#%%


def main():
    # grab command line arguements
    args = parse_args()
    root_dir = args.root_dir
    clean_up_dir = args.clean_up
    clean_up_multiple_dir = args.clean_up_multiple
    study_dir = args.study_dir
    #TODO: this needs to work as relative path
    SAVE_DIR = args.save_dir
    # make the magic happen
    # create a default directory for saving in the current working directory
    # if none given
    if not SAVE_DIR:
        SAVE_DIR = os.path.join(os.getcwd(),'anonymize_to_qbic')
        if not os.path.isdir(SAVE_DIR):
            os.mkdir(SAVE_DIR)
    # this is where one would clean up loose dicom files just lying around
    # and being silly
    if clean_up_multiple_dir:
        multi_sequence_name_to_folder(root_dir=clean_up_multiple_dir)

    if clean_up_dir:
        sequence_name_to_folder(dir_to_clean=clean_up_dir)
    # given the root directory, parse looking specificially for the folders
    # that match a particular regex.
    if root_dir:
        multiple_anonymize_to_qbic(root_folder=root_dir,save_dir=SAVE_DIR,regex=r"S\d+TP\d_\S+")
    # given just that one special folder that you want to anonymize. Here it is.
    if study_dir:
        anonymize_to_qbic(study_folder=root,save_dir=SAVE_DIR)


if __name__ == '__main__':
    main()
