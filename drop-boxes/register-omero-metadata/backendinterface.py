
"""Interface to the image management server
This module contains the functionality to upload and download
imaging data (raw and metadata) form the OMERO server (v5.4).
It requires that the following software be installed within the Python
environment you are loading this module:
	* OMERO.cli (https://docs.openmicroscopy.org/omero/5.4.0/users/cli/index.html)
	* OMERO Python language bindings (https://docs.openmicroscopy.org/omero/5.4.0/developers/Python.html)
This code is based on the following documentation:
    https://docs.openmicroscopy.org/omero/5.4.0/developers/Python.html
It contains the following functions:
    * omero_connect - connects to server
    * TODO...
    
"""


def omero_connect(usr, pwd, host, port):
    """
    Connects to the OMERO Server with the provided username and password.

    Args:
        usr: The username to log into OMERO
        pwd: a password associated with the given username
        host: the OMERO hostname
        port: the port at which the OMERO server can be reached

    Returns:
        Connected BlitzGateway to the OMERO Server with the provided credentials

    """
    from omero.gateway import BlitzGateway

    conn = BlitzGateway(usr, pwd, host=host, port=port)
    connected = conn.connect()
    conn.setSecure(True)

    if not connected:
        print("Error: Connection not available")

    return conn

def print_data_ids(conn):
    """
        Prints all IDs of the data objects(Projects, Datasets, Images) associated with the logged in user on the OMERO server

        Args:
            conn: Established Connection to the OMERO Server via a BlitzGateway

        Returns:
            Nothing except a printed text output to console

        """

    for project in conn.getObjects("Project"):
        print('project: ' + str(project.getName()) + ' -- ' + str(project.getId()))

        for dataset in project.listChildren():
            print('ds: ' + str(dataset.getName()) + ' -- ' + str(dataset.getId()))

            for image in dataset.listChildren():
                print('img: ' + str(image.getName()) + ' -- ' + str(image.getId()))

def get_omero_dataset_id(conn, openbis_project_id, openbis_sample_id):
    """
           Prints all IDs of the data objects(Projects, Datasets, Images) associated with the logged in user on the OMERO server

           Args:
               conn: Established Connection to the OMERO Server via a BlitzGateway
               openbis_project_id: Id specifying the project information stored on OpenBIS
               openbis_sample_id: Id specifying the sample information stored on OpenBIS
           Returns:
               omero_dataset_id:  Id specifying the dataset information stored on OMERO

           """
    omero_dataset_id = -1
    found_id = False

    my_exp_id = conn.getUser().getId()
    default_group_id = conn.getEventContext().groupId

    for project in conn.getObjects("Project"):

        if found_id:
            break

        if project.getName() == openbis_project_id:
            for dataset in project.listChildren():

                if dataset.getName() == openbis_sample_id:
                    omero_dataset_id = dataset.getId()

                    found_id = True
                    break

    return omero_dataset_id

def register_image_file(file_path, project_id, sample_id, usr, pwd, host, port=4064):
    """
    This function imports an image file to an omero server using the OMERO.cli (using Bio-formats)
    This function assumes the OMERO.cli is installed
    Example:
        register_image_file("data/test_img.nd2", "project_x", "sample_y",
         "joe_usr", "joe_pwd", "192.168.2.2")
    Args:
        file_path (string): the path to the fastq file to validate
        project_id (string): the corresponding project ID in openBIS server
        sample_id (string): the corresponding sample ID in openBIS server
        usr (string): username for the OMERO server
        pwd (string): password for the OMERO server
        host (string): OMERO server address
        port (int): OMERO server port
    Returns:
        list of strings: list of newly generated omero IDs for registered images
                (a file can contain many images)
    """

    import subprocess

    image_ids = []

    conn = omero_connect(usr, pwd, host, str(port))
    ds_id = get_omero_dataset_id(conn, project_id, sample_id)

    if ds_id != -1:

        cmd = "omero-importer -s " + host + " -p " + str(port) + " -u " + usr + " -w " + pwd + " -d " + str(int(ds_id)) + " " + file_path
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

def register_image_file_with_dataset_id(file_path, dataset_id, usr, pwd, host, port=4064):
    """
    This function imports an image file to an omero server using the OMERO.cli (using Bio-formats)
    This function assumes the OMERO.cli is installed
    Example:
        register_image_file("data/test_img.nd2", 10,
         "joe_usr", "joe_pwd", "192.168.2.2")
    Args:
        file_path (string): the path to the fastq file to validate
        dataset_id (string): the ID of the omero dataset
        usr (string): username for the OMERO server
        pwd (string): password for the OMERO server
        host (string): OMERO server address
        port (int): OMERO server port
    Returns:
        list of strings: list of newly generated omero IDs for registered images
                (a file can contain many images)
    """

    import subprocess

    logfile = open("/tmp/log_for_dummies.txt", "w")  

    image_ids = []

    ds_id = dataset_id
    logfile.write(str(file_path)+"\n")
    logfile.write("ds_id: "+str(ds_id)+"\n")
    logfile.write(str(usr)+"\n")
    logfile.write(str(host)+"\n")
    logfile.write(str(port)+"\n")


    if ds_id != -1:
        logfile.write("ds_id != -1"+"\n")
        cmd = "omero-importer -s " + host + " -p " + str(port) + " -u " + usr + " -w " + pwd + " -d " + str(int(ds_id)) + " " + file_path
        logfile.write("calling "+cmd+"\n")
        proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True,
                            universal_newlines=True)
        std_out, std_err = proc.communicate()
        logfile.write("code: "+proc.returncode+"\n")
        logfile.write("out:"+"\n")
        logfile.write(std_out+"\n")
        logfile.write("errors:"+"\n")
        logfile.write(std_err+"\n")

        if int(proc.returncode) == 0:

            for line in std_out.splitlines():
                if line[:6] == "Image:":
                    image_ids = line[6:].split(',')
                    break

        else:
            logfile.write("returncode != 0"+"\n")
            image_ids = []

    else:
        logfile.write("ds_id == -1"+"\n")
        image_ids = []
    logfile.write("resulting ids:"+"\n")
    logfile.write(image_ids+"\n")
    logfile.close()
    return image_ids


########################################
#functions to register numpy arrays

def generate_array_plane(new_img):
    """
    TODO
    """

    img_shape = new_img.shape
    size_z = img_shape[4]
    size_t = img_shape[0]
    size_c = img_shape[1]

    for z in range(size_z):              # all Z sections
        for c in range(size_c):          # all channels
            for t in range(size_t):      # all time-points

                new_plane = new_img[t, c, :, :, z]
                yield new_plane

def create_array(conn, img, img_name, img_desc, ds):
    """
    TODO
    """

    dims = img.shape
    z = dims[4]
    t = dims[0]
    c = dims[1]

    new_img = conn.createImageFromNumpySeq(generate_array_plane(img),
                                           img_name,
                                           z, c, t,
                                           description=img_desc,
                                           dataset=ds)

    return new_img.getId()

def register_image_array(img, img_name, img_desc, project_id, sample_id, usr, pwd, host, port=4064):
    """
    This function imports a 5D (time-points, channels, x, y, z) numpy array of an image
    to an omero server using the OMERO Python bindings 
    Example:
        register_image_array(hypercube, "tomo_0", "this is a tomogram",
         "project_x", "sample_y", "joe_usr", "joe_pwd", "192.168.2.2")
    Args:
        file_path (string): the path to the fastq file to validate
        project_id (string): the corresponding project ID in openBIS server
        sample_id (string): the corresponding sample ID in openBIS server
        usr (string): username for the OMERO server
        pwd (string): password for the OMERO server
        host (string): OMERO server address
        port (int): OMERO server port
    Returns:
        int: newly generated omero ID for registered image array
    """

    img_id = -1
    save_flag = 0

    conn = omero_connect(usr, pwd, host, str(port))

    for project in conn.getObjects("Project"):
        if project.getName() == project_id:
            for dataset in project.listChildren():
                if dataset.getName() == sample_id:

                    img_id = create_array(conn, img, img_name, img_desc, dataset)

                    save_flag = 1
                    break
        if save_flag == 1:
            break

    return int(img_id)

def get_image_array(conn, image_id):
    """
    This function retrieves an image from an OMERO server as a numpy array
    TODO
    """

    import numpy as np

    image = conn.getObject("Image", image_id)

    #construct numpy array (t, c, x, y, z)

    size_x = image.getSizeX()
    size_y = image.getSizeY()
    size_z = image.getSizeZ()
    size_c = image.getSizeC()
    size_t = image.getSizeT()

    # X and Y fields have to be aligned this way since during generation of the image from the numpy array the 2darray is expected to be (Y,X)
    # See Documentation here https://downloads.openmicroscopy.org/omero/5.5.1/api/python/omero/omero.gateway.html#omero.gateway._BlitzGateway
    hypercube = np.zeros((size_t, size_c, size_y, size_x, size_z))

    pixels = image.getPrimaryPixels()

    for t in range(size_t):
        for c in range(size_c):
            for z in range(size_z):
                plane = pixels.getPlane(z, c, t)      # get a numpy array.
                hypercube[t, c, :, :, z] = plane

    return hypercube

################################

def add_annotations_to_image(conn, image_id, key_value_data):
    """
    This function is used to add key-value pair annotations to an image
    Example:
        key_value_data = [["Drug Name", "Monastrol"], ["Concentration", "5 mg/ml"]]
        add_annotations_to_image(conn, image_id, key_value_data)
    Args:
        conn: Established Connection to the OMERO Server via a BlitzGateway
        image_id (int): An OMERO image ID
        key_value_data (list of lists): list of key-value pairs
    Returns:
        int: not relevant atm
    """

    import omero

    map_ann = omero.gateway.MapAnnotationWrapper(conn)
    # Use 'client' namespace to allow editing in Insight & web
    namespace = omero.constants.metadata.NSCLIENTMAPANNOTATION
    map_ann.setNs(namespace)
    map_ann.setValue(key_value_data)
    map_ann.save()

    image = conn.getObject("Image", image_id)
    # NB: only link a client map annotation to a single object
    image.linkAnnotation(map_ann)

    return 0


#########################
##app

from optparse import OptionParser
import ConfigParser

config = ConfigParser.RawConfigParser()
config.read("imaging_config.properties")


###OMERO server info
USERNAME = config.get('OmeroServerSection', 'omero.username')
PASSWORD = config.get('OmeroServerSection', 'omero.password')
HOST = config.get('OmeroServerSection', 'omero.host')
PORT = int(config.get('OmeroServerSection', 'omero.port'))


def get_args():
    parser = OptionParser()
    parser.add_option('-f', '--file', dest='file_path', default="None", help='file to register')
    parser.add_option('-d', '--dataset', dest='dataset_id', default="None", help='dataset id for registration')

    parser.add_option('-p', '--project', dest='project_id', default="None", help='project id for dataset id retrieval')
    parser.add_option('-s', '--sample', dest='sample_id', default="None", help='sample id for dataset id retrieval')

    parser.add_option('-i', '--image', dest='image_id', default="None", help='image id for key-value pair annotation')
    parser.add_option('-a', '--annotation', dest='ann_str', default="None", help='annotation string')


    (options, args) = parser.parse_args()
    return options

if __name__ == '__main__':

    args = get_args()

    if args.file_path != "None":
        img_ids = register_image_file_with_dataset_id(args.file_path, int(args.dataset_id), USERNAME, PASSWORD, HOST)

        id_str = ""
        for id_i in img_ids:
            id_str = id_str + id_i + " "

        print id_str

    elif args.project_id != "None":

        conn = omero_connect(USERNAME, PASSWORD, HOST, str(PORT))
        ds_id = get_omero_dataset_id(conn, str(args.project_id), str(args.sample_id))

        print ds_id

    elif args.image_id != "None":

        conn = omero_connect(USERNAME, PASSWORD, HOST, str(PORT))

        #string format: key1::value1//key2::value2//key3::value3//...
        key_value_data = []
        pair_list = args.ann_str.split("//")
        for pair in pair_list:
            key_value = pair.split("::")
            key_value_data.append(key_value)

        #print("backend: key-value pairs: " + str(key_value_data))

        add_annotations_to_image(conn, str(args.image_id), key_value_data)

        print "0"
