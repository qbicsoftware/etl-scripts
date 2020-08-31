
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
    * omero_connect - conects to server
    * TODO...
    
"""

def omero_connect(usr, pwd, host, port):
    """
    TODO
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
    TODO
    """

    my_exp_id = conn.getUser().getId()
    default_group_id = conn.getEventContext().groupId
    
    for project in conn.getObjects("Project"):
        print('project: ' + str(project.getName()) + ' -- ' + str(project.getId()))

        for dataset in project.listChildren():
            print('ds: ' + str(dataset.getName()) + ' -- ' + str(dataset.getId()))
            
            for image in dataset.listChildren():
                print('img: ' + str(image.getName()) + ' -- ' + str(image.getId()))

def get_omero_dataset_id(conn, openbis_project_id, openbis_sample_id):
    """
    TODO
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

        cmd = "omero import -s " + host + " -p " + str(port) + " -u " + usr + " -w " + pwd + " -d " + str(int(ds_id)) + " " + file_path
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

    image_ids = []

    ds_id = dataset_id

    if ds_id != -1:

        cmd = "omero import -s " + host + " -p " + str(port) + " -u " + usr + " -w " + pwd + " -d " + str(int(ds_id)) + " " + file_path
        #print("----cmd: " + cmd)

        proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True,
                            universal_newlines=True)

        std_out, std_err = proc.communicate()

        #print("std_out: " + std_out)
        #print("std_err: " + std_err)
        #print("return_code: " + str(proc.returncode))

        if int(proc.returncode) == 0:

            #print("-->" + std_out)

            fist_line = std_out.splitlines()[0]
            image_ids = fist_line[6:].split(',')

            #print("id list: " + str(image_ids))

        else:
            image_ids = -1
            #print("return code fail")

    else:
        image_ids = -1
        #print("invalid sample_id")

    return image_ids


########################################
#fucntions to register numpy arrays

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
    
    #print "\nImage:%s" % imageId
    #print "=" * 50
    #print image.getName(), image.getDescription()
    # Retrieve information about an image.
    #print " X:", image.getSizeX()
    #print " Y:", image.getSizeY()
    #print " Z:", image.getSizeZ()
    #print " C:", image.getSizeC()
    #print " T:", image.getSizeT()
            
    # List Channels (loads the Rendering settings to get channel colors)
    #for channel in image.getChannels():
    #    print 'Channel:', channel.getLabel(),
    #    print 'Color:', channel.getColor().getRGB()
    #    print 'Lookup table:', channel.getLut()
    #    print 'Is reverse intensity?', channel.isReverseIntensity()
        
    ##construct numpy array (t, c, x, y, z)
    
    
    size_x = image.getSizeX()
    size_y = image.getSizeY()
    size_z = image.getSizeZ()
    size_c = image.getSizeC()
    size_t = image.getSizeT()
    hypercube = np.zeros((size_t, size_c, size_x, size_y, size_z))
    
    pixels = image.getPrimaryPixels()
    
    for t in range(size_t):
        for c in range(size_c):
            for z in range(size_z):
                plane = pixels.getPlane(z, c, t)      # get a numpy array.
                hypercube[t, c, :, :, z] = plane
                
    return hypercube #, image.getParent()

################################

def add_annotations_to_image(conn, image_id, key_value_data):
    """
    TODO
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

###OMERO server info
USERNAME = "zmbp_user_1"
PASSWORD = "prtlUsr01-4Mark01/"
HOST = "134.2.183.129"
PORT = 4064


def get_args():
    parser = OptionParser()
    parser.add_option('-f', '--file', dest='file_path', default="None", help='file to register')
    parser.add_option('-d', '--dataset', dest='dataset_id', default="None", help='dataset id for registration')

    parser.add_option('-p', '--project', dest='project_id', default="None", help='project id for dataset id retrieval')
    parser.add_option('-s', '--sample', dest='sample_id', default="None", help='sample id for dataset id retrieval')

    (options, args) = parser.parse_args()
    return options

if __name__ == '__main__':

    args = get_args()

    if args.file_path != "None":
        img_ids = register_image_file_with_dataset_id(args.file_path, int(args.dataset_id), USERNAME, PASSWORD, HOST)

        id_str = ""
        for id_i in img_ids:
            id_str = id_str + id_i + " "

        #print "-------> backend_interface output (img reg):"
        print id_str
        #print "----------------------------------"
    else:

        conn = omero_connect(USERNAME, PASSWORD, HOST, str(PORT))
        ds_id = get_omero_dataset_id(conn, str(args.project_id), str(args.sample_id))
        
        print ds_id
