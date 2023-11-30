import random
import numpy as np
import nibabel as nib
from nibabel.processing import resample_to_output
import os
import boto3 


# To do:
# Function to save outputs with sensible naming convention
# Expand functions so they can work with fMRI and DWI data
# Function to add various types of noise to image
# Function to add random lesions (black spots) or signal drop-out


def main() : 
    run = 'local'
    
    if run == 'global' :
        local_data_path = 'src/'
        input_bucket = os.environ['INPUT_BUCKET'] 
        input_prefix = os.evnciron['INPUT_PREFIX']
        output_bucket = os.environ['OUTPUT_BUCKET']
        output_prefix = os.environ['OUTPUT_PREFIX']
       

    elif run == 'local' : 
        local_data_path = 'src/'
        input_bucket = "loni-data-curated-20230501"
        input_prefix = 'ppmi_500_updated_cohort/curated/data/PPMI/'
        output_bucket = 'tempamr' # FIXME 
        output_prefix = 'output_prefix/'  # FIXME


    ## Get filepaths
    modality = 'T1w'
    keys = search_s3(input_bucket, input_prefix, modality, '.nii.gz')
    key = keys[0] # FIXME
    file_path = get_object(input_bucket, key, local_data_path)

    
    ## Processing 
    img = read_img(file_path)
    img = resize_vox(img, [3,3,3])
    img = rotate_img(img, 30)
    img = remove_slices(img, 40)


    ## Write output to output_bucket 
    write_to_s3(file_path, img, output_bucket, output_prefix)



    
# simple function to read files, maybe expand later
def read_img(filepath):
    """
    Read nifti file from path and return a nibabel image object
    Filepath needs to be full path to file
    """
    img=nib.load(filepath)
    return(img)



# function to resample image to new voxel resolution
# use nearest neighbour interpolation
def resize_vox(img, dimensions):
    """
    Use nibabel to resize image using nearest neighbour interpolation.
    Img needs to be a nibabel image object
    Dimensions can be single value or tuple
    """
    lr_img=resample_to_output(img, voxel_sizes=dimensions, order=3, mode='nearest')
    return(lr_img)



def rotate_img(img, rotation, affine=None, axis=0):
    """
    Rotate image along a single axis by adjusting affine matrix.
    Img needs to be a nibabel image object
    Rotation should be given in degrees
    Specify axis to apply rotation (default is 0)
    Affine can be used as optional argument to combine multiple rotations (untested)
    """
    # convert rotation from degrees to radians
    rot_radians=rotation*np.pi/180
    # don't use this with large enough rotations that it means changing orientation of the image?
    # rotate along first axis
    if axis==0:
        rotation_affine = np.array([
            [1, 0, 0, 0],
            [0, np.cos(rot_radians), -np.sin(rot_radians), 0],
            [0, np.sin(rot_radians), np.cos(rot_radians), 0],
            [0, 0, 0, 1]])
    # rotate along second axis
    elif axis==1:
        rotation_affine = np.array([
            [np.cos(rot_radians), 0, -np.sin(rot_radians), 0],
            [0, 1, 0, 0],
            [np.sin(rot_radians), 0, np.cos(rot_radians), 0],
            [0, 0, 0, 1]])
    # rotate along third axis
    elif axis==2:
        rotation_affine = np.array([
            [np.cos(rot_radians), -np.sin(rot_radians), 0, 0],
            [np.sin(rot_radians), np.cos(rot_radians), 0, 0],
            [0, 0, 1, 0],
            [0, 0, 0, 1]])
    # add way to include different affine to combine rotations before applying to image
    if affine is not None:
        # not tested this properly yet
        new_affine = rotation_affine.dot(affine)
    else:
        new_affine = img.affine.dot(rotation_affine)
    rot_img = nib.Nifti1Image(img.dataobj, new_affine, img.header)
    return(rot_img)



def remove_slices(img, percentage, axis=2, pattern='random'):
    """
    Img needs to be a nibabel image object
    Percentage can be either 0-100 or 0-1 to specify amount of slices to remove relative to image size
    Axis specifies along with dimension slices are specified
    Pattern can be random or set to interleaved to match certain image acquisitions
    """
    slices=img.shape[2]-1
    if percentage<1:
        to_remove=round(slices*percentage)
    else:
        to_remove=round(slices*(percentage/100))
    # match certain acquisitions so that missing slices fit the same pattern
    if pattern=='interleaved':
        pick = random.choice(['odd','even'])
        if pick=='odd':
            slices_odd = [i for i in range(slices) if i % 2 != 0]
            remove_slices=random.sample(slices_odd, to_remove)
        elif pick=='even':
            slices_even = [i for i in range(slices) if i % 2 == 0]
            remove_slices=random.sample(slices_even, to_remove)
    elif pattern=='random':
        remove_slices=random.sample(range(slices), to_remove)
    # copy image data and 'remove' slices
    new_img = img.get_fdata().copy()
    new_img[:,:,remove_slices] = 0
    new_img=nib.Nifti1Image(new_img, img.affine, img.header)
    return(new_img)



def search_s3(bucket, prefix, modality, search_string):
    client = boto3.client('s3', region_name="us-east-1")
    paginator = client.get_paginator('list_objects')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    keys = [] 
    for page in pages:
        contents = page['Contents']
        for c in contents:
            keys.append(c['Key'])
    if modality:
        keys = [key for key in keys if modality in key]
    if search_string : 
        keys = [key for key in keys if search_string in key]
    return keys



def get_object(bucket, key, local_data_path ):
    print(f"Downloading: {key} to {local_data_path}") 
    s3 = boto3.client('s3')
    os.makedirs(local_data_path, exist_ok=True)    
    filename = key.split('/')[-1]
    local_path = local_data_path + filename
    s3.download_file(bucket, key, local_path)
    return local_path



def write_to_s3(file_path, img, output_bucket, output_prefix) :         
    client = boto3.client('s3')
    nrg_path = nrg(file_path)
    nib.save(img, file_path) 
    client.upload_file(file_path, output_bucket, output_prefix + nrg_path)   
    


def nrg(file_path) : 
    img_name = file_path.split('/')[-1]
    remove_ext = img_name.split('.')[0]
    split = remove_ext.split('-')
    project = split[0]
    subject = split[1]
    date = split[2]
    modality = split[3]
    object = split[4]
    full_path = project + '/' + subject + '/' + date + '/' + modality + '/' + object + '/' + img_name 
    return full_path
    


if __name__ == "__main__":
    main()
