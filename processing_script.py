import random
import numpy as np
import nibabel as nib
from nibabel.processing import resample_to_output
import os
import boto3
import ast 


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
        input_prefix = 'ppmi_500_updated_cohort/curated/data/PPMI/137482/20220411/T1w/1575181/'
        output_bucket = 'tempamr' # FIXME
        output_prefix = 'output_prefix/'  # FIXME


    ## Get filepaths
    keys = search_s3(input_bucket, input_prefix, '.nii.gz')
    for key in keys :
        file_path = get_object(input_bucket, key, local_data_path)


    ## Read in image 
    img = read_img(file_path)
    
    
    ## Image Processing (EXAMPLES)
    # choose_function(img, 'remove_slices', file_path, output_bucket, output_prefix)
    # choose_function(img, 'resize_vox', file_path, output_bucket, output_prefix)
    choose_function(img, 'rotate_img', file_path, output_bucket, output_prefix)
    # choose_function(img, 'add_noise', file_path, output_bucket, output_prefix)




def choose_function(img, function_name, file_path, output_bucket, output_prefix) : 
    n_images = int(input("Enter the number of output images you'd like to create : "))

    if function_name == 'remove_slices' :
        percentage_min = float(input("Enter the minimum slice percentage you'd like to remove: "))
        percentage_max = float(input("Enter the max slice percentagage you'd like to remove : "))
        pattern = input("Enter the pattern you'd like to apply (or type 'None' to skip): ")
        axis = input("Enter the axis you'd like to apply (or type 'None' to skip): ")
        if pattern.lower() == 'none':
            pattern = None
        if axis.lower() == 'none':
            axis = None
            
        step_size = (percentage_max - percentage_min) / (n_images - 1)
        for i in range(n_images): 
            current_percentage = percentage_min + i * step_size
            out_img = remove_slices(img, current_percentage)
            output_name = '_' + function_name + '_' + 'percentage=' + str(current_percentage) + '_' + 'pattern=' + str(pattern) + '_' + 'axis=' + str(axis) + '.nii.gz'
            write_to_s3(file_path + output_name, out_img, output_bucket, output_prefix) 


    elif function_name == 'resize_vox' : 
        dimensions_min = input("Enter the minimum dimensions you'd like to resize to (Enter three numbers separated by a comma and enclosed in square brackets (e.g., [1, 1, 1])): ")
        dimensions_max = input("Enter the max dimensions  you'd like to resize to (Enter three numbers separated by a comma and enclosed in square brackets (e.g., [3, 3, 3]): ")
        dimensions_min = ast.literal_eval(dimensions_min)
        dimensions_min = np.array(dimensions_min)
        dimensions_max = ast.literal_eval(dimensions_max)
        dimensions_max = np.array(dimensions_max)
        step_size = (dimensions_max - dimensions_min) / (n_images - 1)
        for i in range(n_images) : 
            current_dimensions = dimensions_min + i * step_size
            out_img = resize_vox(img, current_dimensions)
            output_name = '_' + function_name + '_' + 'dimensions=' + str(current_dimensions) + '.nii.gz'
            write_to_s3(file_path + output_name, out_img, output_bucket, output_prefix) 

            
    elif function_name == 'rotate_img' : 
        rotate_min = float(input("Enter the minimum image rotation you'd like to apply: "))
        rotate_max = float(input("Enter the max image rotation you'd like to apply: "))
        affine = input("Enter the affine you'd like to apply (or type 'None' to skip): ")
        axis = input("Enter the axis you'd like to apply (or type 'None' to skip): ")
        if axis.lower() == 'none':
            axis = None
        if affine.lower() == 'none':
            affine = None
            
        step_size = (rotate_max - rotate_min) / (n_images - 1)
        for i in range(n_images) : 
            current_rotation = rotate_min + i * step_size
            out_img = rotate_img(img, current_rotation)
            output_name = '_' + function_name + '-' + 'rotation_' + str(current_rotation) + '-' + 'affine_' + str(affine) + '-' + 'axis_' + str(axis) + '.nii.gz'
            write_to_s3(file_path, out_img, output_bucket, output_prefix, output_name) 


    elif function_name == 'add_noise' : 
        factor_min = float(input("Enter the minimum factor you'd like to apply: "))
        factor_max = float(input("Enter the max factor you'd like to apply: "))  
        step_size = (factor_max - factor_min) / (n_images - 1)
        for i in range(n_images) : 
            current_factor = factor_min + i * step_size
            out_img = rotate_img(img, current_factor)
            output_name = '_' + function_name + '_' + 'factor=' + str(current_factor) + '.nii.gz'
            write_to_s3(file_path, out_img, output_bucket, output_prefix, output_name) 










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



def add_noise(img, factor=2):
    """"
    Img needs to be a nibabel image object
    Factor is used to scale up and down the amount of Gaussian noise added to the image
    """
    # copy data
    noise_img = img.get_fdata().copy()
    # grab mean / sd to scale random noise
    img_mean=np.mean(noise_img)
    img_sd=np.std(noise_img)
    # just add Gaussian noise for now using the specified scaling params
    # only add positive values to keep minimum value of image positive (like a half-normal distribution)
    noise =  abs(np.random.normal(loc=img_mean, scale=img_sd, size=img.shape))
    # increase or decrease amount of noise using scaling factor
    noise *= factor
    # use min for clipping or set to 0?
    noise_img = np.clip(noise_img + noise, np.min(noise_img), np.max(noise_img))
    noisy_img = nib.Nifti1Image(noise_img, img.affine, img.header)
    return(noisy_img)



def search_s3(bucket, prefix, search_string):
    client = boto3.client('s3', region_name="us-east-1")
    paginator = client.get_paginator('list_objects')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    keys = []
    for page in pages:
        contents = page['Contents']
        for c in contents:
            keys.append(c['Key'])
    if search_string:
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



def write_to_s3(file_path, img, output_bucket, output_prefix, output_name) :
    client = boto3.client('s3')
    nrg_path = nrg(file_path, output_name)
    nib.save(img, file_path)
    client.upload_file(file_path, output_bucket, output_prefix + nrg_path)



def nrg(file_path, output_name) :
    img_name = file_path.split('/')[-1]
    remove_ext = img_name.split('.')[0]
    split = remove_ext.split('-')
    project = split[0]
    subject = split[1]
    date = split[2]
    modality = split[3]
    object = split[4]
    full_path = project + '/' + subject + '/' + date + '/' + modality + '/' + object + '/' + remove_ext + output_name + '.nii.gz'
    return full_path



if __name__ == "__main__":
    main()
