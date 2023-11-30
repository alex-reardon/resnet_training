
import random
import numpy as np
import nibabel as nib
from nibabel.processing import resample_to_output


# To do:
# Function to save outputs with sensible naming convention
# Expand functions so they can work with fMRI and DWI data
# Function to add various types of noise to image
# Function to add random lesions (black spots) or signal drop-out


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
