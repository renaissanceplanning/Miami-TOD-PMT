# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 09:49:14 2020

@author: AZ7
"""

# %% Imports

import arcpy
import os
import zipfile
import re

# %% Functions

def prep_imperviousness(impervious_zip_path,
                        clip_geometry_path,
                        save_directory,
                        transform_crs=None):
    '''
    Clean a USGS impervious surface raster by:
        1. Clipping to the bounding box of a study area
        2. Transforming the clipped raster to a desired CRS
    
    Parameters 
    ----------
    impervious_zip_path: Path 
        .zip folder of downloaded imperviousness raster (see the
        `dl_imperviousness` function)
    clip_geometry_path: Path
        path of study area polygon(s) whose bounding box will be used to clip 
        the raster
    save_directory: Path
        save location for the clipped and transformed raster
    transform_epsg: Anything accepted by arcpy.SpatialReference(), optional
        Identifier of spatial reference to which to transform the clipped
        raster; can be any form accepted by arcpy.SpatialReference(), see
        https://pro.arcgis.com/en/pro-app/latest/arcpy/classes/spatialreference.htm
        Default `None`, CRS of shape from `clip_geometry_path` will be used
        for the transform
    
    Returns
    -------
    File will be clipped, transformed, and saved to the save directory; the
    save path will be returned upon completion
    '''

    # 1. Unzip the raw raster
    # -----------------------
    print("")
    print("1. Unzipping the imperviousness raster")
    
    # Extract the raster from the zip
    # Thanks: https://stackoverflow.com/questions/3451111/unzipping-files-in-python
    print("1.1 extracting the raster (please be patient, may take 10-20 minutes)")
    
    with zipfile.ZipFile(impervious_zip_path, 'r') as z:
        z.extractall(save_directory)
       
    # Get the name of the raster from within the zip (the .img file)
    print("1.2 identifying the raster file itself")
    
    zipped_files = zipfile.ZipFile(impervious_zip_path, 'r').namelist()
    ras_path = [f for f in zipped_files if bool(re.search(".img$", f))][0]
    unzipped_file = os.path.join(save_directory,
                                 ras_path) 
    
    # 2. Clip the unzipped raster
    # ---------------------------
    print("")
    print("2. Clipping the unzipped raster to the clip geometry")
    
    # Set a file name to save to
    print("2.1 setting a file name for the clipped raster")
    
    clipped_file = os.path.join(save_directory,
                                "Clipped.img")
    
    # Get the raster crs -- if the polygon is not in the CRS of the raster,
    # we'll need to transform prior to doing the clip
    print("2.2 identifying the raster CRS")
    
    raster_crs = arcpy.Describe(unzipped_file).spatialReference
    raster_crs_string = raster_crs.name
    
    # Grab clip geometry CRS to check if it's the same as the raster
    print("2.3 checking if a transformation of the clip geometry is necessary")
    
    clip_crs = arcpy.Describe(clip_geometry_path).spatialReference
    clip_crs_string = clip_crs.name
    
    # Transform the clip geometry if necessary
    if clip_crs_string != raster_crs_string:
        print("2.3.1 transforming the clip geometry to match the raster CRS")
        
        project_file = os.path.join(save_directory,
                                    "Project.shp")
        arcpy.Project_management(in_dataset = clip_geometry_path,
                                 out_dataset = project_file,
                                 out_coor_system = raster_crs)
        clip_geometry_path = project_file
    
    # Grab the bounding box of the clipping file
    print("2.4 grabbing the bounding box for the raster clip")
    
    bbox = arcpy.Describe(clip_geometry_path).Extent
    
    # Clip raster by the extent
    print("2.5 clipping the unzipped raster to the clip geometry bounds")
    
    arcpy.Clip_management(in_raster = unzipped_file,
                          rectangle = "",
                          out_raster = clipped_file,
                          in_template_dataset = bbox.polygon,
                          clipping_geometry = "ClippingGeometry")
    
    # Delete the unzipped file and the project file (if it exists)
    print("2.6 deleting intermediates")
    
    for uzf in zipped_files:
        ftd = os.path.join(save_directory,
                           uzf)
        arcpy.Delete_management(ftd)
        
    if arcpy.Exists(project_file):
        arcpy.Delete_management(project_file)
    
    # 3. Transform the clipped raster
    # -------------------------------
    print("")
    print("3. Transforming the clipped raster to the transform EPSG")
    
    # Set a path to save the output raster
    print("3.1 setting a name for the transformed raster")
    
    transformed_name = os.path.basename(impervious_zip_path)
    transformed_name = transformed_name.replace(".zip", "_Clipped.img")
    transformed_file = os.path.join(save_directory,
                                    transformed_name)
    
    # Set the spatial reference for the epsg
    print("3.2 defining the CRS for the transform")
    
    if transform_epsg is None:
        # Take the transform crs as the clip geometry crs
        transform_sr = clip_crs
    else:
        # Set the crs from the provided
        transform_sr = arcpy.SpatialReference(transform_epsg)
    
    # Reproject the raster (if the transform is unique from the raster itself)
    
    if transform_sr.name != raster_crs_string:
        # Reprojection is required
        print("3.3 reprojecting the clipped raster to the transform CRS")
        
        arcpy.ProjectRaster_management(in_raster = clipped_file,
                                       out_raster = transformed_file,
                                       out_coor_system = transform_sr,
                                       resampling_type = "NEAREST")
    else:
        # The clip file is our final file, so rename if we're not transforming
        print("3.3 requested transform is same as raster CRS; renaming clipped file")
        
        arcpy.Rename_management(in_file = clipped_file,
                                out_file = transformed_file)
    
    # Delete the clipped file
    if arcpy.Exists(clipped_file):
        print("3.4 deleting intermediates")
        arcpy.Delete_management(clipped_file)
 
    # 4. Done
    # -------
    print("")
    print("Done!")
    print("Cleaned imperviousness raster saved to: " + transformed_file)
    print("")
    return(transformed_file)
        
# %% Main

if __name__ == "__main__":
    # Set inputs
    impervious_zip_path = r"K:\Projects\MiamiDade\PMT\Data\Raw\Imperviousness\Imperviousness_2016.zip"
    clip_geometry_path = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\parcels.gdb\Miami_2019"
    save_directory = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\Imperviousness"
    transform_epsg = None
    
    # Run the function
    prep_imperviousness(impervious_zip_path = impervious_zip_path,
                        clip_geometry_path = clip_geometry_path,
                        save_directory = save_directory,
                        transform_epsg = transform_epsg)
    
    
    

    
    
    
    
    
    
    
    
    
    
    
    
    