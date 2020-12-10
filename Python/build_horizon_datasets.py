# -*- coding: utf-8 -*-
"""
Created on Thu Dec 10 12:16:27 2020

@author: AZ7
"""

# %% Imports
import arcpy
import re

# %% Functions

def build_snapshot_dataset(inputs_path,
                           save_path):
    '''
    copies a dataset reflecting "snapshot" conditions to a new location
    (ideally, one containing the "snapshot" horizon results)

    Parameters
    ----------
    inputs_path : Path
        file path to the dataset reflecting the snapshot conditinons
    save_path : Path
        file path to the save location for the snapshot dataset. If the file
        already exists, it will be deleted before a new version is created

    Returns
    -------
    the `inputs_path` dataset will be copied to `save_path`. the save path is
    returned
    '''
    
    print("")
    
    # 1. Check if the save_path exists. If it does, delete it -- this will
    # allow us to copy the updated file from inputs_path
    
    if arcpy.Exists(save_path):
        print("Deleting old version of this snapshot dataset")
        arcpy.Delete_management(save_path)
        
    # 2. Copy the inputs to the save_path
    print("Copying inputs to location for snapshot data")
    
    arcpy.CopyFeatures_management(in_features = inputs_path,
                                  out_features = save_path)
    
    # Then we're done!
    print("Done!")
    print("Snapshot data saved to: " + save_path)
    print("")
    return(save_path)

# ----------------------------------------------------------------------------

def build_trend_dataset(inputs_paths,
                        save_path,
                        years=None):
    '''
    merge the same data from multiple years into a single trend dataset
    (ideally saved to a location containing the "trend" horizon results)

    Parameters
    ----------
    inputs_paths : Path, or list of Paths
        Either a singular path with a fixed-string wild card for year
        (representing a consistent file path/naming structure for that varies
        only on year), or a list of paths to feature classes you wish to
        merge into a singular trend dataset. See notes for guidance
    save_path : Path
        file path to the save location for the trend dataset. If the file
        already exists, it will be deleted before a new version is created
    years : list of ints, optional
        If `inputs_paths` takes the format of the fixed-string wild card for 
        year, the years for which you want to merge feature classes
    
    Notes
    -----
    `inputs_paths` offers two ways of specifying the files to be merged into
    a trend dataset
        1. The "fixed-string" method can be used when all the files you want
        to merge have the same path/name, except for varying on year (e.g.
        you want to merge files from 2010-2011, and all files are saved to
        Data.gdb/Features_YYYY). In this case, replace the location of the
        year in the file path/name with {Year} (so, for the example above,
        `inputs_paths = 'Data.gdb/Features_{Year}'). If this method is used
        for `inputs_paths`, `years` MUST be provided, and span the years of the
        data you want to merge (so, for our example, `years = [2010, 2011]`)
        2. The "list" method can be used to explicitly name all the files you
        want to merge, which is helpful when they do not follow a consistent
        naming pattern. In our example from (1), this method would used as
        `inputs_paths = ['Data.gdb/Features_2010', 'Data.gdb/Features_2011']`.
        If this method is used, years should be left as the default `None`; if
        provided, it will be ignored
    
    Returns
    -------
    the datasets specified by `inputs_paths` [and potentially `years`] are
    merged and written to `save_path`. the save path is returned
    '''
    
    print("")
    
    # 1. Verify the file pathing in inputs_paths. 
    # If it's a list, just make sure it's > length 1. 
    # If it's a string, make sure it has {Year} in it, and make sure years
    # is provided with length > 1. Then replace {Year} with the years  
    
    if type(inputs_paths) is list:
        # Check for list length > 1
        if len(inputs_paths) <= 1:
            raise ValueError("a list for 'inputs_paths' must have length >= 2")
    else:
        # It's a string -- check if {Year} is present
        if not bool(re.search("{Year}", inputs_paths)):
            raise ValueError("'inputs_paths' is a single string but does not contain '{Year}'")
        else:
            # {Year} is prsent -- check if years are provided 
            if years is None:
                raise ValueError("'inputs_paths' is a single string but 'years' is not given")
            else:
                # years are provided -- check for list length > 1
                if len(years) <= 1:
                    raise ValueError("the list of years must have length >= 2")
            
            # All the checks are passed! Do the replacement
            print("Formatting the input paths from fixed-string wild card")  
            ip = []
            for yr in years:
                ip.append(inputs_paths.replace("{Year}", str(yr)))
            inputs_paths = ip
                
    
    # 2. Check if the save_path exists. If it does, delete it -- this will
    # allow us to copy the updated file from inputs_paths
    
    if arcpy.Exists(save_path):
        print("Deleting old version of this trend dataset")
        arcpy.Delete_management(save_path)
        
    # 3. Conduct a merge
    print("Merging inputs and saving to location for trend data")
    
    arcpy.Merge_management(inputs = inputs_paths,
                           output = save_path)
    
    # Then we're done!
    print("Done!")
    print("Trend data saved to: " + save_path)
    print("")
    return(save_path)
        
        