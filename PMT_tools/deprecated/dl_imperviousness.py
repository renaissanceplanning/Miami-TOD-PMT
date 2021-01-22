# -*- coding: utf-8 -*-

# %% Imports

import requests
import os

# %% Functions

def download_imperviousness(year,
                            save_directory):
    """
    Download and save the raw zipfile(s) of NLCD imperviousness raster(s) for
    the lower 48 in specified years
    
    Parameters
    ----------
    year: int
        year for which to pull NLCD imperviousness rasters. see Note for
        possible years
    save_directory: directory
        location to save zip of NLCD imperviousness rasters
    
    Notes
    -----
    `year` can only take the values 2001, 2006, 2011, and/or 2016, as NLCD
    imperviousness is only available for these four years
    
    Returns
    -------
    File will be downloaded and saved to `save_directory` with the file name
    "Impervious_{Year}.zip"
    
    Raises
    ------
    TypeError
        If `year` is not an int
        If `save_directory` is not a string
    ValueError
        If `year` is not 2001, 2006, 2011, 2016
        If `save_directory` does not exist and cannot be created
    
    """
    
    # Validation -------------------------------------------------------------
    
    # year: 
    # 1. must be list, 
    # 2. must be all integers
    # 3. can only include 2001, 2006, 2011, or 2016
    if type(year) is not int:
        raise TypeError("'year' must be an int")
    if year not in [2001, 2006, 2011, 2016]:
        raise ValueError(''.join(["'year' is invalid; NLCD imperviousness "
                                  "is only available for 2001, 2006, 2011, ",
                                  "and 2016"]))
    
    # save directory: 
    # 1. must be a string
    # 2. must be an existing or creatable directory
    if type(save_directory) is not str:
        raise TypeError("'save_directory' must be a string")
    if not os.path.exists(save_directory):
        try: 
            os.mkdir(save_directory)
        except:
            raise ValueError(''.join(["'save_directory' is not a valid ",
                                      "directory (or otherwise cannot be ",
                                      "created)"]))
    
    # Download/saving --------------------------------------------------------
 
    print("Prepping for download...")
    # 1. set up NLCD imperviousness download URL using 'year'    
    imp_url = ''.join(["https://s3-us-west-2.amazonaws.com/mrlc/NLCD_",
                       str(year),
                       "_Impervious_L48_20190405.zip"])
    
    # 2. Set up a save path
    save_path = os.path.join(save_directory,
                             ''.join(["Imperviousness_", str(year), ".zip"]))
    
    print("Requesting download...")
    req = requests.get(imp_url)
    
    print("Saving download...")
    with open(save_path, 'wb') as f:
        f.write(req.content)
    print("-- saved to: " + save_path)
    
    # Done -------------------------------------------------------------------
    
    print("Done!\n")
    return(save_path)

# %% Main
if __name__ == "__main__":
    # 1. Inputs
    year = 2016
    #save_directory = ?
    
    # 2. Function
    # download_imperviousness(year = year,
    #                         save_directory = save_directory)
    
        
    