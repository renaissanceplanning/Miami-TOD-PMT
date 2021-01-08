# -*- coding: utf-8 -*-

source_file_directory = "C:/Users/rpg_f.FL3/Downloads/PMT_Temp"
year = 2016
transform_epsg = None
zone_geometries_path = "C:/Users/rpg_f.FL3/Downloads/PMT_Temp/Miami_2019.shp"
raw_save_directory = "C:/Users/rpg_f.FL3/Downloads/ITry"
cleaned_save_directory = "C:/Users/rpg_f.FL3/Downloads/ITry"
summarized_save_directory = "C:/Users/rpg_f.FL3/Downloads/ITry"

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

def process_imperviousness(source_file_directory,
                           year,
                           zone_geometries_path,
                           raw_save_directory,
                           cleaned_save_directory,
                           transform_epsg=None,
                           summarized_save_directory=None):
    """
    fully process percent impervious by zone geometry, including
        1. Download of raw impervious surface raster for the US lower 48
        2. Cleaning of raw raster, including clipping and transformation
        3. Summary of percent impervious at the zone level
    This is a wrapper function for `dl_imperviousness`, `prep_imperviousness`,
    and `analyze_imperviousness`
    
    Parameters
    ----------------
    source_file_directory: Path
        directory including the functions `dl_imperviousness`, 
        `prep_imperviousness`, and `analyze_imperviousness`
    year: int
        year for which to pull NLCD imperviousness raster
    transform_epsg: Int
        EPSG for desired raster transform
        Default `None`, CRS of shape from `zone_geometries_path` will be used
        for the transform
    zone_geometries_path: Path
        .shp file of geometries in which imperviousness will be summarized.
        Its bounding box will also be used to clip the raster
    raw_save_directory: Path
        location to save zip of raw NLCD imperviousness rasters (whole raster
        for the lower 48 of the US)
    cleaned_save_directory: Path
        save location for the clipped and transformed raster
    sumarized_save_directory: Path
        save location for zonal imperviousness summaries
        Default `None`, no save performed
    
    Returns
    --------------
    geopandas GeoDataFrame of `zone_geometries`, with percent impervious 
    added as the column "PerImp" (at the last index). In addition, 
        - the .zip of the impervious surface raster for the US lower 48 will 
        be saved to `raw_save_directory`
        - the clipped and transformed subset of the raster will be saved to 
        `cleaned_save_directory`
        - the zonal summaries of percent impervious will be saved to
        `summarized_save_directory` [if it is provided]
        
    Raises
    ---------------
    TypeError
        If `source_file_directory` is not a string
        If `year` is not an int
        If `transform_epsg` is not an int
        If `zone_geometries_path` is not a string
        If `raw_save_directory` is not a string
        If `cleaned_save_directory` is not a string
        If `summarized_save_directory` is not a string (or None)
    FileNotFoundError:
        If `source_file_directory` is not a valid directory
        If `zone_geometries_path` is not a valid path
    ValueError
        If `year` is not 2001, 2006, 2011, 2016
        If `raw_save_directory` does not exist and cannot be created
        If `cleaned_save_directory` does not exist and cannot be created
        If `summarized_save_directory` does not exist and cannot be created
    """
    
    # Validation (imports are handled by source scripts) ---------------------
    
    import os
    
    # source_file_location:
    # 1. must be string
    # 2. must exist
    if type(source_file_directory) is not str:
        raise TypeError(''.join(["'source_file_directory' must be a string ",
                                 "pointing to directory containing the ",
                                 "three imperviousness functions"]))
    if not os.path.exists(zone_geometries_path):
        raise FileNotFoundError("'source_file_directory' is not a valid directory")
    
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
    
    # transform_epsg: 
    # 1. must be an int
    if transform_epsg is not None:
        if type(transform_epsg) is not int:
            raise TypeError("'transform_epsg' must be an int")
            
    # zone_geometry_path: 
    # 1. must be a string
    # 2. must be an existing file
    # 3. must be .shp
    if type(zone_geometries_path) is not str:
        raise TypeError(''.join(["'zone_geometries_path' must be a string ",
                                 "pointing to a .shp of zone geometries"]))
    if not os.path.exists(zone_geometries_path):
        raise FileNotFoundError("'zone_geometries_path' is not a valid path")
    if zone_geometries_path.split(".")[-1] != "shp":
        raise ValueError("'zone_geometries_path' does not point to a shapefile")
    
    # raw save directory: 
    # 1. must be a string
    # 2. must be an existing or creatable directory
    if type(raw_save_directory) is not str:
        raise TypeError("'raw_save_directory' must be a string")
    if not os.path.exists(raw_save_directory):
        try: 
            os.mkdir(raw_save_directory)
        except:
            raise ValueError(''.join(["'raw_save_directory' is not a valid ",
                                      "directory (or otherwise cannot be ",
                                      "created)"]))
    
    # cleaned save directory: 
    # 1. must be a string
    # 2. must be an existing or creatable directory
    if type(cleaned_save_directory) is not str:
        raise TypeError("'cleaned_save_directory' must be a string")
    if not os.path.exists(cleaned_save_directory):
        try: 
            os.mkdir(cleaned_save_directory)
        except:
            raise ValueError(''.join(["'cleaned_save_directory' is not a ",
                                      "valid directory (or otherwise cannot ",
                                      "be created)"]))
    
    # summarized save directory: 
    # 1. must be a string
    # 2. must be an existing or creatable directory
    if summarized_save_directory is not None:
        if type(summarized_save_directory) is not str:
            raise TypeError("'summarized_save_directory' must be a string")
        if not os.path.exists(summarized_save_directory):
            try: 
                os.mkdir(summarized_save_directory)
            except:
                raise ValueError(''.join(["'summarized_save_directory' is ",
                                          "not a valid directory (or ",
                                          "otherwise cannot be created)"]))
    
    # Functions --------------------------------------------------------------
    
    # File locations for the functions we need
    dl = os.path.join(source_file_directory, "deprecated/dl_imperviousness.py")
    prep = os.path.join(source_file_directory, "prep_imperviousness.py") 
    analyze = os.path.join(source_file_directory, "analyze_imperviousness.py") 
    
    # Sourcing functions
    exec(open(dl).read())
    exec(open(prep).read())
    exec(open(analyze).read())
    
    # Download ---------------------------------------------------------------
    
    print("(1/3) Downloading")
    
    # Download
    download_imperviousness(year = year,
                            save_directory = raw_save_directory)
    
    # Prep -------------------------------------------------------------------
    
    print("(2/3) Prepping")
    
    # Set up name for impervious_zip_path
    izp = os.path.join(raw_save_directory,
                       ''.join(["Imperviousness_", str(year), ".zip"]))
    
    # Prep    
    prep_imperviousness(impervious_zip_path = izp,
                        clip_geometry_path = zone_geometries_path,
                        transform_epsg = transform_epsg,
                        save_directory = cleaned_save_directory)
    
    # Analyzed ---------------------------------------------------------------
    
    print("(3/3) Analyzing")
    
    # Set up impervious_path
    bp = os.path.basename(izp)
    ip = os.path.join(cleaned_save_directory,
                      '_'.join([bp.split(".zip")[0], "Clipped.tif"]))
    
    # Analyze    
    gdf = analyze_imperviousness(impervious_path = ip,
                                 zone_geometries_path = zone_geometries_path,
                                 save_directory = summarized_save_directory)
    
    # Done -------------------------------------------------------------------
    
    print("Done!\n")
    return gdf
    
    
    
    
    
    
    
    
        