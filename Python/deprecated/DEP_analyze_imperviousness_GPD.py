# -*- coding: utf-8 -*-

#impervious_path = r"C:\Users\rpg_f.FL3\Downloads\Impervious\Imperviousness_2016_Clipped.tif"
#zone_geometries_path = r"C:\Users\rpg_f.FL3\Downloads\Impervious\MDC.shp"
#save_directory = r"C:\Users\rpg_f.FL3\Downloads\Impervious"

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

def analyze_imperviousness(impervious_path,
                           zone_geometries_path,
                           save_directory=None):
    """
    Summarize percent impervious surface cover in each of a collection of zones
    
    Parameters 
    --------------
    impervious_path: Path 
        .tif of clipped, transformed imperviousness raster (see 
        `prep_imperviousness`)
    zone_geometries_path: Path
        .shp file of geometries in which imperviousness will be summarized
    save_directory: Path
        save location for zonal imperviousness summaries
        Default `None`, no save performed
    
    Returns
    ---------------
    geopandas GeoDataFrame of `zone_geometries`, with percent impervious 
    added as the column "PerImp" (at the last index)
    
    Raises
    ---------------
    TypeError
        If `impervious_path` is not a string
        If `zone_geometries_path` is not a string
        If `save_directory` is not a string (or None)
    FileNotFoundError:
        If `impervious_path` is not a valid path
        If `zone_geometries_path` is not a valid path
    ValueError
        If `impervious_zip_path` does not point to a .zip
        If `clip_geometry_path` does not point to a .shp
        If `save_directory` does not exist and cannot be created
    """
    
    # Imports ----------------------------------------------------------------
    
    import os
    import rasterio
    import geopandas as gpd
    from rasterstats import zonal_stats
    
    # Validation of inputs ---------------------------------------------------
    
    # impervious_zip_path: 
    # 1. must be a string
    # 2. must be an existing file
    # 3. must be .zip
    if type(impervious_path) is not str:
        raise TypeError(''.join(["'impervious_path' must be a string ",
                                 "pointing to a .tif of an imperviousness ",
                                 "raster"]))
    if not os.path.exists(impervious_path):
        raise FileNotFoundError("'impervious_path' is not a valid path")
    if impervious_path.split(".")[-1] != "tif":
        raise ValueError("'impervious_path' does not point to a .tif")
    
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
        
    # save directory: 
    # 1. must be a string
    # 2. must be an existing or creatable directory
    if save_directory is not None:
        if type(save_directory) is not str:
            raise TypeError("'save_directory' must be a string")
        if not os.path.exists(save_directory):
            try: 
                os.mkdir(save_directory)
            except:
                raise ValueError(''.join(["'save_directory' is not a valid ",
                                          "directory (or otherwise cannot be ",
                                          "created)"]))
    
    # Read -------------------------------------------------------------------
    
    print("Reading impervious raster...")
    
    # Read the raster, and also get it's CRS and affine transform
    with rasterio.open(impervious_path, "r") as rp:
        A = rp.transform
        raster_crs = rp.crs
        ras = rp.read(1)
        
    # Reset nodata values (> 100, these are non-modeled areas in the
    # impervious surface raster) to 0
    ras[ras > 100] = 0
        
    print("Reading zone geometries...")
    
    # Read zone geometries
    zg = gpd.read_file(zone_geometries_path)
    
    # If zone CRS != raster CRS, transform zone to match raster
    if not zg.crs.equals(raster_crs):
        print("-- transforming zone geometries to match CRS of raster...")
        zg = zg.to_crs(raster_crs)
    
    # Zonal summary ----------------------------------------------------------
    
    print("Summarizing imperviousness to zones...")
    
    # Use zonal_stats to pull
    zone_summary = zonal_stats(vectors = zg,
                               raster = ras,
                               affine = A,
                               stats = "mean",
                               nodata = 0)
    
    # Turn to list to add to dataframe
    zone_summary = [zone["mean"] for zone in zone_summary]
    
    # Add back to dataframe
    idx = len(zg.columns) - 1
    zg.insert(idx, "PerImp", zone_summary)
    
    # Save -------------------------------------------------------------------
    
    if save_directory is not None:
        print("Saving...")
        
        # Setting up file name
        ras_bp = os.path.basename(impervious_path)
        geo_bp = os.path.basename(zone_geometries_path)
        save_path = os.path.join(save_directory,
                                 ''.join([ras_bp, "_", geo_bp, ".shp"]))
        
        # Saving
        zg.to_file(save_path)
        print("-- saved to: " + save_path)
    
    # Done -------------------------------------------------------------------
    
    print("Done!\n")
    return zg    
    
    
    
    
    
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    