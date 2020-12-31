# -*- coding: utf-8 -*-

#clip_geometry_path = r"C:\Users\rpg_f.FL3\Downloads\Impervious\MDC.shp"
#impervious_zip_path = r"C:\Users\rpg_f.FL3\Downloads\Impervious\Imperviousness_2016.zip"
#transform_epsg = None
#save_directory = r"C:\Users\rpg_f.FL3\Downloads\Impervious"

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

def prep_imperviousness(impervious_zip_path,
                        clip_geometry_path,
                        save_directory,
                        transform_epsg=None):
    """
    Clean a USGS impervious surface raster by:
        1. Clipping to the bounding box of a study area
        2. Transforming the clipped raster to a desired CRS
    
    Parameters 
    --------------
    impervious_zip_path: Path 
        .zip folder of downloaded imperviousness raster (see `dl_imperviousness`)
    clip_geometry_path: Path
        .shp file of study area whose bounding box will be used to clip the raster
    transform_epsg: Int
        EPSG for desired raster transform
        Default `None`, CRS of shape from `clip_geometry_path` will be used
        for the transform
    save_directory: Path
        save location for the clipped and transformed raster
    
    Returns
    ---------------
    No return; file will be clipped, transformed, and saved, and the save path
    will be printed at the completion of saving
    
    Raises
    ---------------
    TypeError
        If `impervious_zip_path` is not a string
        If `clip_geometry_path` is not a string
        If `transform_epsg` is not an int (or None)
        If `save_directory` is not a string
    FileNotFoundError:
        If `impervious_zip_path` is not a valid path
        If `clip_geometry_path` is not a valid path
    ValueError
        If `impervious_zip_path` does not point to a .zip
        If `clip_geometry_path` does not point to a .shp
        If `save_directory` does not exist and cannot be created
    """
    
    # Imports ----------------------------------------------------------------
    
    import os
    import zipfile
    import re
    import rasterio
    from rasterio.warp import calculate_default_transform, reproject, Resampling
    import geopandas as gpd
    
    # Validation of inputs ---------------------------------------------------
    
    # impervious_zip_path: 
    # 1. must be a string
    # 2. must be an existing file
    # 3. must be .zip
    if type(impervious_zip_path) is not str:
        raise TypeError(''.join(["'impervious_zip_path' must be a string ",
                                 "pointing to a .zip of an imperviousness ",
                                 "raster"]))
    if not os.path.exists(impervious_zip_path):
        raise FileNotFoundError("'impervious_zip_path' is not a valid path")
    if impervious_zip_path.split(".")[-1] != "zip":
        raise ValueError("'impervious_zip_path' does not point to a .zip")
    
    # clip_geometry_path: 
    # 1. must be a string
    # 2. must be an existing file
    # 3. must be .shp
    if type(clip_geometry_path) is not str:
        raise TypeError(''.join(["'clip_geometry_path' must be a string ",
                                 "pointing to a .shp of a study area"]))
    if not os.path.exists(clip_geometry_path):
        raise FileNotFoundError("'clip_geometry_path' is not a valid path")
    if clip_geometry_path.split(".")[-1] != "shp":
        raise ValueError("'clip_geometry_path' does not point to a shapefile")
    
    # transform_epsg: 
    # 1. must be an int
    if transform_epsg is not None:
        if type(transform_epsg) is not int:
            raise TypeError("'transform_epsg' must be an int")
    
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
    
    # Read -------------------------------------------------------------------
    
    print("Reading study area...")
    # We need to:
    # 1. Read the study area file
    # 2. If a transform EPSG is given, transform to this CRS
    # 3. Extract the bbox and CRS
    geom = gpd.read_file(clip_geometry_path)
    if transform_epsg is not None:
        geom = geom.to_crs(epsg = transform_epsg)
    write_crs = geom.crs

    # Clip (more specifically, read windowed raster) -------------------------
    
    print("Setting up raster clipping geometry from study area bounds...")    
    # Get the name of the raster from within the zip (the .img file)
    zipped_files = zipfile.ZipFile(impervious_zip_path, 'r').namelist()
    ras_path = [f for f in zipped_files if bool(re.search(".img$", f))][0]
    
    # Obtain the raster CRS
    zip_dir = open(impervious_zip_path, 'rb')
    with rasterio.io.ZipMemoryFile(zip_dir) as zmf:
        with zmf.open(ras_path) as rp:
            imp_crs = rp.crs
    
    # Now we can do a windowed read
    # 1. Transform geom to match the imperviousness CRS
    # 2. Take the bbox of the study area
    # 3. Set up a rasterio window from these coordinates
    geom = geom.to_crs(imp_crs)
    transformed_bbox = geom.total_bounds
    zip_dir = open(impervious_zip_path, 'rb')
    with rasterio.io.ZipMemoryFile(zip_dir) as zmf:
        with zmf.open(ras_path) as rp:
            cw = rp.window(*transformed_bbox)
    
    print("Reading imperviousness raster within clipping geometry...")
    # Read the raster within the window
    zip_dir = open(impervious_zip_path, 'rb')
    with rasterio.io.ZipMemoryFile(zip_dir) as zmf:
        with zmf.open(ras_path) as rp:        
            ras = rp.read(1, window = cw)
    
    # Transform (more specifically, save, then transform, then resave) -------
    
    print("Saving copy of clipped raster in original CRS...")
    
    # Extract window transform
    zip_dir = open(impervious_zip_path, 'rb')
    with rasterio.io.ZipMemoryFile(zip_dir) as zmf:
        with zmf.open(ras_path) as rp:    
            ras_transform = rp.window_transform(cw)
    
    # Set up save path for the copy
    bp = os.path.basename(impervious_zip_path)
    save_path_copy = os.path.join(save_directory,
                                  '_'.join([bp.split(".zip")[0], 
                                            "COPY.tif"]))
    
    # Save copy
    with rasterio.open(save_path_copy, 
                       'w', 
                       driver = 'GTiff',
                       height = ras.shape[0], 
                       width = ras.shape[1],
                       count = 1, 
                       dtype = str(ras.dtype),
                       crs = imp_crs,
                       transform = ras_transform) as wp:
        wp.write(ras, 1)
        
    print("Reprojecting copy to desired CRS (and re-saving)...")
    
    # Set up save path for the reprojection
    save_path_true = os.path.join(save_directory,
                                  '_'.join([bp.split(".zip")[0], 
                                            "Clipped.tif"]))
    
    # Open and get info, then save a reprojected version
    with rasterio.open(save_path_copy) as src:
        transform, width, height = calculate_default_transform(
            src.crs, write_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update({'crs': write_crs,
                       'transform': transform,
                       'width': width,
                       'height': height})

        with rasterio.open(save_path_true, 'w', **kwargs) as dst:
            reproject(source = rasterio.band(src, 1),
                      destination = rasterio.band(dst, 1),
                      src_transform = src.transform,
                      src_crs = src.crs,
                      dst_transform = transform,
                      dst_crs = write_crs,
                      resampling = Resampling.nearest)
    
    # Deleting the copy file
    os.remove(save_path_copy)
    
    # Confirm save
    print("-- saved to: " + save_path_true) 

    # Done -------------------------------------------------------------------
    
    print("Done!\n")
    return None
    
    
    
    
    
    
    
    
    
    
    
    
    