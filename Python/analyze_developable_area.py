# -*- coding: utf-8 -*-
"""
Created on Mon Sep 21 12:49:32 2020

@author: rpg_f
"""

import sys
from rasterio.features import rasterize
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry.polygon import Polygon
import math
import os

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

# parcel_polygons_path: string of path to parcel polygons shapefile.
# building_polygons_path: string of path to building polygons shapefile.
# cell_size: float or int of desired cell size (in units of parcel CRS) for
#            rasterized version of parcels.
# save_directory: string of path to desired save directory
#                 If None, no save will be completed

# Testing specs
parcel_polygons_path = "C:/Users/rpg_f.FL3/Downloads/Ex_Parcels.shp"
building_polygons_path = "C:/Users/rpg_f.FL3/Downloads/Ex_Buildings.shp"
cell_size = 1
save_directory = None
# Use this for testing after the differencing step (will need to load rasterio first)
# raster_path = "C:/Users/rpg_f.FL3/Downloads/Ex_Raster.tif"

def create_developable_area_table(parcel_polygons_path,
                                  building_polygons_path,
                                  cell_size,
                                  min_width = None,
                                  min_height = None,
                                  min_area = None,
                                  save_directory = None):
    
    # Validation of inputs ---------------------------------------------------
    
    # parcel_polygons_path: must be a string, valid path, and be a .shp
    if type(parcel_polygons_path) is not str:
        sys.exit("'parcel_polygons_path' must be a string pointing to a shapefile of parcel polygons")
    if not os.path.exists(parcel_polygons_path):
        sys.exit("'parcel_polygons_path' is not a valid path")
    if parcel_polygons_path.split(".")[-1] != "shp":
        sys.exit("'parcel_polygons_path' does not point to a shapefile")
       
    # building_polygons_path: must be a string, valid path, and be a .shp
    if type(building_polygons_path) is not str:
        sys.exit("'building_polygons_path' must be a string pointing to a shapefile of building polygons")
    if not os.path.exists(building_polygons_path):
        sys.exit("'building_polygons_path' is not a valid path")
    if building_polygons_path.split(".")[-1] != "shp":
        sys.exit("'pbuilding_polygons_path' does not point to a shapefile")
        
    # cell_size: must be a number
    if type(cell_size) is not int and type(cell_size) is not float:
        sys.exit("'cell_size' must be an int or a float")
        
    # save_directory
    if save_directory is not None:
        if type(save_directory) is not str:
            sys.exit("'save_directory' must be a string of the desired save location")
        if not os.path.exists(save_directory):
            try: 
                os.mkdir(save_directory)
            except:
                sys.exit("'save_directory' is not a valid directory (or otherwise cannot be created)")
    
    # Spatial differencing parcels - buildings -------------------------------
    
    print("Calculating spatial difference of parcels and buildings...")
    
    parcels = gpd.read_file(parcel_polygons_path)
    buildings = gpd.read_file(building_polygons_path)
    
    if parcels.crs != buildings.crs:
        print("-- transforming building CRS to match parcels...")
        buildings = buildings.to_crs(parcels.crs)
        
    diffed = gpd.overlay(parcels, buildings, how="difference")
    geom_types = [geom.type for geom in diffed["geometry"]]
    
    if "MultiPolygon" in geom_types:
        # Adapted from https://gist.github.com/mhweber/cf36bb4e09df9deee5eb54dc6be74d26
        def multipolygon_to_polygon(gdf):
            poly_df = gpd.GeoDataFrame(columns=gdf.columns)
            for idx, row in gdf.iterrows():
                if type(row.geometry) == Polygon:
                    poly_df = poly_df.append(row, ignore_index=True)
                else:
                    mult_df = gpd.GeoDataFrame(columns=gdf.columns)
                    recs = len(row.geometry)
                    mult_df = mult_df.append([row]*recs, ignore_index=True)
                    for geom in range(recs):
                        mult_df.loc[geom,"geometry"] = row.geometry[geom]
                    poly_df = poly_df.append(mult_df, ignore_index=True)
            return poly_df
        diffed = multipolygon_to_polygon(diffed)
                
    # Rasterize the differenced polygons -------------------------------------
   
    print("Rasterizing differenced polygons...")
    
    diffed["Numeric"] = np.arange(1, diffed["geometry"].count()+1)
    parcel_id_table = pd.DataFrame(diffed.drop(columns='geometry'))
    shapes = ((geom,value) for geom, value in zip(diffed["geometry"], diffed["Numeric"]))
    
    bbox = diffed.total_bounds
    xrange = bbox[2] - bbox[0]
    yrange = bbox[3] - bbox[1]
    row_dim = math.ceil(xrange/cell_size)
    col_dim = math.ceil(yrange/cell_size)

    ras = rasterize(shapes = shapes,
                    out_shape = [row_dim, col_dim],
                    fill = 0)
    
    # Calculating developable rectangles -------------------------------------
       
    print("Calculating the largest developable spaces including each cell...")
    
    ras_dim = ras.shape
    nrow = ras_dim[0]
    ncol = ras_dim[1]
    
    height = np.zeros((nrow, ncol))
    width = np.zeros((nrow, ncol))
    un = np.zeros((nrow, ncol))
    
    dev_rects = pd.DataFrame()
    for i in range(nrow):
        for j in range(ncol):
            num_id = ras[i,j]
            if num_id == 0:
                continue
            else:                
                in_row = np.equal(ras[i,:], num_id)
                in_col = np.equal(ras[:,j], num_id)
                
                row_diff = np.where(in_row == False)[0]
                col_diff = np.where(in_col == False)[0]
                
                if len(row_diff) == 0:
                    wd = nrow
                    row_lower = 0
                    row_upper = nrow - 1
                else:
                    try:
                        row_lower = max(row_diff[np.where(row_diff < j)] + 1)
                    except:
                        row_lower = 0                    
                    try:
                        row_upper = min(row_diff[np.where(row_diff > j)] - 1)
                    except:
                        row_upper = nrow - 1
                    wd = row_upper - row_lower + 1
                        
                if len(col_diff) == 0:
                    ht = ncol
                    col_lower = 0
                    col_upper = ncol - 1
                else:
                    try:
                        col_lower = max(col_diff[np.where(col_diff < i)] + 1)
                    except:
                        col_lower = 0                    
                    try:
                        col_upper = min(col_diff[np.where(col_diff > i)] - 1)
                    except:
                        col_upper = ncol - 1
                    ht = col_upper - col_lower + 1
                
                row_range = np.arange(row_lower, row_upper + 1)
                col_range = np.arange(col_lower, col_upper + 1)
                rectangular_num_ids = ras[np.ix_(col_range, row_range)].flatten()
                unique_rni = np.unique(rectangular_num_ids)
                length_urni = len(unique_rni)
                
                height[i,j] = ht
                width[i,j] = wd
                un[i,j] = length_urni
                
                row_char = np.array_str(row_range).replace("]","").replace("[","")
                col_char = np.array_str(col_range).replace("]","").replace("[","")
                dev_specs = pd.DataFrame({"Row": i,
                                          "Col": j,
                                          "Numeric": num_id,
                                          "RowChar": col_char,
                                          "ColChar": row_char,
                                          # "RowInc": [row_range.tolist()],
                                          # "ColInc": [col_range.tolist()],
                                          "W": wd,
                                          "H" : ht,
                                          "A": wd*ht,
                                          "Unique": length_urni},
                                         index = [0])
                dev_rects = dev_rects.append(dev_specs)
    
    developable_area_table = pd.merge(dev_rects,
                                      parcel_id_table,
                                      how = "left",
                                      left_on = "Numeric",
                                      right_on = "Numeric")
    developable_area_table = developable_area_table.drop(columns="Numeric")
    developable_area_table = developable_area_table[["Row","Col","PARCELID",
                                                     "W","H","A",
                                                     # "RowInc","ColInc",
                                                     "RowChar","ColChar",
                                                     "Unique"]]
            
    # Saving -----------------------------------------------------------------
    
    if save_directory is not None:
        print("Saving...")
        save_path = os.path.join(save_directory, "Parcel_Developable_Area_Table_Full.csv")
        developable_area_table.to_csv(save_path)
        print("-- saved to: " + save_path)
                
    # Done -------------------------------------------------------------------
    
    print("Done!\n")
    return developable_area_table

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

# developable_area_table: pandas DataFrame output of the 'create_developable_
#                         area_table' function, or string of a .csv file path
#                         pointing to that output.
# how: string of 'max', 'sum', or 'count', defining how the parcel-level
#      summary of developable area should be performed.
#      'max' will give the area of the largest rectangle.
#      'sum' will give the total area of all rectangles.
#      'count' will give the number of unique rectangles.
#      all are subject to the constraints on rectangle size implied by
#      min_width, min_height, and min_area.
# min_width: float or int of minimum width (in units of parcel CRS) for an 
#            open space to be considered developable (i.e., can represent a 
#            minimum building width for a theoretical structure to be built).
#            Default is 1, no constraint applied.
# min_height: float or int of minimum height (in units of parcel CRS) for an 
#             open space to be considered developable (i.e., can represent a 
#             minimum building height for a theoretical structure to be built).
#             Default is 1, no constraint applied.
# min_area: float or int of minimum area (in square units of parcel CRS) for
#           an open space to be considered developable (i.e., can represent a 
#           minimum building area for a theoretical structure to be built).
#           Default is 1, no constraint applied.
# save_directory: string of path to desired save directory
#                 If None, no save will be completed
#
# Note: currently, only how='max' is operationalized. how='sum' and how='count'
# are still in development.

# Testing specs
# developable_area_table is the output from 'create_developable_area_table'
how = 'max'
min_width = 1
min_height = 1
min_area = 1
save_directory = None

def summarize_developable_area(developable_area_table,
                               how = 'max',
                               min_width = 1,
                               min_height = 1,
                               min_area = 1,
                               save_directory = None):
    
    # Validation of inputs ---------------------------------------------------
    
    # developable_area_table: must be able to be read in from a .csv, and/or
    # must be the output of the 'create_developable_area_table' function
    if type(developable_area_table) is str:
        if os.path.exists(developable_area_table):
            if developable_area_table.split(".")[-1] != "csv":
                sys.exit("'developable_area_table' is a valid file path, does not point to a .csv")
            else:
                developable_area_table = pd.read_csv(developable_area_table)
        else:
            sys.exit("'developable_area_table' is a string, but is not a valid file path")
    
    need_columns = ["Row","Col","PARCELID",
                    "W","H","A",
                    # "RowInc","ColInc",
                    "RowChar","ColChar",
                    "Unique"] 
    present_columns = developable_area_table.columns.to_list()       
    missing = set(need_columns) - set(present_columns)
    not_needed = set(present_columns) - set(need_columns)
    if len(missing) > 0 or len(not_needed) > 0:
        sys.exit("'developable_area_table' is not the output of the 'create_developable_area_table' function") 
    
    # how: must be a string, must be 'max', 'sum', or 'count'
    if type(how) is not str:
        sys.exit("'how' must be a string")
    if how not in ['max','sum','count']:
        sys.exit("'how' must be 'max', 'sum', or 'count'")
    
    # min_width: must be None or a number
    if type(min_width) not in [int, float]:
        sys.exit("'min_width' must be an int, float, or None")
        
    # min_width: must be None or a number
    if type(min_height) not in [int, float]:
        sys.exit("'min_height' must be an int, float, or None")
        
    # min_width: must be None or a number
    if type(min_width) not in [int, float]:
        sys.exit("'min_area' must be an int, float, or None")
        
    # save_directory
    if save_directory is not None:
        if type(save_directory) is not str:
            sys.exit("'save_directory' must be a string of the desired save location")
        if not os.path.exists(save_directory):
            try: 
                os.mkdir(save_directory)
            except:
                sys.exit("'save_directory' is not a valid directory (or otherwise cannot be created)")
    
    # Calculating developable rectangles -------------------------------------
       
    print("Extracting developable area by parcel in accordance with provided inputs...")
    
    if how != "max": # This is needed until I get the other ones working
        how = "max"
    
    valid_rectangles = developable_area_table[developable_area_table["Unique"] == 1]
    and_statement = np.logical_and.reduce((valid_rectangles["W"] >= min_width,
                                           valid_rectangles["H"] >= min_height,
                                           valid_rectangles["A"] >= min_area))
    dim_red = valid_rectangles[and_statement]
    unr = dim_red[["PARCELID","W","H","A","RowChar","ColChar"]].drop_duplicates()
     
    if how == "max":
        da = unr.groupby("PARCELID").apply(lambda x: max(x["A"])).reset_index()
        da.columns = ["PARCELID", "Max_Area"]
        # Need to change columns names
    else:
        if how == "sum":
            da = unr
            # In dev
            # unr["Fits"] = np.multiply(np.floor(np.divide(unr["W"], min_width)),
            #                           np.floor(np.divide(unr["H"], min_height)))
            # for i in unr["RowChar"]:
            #    for j in unr["ColChar"]:
            #        r = np.fromstring(i, sep=" ")
            #        c = np.fromstring(j, sep=" ")
            #        minr = min(r)
            #        maxr = max(r)
            #        minc = min(c)
            #        maxc = max(c)
            #        rowi = np.arange(minr, maxr - min_height + 2)
            #        coli = np.arange(minc, maxc - min_width + 2)        
            # Then find all combos of the shape
            # Then find all indices in that shape
            # Then filter to shapes such that all indices are unique
            # How do we deal with overlapping polygons
        else: #how == "count"
            da = unr
    
    # Saving -----------------------------------------------------------------
    
    if save_directory is not None:
        print("Saving...")
        save_path = os.path.join(save_directory, "Parcel_Developable_Area_Summarized.csv")
        da.to_csv(save_path)
        print("-- saved to: " + save_path)
    
    # Done -------------------------------------------------------------------
    
    print("Done!\n")
    return da
    
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        