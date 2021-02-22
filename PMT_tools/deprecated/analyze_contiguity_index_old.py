# -*- coding: utf-8 -*-
"""
contiguity_index

calculates FRAGSTATS contiguity index for polygons (parcels) split by
a second set of polygons (buildings)

Created on Fri Sep 18 16:26:38 2020
Last updated Sun Sep 20 14:24:XX 2020

@author: Aaron Weinstock
"""

import sys
from rasterio.features import rasterize
import pandas as pd
import numpy as np
import geopandas as gpd
import math
import os
import re
import shapely
import itertools

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

# parcel_polygons_path: string of path to parcel polygons shapefile.
# building_polygons_path: string of path to building polygons shapefile.
# cell_size: float or int of desired cell size (in units of parcel CRS) for
#            rasterized version of parcels.
# weights_num: either the string 'knn', or a dictionary with keys 1:9. 
#              Use if you want to assign weights based of NUMBER of neighbors.
#              Dictionary keys correspond to the number of neighbors, and
#              values should correspond to the desired weights. 
#              'knn' sets up a weights such that weight = number of neighbors.
# weights_dir: either the string 'rook', 'queen', or 'queen-rook', or a
#              dictionary with keys top_left, top_center, top_right, 
#              middle_left, self, middle_right, bottom_left, bottom_center,
#              bottom_right
#              Use if you want to assign weights based on LOCATION of neighbors.
#              Values should correspond to the desired weights.
#              'rook' sets up weights such that lateral weights are 1, self is
#              1, and diagonals are 0
#              'queen' sets up weights such that all weights are 1 (equivalent
#              to weights_num = 'knn')
#              'queen-rook' sets up weights such that lateral weights are 2,
#              self is 1, and diagonals are 1
# chunks: int (>1), how many chunks should analysis be performed in?
#         must be a square number (1, 2, 4, 9, 16, 25, ...)
#         Default 1, no chunking
# save_directory: string of path to desired save directory
#                 If None, no save will be completed
#
# Note: weights_num and weights_dir cannot both be specified; one must be
# specified, and the other must be None

# Testing specs
parcel_polygons_path = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\Parcels\Parcel_Geometry\Miami_2019.shp"
building_polygons_path = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\OSM_Buildings\OSM_Buildings_20201001111703.shp"
cell_size = 40
weights_num = None
weights_dir = "queen"
chunks = 25
save_directory = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\Contiguity"

def create_contiguity_index_table(parcel_polygons_path,
                                  building_polygons_path,
                                  cell_size,
                                  weights_num = None, #or knn, or a dictionary
                                  weights_dir = "queen-rook", #or queen, rook, or a dictionary
                                  chunks = 1, 
                                  save_directory = None): 
    
    # Validation of non-weights inputs ---------------------------------------
    
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
        
    # chunks: must be a square integer
    if type(chunks) is not int or math.sqrt(chunks) % 1 != 0:
        sys.exit("'chunks' must be a square integer (e.g. 1,2,4,9,16,25,...)")
    
    # save_directory
    if save_directory is not None:
        if type(save_directory) is not str:
            sys.exit("'save_directory' must be a string of the desired save location")
        if not os.path.exists(save_directory):
            try: 
                os.mkdir(save_directory)
            except:
                sys.exit("'save_directory' is not a valid directory (or otherwise cannot be created)")
        
    # Validation/setup of dictionary of weights ------------------------------
    
    if weights_num is not None and weights_dir is None:
        if type(weights_num) == str:
            weights_num = weights_num.lower()
            if weights_num == "knn":
                weights = dict({"1": 1,
                                "2": 2,
                                "3": 3,
                                "4": 4,
                                "5": 5,
                                "6": 6,
                                "7": 7,
                                "8": 8,
                                "9": 9})
            else:
                sys.exit(''.join(["Invalid string specification for 'weights_num'; ",
                                  "'weights_num' can only take 'knn' as a string\n"]))
        elif type(weights_num) == dict:
            k = weights_num.keys()
            missing = list(set([1,2,3,4,5,6,7,8,9]) - set(k))
            if len(missing) == 0:
                weights = weights_num  
            else:
                sys.exit(''.join(["Necessary keys missing from dict of 'weights_num'; ",
                                  "missing keys include: ",
                                  ', '.join([str(m) for m in missing]),
                                  "\n"]))
        else:
            sys.exit(''.join(["'weights_num' must be a string or dictionary; ",
                              "if string, it must be 'knn', and "
                              "if dictionary, it must have keys 1,2,3,4,5,6,7,8,9\n"]))
            
    elif weights_dir is not None and weights_num is None:
        if type(weights_dir) == str:
            weights_dir = weights_dir.lower()
            if weights_dir == "rook":
                weights = dict({"top_left": 0,
                                "top_center": 1,
                                "top_right": 0,
                                "middle_left": 1,
                                "self": 1,
                                "middle_right": 1,
                                "bottom_left": 0,
                                "bottom_center": 1,
                                "bottom_right": 0})
            elif weights_dir == "queen":
                weights = dict({"top_left": 1,
                                "top_center": 1,
                                "top_right": 1,
                                "middle_left": 1,
                                "self": 1,
                                "middle_right": 1,
                                "bottom_left": 1,
                                "bottom_center": 1,
                                "bottom_right": 1})
            elif weights_dir == "queen-rook":
                weights = dict({"top_left": 1,
                                "top_center": 2,
                                "top_right": 1,
                                "middle_left": 2,
                                "self": 1,
                                "middle_right": 2,
                                "bottom_left": 1,
                                "bottom_center": 2,
                                "bottom_right": 1})
            else:
                sys.exit(''.join(["Invalid string specification for 'weights_dir'; ",
                                  "'weights_dir' can only take 'rook', 'queen', or 'queen-rook' as a string\n"]))
        elif type(weights_dir) == dict:
            k = weights_dir.keys()
            missing = list(set(["top_left","top_center","top_right",
                                "middle_left","self","middle_right",
                                "bottom_left","bottom_center","bottom_right"]) - set(k))
            if len(missing) == 0:
                weights = weights_num
            else:
                sys.exit(''.join(["Necessary keys missing from 'weights_num'; ",
                                  "missing keys include: ",
                                  ', '.join([str(m) for m in missing]),
                                  "\n"]))
        else:
            sys.exit(''.join(["'weights_dir' must be a string or dictionary; ",
                              "if string, it must be 'rook', 'queen', or 'queen-rook', and "
                              "if dictionary, it must have keys 'top_left','top_center','top_right','middle_left','self','middle_right','bottom_left','bottom_center','bottom_right'\n"]))
    
    elif weights_dir is None and weights_num is None:
        sys.exit(''.join(["One of 'weights_num' and weights_dir' must be provided ",
                          "(and the other must be 'None')\n"]))  
    
    else:
        sys.exit(''.join(["Only one of 'weights_num' and weights_dir' should be provided; ",
                          "the other must be 'None'\n"]))   
   
    # Spatial differencing parcels - buildings -------------------------------
    
    print("Reading parcels and buildings...")
    # Read the buildings and parcels
    parcels = gpd.read_file(parcel_polygons_path)
    buildings = gpd.read_file(building_polygons_path)
    
    # Verify buildings and parcels are same crs. If not, transform
    pcrs = parcels.crs
    bcrs = buildings.crs
    if not pcrs.equals(bcrs):
        print("-- transforming building CRS to match parcels...")
        buildings = buildings.to_crs(pcrs)
        
    print("Setting up chunking by assigning parcels to a quadrat...")
    # Set up chunking
    # We want parcels chunked by area so we can decrease raster size
    # So, we want to identify parcels in quadrats
    # First, we need the parcel centroids and a spatial index
    pcen = gpd.GeoDataFrame(parcels.drop(columns="geometry"),
                            geometry = parcels.geometry.centroid)
    spatial_index = pcen.sindex
    
    # Now, we need to create quadrats
    # First, we get the total bounding region
    pbbox = parcels.total_bounds
    # Then, given a desired number of quadrats, we use spacing of the parcel
    # bounds to create cutoff points
    nq = int(math.sqrt(chunks) + 1)
    x_points = np.linspace(pbbox[0], pbbox[2], num=nq)
    y_points = np.linspace(pbbox[1], pbbox[3], num=nq)
    # Finally, we can iterate through the cutoff points to create quadrats
    quadrats = []
    for i in np.arange(5):
        for j in np.arange(5):
            quadrats.append(shapely.geometry.box(minx = x_points[i],
                                                 miny = y_points[j],
                                                 maxx = x_points[i+1],
                                                 maxy = y_points[j+1]))
    quadrats = gpd.GeoDataFrame(geometry = quadrats)
    quadrats.crs = pcrs
    # Now, we can use the spatial index to intersect
    # Because this is point to polygon and we used the bbox of the parcel
    # polygons (not centroids) to create the quadrats, we should have 100%
    # 1-quadrat-to-1-parcel coverage
    # First, intersect
    pidx = []
    for q in quadrats.geometry:
        pidx.append(list(spatial_index.intersection(q.bounds)))
    # Then, format
    lens = [len(x) for x in pidx]
    quad_id = np.arange(len(pidx))
    quad_id = [np.repeat(q, l) for q,l in zip(quad_id, lens)]
    quad_id = np.concatenate(quad_id)
    parcel_idx = list(itertools.chain.from_iterable(pidx))
    pidx = pd.DataFrame({"PIDX":parcel_idx, "QID":quad_id})
   
    # Now, the order of "pidx" will match that of the parcels EXACTLY if we
    # sort by the variable "PIDX". This will then give us a vector of quadrat
    # IDs that we can tie back to the parcels
    pidx = pidx.sort_values(by="PIDX")
    pidx = pidx.reset_index(drop=True)
    parcels["QID"] = pidx["QID"]
    
    print("Calculating spatial difference of parcels and buildings...")
    # Chunking is done! Moving on...
    # Perform the spatial difference
    diffed = gpd.overlay(parcels, buildings, how="difference")
    
    # Extract geometry types of differenced output
    geom_types = [geom.type for geom in diffed["geometry"]]
    
    # Set up a "string split" constant: this will be used in subgroup 
    # identification (if applicable), but we want it as a global option
    spl = "x" # we might want to option this out in the future
    
    # If any are MultiPolygon, we need to make them single polygon,
    # and change their identifier so they are treated as separate entities
    # when we shift to numeric identification for rasterization
    if "MultiPolygon" in geom_types:
        print("-- converting multipolygons in difference output to polygon...")
        # Create a list of geometries. If it's a polygon, give the polygon.
        # If it's a multipolygon, give all componennt parts
        current_geometries = list(diffed["geometry"])
        lcg = len(current_geometries)
        as_polygons = [current_geometries[x] 
                       if current_geometries[x].type == "Polygon" 
                       else list(current_geometries[x]) 
                       for x in np.arange(lcg)]
        # If it's a multipolygon, we'll also need the number of components,
        # so we know how many times to repeat the associated attributes
        geometry_lengths = [1 if type(x) is not list else len(x) for x in as_polygons]
        
        # Unlist the polygon geometries into a list, so they can be added
        # to a polygon geodataframe
        polygon_geometries = []
        for pl in as_polygons:
            if type(pl) is list:
                for p in pl:
                    polygon_geometries.append(p)
            else:
                polygon_geometries.append(pl)
        
        # Repeat all attributes for each geometry according to the number of
        # component polygons
        attr = pd.DataFrame(diffed.drop(columns='geometry'))
        attr_rep = attr.loc[attr.index.repeat(geometry_lengths)].reset_index(drop=True)
       
        # Create geodataframe from repeated attributes and polygon geometries
        diffed = gpd.GeoDataFrame(attr_rep,
                                  geometry = polygon_geometries)     
        
        # Count unique parcel IDs in the data
        diffed = diffed.sort_values(by = "PARCELNO")
        unids = diffed.groupby(["PARCELNO"]).size().to_dict() # might want to option out ID field in the future
        
        # Turn counts into subgroup IDs. If the count is one, there is no
        # subgroup, so ID is just ''. If there's more than one, we want a 
        # sequence to that number (e.g. a parcel ID showing up 3 times means
        # there were 3 polygons within a multipolygon for that parcel, so we
        # want subgroup IDs 0, 1, and 2)
        times = [np.arange(v).astype(str) 
                 if v > 1 
                 else np.array(['']) 
                 for k,v in unids.items()]
        times = np.concatenate(times)
        
        # Format subgroup IDs to string and remove dangling joins, which 
        # creates the new parcel ID
        sgid = [spl.join([pid, sid]) for pid,sid in zip(diffed["PARCELNO"], times)]
        sgid = [re.sub("x$", "", sid) for sid in sgid]
        
        # Overwrite previous ID field with our new data
        # (Overwrite is okay because we know we can get old parcel ID by
        # splitting on our preset constant. Plus, we need it to be the
        # existing ID field in case there are no subgroups created, i.e. this
        # field wouldn't exist)
        diffed["PARCELNO"] = sgid
    
    # Once all multipolygons have been coverted, "diffed" is in it's final    
    # form. Now, we set up a numeric ID for each parcel ID, because rasters 
    # can't take characters, so we need this unique numeric identifier
    diffed["Numeric"] = np.arange(1, len(diffed.index) + 1)
    # We also need to assign the proper CRS!
    diffed.crs = pcrs
    
    diffed = gpd.read_file(r"D:\Users\AZ7\Downloads\Diffed_Q25_Final.shp")
    
    # We'll also create a table associating numeric IDs with parcel IDs. 
    # We'll use this to reference raster results back to our parcels later
    parcel_id_table = pd.DataFrame(diffed.drop(columns='geometry'))
    

    # Looping through the contiguity calculation by chunks -------------------

    quadrat_results = []

    for i in np.arange(chunks):
        
        # Step 0: Subset the difference parcels to only include the loop chunk
        print("Chunk " + str(i+1) + "/" + str(chunks))
        dsub = diffed[diffed.QID == i].reset_index(drop=True)

        # Step 1: Rasterize the chunk difference
        print("-- Rasterizing differenced polygons...")
                
        # Set up shape/value pairs for rasterize function (requires this form)
        shapes = ((geom,value) for geom, value in zip(dsub["geometry"], dsub["Numeric"]))
        
        # Set up raster dimensions based on user cell size input
        bbox = dsub.total_bounds
        xrange = bbox[2] - bbox[0]
        yrange = bbox[3] - bbox[1]
        row_dim = math.ceil(yrange/cell_size)
        col_dim = math.ceil(xrange/cell_size)
    
        # Rasterize!
        ras = rasterize(shapes = shapes,
                        out_shape = (row_dim, col_dim),
                        fill = 0)
    
        # Step 2. Matricizing raster and creating ID tables
        print("-- Creating ID tables from rasterized polygon data...")
        
        # Get row and column counts, and sequence their indeces
        ras_dim = ras.shape
        nrow = ras_dim[0]
        ncol = ras_dim[1]
        row_idx = np.arange(nrow)
        col_idx = np.arange(ncol)
        
        # Create ID tables for future neighbor matching
        rows = np.repeat(row_idx, ncol)
        cols = np.tile(col_idx, nrow)
        id_tab_self = pd.DataFrame({"Row": rows, 
                                    "Col": cols,
                                    "ID": ras.flatten()}) 
        id_tab_neighbor = pd.DataFrame({"NRow": rows, 
                                        "NCol": cols,
                                        "NID": ras.flatten()})
    
        # Step 3. Creating neighbor table
        print("-- Creating a neighbors table for raster cells...")
        
        # Neighbor indices
        row_basic = [np.arange(x-1, x+2) for x in row_idx]
        col_basic = [np.arange(x-1, x+2) for x in col_idx]
        # Repeat neighbor indices
        row_extended = [a for a in row_basic for i in range(ncol)]
        col_extended = [a for i in range(nrow) for a in col_basic]
        
        # Row/Col indices
        row_id = np.repeat(row_idx, 9)
        row_id = np.repeat(row_id, ncol)
        # Repeat Row/Col indices
        col_id = np.repeat(col_idx, 9)
        col_id = np.tile(col_id, nrow)
        
        # Mesh grid neighbors
        meshed = [np.array(np.meshgrid(x,y)).reshape(2,9).T 
                  for x,y in zip(row_extended, col_extended)]
        meshed = np.concatenate(meshed, axis=0)
        meshed = pd.DataFrame(meshed, columns = ["NRow","NCol"])
        # Add in row and col IDs
        meshed.insert(1, "Col", col_id)
        meshed.insert(0, "Row", row_id)
        # Filter to valid neighbors
        meshed = meshed[(meshed.NRow >= min(row_idx)) 
                        & (meshed.NRow <= max(row_idx))
                        & (meshed.NCol >= min(col_idx))
                        & (meshed.NCol <= max(col_idx))]
        
        # Attribute with ID values
        meshed = pd.merge(meshed, 
                          id_tab_self, 
                          left_on = ["Row","Col"],
                          right_on = ["Row","Col"],
                          how = "left")
        meshed = pd.merge(meshed, 
                          id_tab_neighbor, 
                          left_on = ["NRow","NCol"],
                          right_on = ["NRow","NCol"],
                          how = "left")
        
        # Filter out invalid neighbors: non-matching ID and NID, or any 0
        meshed = meshed[meshed.ID == meshed.NID]
        meshed = meshed[meshed.ID != 0]
        valid_neighbors = meshed.drop("NID", axis=1)
        
        # Step 4: weights by type 
        print("-- Assigning weights according to weights inputs...")
        
        if weights_num is not None:
            wtab = valid_neighbors.groupby(["Row","Col"]).size().reset_index(name="N")
            wtab["N"] = [str(n) for n in wtab["N"]]
            wtab["Weight"] = [weights[key] for key in wtab["N"]]
            
            wtab = pd.merge(wtab, 
                            id_tab_self, 
                            left_on = ["Row","Col"],
                            right_on = ["Row","Col"],
                            how = "left")
            weight_max = max(weights.values())
         
        else:
            conditions = [(np.logical_and(valid_neighbors["NRow"] == valid_neighbors["Row"] - 1, valid_neighbors["NCol"] == valid_neighbors["Col"] - 1)),
                          (np.logical_and(valid_neighbors["NRow"] == valid_neighbors["Row"] - 1, valid_neighbors["NCol"] == valid_neighbors["Col"])),
                          (np.logical_and(valid_neighbors["NRow"] == valid_neighbors["Row"] - 1, valid_neighbors["NCol"] == valid_neighbors["Col"] + 1)),
                          (np.logical_and(valid_neighbors["NRow"] == valid_neighbors["Row"], valid_neighbors["NCol"] == valid_neighbors["Col"] - 1)),
                          (np.logical_and(valid_neighbors["NRow"] == valid_neighbors["Row"], valid_neighbors["NCol"] == valid_neighbors["Col"])),
                          (np.logical_and(valid_neighbors["NRow"] == valid_neighbors["Row"], valid_neighbors["NCol"] == valid_neighbors["Col"] + 1)),
                          (np.logical_and(valid_neighbors["NRow"] == valid_neighbors["Row"] + 1, valid_neighbors["NCol"] == valid_neighbors["Col"] - 1)),
                          (np.logical_and(valid_neighbors["NRow"] == valid_neighbors["Row"] + 1, valid_neighbors["NCol"] == valid_neighbors["Col"])),
                          (np.logical_and(valid_neighbors["NRow"] == valid_neighbors["Row"] + 1, valid_neighbors["NCol"] == valid_neighbors["Col"] + 1))]
            choices = ["top_left","top_center","top_right","middle_left","self","middle_right","bottom_left","bottom_center","bottom_right"]
            valid_neighbors["Type"] = np.select(conditions, choices)
            valid_neighbors["Weight"] = [weights[key] for key in valid_neighbors["Type"]]
            wtab = valid_neighbors.groupby(["Row","Col"]).apply(lambda x: sum(x.Weight)).reset_index(name="Weight")
            
            wtab = pd.merge(wtab, 
                            id_tab_self, 
                            left_on = ["Row","Col"],
                            right_on = ["Row","Col"],
                            how = "left")
            weight_max = sum(weights.values())
        
        # Step 5: Calculating contiguity and formatting
        print("-- calculating contiguity and matching to polygon IDs...")
        
        # Calculating contiguity
        contiguity = wtab.groupby("ID").apply(lambda x: (sum(x.Weight) / len(x.Weight) - 1) / (weight_max - 1)).reset_index(name="Contiguity")
        
        # Merging back with parcel IDs
        contiguity = pd.merge(contiguity, 
                              parcel_id_table,
                              left_on = "ID",
                              right_on = "Numeric",
                              how = "left")
        contiguity = contiguity.drop(columns=["ID","Numeric"])
        
        # Separating group and subgroup
        id_num = [pid.split(spl) for pid in contiguity["PARCELID"]]
        parcel_id = [sp[0] for sp in id_num]
        subgroup_id = [None if len(sp) == 1 else int(sp[1])+1 for sp in id_num]
        contiguity["PARCELID"] = parcel_id
        contiguity["SUBGROUPID"] = subgroup_id
        contiguity_index_table = contiguity[["PARCELID","SUBGROUPID","Contiguity"]]
        
        # Step 6: Add table to list of quadrat results
        print("Done with chunk " + str(i+1))
        quadrat_results.append(contiguity_index_table)
     
    # Saving -----------------------------------------------------------------
    
    # Bind into singular table
    contiguity_index_table = pd.concat(quadrat_results, axis = 0)
    
    # Then we want to bind back to parcels spatial data.
    
    if save_directory is not None:
        print("Saving...")
        save_path = os.path.join(save_directory, "Parcel_Contiguity_Index_Table_Full.csv")
        contiguity_index_table.to_csv(save_path)
        print("-- saved to: " + save_path)
    
    # Done -------------------------------------------------------------------
    
    print("Done!\n")
    return contiguity_index_table

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

# contiguity_index_table: pandas DataFrame output of the 'create_contiguity_
#                         index_table' function, or string of a .csv file path
#                         pointing to that output.
# how: string 'max', 'min', 'mean', or 'median' 
#      Defines the function used to summarize contiguity for parcels which
#      are broken into multiple patches by the buildings
# save_directory: string of path to desired save directory
#                 If None, no save will be completed
# SET UP FUNCTION TO TAKE MORE THAN ONE HOW

def summarize_contiguity_index(contiguity_index_table,
                               how,
                               save_directory = None):
    
    # Validation of inputs ---------------------------------------------------

    # contiguity_index_table: must be able to be read in from a .csv, and/or
    # must be the output of the 'create_developable_area_table' function
    if type(contiguity_index_table) is str:
        if os.path.exists(contiguity_index_table):
            if contiguity_index_table.split(".")[-1] != "csv":
                sys.exit("'contiguity_index_table' is a valid file path, does not point to a .csv")
            else:
                contiguity_index_table = pd.read_csv(contiguity_index_table)
        else:
            sys.exit("'contiguity_index_table' is a string, but is not a valid file path")
    
    need_columns = ["PARCELID","SUBGROUPID","Contiguity"] 
    present_columns = contiguity_index_table.columns.to_list()       
    missing = set(need_columns) - set(present_columns)
    not_needed = set(present_columns) - set(need_columns)
    if len(missing) > 0 or len(not_needed) > 0:
        sys.exit("'contiguity_index_table' is not the output of the 'create_contiguity_index_table' function") 
    
    # how: must be a string, must be 'max', 'sum', or 'count'
    if type(how) is not str:
        sys.exit("'how' must be a string")
    if how not in ['max','min','mean','median']:
        sys.exit("'how' must be 'max', 'min', 'mean', or 'median'")
    
    # save_directory
    if save_directory is not None:
        if type(save_directory) is not str:
            sys.exit("'save_directory' must be a string of the desired save location")
        if not os.path.exists(save_directory):
            try: 
                os.mkdir(save_directory)
            except:
                sys.exit("'save_directory' is not a valid directory (or otherwise cannot be created)")
                
    # Summarizing ------------------------------------------------------------
    
    print("Summarizing contiguity to parcel ID using provided function...")
    ci = contiguity_index_table.groupby("PARCELID").agg({"Contiguity": getattr(np, how)}).reset_index()
    
    # Saving -----------------------------------------------------------------
    
    if save_directory is not None:
        print("Saving...")
        save_path = os.path.join(save_directory, "Parcel_Contiguity_Index_Summarized.csv")
        ci.to_csv(save_path)
        print("-- saved to: " + save_path)
    
    # Done -------------------------------------------------------------------
    
    print("Done!\n")
    return ci


      
                
                
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
    