# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 13:12:35 2020

@author: Aaron Weinstock
"""

# rasterio to extract values instead of intersect method


# %% Imports

import arcpy
import tempfile
import os
import pandas as pd
import numpy as np

# %% Function

def analyze_imperviousness(impervious_path,
                           zone_geometries_path,
                           zone_geometries_id_field,
                           save_gdb_location):
    '''
    Summarize percent impervious surface cover in each of a collection of zones
    
    Parameters 
    ----------
    impervious_path: Path 
        path to clipped/transformed imperviousness raster (see the
        `prep_imperviousness` function)
    zone_geometries_path: Path
        path to polygon geometries to which imperviousness will be summarized
    zone_geometries_id_field: str
        id field in the zone geometries
    save_gdb_location: Path
        save location for zonal imperviousness summaries
    
    Returns
    -------
    Table of zonal summaries will be saved to the save_gdb_location; the save
    path will be returned
    '''

    # Matching imperviousness to zone geometries
    # ------------------------------------------
    print("")
    print("1. Matching imperviousness to zone geometries")
        
    # Set up an intermediates gdb
    print("1.1. setting up an intermediates gdb")
    
    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(out_folder_path = temp_dir,
                                   out_name = "Intermediates.gdb")
    intmd_gdb = os.path.join(temp_dir, "Intermediates.gdb")
    
    # Convert raster to point (and grabbing cell size)
    print("1.2. converting raster to point")
    
    rtp_path = os.path.join(intmd_gdb,
                            "raster_to_point")
    arcpy.RasterToPoint_conversion(in_raster = impervious_path, 
                                   out_point_features = rtp_path)
    
    # Intersect raster with zones
    print("1.3. matching raster points to zones")
    
    intersection_path = os.path.join(intmd_gdb,
                                     "intersection")
    arcpy.Intersect_analysis(in_features = [rtp_path,
                                            zone_geometries_path], 
                             out_feature_class = intersection_path)
    
    # Load the intersection data
    print("1.4. Loading raster/zone data")
    
    load_fields = [zone_geometries_id_field, "grid_code"]
    df = arcpy.da.FeatureClassToNumPyArray(in_table = intersection_path,
                                           field_names = load_fields)
    df = pd.DataFrame(df)
    # arcpy.Delete_management(intmd_gdb)
    
    # Values with 127 are nulls -- replace with 0
    print("1.5. replacing null impervious values with 0")
    
    df['grid_code'] = df['grid_code'].replace(127, 0)
        
    # Summarizing to the zone level
    # -----------------------------
    print("")
    print("2. Summarizing zonal imperviousness statistics")
    
    # Identify the raster cell size 
    print("2.1 grabbing impervious raster cell size")
    
    cellx = arcpy.GetRasterProperties_management(in_raster = impervious_path,
                                                 property_type = "CELLSIZEX")
    celly = arcpy.GetRasterProperties_management(in_raster = impervious_path,
                                                 property_type = "CELLSIZEY")
    cs = float(cellx.getOutput(0)) * float(celly.getOutput(0))
    
    # # Summarize imperviousness to the zone only
    # print("2.2 summarizing impervious percent")
    
    # mean_imp = df.groupby(zone_geometries_id_field)["grid_code"].agg([("Imperviousness","mean")])
    # mean_imp = mean_imp.reset_index()
    
    # # Now, summarize all the area statistics
    # print("2.3 summarizing impervious area")
    
    # df["Class"] = None
    # df.loc[df.grid_code == 0, "Class"] = "NonDevArea"
    # df.loc[df.grid_code.between(1, 19), "Class"] = "DevOSArea"
    # df.loc[df.grid_code.between(20, 49), "Class"] = "DevLowArea"
    # df.loc[df.grid_code.between(50, 79), "Class"] = "DevMedArea"
    # df.loc[df.grid_code >= 80, "Class"] = "DevHighArea"
    # df["Area"] = cs
    # area_sum = pd.pivot_table(data = df,
    #                           values = "Area",
    #                           index = [zone_geometries_id_field],
    #                           columns = ["Class"], 
    #                           aggfunc = np.sum,
    #                           fill_value = 0)
    # area_sum = area_sum.reset_index()
    
    # # Now, all that's left is to join everything up
    # print("2.4 joining percent and area summaries)
    
    # zonal = pd.merge(mean_imp,
    #                  area_sum,
    #                  left_on = zone_geometries_id_field,
    #                  right_on = zone_geometries_id_field,
    #                  how = "outer")
    # zonal = zonal.reset_index()
    # zonal = zonal.fillna(0)    
               
    # Groupby-summarise the variables of interest
    print("2.2 calculating zonal summaries")
    
    zonal = df.groupby(zone_geometries_id_field)["grid_code"].agg([("IMP_PCT", np.mean),
                                                                   ("TotalArea", lambda x: x.count() * cs),
                                                                   ("NonDevArea", lambda x: x[x == 0].count() * cs), 
                                                                   ("DevOSArea", lambda x: x[x.between(1, 19)].count() * cs), 
                                                                   ("DevLowArea", lambda x: x[x.between(20, 49)].count() * cs), 
                                                                   ("DevMedArea", lambda x: x[x.between(50, 79)].count() * cs),
                                                                   ("DevHighArea", lambda x: x[x >= 80].count() * cs)])
    zonal = zonal.reset_index()
    
    # Writing results
    # ---------------
    print("")
    print("3. Writing results")
    
    # Set the save name
    print("3.1 setting a save name/path")
    
    save_path = os.path.join(save_gdb_location,
                             "Imperviousness_blocks")
    
    # Write the df to table
    print("3.2 saving results to table")
    
    zonal_et = np.rec.fromrecords(recList = zonal.values, 
                                  names = zonal.dtypes.index.tolist())
    zonal_et = np.array(zonal_et)
    arcpy.da.NumPyArrayToTable(in_array = zonal_et,
                               out_table = save_path)
    
    # Done
    # ----
    print("")
    print("Done!")
    print("Imperviousness summary table saved to: " + save_path)
    print("")
    return(save_path)

# %% Main

if __name__ == "__main__":
    
    # This will create a table of imperviousness by block for 2019. Then, it
    # will copy it to the 2014-2018 geodatabases (since neither the blocks or
    # impervious raster changes in the span 2014-2019)
    
    # Inputs
    impervious_path = 'K:\\Projects\\MiamiDade\\PMT\\Data\\Cleaned\\Imperviousness\\Imperviousness_2016_Clipped.img'
    zone_geometries_path = 'K:\\Projects\\MiamiDade\\PMT\\Data\\Cleaned\\blocks.gdb\\Blocks_2019'
    zone_geometries_id_field = 'GEOID10'
    save_gdb_location = 'K:\\Projects\\MiamiDade\\PMT\\Data\\PMT_2019.gdb'
    
    # Function
    first_run = analyze_imperviousness(impervious_path = impervious_path,
                                       zone_geometries_path = zone_geometries_path,
                                       zone_geometries_id_field = zone_geometries_id_field,
                                       save_gdb_location = save_gdb_location)
    
    # Copy to other years
    for year in [2014, 2015, 2016, 2017, 2018]:
        yr = str(year)
        print("Copying to " + yr + " geodatabase")
        save_path = os.path.join("K:\\Projects\\MiamiDade\\PMT\\Data",
                                 ''.join(["PMT_", yr, ".gdb"]),
                                 "Imperviousness_blocks")
        arcpy.Copy_management(in_data = first_run,
                              out_data = save_path)
        
    

