# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 11:08:08 2020

@author: Aaron Weinstock
"""

# %% Imports

import arcpy
import tempfile
import os
import pandas as pd
import numpy as np

# %% Functions

# Function to create intermediate gdb
# ----------------------------------------------------------------------------

def temp_gdb():
    '''
    creates a gdb for function intermediates in a temporary directory

    Returns
    -------
    file path to the temporary gdb
    '''
    
    # Set up the temporary directory
    temp_dir = tempfile.mkdtemp()
    
    # Create a .gdb there
    arcpy.CreateFileGDB_management(out_folder_path = temp_dir,
                                   out_name = "Intermediates.gdb")
    intmd_gdb = os.path.join(temp_dir, "Intermediates.gdb")
    
    # Done
    return(intmd_gdb)
    
    
# Function to convert to centroid
# ----------------------------------------------------------------------------

def polygon_to_centroid(polygon_path,
                        fields,
                        centroid_path,
                        compare):
    '''
    convert polygon to centroid and save, maintaining only requested fields

    Parameters
    ----------
    polygon_path : str
        path to polygons (to be converted to centroids)
    fields : list of str
        list of field names in `polygon_path` to retain in the centroids
    centroid_path : str
        path to which to save the centroids
    compare : str
        file with the desired spatial reference of the centroids output

    Returns
    -------
    file path to the centroids
    '''
    
    # Grab the spatial reference from "compare"
    sr = arcpy.Describe(compare).spatialReference
    
    # Define the fields to retain
    keep_fields = fields + ["SHAPE@X", "SHAPE@Y"]
    
    # Read in polygons as points
    npar = arcpy.da.FeatureClassToNumPyArray(in_table = polygon_path,
                                             field_names = keep_fields)
    
    # Write to centroids
    arcpy.da.NumPyArrayToFeatureClass(in_array = npar,
                                      out_table = centroid_path,
                                      shape_fields = ["SHAPE@X", "SHAPE@Y"],
                                      spatial_reference = sr)
    
    # Done
    return(centroid_path)


# Function to grouby-summarize attribute(s) in a shape
# ----------------------------------------------------------------------------

def gb_summarize(shape_path,
                 index_field,
                 summary_specs):
    '''
    for the file at `shape_path`, produce a summary table by grouping by
    `index_field` and summarizing `summary_field` with the function `how`

    Parameters
    ----------
    shape_path : str
        path to shape containing the data to be summarized
    index_field : str
        the field in `shape` over which to perform the summary
    summary_specs : dict
        dict of format {field : function} (both strings), where "field" is
        the field in `shape` to be summarized, and "function" is the function
        used to summarize "field" (must be a basic function, e.g. mean, sum)

    Returns
    -------
    pandas DataFrame of summarized data
    '''
    
    # First, we need to load the data
    keep_fields = [index_field] + list(summary_specs.keys())
    df = arcpy.da.FeatureClassToNumPyArray(in_table = shape_path,
                                           field_names = keep_fields)
    df = pd.DataFrame(df)
    
    # Now, we group-by and summarize
    df = df.groupby(index_field).agg(summary_specs)
    
    # Done
    return(df)


# Function to save or extend table, based on file existence
# ----------------------------------------------------------------------------

def save_or_extend(save_array,
                   save_path,
                   join_field):
    '''
    save `save_array` to a table if `save_path` doesn't exist, or extend
    `save_path` with `save_array` if `save_path` does exist. In the latter
    case, any data fields in `save_path` that exist in `save_array` will be
    updated

    Parameters
    ----------
    save_array : numpy array
        numpy array to be saved
    save_path : str
        desired save path
    join_field : str
        if extend table is required, a field in both `save_path` and
        `save_array` to join on

    Returns
    -------
    save path of new table or extended table
    '''
    
    # If the file exists, extend the table (overwriting any columns that
    # appear in the results). Otherwise, just write to table
    
    if arcpy.Exists(save_path):
        # Identify fields we may want to update
        to_overwrite = [f for f in save_array.dtype.names() if f != join_field]
        
        # Search for fields we need to delete in save path
        delete_fields = [f.name 
                         for f in arcpy.ListFields(save_path)
                         if f in to_overwrite]
        
        # Delete if necessary
        if len(delete_fields) >= 1:
            arcpy.DeleteField_management(in_table = save_path,
                                         drop_field = delete_fields)
            
        # Extend the table
        arcpy.da.ExtendTable(in_table = save_path, 
                             table_match_field = join_field, 
                             in_array = save_array, 
                             array_match_field = join_field)
    else:
        arcpy.da.NumPyArrayToTable(in_array = save_array,
                                   out_table = save_path)
        
    # Done
    return(save_path)


# Function to calculate JHB 
# ----------------------------------------------------------------------------

def analyze_jhb(parcels_path,
                res_units_field,
                employment_allocation_path,
                total_employment_field,
                aggregating_geometry_path,
                id_field,
                save_gdb_path,
                typology = True):
    '''
    calculates ratio of residential units : total jobs within the
    `aggregating_geometry`. If requested, additionally define the TOD typology 
    of the `aggregating_geometry` based on JHB 
    
    Parameters
    ----------
    parcels_path : str
        path to parcels shape
    res_units_field : str
        field name for residential units in `parcels_path`               
    employment_allocation_path : str
        path to shape containing employment allocation data
    total_employment_field : str
        field name for total employment in `employment_allocation_path`
    aggregating_geometry_path : str
        path to shape of aggregating geometry; JHB will be calculated within
        the geometries provided here
    id_field : str
        field name for unique ID in `aggregating_geometry_path`
    save_gdb_path : str
        path within gdb to save the results. If the path already exists, the
        results will be joined to the existing table; otherwise, a new table
        is created
    typology : bool, optional
        should an assessment on development type (based on FL TOD standards
        relative to JHB, see notes) be made for the aggregating geometries?
        Most relevant for station areas. Default `True`
        
    Notes
    -----
    The standards for Florida TOD typology based on JHB used in this function
    can be found in "A Framework for TOD in Florida" (March 2011), pp. 44-46
    http://fltod.com/renaissance/docs/Products/FrameworkTOD_0715.pdf

    Returns
    -------
    file location of the saved results
    '''
    
    # Set up a temp gdb for intermediates
    # -----------------------------------
    print("")
    print("1. Setting up a temp .gdb for intermediates")
    
    intmd_gdb = temp_gdb()
    
    # Centroidize the res units and employment data
    # ---------------------------------------------
    print("2. Centroidizing the res units and employment data")
    
    if parcels_path == employment_allocation_path:
        # They're the same path, we only need to read once
        cp = polygon_to_centroid(
            polygon_path = parcels_path,
            fields = [res_units_field, total_employment_field],
            centroid_path = os.path.join(intmd_gdb, "centroids"),
            compare = aggregating_geometry_path
        )
        centroid_paths = [cp]
    else:
        # They're different paths, so we need to read each separately
        cp_parcel = polygon_to_centroid(
            polygon_path = parcels_path,
            fields = [res_units_field],
            centroid_path = os.path.join(intmd_gdb, "centroids_parcels"),
            compare = aggregating_geometry_path
        )
        cp_employment = polygon_to_centroid(
            polygon_path = employment_allocation_path,
            fields = [total_employment_field],
            centroid_path = os.path.join(intmd_gdb, "centroids_employment"),
            compare = aggregating_geometry_path
        )
        centroid_paths = [cp_parcel, cp_employment]
    
    # Intersect the data with the aggregating geometry
    # ------------------------------------------------
    print("3. Intersecting the data with the aggregating geometry")
    
    intersect_paths = []
    
    for file_path in centroid_paths:
        # Set a new file path
        ip = file_path.replace("centroids", "intersect")
        
        # Intersect
        arcpy.Intersect_analysis(
            in_features = [file_path, aggregating_geometry_path],
            out_feature_class = ip
        )
        
        # Add to list
        intersect_paths.append(ip)
    
    # Summarize the data to the aggregating geometry
    # ----------------------------------------------
    print("4. Summarizing the data to the aggregating geometry")
    
    summary_tables = []
    
    for file_path in intersect_paths:
        # Identify the fields to be read from the file
        flds = [f.name 
                for f in arcpy.ListFields(file_path) 
                if f.name in [res_units_field, total_employment_field]]
        specs = {}
        for f in flds:
            specs[f] = "sum"
            
        # group-by summarize
        dt = gb_summarize(shape_path = file_path,
                          index_field = id_field,
                          summary_specs = specs)
        
        # Add to list
        summary_tables.append(dt)
    
    # Format the results 
    # ------------------
    print("5. Formatting the results")
    
    # Merge up the list items (won't do anything if it's a single but it
    # won't hurt!)
    df = pd.concat(summary_tables)
    df = df.reset_index()
    
    # Reset names
    df = df.rename({res_units_field: "RES_UNITS",
                    total_employment_field: "JOBS"})
    
    # Calculate the JHB
    df["JHB"] = df["JOBS"] / df["RES_UNITS"]
    
    # Set the typologies if requested
    # -------------------------------
    
    if typology == True:
        print("6. Assessing the TOD typologies of the aggregating geometry")
        
        # Set the conditions and choices according to FL DOT standards
        conditions = [
            (df["JHB"].lt(0.5)),
            (df["JHB"].ge(0.5) & df["JHB"].lt(1.5)),
            (df["JHB"].ge(1.5) & df["JHB"].lt(2.5)),
            (df["JHB"].ge(2.5) & df["JHB"].lt(3.5)),
            (df["JHB"].ge(3.5) & df["JHB"].lt(5.5)),
            (df["JHB"].ge(5.5) & df["JHB"].lt(6.5)),
            (df["JHB"].ge(6.5))
        ]
        choices = ["Below neighborhood center", 
                   "Neighborhood center", 
                   "Between neighborhood and community center", 
                   "Community center", 
                   "Between community and regional center",
                   "Regional center",
                   "Above regional center"]
        
        # Assess the typology
        df["TOD_TYPE"] = np.select(condlist = conditions, 
                                   choicelist = choices)
        
    # Writing results
    # ---------------
    if typology == True:
        print("7. Writing results")
    else:
        print("6. Writing results")
    
    # Format as array for writing
    df_tt = np.rec.fromrecords(recList = df.values,
                               names = df.dtypes.index.tolist())
    df_tt = np.array(df_tt)
    
    # Save or extend
    saved = save_or_extend(save_array = df_tt,
                           save_path = save_gdb_path,
                           join_field = id_field)
    
    # Done
    # ----
    print("Done!")
    if arcpy.Exists(save_gdb_path):
        print("Results added to the table at:", save_gdb_path)
    else:
        print("Results saved to new table at:", save_gdb_path)
    print("")
    return(saved)


# %% Main

# if __name__ == "__main__":
    
    # Some PMT stuff...

     