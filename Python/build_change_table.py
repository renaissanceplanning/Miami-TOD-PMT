# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 11:12:52 2020

@author: AZ7
"""

# %% Imports
import arcpy
import pandas as pd
import numpy as np

# %% Functions

def build_change_table(snapshot_year_table,
                       base_year_table,
                       geography_field,
                       save_gdb_path,
                       how = "all"):
    '''
    builds a table of absolute and/or percent change between a snapshot and a
    base year

    Parameters
    ----------
    snapshot_year_table : str
        path to a snapshot year table. should have the same attributes and
        geographic level as `base_year_table`
    base_year_table : TYPE
        path to a base year table. should have the same attributes and
        geographic level as snapshot_year_table`
    geography_field : str
        field name for the geographic level of `snapshot_year_table` and
        `base_year_table`. should be the same for both tables
    save_path : str
        file path within a .gdb to save the resulting table
    how : str, or iterable of str
        how should change be calculated? can be either "absolute", "percent",
        or "all" (in which case both absolute and percent change will be
        calculated). Default is `"all"`
    
    Notes
    -----
    If `how = "all"`, then "_absolute" and "_percent" will be added to the end 
    of the file for differentiation; if `how` is length one, the single, 
    unmodified `save_path` will be used

    Returns
    -------
    dict of path(s) to the change table(s), keyed by `how`
    '''
    
    # First, we want to read the two tables. We'll read them into a list
    # where base_year is list element 0 and snapshot_year is list element 1
    # (which corresponds with the year 0 and year 1 notation we've been using).
    # We'll also store the fields in the same way
    print("1. Loading the base and snapshot tables")
    
    df = []
    fld = []
    for file in [base_year_table, snapshot_year_table]:
        fields = [f.name for f in arcpy.ListFields(file)]
        fld.append(fields)
        np_ar = arcpy.da.TableToNumPyArray(in_table = file,
                                           field_names = fields)
        pd_df = pd.DataFrame(np_ar)
        df.append(pd_df)
    
    # Once we read the two tables, we want to see what field names we can
    # actually difference -- those fields will be fields that are in both
    # tables that are NOT the geography field
    print("2. Identifying fields to difference")
    
    # Retain the fields that are in both tables (including the geography)
    fld[0].remove(geography_field)
    fld[1].remove(geography_field)
    matched_fields = [f for f in fld[0] if f in fld[1]]
    for i in [0,1]:
        keep_fields = [geography_field] + matched_fields
        df[i] = df[i][keep_fields]
        df[i] = df[i].set_index(geography_field)
        
    # Print to the user the fields that will be excluded
    in_base_only = ', '.join([f for f in fld[0] if f not in fld[1]])
    in_snapshot_only = ', '.join([f for f in fld[1] if f not in fld[0]])
    if in_base_only != []:
        if len(in_base_only) == 1:
            base_verb = "is"
        else:
            base_verb = "are"
        print("-->", 
              in_base_only, 
              base_verb,
              "only in base table, will not be included")
    if in_snapshot_only != []:
        if len(in_snapshot_only) == 1:
            snapshot_verb = "is"
        else:
            snapshot_verb = "are"
        print("-->", 
              in_snapshot_only, 
              snapshot_verb,
              "only in snapshot table, will not be included")
    
    # Now we want to difference the tables for absolute difference, or divide
    # and subtract 1 for percent difference. We'll also go ahead and convert
    # the result from dataframe to array, which we'll need for writing to
    # table using arcpy
    print("3. Calculating change between the base and snapshot years")
    
    change_tables = {}
    if how == "all" or how == "absolute":
        abs_df = df[1].sub(df[0]).reset_index()
        abs_tt = np.rec.fromrecords(recList = abs_df.values,
                                    names = abs_df.dtypes.index.tolist())
        abs_tt = np.array(abs_tt)
        change_tables["absolute"] = abs_tt
    if how == "all" or how == "percent":
        per_df = df[1].divide(df[0], fill_value = 0) - 1
        per_tt = np.rec.fromrecords(recList = per_df.values,
                                    names = per_df.dtypes.index.tolist())
        per_tt = np.array(per_tt)
        change_tables["percent"] = per_tt
        
    # Now we're ready to save -- we just reference the tables created above
    # and write out. Remember that if how="all", we'll also need to format
    # the save path to differentiate between absolute and percent
    print("4. Saving")
    
    if how == "all":
        # Format save paths
        absolute_save_path = '_'.join([save_gdb_path, "absolute"])
        percent_save_path = '_'.join([save_gdb_path, "percent"])
        # Save
        arcpy.da.NumPyArrayToTable(in_array = change_tables["absolute"],
                                   out_table = absolute_save_path)
        arcpy.da.NumPyArrayToTable(in_array = change_tables["percent"],
                                   out_table = percent_save_path)
        # Format return
        ret_dict = {"absolute": absolute_save_path,
                    "percent": percent_save_path}
    elif how == "absolute":
        # Save
        arcpy.da.NumPyArrayToTable(in_array = change_tables["absolute"],
                                   out_table = save_gdb_path)
        # Format return
        ret_dict = {"absolute": save_gdb_path}
    else:
        # Save
        arcpy.da.NumPyArrayToTable(in_array = change_tables["percent"],
                                   out_table = save_gdb_path)
        # Format return
        ret_dict = {"percent": save_gdb_path}
    
    # And now we're done!
    # Return the result
    print("Done!")
    print("")
    return(ret_dict)

# %% Main
if __name__ == "__main__":
    
    # Some PMT stuff...

        
        
    
        
    
    
    
    
    
    
    