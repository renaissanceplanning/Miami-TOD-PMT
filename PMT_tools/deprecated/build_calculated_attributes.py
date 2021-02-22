# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 12:47:40 2020

@author: AZ7
"""

# %% Imports

import arcpy
import re
import pandas as pd
import numpy as np
import os


# %% Functions

def build_calculated_attributes(features_path,
                                features_id_field,
                                funs):
    '''
    Parameters
    ----------
    features_path : Path
        path to features you wish to add attributes to via calculation
    features_id_field : TYPE
        unique ID field for the features in `features_path`
    funs : str, or list of str
        functions defining how new attributes should be calculated. See notes
        
    Notes
    -----
    the functions in `funs` can handle simple operations only: +, -, *, and /.
    they can also handle grouping by parentheses. When writing a function, the
    LHS MUST be separated from the RHS by `=` -- otherwise, the function will
    not be recognized. The LHS should contain the name of the new attribute
    you wish to create; the RHS should contain the function, and may be written
    like any mathematical function in python. 
    For example, imagine you wanted a new field "Share" to be the amount of
    "Field1" relative to "Field2". Then, the function should be specified as
    follows: `"Share = Field1 / (Field1 + Field2)

    Returns
    -------
    the `features_path` will be updated in place to add all the new attributes
    specified in `funs`. the save path (i.e. `features_path`) will be returned
    as a confirmation of completion.
    '''

    # 1. Loading data
    # ---------------
    print("")
    print("Loading data")

    # First, we need to identify the fields we want to load. We'll need the
    # ProcessID, along with any fields specified in "funs"
    print("-- identifying fields to load")

    load_fields = [re.findall("\w+", fun.split("=")[1]) for fun in funs]
    load_fields = [n for item in load_fields for n in item]
    load_fields = np.unique(load_fields).tolist()
    load_fields = [features_id_field] + load_fields

    # Now, we read the data with the identified fields
    print("-- reading identified fields from features")
    df = arcpy.da.FeatureClassToNumPyArray(in_table=features_path,
                                           field_names=load_fields,
                                           null_value=np.nan)
    df = pd.DataFrame(df)

    # 2. Functions of fields
    # ----------------------
    print("")
    print("Executing functions of fields")

    # To perform the functions, we first create each function as a string that 
    # references "df"
    print("-- formatting functions")
    executables = [re.sub('(\w+)', lambda m: 'df["' + m.group(1) + '"]', fun)
                   for fun in funs]

    # Then, we just execute the strings
    print("-- executing functions")
    for e in executables:
        exec(e)

    # 3. Writing results
    # ------------------
    print("")
    print("Writing results")

    # First, we want to select out only the new fields we've created, because
    # these are the only ones we need to join back (we'll also retain
    # the id field for the join)
    print("-- selecting newly calculated columns")

    result_fields = [re.findall("\w+", fun.split("=")[0])[0] for fun in funs]
    result_fields = [features_id_field] + result_fields
    df = df[result_fields]

    # Next, we want to identifying any fields that exist in the data already. 
    # We assume that if they exist but they're being calculated again, this 
    # means we want to update them. So, to make the extend table work, we need 
    # to delete those fields from the existing dataset
    existing_fields = [f.name for f in arcpy.ListFields(features_path)]
    current_fields = df.columns.tolist()
    current_fields = [c for c in current_fields if c != features_id_field]
    del_fields = [c for c in current_fields if c in existing_fields]
    if len(del_fields) > 0:
        print("-- deleting existing fields that are being updated")
        arcpy.DeleteField_management(in_table=features_path,
                                     drop_field=del_fields)

    # Now, we use extend table to join the results back to our initialized
    # save feature class
    print("-- joining results back to features")

    df_et = np.rec.fromrecords(recList=df.values,
                               names=df.dtypes.index.tolist())
    df_et = np.array(df_et)
    arcpy.da.ExtendTable(in_table=features_path,
                         table_match_field=features_id_field,
                         in_array=df_et,
                         array_match_field=features_id_field)

    # 4. Done
    # -------
    print("")
    print("Done!")
    print("Attributes added to: " + features_path)
    print("")
    return (features_path)


# %% Main

if __name__ == "__main__":

    # Add non-motorized share, WFH share, and jobs-housing balance to
    # 1. PMT_Snapshot/blocks/Each_block
    # 2. PMT_Snapshot/blocks/Blocks_floor_area_by_use
    # 3. PMT_Trend/blocks/Blocks_by_year
    # 4. PMT_Trend/blocks/Blocks_floor_area_by_use_by_year

    # Files to update
    file_rhs = ["PMT_Snapshot.gdb/blocks/Each_block",
                "PMT_Snapshot.gdb/Blocks_floor_area_by_use",
                "PMT_Trend.gdb/blocks/Blocks_by_year",
                "PMT_Trend.gdb/Blocks_floor_area_by_use_by_year"]
    file_lhs = "K:/Projects/MiamiDade/PMT/Data"
    file_paths = [os.path.join(file_lhs, rhs) for rhs in file_rhs]

    # ID field is the same in all files
    features_id_field = "ProcessID"

    # Functions are the same for all files
    funs = ["NM_Share = (Transit_PAR + AllOther_PAR + NonMotor_PAR) / Total_Commutes",
            "WFH_Share = Work_From_Home_PAR / Total_Commutes",
            "JHB = Total_Employment / (Total_Employment + Total_Population)"]

    # Run the functions
    for features_path in file_paths:
        print("")
        print(features_path)
        build_calculated_attributes(features_path=features_path,
                                    features_id_field=features_id_field,
                                    funs=funs)
