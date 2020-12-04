# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 15:25:47 2020

@author: AZ7
"""

# %% Imports

import arcpy
import re
import pandas as pd
import numpy as np

# %% Functions

def prep_historical_summaries(features_path,
                              funs):
    
    # Add an ID field to the features ----------------------------------------
    print("")
    print("Creating unique feature ID")
    
    # We add a unique ID to the features to give us something to join our
    # results back on. We do this in place since we'll ultimately read and 
    # edit the "features_path" directly
    print("-- adding and calculating a unique ID")
    
    codeblock = 'val = 0 \ndef processID(): \n    global val \n    start = 1 \n    if (val == 0):  \n        val = start\n    else:  \n        val += 1  \n    return val'
    arcpy.AddField_management(in_table = features_path,
                              field_name = "ProcessID",
                              field_type = "LONG",
                              field_is_required = "NON_REQUIRED")
    arcpy.CalculateField_management(in_table = features_path,
                                    field = "ProcessID",
                                    expression = "processID()",
                                    expression_type = "PYTHON3",
                                    code_block = codeblock)
    
    # Load the data ----------------------------------------------------------
    print("")
    print("Loading data")
    
    # First, we need to identify the fields we want to load. We'll need the
    # ProcessID, along with any fields specified in "funs"
    print("-- identifying fields to load")
    
    load_fields = [re.findall("\w", fun.split("=")[1]) for fun in funs]
    load_fields = [n for item in load_fields for n in item]
    load_fields = np.unique(load_fields).tolist()
    load_fields = ["ProcessID"] + load_fields
    
    # Now, we read the data
    print("-- reading identified fields from features")
    df = arcpy.da.FeatureClassToNumPyArray(in_table = features_path,
                                           field_names = load_fields,
                                           null_value = np.nan)
    df = pd.DataFrame(df)
    
    # Functions of fields ----------------------------------------------------
    print("")
    print("Executing functions of fields")
    
    # To perform the functions, we first create each function as a string that 
    # references "df"
    print("-- formatting functions")
    executables = [re.sub('(\w+)', lambda m: 'df[' + m.group(1) + ']', fun)
                   for fun in funs]
    
    # Then, we just execute the strings
    print("-- executing functions")
    for e in executables:
        exec(e)
    
    # Writing results --------------------------------------------------------
    print("")
    print("Writing results")
    
    # First, we want to select out only the new fields we've created, because
    # these are the only ones we need to join back (we'll also retain
    # ProcessID for the join)
    print("-- selecting newly calculated columns")
    
    result_fields = [re.findall("\w", fun.split("=")[0])[0] for fun in funs]
    result_fields = ["ProcessID"] + result_fields
    df = df[result_fields]
    
    # Next, we have to turn the results to a numpy array (to be able to use
    # extend table)
    print("-- formatting results for write")
    
    df_et = np.rec.fromrecords(recList = df.values, 
                               names = df.dtypes.index.tolist())
    df_et = np.array(df_et)
    
    # Now, we use extend table to join the results back to our initialized
    # save feature class. Once completed, we also delete the ProcessID from 
    # this feature class because we were only using it for the merge
    print("-- joining results back to features (be patient, this may take a while")
    
    arcpy.da.ExtendTable(in_table = features_path,
                         table_match_field = "ProcessID",
                         in_array = df_et,
                         array_match_field = "ProcessID")    
    arcpy.DeleteField_management(in_table = features_path,
                                 drop_field = "ProcessID")
    
    
    
    
    
    
    