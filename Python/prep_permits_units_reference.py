# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 08:48:44 2020

@author: AZ7
"""

# %% Imports

import arcpy
import re
import pandas as pd
import numpy as np
import os

# %% Functions

def prep_permits_units_reference(parcels_path,
                                 permits_path,
                                 lu_match_field,
                                 parcels_living_area_field,
                                 permits_units_field,
                                 permits_living_area_name,
                                 save_directory,
                                 units_match_dict={}):
    '''
    Parameters
    ----------
    parcels_path: path
        path to MOST RECENT parcels data (see notes)
    permits_path: path
        path to the building permits data
    lu_match_field: str
        field name for a land use field present in BOTH the parcels and
        permits data
    parcels_living_area_field: str
        field name in the parcels for the total living area (building area)
    permits_units_field: str
        field name in the permits for the unit of measurement for permit
        types
    permits_living_area_name: str
        unit name for building area in the `permits_units_field`
    save_directory: directory
        path to a save location for a reference units multipliers table
    units_match_dict: dict
        a dictionary of the format `{unit_name: parcel_field, ...}`, where the
        `unit_name` is one of the unit names present in the permits data's
        `permits_units_field`, and the `parcel_field` is the field name in the
        parcels corresponding to that unit. It should be used to identify
        non-building area fields in the parcels for which we can calculate
        a building area/unit for the multiplier. `parcel_field` can also
        take the form of a basic function (+,-,/,*) of a column, see Notes for
        specifications
        
    Notes
    -----
    The most up-to-date parcels data available should be used, because units 
    multipliers for the short term should be based on the most current data
    
    To specify a function for the units_match_field, use the format
    "{field} {function sign} {number}". So, for example, to map an 'acre' unit
    in the permits to a 'land_square_foot' field in the parcels, you'd use the
    dictionary entry `'acre': 'land_square_foot' / 43560`
    
    Returns
    -------
    complete save path for a reference units multipliers table; the saving of
    this table will be completed as part of the function
    '''
    
    # -----------------------------------------------------------------------
    # First, we need to load our two datasets
    print("")
    print("Loading data")
    
    # To set up for loading the parcels, we'll need to identify desired unit 
    # fields that are not building square footage. These are provided in the 
    # values of the units_match_dict; however, some of them might be given
    # as functions (i.e. to match to a unit, a function of a parcels field is
    # required). So, we need to isolate the field names and the functional 
    # components
    print("-- identifying parcel fields")
    
    match_fields = []
    match_functions = []
    
    for key in units_match_dict.keys():
        # Field
        field = re.findall("^(.*[A-Za-z])", units_match_dict[key])[0]
        if field not in match_fields:
            match_fields.append(field)
        # Function
        fun = units_match_dict[key].replace(field, "")
        if fun != "":
            fun = ''.join(['parcels["',
                           field,
                           '"] = parcels["',
                           field,
                           '"]',
                           fun])
            if field not in match_functions:
                match_functions.append(fun)   
        # Overwrite value
        units_match_dict[key] = field
    
    # Parcels: we only need to keep a few fields from the parcels: the 
    # lu_match_field, the parcels_living_area_field, and any values in the 
    # match_fields (calculated above from units_match_dict). If we have any 
    # functions to apply (again, calcualted above from units_match_dict),
    # we'll do that too.
    print("-- reading/formatting parcels")
    
    parcels_sr = arcpy.Describe(parcels_path).spatialReference
    parcels_fields = [lu_match_field, parcels_living_area_field] + match_fields
    parcels = arcpy.da.FeatureClassToNumPyArray(in_table = parcels_path,
                                                field_names = parcels_fields,
                                                spatial_reference = parcels_sr,
                                                null_value = 0)
    parcels = pd.DataFrame(parcels)
    for fun in match_functions:
        exec(fun)
    
    # Permits: like with the parcels, we only need to keep a few fields: 
    # the lu_match_field, and the units_field. Also, we only need to keep
    # unique rows of this frame (otherwise we'd just be repeating
    # calculations!)
    print("-- reading/formatting permits")
    
    permits_sr = arcpy.Describe(permits_path).spatialReference
    permits_fields = [lu_match_field, permits_units_field]
    permits = arcpy.da.FeatureClassToNumPyArray(in_table = permits_path,
                                                field_names = permits_fields,
                                                spatial_reference = permits_sr,
                                                null_value = 0)
    permits = pd.DataFrame(permits)
    permits = permits.drop_duplicates().reset_index(drop=True)
    
    # ------------------------------------------------------------------------
    # Now, we loop through the units to calculate multipliers. We'll actually
    # have two classes: multipliers and overwrites. Multipliers imply that
    # the unit can be converted to square footage using some function of the 
    # unit; overwrites imply that this conversion unavailable
    print("")
    print("Calculating units multipliers and overwrites")
    
    # First we initialize the loop with a looping index and lists to store
    # the results
    print("-- initializing loop processing")
    
    rows = np.arange(len(permits.index))
    units_multipliers = []
    units_overwrites = []
    
    
    # Now, we loop over the rows of permits. There are 3 possible paths for
    # each row
    # 1. If the unit of the row is already square footage, we don't need any
    # additional processing
    #   Multiplier: 1
    #   Overwrite: None
    # 2. If the unit is one of the keys in units_match_dict, this means we can
    # calculate a square footage / unit from the parcels. We do this relative
    # to all parcels with that row's land use
    #   Multiplier: median(square footage / unit)
    #   Overwrite: None
    # 2. If the unit is NOT one of the keys in units_match_dict, this means 
    # we have to rely on average square footage. This is an overwrite, not a
    # multiplier, and is calculated relative to all parcels with that row's
    # land use
    #   Multiplier: None
    #   Overwrite: median(square footage)
    print("-- calculating multipliers and overwrites")
    
    for row in rows:
        # Unit and land use for the row
        unit = permits[permits_units_field][row]
        lu = permits[lu_match_field][row]
        
        # Case (1)
        if unit == permits_living_area_name:
            units_multipliers.append(1)
            units_overwrites.append(None)
        
        # Cases (2) and (3)
        else:
            # Parcels of the row's land use
            plu = parcels[parcels[lu_match_field] == lu]
            
            # Case (2)
            if unit in units_match_dict.keys():
                per = plu[parcels_living_area_field] / plu[units_match_dict[unit]]
                median_value = np.median(per)
                units_multipliers.append(median_value)
                units_overwrites.append(None)
            
            # Case (3)
            else: 
                per = plu[parcels_living_area_field]
                median_value = np.median(per)
                units_multipliers.append(None)
                units_overwrites.append(median_value)
            
    # Since our results are lists, we can just attach them back to the permits
    # as new columns
    print("-- binding results to the permits data")
      
    permits["Multiplier"] = units_multipliers
    permits["Overwrite"] = units_overwrites
    
    # ------------------------------------------------------------------------
    # The permits data frame can now be saved as our results; in future
    # processing, we can bind this reference table back to the permits on
    # lu_match_field and permits_units_field
    print("")
    print("Writing results")
    
    # First, we create the save path by joining a fixed name to the provided
    # save directory. If we're saving to a .gdb, we won't need a file
    # extension on the name; otherwise, we will, and we'll save as a .csv
    print("-- setting a save path")
    
    gdb = bool(re.search("\.gdb", save_directory))
    if gdb == True:
        save_path = os.path.join(save_directory,
                                 "permits_units_reference_table")
    else:
        save_path = os.path.join(save_directory,
                                 "permits_units_reference_table.csv")
        
    # Now we just write appropriately
    print("-- saving the reference table")
    
    if gdb == True:
        permits_array = np.rec.fromrecords(recList = permits.values,
                                           names = permits.dtypes.index.tolist())
        permits_array = np.array(permits_array)
        arcpy.da.NumPyArrayToTable(in_array = permits_array,
                                   out_table = save_path)
    else:
        permits.to_csv(path_or_buf = save_path,
                       index = False)
        
    # ------------------------------------------------------------------------
    # We're done!
    print("")
    print("Done!")
    print("Results saved to: " + save_path)
    print("")
    return(save_path)
        
    
        
    
# %% Main
if __name__ == "__main__":
    # Inputs
    parcels_path = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\parcels.gdb\Miami_2019"
    permits_path = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\BuildingPermits\Miami_Dade_BuildingPermits.shp"
    lu_match_field = "DOR_UC"
    parcels_living_area_field = "TOT_LVG_AREA"
    permits_units_field = "UNITS"
    permits_living_area_name = "sq. ft."
    save_directory = r"K:\Projects\MiamiDade\PMT\Data\Reference"
    units_match_dict = {"bed": "NO_RES_UNTS",
                        "room": "NO_RES_UNTS",
                        "unit": "NO_RES_UNTS",
                        "acre": "LND_SQFOOT / 43560"}
    
    # Function
    prep_permits_units_reference(parcels_path = parcels_path,
                                 permits_path = permits_path,
                                 lu_match_field = lu_match_field,
                                 parcels_living_area_field = parcels_living_area_field,
                                 permits_units_field = permits_units_field,
                                 permits_living_area_name = permits_living_area_name,
                                 save_directory = save_directory,
                                 units_match_dict = units_match_dict)
    
    

    