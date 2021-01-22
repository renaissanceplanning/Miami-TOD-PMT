# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 13:26:43 2020

@author: AZ7
"""

# %% Imports

import arcpy
import pandas as pd
import os
import numpy as np
import re

# %% Main
def prep_short_term_parcels(parcels_path,
                            permits_path,
                            permits_reference_path,
                            parcels_living_area_field,
                            permits_units_field,
                            permits_values_field,
                            id_match_field,
                            lu_match_field,
                            save_gdb_location,
                            units_field_match_dict={}):
    
    # ------------------------------------------------------------------------
    # First, we need to initialize a save feature class. The feature class
    # will be a copy of the parcels with a unique ID (added by the function)
    print("")
    print("Initializing a save feature class")
    
    # Add a unique ID field to the parcels called "ProcessID"
    print("-- adding a unique ID field for individual parcels")
    
    codeblock = 'val = 0 \ndef processID(): \n    global val \n    start = 1 \n    if (val == 0):  \n        val = start\n    else:  \n        val += 1  \n    return val'
    arcpy.AddField_management(in_table = parcels_path,
                              field_name = "ProcessID",
                              field_type = "LONG",
                              field_is_required = "NON_REQUIRED")
    arcpy.CalculateField_management(in_table = parcels_path,
                                    field = "ProcessID",
                                    expression = "processID()",
                                    expression_type = "PYTHON3",
                                    code_block = codeblock)
    
    # Copy the parcels to a new feature class, only retaining the Proce
    print("-- copying modified parcels to new feature class")
    # Thanks to: https://gis.stackexchange.com/questions/229187/copying-only-certain-fields-columns-from-shapefile-into-new-shapefile-using-mode
    
    fmap = arcpy.FieldMappings()
    fmap.addTable(parcels_path)
    fields = {f.name: f for f in arcpy.ListFields(parcels_path)}
    for fname, fld in fields.items():
        if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
            if fname != "ProcessID":
                fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
    arcpy.conversion.FeatureClassToFeatureClass(in_features = parcels_path, 
                                                out_path = save_gdb_location,
                                                out_name = "short_term_parcels",
                                                field_mapping = fmap) 
    save_path = os.path.join(save_gdb_location,
                             "short_term_parcels")
    
    # ------------------------------------------------------------------------
    # Next, we need to read in all of our data. First, we'll read the parcels
    # (after which we'll remove the added unique ID from the original data).
    # Then we'll read the permits and permits reference table, and join the
    # two together on the permits_lu_field and permits_units_field
    print("")
    print("Loading data")
    
    # Read the parcels
    print("-- reading/formatting parcels")
    
    parcels_sr = arcpy.Describe(parcels_path).spatialReference
    parcels_fields = [f.name for f in arcpy.ListFields(parcels_path)]
    parcels_fields = [f for f in parcels_fields if f != "Shape"]
    # parcels_fields = [id_match_field,
    #                   lu_match_field,
    #                   parcels_living_area_field]
    # unit_match_fields = np.unique([v for v in units_field_match_dict.values()]).tolist()
    # parcels_fields = parcels_fields + unit_match_fields
    parcels = arcpy.da.FeatureClassToNumPyArray(in_table = parcels_path,
                                                field_names = parcels_fields,
                                                spatial_reference = parcels_sr,
                                                null_value = 0)
    parcels = pd.DataFrame(parcels)
    arcpy.DeleteField_management(parcels_path,
                                 "ProcessID")
    
    # Read the permits
    print("-- reading/formatting permits")
    
    permits_sr = arcpy.Describe(permits_path).spatialReference
    permits_fields = [id_match_field, 
                      lu_match_field,
                      permits_units_field,
                      permits_values_field]
    permits = arcpy.da.FeatureClassToNumPyArray(in_table = permits_path,
                                                field_names = permits_fields,
                                                spatial_reference = permits_sr,
                                                null_value = 0)
    permits = pd.DataFrame(permits)
    permits = permits[permits[permits_values_field] >= 0]    
    # Read the permits reference
    print("-- reading/formatting permits reference")
    
    if bool(re.search("\.gdb", permits_reference_path)) == True:
        ref_fields = [f.name for f in arcpy.ListFields(permits_reference_path)]
        ref = arcpy.da.TableToNumPyArray(in_table = permits_reference_path,
                                         field_names = ref_fields)
    else:
        ref = pd.read_csv(permits_reference_path)
    
    # Merge the permits and permits reference
    print("-- merging permits and permits reference")
    
    permits = pd.merge(permits,
                       ref,
                       left_on = [lu_match_field, permits_units_field],
                       right_on = [lu_match_field, permits_units_field],
                       how = "left")
    
    # Now we add in field matches to the parcels (which will be helpful come
    # the time of updating parcels from the permits)
    
    if units_field_match_dict is not None:
        print("-- joining units-field matches")
        
        ufm = pd.DataFrame.from_dict(units_field_match_dict, orient="index").reset_index()
        ufm.columns = [permits_units_field, "Parcel_Field"]
        permits = pd.merge(permits,
                           ufm,
                           left_on = permits_units_field,
                           right_on = permits_units_field,
                           how = "left")
    
    # ------------------------------------------------------------------------
    # Next, we need to calculate the new building square footage associated 
    # with parcel in the permit features using the reference table multipliers
    # and overwrites   
    print("")
    print("Calculating total living area for new development")
    
    # First, we calculate those new values. If a multiplier is given, take
    # value * multiplier. Otherwise, use the overwrite value. If both the
    # multiplier and overwrite are NA, return a 0
    print("-- applying unit multipliers and overwrites")
    
    z = zip(permits[permits_values_field], permits["Multiplier"], permits["Overwrite"])
    new_living_area = [v * m if np.isnan(o) else o for v,m,o in z]
    new_living_area = [0 if np.isnan(n) else n for n in new_living_area]
    
    # The results are a list with the same order as the permits data frame
    # index, so we can add the new values back as a new column
    print("-- appending new living area values to permits data")
    
    permits["UpdTLA"] = new_living_area 
    permits.drop(columns = ["Multiplier", "Overwrite"],
                 inplace = True)
    
    # ------------------------------------------------------------------------
    # Next, we need to update the parcels with the info from the permits. We
    # match building permits to the parcels using id_match_field, and
    # overwrite parcel LU, building square footage, and anything specified in
    # the match dict with the information from the permits. We'll also add a
    # flag for replacement
    print("")
    print("Updating parcels")
    
    # We use iteration to update the parcels. For each parcel ID, we collect
    # and organize the relevant information that we'll use to overwrite the
    # parcel data for these parcel IDs
    print("-- collecting updated parcel data")
    
    pids = np.unique(permits[id_match_field])
    upd = []
    
    for i in pids:
        # Subset permits to only the ones with the iteration's parcel id
        df = permits[permits[id_match_field] == i]
        
        # If we have more than one row, we'll need to do some summarization.
        # If we have just one row, all we'll need to do is reformat
        if len(df.index) > 1:
            # Series for the parcel ID
            pid = pd.Series(i,
                            index = [id_match_field])
            # Living area by land use
            tla = df.groupby(lu_match_field)["UpdTLA"].agg("sum").reset_index()
            # Series for land use [with most living area]
            lu = pd.Series(tla[lu_match_field][np.argmax(tla["UpdTLA"])],
                           index = [lu_match_field])
            # Series for living area (total across all land uses)
            ba = pd.Series(sum(tla["UpdTLA"]),
                           index = [parcels_living_area_field])
            # Series for other fields (from units-field match)
            others = df.groupby("Parcel_Field")[permits_values_field].agg("sum")
            # Return as 1-row data frame
            df = pd.DataFrame(pd.concat([pid, lu, ba, others], axis = 0)).T
        else:
            # Rename columns to match parcels
            df.rename(columns = {"UpdTLA": parcels_living_area_field,
                                 permits_values_field: df.Parcel_Field.values[0]},
                      inplace = True)
            # Drop unnecessary columns (including nulls from units-field match)
            df.drop(columns = [permits_units_field, "Parcel_Field"],
                    inplace = True)
            df = df.loc[:, df.columns.notnull()]
        
        # Append the results to our storage list
        upd.append(df)
    
    # Now we just merge up the rows. We'll also add a column to indicated that
    # these are update parcels from the permits
    upd = pd.concat(upd, axis = 0).reset_index(drop=True)
    upd.fillna(0, 
               inplace = True)
    upd["Permit"] = 1
    
    # Now, we make the replacements. This is pretty simple: we drop all the
    # rows from the parcels whose IDs are in the permits, and add all the
    # rows for the data we just collected. However, we'll need to be sure
    # we retain the process ID from the parcels we're dropping for the sake
    # of joining
    print("-- replacing parcel data with updated information")
    
    to_drop = parcels[parcels[id_match_field].isin(pids)]
    process_ids = to_drop.groupby(id_match_field)["ProcessID"].min().reset_index()
    upd = pd.merge(upd,
                   process_ids,
                   left_on = id_match_field,
                   right_on = id_match_field,
                   how = "left")
    pu = parcels[~parcels[id_match_field].isin(pids)]
    pu["Permit"] = 0
    final_update = pd.concat([pu, upd], axis = 0).reset_index(drop=True)
        
    # ------------------------------------------------------------------------
    # Finally, we write the results by joining them back to the initialized
    # save feature class on the unique ID. After joining, we can remove the
    # unique ID
    print("")
    print("Writing results")
    
    # First, we have to turn the parcels to a numpy array (to be able to use
    # extend table)
    print("-- formatting results for write")
    
    final_update.drop(columns = ["OBJECTID", "Shape_Length", "Shape_Area"],
                      inplace = True)
    df_et = np.rec.fromrecords(recList = final_update.values, 
                               names = final_update.dtypes.index.tolist())
    df_et = np.array(df_et)
    
    # Now, we use extend table to join the results back to our initialized
    # save feature class. Once completed, we also delete the ProcessID from 
    # this feature class because we were only using it for the merge
    print("-- joining results to save feature class (be patient, this will take a while")
    
    arcpy.da.ExtendTable(in_table = save_path,
                         table_match_field = "ProcessID",
                         in_array = df_et,
                         array_match_field = "ProcessID")    
    arcpy.DeleteField_management(in_table = save_path,
                                 drop_field = "ProcessID")
    
    # ------------------------------------------------------------------------
    # Then we're done!
    print("")
    print("Done!")
    print("Results saved to: " + save_path)
    print("")
    
# %% Main
if __name__ == "__main__":
    # Inputs
    parcels_path = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\parcels.gdb\Miami_2019"
    permits_path = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\BuildingPermits\Miami_Dade_BuildingPermits.shp"
    permits_reference_path = r"K:\Projects\MiamiDade\PMT\Data\Reference\permits_units_reference_table.csv"
    parcels_living_area_field = "TOT_LVG_AREA"
    permits_units_field = "UNITS"
    permits_values_field = "UNITS_VAL"
    id_match_field = "PARCELNO"
    lu_match_field = "DOR_UC"
    save_gdb_location = r"K:\Projects\MiamiDade\PMT\Data\PMT_Trend.gdb\parcels"
    units_field_match_dict = {"bed": "NO_RES_UNTS",
                              "room": "NO_RES_UNTS",
                              "unit": "NO_RES_UNTS"}
    
    # Function
    prep_short_term_parcels(parcels_path = parcels_path,
                            permits_path = permits_path,
                            permits_reference_path = permits_reference_path,
                            parcels_living_area_field = parcels_living_area_field,
                            permits_units_field = permits_units_field,
                            permits_values_field = permits_values_field,
                            id_match_field = id_match_field,
                            lu_match_field = lu_match_field,
                            save_gdb_location = save_gdb_location,
                            units_field_match_dict = units_field_match_dict)
    