# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 13:26:43 2020

@author: AZ7
"""

# %% Imports

import fnmatch

import arcpy
import numpy as np
import pandas as pd

import PMT_tools.PMT as PMT


# %% Main
def build_short_term_parcels(parcels_path, permits_path, permits_reference_path,
                             parcels_living_area_field, permits_units_field, permits_values_field,
                             id_match_field, lu_match_field, save_gdb_location, units_field_match_dict={}):
    # ------------------------------------------------------------------------
    # First, we need to initialize a save feature class. The feature class
    # will be a copy of the parcels with a unique ID (added by the function)
    print("")
    print("Initializing a save feature class")

    # Add a unique ID field to the parcels called "ProcessID"
    print("...adding a unique ID field for individual parcels")
    # creating a temporary copy of parcels
    temp_parcels = PMT.makePath("in_memory", "temp_parcels")
    arcpy.FeatureClassToFeatureClass_conversion(in_features=parcels_path, out_path="in_memory", out_name="temp_parcels")
    process_id_field = PMT.add_unique_id(feature_class=temp_parcels)

    print("...reading/formatting parcels")
    # read in all of our data
    #   - read the parcels (after which we'll remove the added unique ID from the original data). T
    parcels_sr = arcpy.Describe(temp_parcels).spatialReference
    parcels_fields = [f.name for f in arcpy.ListFields(temp_parcels) if not f.required]
    parcels_arr = arcpy.da.FeatureClassToNumPyArray(in_table=temp_parcels, field_names=parcels_fields,
                                                    spatial_reference=parcels_sr, null_value=0)
    parcels_df = pd.DataFrame(parcels_arr)

    # create output dataset keeping only process_id and delete temp file
    fmap = arcpy.FieldMappings()
    fm = arcpy.FieldMap()
    fm.addInputField(temp_parcels, process_id_field)
    fmap.addFieldMap(fm)
    short_term_parcels = arcpy.FeatureClassToFeatureClass_conversion(
        in_features=temp_parcels, out_path=save_gdb_location, out_name="short_term_parcels", field_mapping=fmap
    )[0]
    arcpy.Delete_management(in_data=temp_parcels)

    #   - read the permits and permits reference table,
    print("...reading/formatting permits_df")
    permits_sr = arcpy.Describe(permits_path).spatialReference
    permits_fields = [id_match_field, lu_match_field, permits_units_field, permits_values_field]
    permit_array = arcpy.da.FeatureClassToNumPyArray(in_table=permits_path, field_names=permits_fields,
                                                     spatial_reference=permits_sr, null_value=0)
    permits_df = pd.DataFrame(permit_array)
    permits_df = permits_df[permits_df[permits_values_field] >= 0]

    print("...reading/formatting permits reference")
    if fnmatch.fnmatch(name=permits_reference_path, pat="*gdb*"):
        ref_fields = [f.name for f in arcpy.ListFields(permits_reference_path)]
        ref_array = arcpy.da.TableToNumPyArray(in_table=permits_reference_path,
                                               field_names=ref_fields)
        ref_df = pd.DataFrame(ref_array)
    else:
        ref_df = pd.read_csv(permits_reference_path)

    # Merge the permits_df and permits_df reference
    #   - join the two together on the permits_lu_field and permits_units_field
    print("...merging permits_df and permits_df reference")
    permits_df = pd.merge(left=permits_df, right=ref_df,
                          left_on=[lu_match_field, permits_units_field],
                          right_on=[lu_match_field, permits_units_field], how="left")

    # Now we add in field matches to the parcels (which will be helpful come
    # the time of updating parcels from the permits_df)
    if units_field_match_dict is not None:
        print("...joining units-field matches")
        ufm = pd.DataFrame.from_dict(units_field_match_dict, orient="index").reset_index()
        ufm.columns = [permits_units_field, "Parcel_Field"]
        permits_df = pd.merge(left=permits_df, right=ufm,
                              left_on=permits_units_field, right_on=permits_units_field, how="left")

    # calculate the new building square footage for parcel in the permit features
    # using the reference table multipliers and overwrites
    print("...applying unit multipliers and overwrites")
    new_living_area = []
    for value, multiplier, overwrite in zip(
            permits_df[permits_values_field],
            permits_df["Multiplier"],
            permits_df["Overwrite"]):
        if np.isnan(overwrite):
            new_living_area.append(value * multiplier)
        elif overwrite:
            new_living_area.append(overwrite)
    new_living_area = [0 if np.isnan(n) else n for n in new_living_area]

    print("...appending new living area values to permits_df data")
    permits_df["UpdTLA"] = new_living_area
    permits_df.drop(columns=["Multiplier", "Overwrite"], inplace=True)

    # update the parcels with the info from the permits_df
    #   - match building permits_df to the parcels using id_match_field,
    #   - overwrite parcel LU, building square footage, and anything specified in the match dict
    #   - add replacement flag
    print("...collecting updated parcel data")
    pids = np.unique(permits_df[id_match_field])
    update = []
    for i in pids:
        df = permits_df[permits_df[id_match_field] == i]
        if len(df.index) > 1:
            pid = pd.Series(i, index=[id_match_field])
            # Living area by land use
            total_living_area = df.groupby(lu_match_field)["UpdTLA"].agg("sum").reset_index()
            # Series for land use [with most living area]
            land_use = pd.Series(total_living_area[lu_match_field][np.argmax(total_living_area["UpdTLA"])],
                                 index=[lu_match_field])
            # Series for living area (total across all land uses)
            ba = pd.Series(sum(total_living_area["UpdTLA"]), index=[parcels_living_area_field])
            # Series for other fields (from units-field match)
            others = df.groupby("Parcel_Field")[permits_values_field].agg("sum")
            df = pd.DataFrame(pd.concat([pid, land_use, ba, others], axis=0)).T
        else:
            # Rename columns to match parcels
            df.rename(columns={"UpdTLA": parcels_living_area_field,
                               permits_values_field: df.Parcel_Field.values[0]},
                      inplace=True)
            # Drop unnecessary columns (including nulls from units-field match)
            df.drop(columns=[permits_units_field, "Parcel_Field"], inplace=True)
            df = df.loc[:, df.columns.notnull()]
        # Append the results to our storage list
        update.append(df)

    # Now we just merge up the rows. We'll also add a column to indicated that
    # these are update parcels from the permits_df
    update = pd.concat(update, axis=0).reset_index(drop=True)
    update.fillna(0, inplace=True)
    update["Permit"] = 1

    # make the replacements. - drop all the rows from the parcels whose IDs are in the permits_df, - add all the rows
    # for the data we just collected. and retain the process ID from the parcels we're dropping for the sake of joining
    print("...replacing parcel data with updated information")
    to_drop = parcels_df[parcels_df[id_match_field].isin(pids)]
    process_ids = to_drop.groupby(id_match_field)["ProcessID"].min().reset_index()
    update = pd.merge(update, process_ids, left_on=id_match_field,
                      right_on=id_match_field, how="left")
    parcel_update = parcels_df[~parcels_df[id_match_field].isin(pids)]
    parcel_update["Permit"] = 0
    final_update = pd.concat([parcel_update, update], axis=0).reset_index(drop=True)

    print("Writing results")
    print("...formatting results for write")
    final_update.drop(columns=["OBJECTID", "Shape_Length", "Shape_Area"], inplace=True)
    print("...joining results to save feature class (be patient, this will take a while")
    PMT.extendTableDf(in_table=short_term_parcels, table_match_field=process_id_field,
                      df=final_update, df_match_field="ProcessID")

    arcpy.DeleteField_management(in_table=short_term_parcels, drop_field=process_id_field)


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
    build_short_term_parcels(parcels_path=parcels_path,
                             permits_path=permits_path,
                             permits_reference_path=permits_reference_path,
                             parcels_living_area_field=parcels_living_area_field,
                             permits_units_field=permits_units_field,
                             permits_values_field=permits_values_field,
                             id_match_field=id_match_field,
                             lu_match_field=lu_match_field,
                             save_gdb_location=save_gdb_location,
                             units_field_match_dict=units_field_match_dict)

# TODO: split this up into multiple funcitons, too many inputs to one function