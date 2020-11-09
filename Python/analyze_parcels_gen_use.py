"""
Created: October 2020
@Author: Alex Bell

Defines a function to join generalized parcel land use codes from a csv table
to a feature class.

If run as "main", land use codes and parcel valuation fields for Miami-Dade
County are exported to year-specific PMT geodatabases (analysis outputs).
"""

# %% IMPORTS
import PMT
import arcpy
import pandas as pd
import os

# %% GLOBALS
KEEP_FIELDS = [
    "PARCELNO", 
    "ASMNT_YR",
    "DOR_UC",
    "JV",
    "JV_CHNG",
    "JV_CHNG_CD",
    "AV_SD",
    "AV_NSD",
    "TV_SD",
    "TV_NSD",
    "JV_HMSTD",
    "AV_HMSTD",
    "JV_NON_HMSTD_RESD",
    "AV_NON_HMSTD_RESD",
    "JV_RESD_NON_RESD",
    "AV_RESD_NON_RESD",
    "JV_CLASS_USE",
    "AV_CLASS_USE",
    "JV_H2O_RECHRGE",
    "AV_H2O_RECHRGE",
    "JV_CONSRV_LND",
    "AV_CONSRV_LND",
    "JV_HIST_COM_PROP",
    "AV_HIST_COM_PROP",
    "JV_HIST_SIGNF",
    "AV_HIST_SIGNF",
    "JV_WRKNG_WTRFNT",
    "AV_WRKNG_WTRFNT",
    "NCONST_VAL",
    "DEL_VAL",
    "LND_VAL",
    "LND_SQFOOT",
    "CONST_CLASS",
    "EFF_YR_BLT",
    "ACT_YR_BLT",
    "TOT_LVG_AREA",
    "NO_BULDNG",
    "NO_RES_UNTS"
    ]


# %% FUNCTION
def generalizeLandUseByParcel(parcel_fc, out_fc, ref_table,
                              field_mappings=None, par_code="DOR_UC",
                              join_code="DOR_UC", dtype={}, 
                              overwrite=False, **kwargs):
    """
    """
    # Read in the reference table
    ref_table = pd.read_csv(ref_table, dtype=dtype, **kwargs)

    # Copy parcel_fc to out_fc
    if arcpy.Exists(out_fc):
        if overwrite:
            arcpy.Delete_management(out_fc)
        else:
            raise RuntimeError(f"Output {out_fc} already exists")
    print("...copying parcels to analysis gdb")
    out_ws, out_name = os.path.split(out_fc)
    arcpy.FeatureClassToFeatureClass_conversion(parcel_fc, out_ws, out_name,
                                                field_mapping=field_mappings)
    
    # Extend table
    print("...extending table")
    try:
        PMT.extendTableDf(out_fc, par_code, ref_table, join_code)
    except:
        print("... ... error, rolling back")
        arcpy.Delete_management(out_fc)
        raise


# %% MAIN
if __name__ == "__main__":
    for year in PMT.YEARS:
        print(year)
        # Parcels
        parcel_fc = PMT.makePath(PMT.CLEANED, "parcels.gdb", f"Miami_{year}")
        # Reference table
        ref_table = PMT.makePath(PMT.REF, "Land_Use_Recode.csv")
        # Output
        out_fc = PMT.makePath(
            PMT.ROOT, f"PMT_{year}.gdb", "Parcels", "land_use_and_value")
        # Prepare field map
        FM = arcpy.FieldMappings()
        FM.addTable(parcel_fc)
        for KF in KEEP_FIELDS:
            fm = arcpy.FieldMap()
            try:
                fm.addInputField(parcel_fc, KF)
                fm.outputField.name = KF
                fm.outputField.aliasName = KF
                FM.addFieldMap(fm)
            except:
                print(f"-- no input field {KF}")
            
        # Run it
        par_code = "DOR_UC"
        join_code="DOR_UC"
        dtype={"DOR_UC": int}
        generalizeLandUseByParcel(parcel_fc, out_fc, ref_table,
                                  field_mappings=FM, par_code=par_code,
                                  join_code=join_code, dtype=dtype,
                                  overwrite=True)

