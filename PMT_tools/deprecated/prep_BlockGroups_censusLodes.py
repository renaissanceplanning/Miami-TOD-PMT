"""
Created: October 2020
@Author: Alex Bell

Move raw block group features and census tables to a cleaned folder
adding key census and LODES estimates of demographic, economic, and other
PMT data in the process.

If run as "main" LODES jobs estimates and ACS race and commute data are
joined to block group features and exported to the `block_groups` gdb in 
the PMT's "cleaned" data folder. If the gdb already exists, it is deleted
and replaced with the output of this script.

###
DEPRECATED as we now have a mechanism for joining data in the analysis steps
###
"""
# %% IMPORTS
import PMT_tools.PMT as PMT
import pandas as pd
import geopandas as gpd
from six import string_types
from collections.abc import Iterable
import arcpy
import os
import re

# %% GLOBALS
# The DTYPE global variable initializes a dictionary that ensures expected
# columns in csv files are read in as strings rather than numeric fields.
# The names for these columns are set in `dl_acs_tables.py` and `dl_LODES.R`
DTYPE = {"GEOID10": str,
         "GEOID": str,
         "state": str,
         "county": str,
         "tract": str,
         "block group": str,
         "createdate": str}


# %% FUNCTIONS
def joinBGData(in_fc, in_fc_id, join_tables, on_fields, out_fc, dtype={},
               **kwargs):
    #TODO: handle column naming collisions and update docstring
    """
    Starting with block group features and a collection of raw data tables
    (from ACS, LODES, etc.), join attributes in a single cleaned feature
    class. If `join_tables` have common column_names, the values from the
    last table joined will appear in the output feature class.

    Parameters
    -----------
    in_fc: Path
    in_fc_id: String
    join_tables: [Path,...] or Path
    on_fields: [String,...] or String
    out_fc: Path
    dtype: dict

    Returns
    --------
    out_fc: Path
    """
    # Handle inputs
    # - join_tables
    if isinstance(join_tables, string_types):
        join_tables = [join_tables]
    elif not isinstance(join_tables, Iterable):
        raise TypeError(
            f"Expected string or iterable for `join_tables`, got {type(join_tables)}")
    # - on_fields
    if isinstance(on_fields, string_types):
        on_fields = [on_fields for _ in join_tables]
    elif not isinstance(on_fields,  Iterable):
        raise TypeError(
            f"Expected string or iterable for `on_fields`, got {type(on_fields)}")
    
    # Read the data
    ##gdf = gpd.read_file(in_fc)

    # Read input tables
    # Update to use arcpy
    # for join_table, on_field in zip(join_tables, on_fields):
    #     df = pd.read_csv(join_table, dtype=dtype, **kwargs)
    #     # - Merge to gdf
    #     gdf = gdf.merge(df, how="left", left_on=in_fc_id, right_on=on_field)
    
    # # Export output
    # gdf.to_file(out_fc)

    # Push input features to output fc
    print("...copying geometries")
    out_folder, out_name = os.path.split(out_fc)
    arcpy.FeatureClassToFeatureClass_conversion(in_fc, out_folder, out_name)

    # Extend with csv tables
    print("...joining attributes")
    for join_table, on_field in zip(join_tables, on_fields):
        print(f"... ...{join_table}")
        df = pd.read_csv(join_table, dtype=dtype, **kwargs)
        cols = df.columns.to_list()
        df.columns = [re.sub("[^a-zA-Z0-9_]", "_", c) for c in cols]
        PMT.extend_table_df(out_fc, in_fc_id, df, on_field, append_only=False)

    return out_fc

def prepBlockGroups(raw_dir, out_gdb, years, overwrite=False, dtype={}):
    """
    ...

    Parameters
    ------------
    raw_dir: Path
    out_gdb: Path (to gdb)
    years: [Int,...]
    overwrite: Boolean, default=False
    dtype: dict

    Returns
    -------
    None
        Block group feature classes for each year in `years` are written to
        `out_gdb`.
    """
    if arcpy.Exists(out_gdb):
        if overwrite:
            arcpy.Delete_management(out_gdb)
        else:
            raise RuntimeError(f"out_gdb `{out_gdb}` already exists")
    out_folder, out_name = os.path.split(out_gdb)
    arcpy.CreateFileGDB_management(out_folder, out_name)
    
    # Push data for each year
    bg_path = PMT.makePath(raw_dir, "BlockGroups")
    
    for year in years:
        print(year)
        bg_features = PMT.makePath(bg_path, "CensusBG.shp")
        LODES_table = PMT.makePath(bg_path, f"LODES_{year}_jobs.csv")
        race_table = PMT.makePath(bg_path, f"ACS_{year}_race.csv")
        mode_table = PMT.makePath(bg_path, f"ACS_{year}_commute.csv")
    
        # Check to see if the year has available jobs and demographic data
        #  if data are unavailable, these columns will be developed through
        #  modeling.
        join_tables = []
        on_fields = []
        jobs_source = '"EXTRAP"'
        dem_source = '"EXTRAP"'
        # - Jobs
        if arcpy.Exists(LODES_table):
            join_tables.append(LODES_table)
            on_fields.append("GEOID10")
            jobs_source = '"LODES"'
        # - Demographic data
        if arcpy.Exists(race_table):
            # If the race table was found, the commute table should be there too
            join_tables += [race_table, mode_table]
            on_fields += ["GEOID10", "GEOID10"]
            dem_source = '"ACS"'
        # If any join data are found, run the joinBGData function
        out_fc = PMT.makePath(out_gdb, f"BlockGroups_{year}")
        if join_tables:
            out_fc = joinBGData(bg_features, "GEOID10", join_tables,
                                on_fields, out_fc, dtype=dtype)
        else:
            out_fc = PMT.copy_features(bg_features, out_fc)
        
        # Add a column to the output feature class to indicate extapolation
        #  requirements
        arcpy.AddField_management(out_fc, "JOBS_SOURCE", "TEXT", field_length=10)
        arcpy.AddField_management(out_fc, "DEM_SOURCE", "TEXT", field_length=10)
        arcpy.CalculateField_management(out_fc, "JOBS_SOURCE", jobs_source)
        arcpy.CalculateField_management(out_fc, "DEM_SOURCE", dem_source)


# %% MAIN
if __name__ == "__main__":
    # GDB 
    out_gdb = PMT.makePath(PMT.CLEANED, "BlockGroups.gdb")
    prepBlockGroups(PMT.RAW, out_gdb, PMT.YEARS, overwrite=True, dtype=DTYPE)

