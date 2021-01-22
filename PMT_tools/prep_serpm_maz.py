"""
Created: December 2020
@Author: Alex Bell


"""

# %% IMPORTS
import PMT
import arcpy
import pandas as pd
import os
from six import string_types

# %% GLOBALS
MODEL_YEARS = [2015, 2045]
BASE_FIELDS = ["TAZ", "HH", "POP"]
CONSOLIDATE_COLS={
    "TotalJobs": [
        "emp_total"
        ],
    "Consumer": [
        "emp_retail",
        "emp_amusement",
        "emp_hotel",
        "emp_restaurant_bar",
        "emp_personal_svcs_retail",
        "emp_state_local_gov_ent"
        ],
    "Education": [
        "emp_pvt_ed_k12",
        "emp_pvt_ed_post_k12_oth",
        "emp_public_ed"
        ],
    "HealthCare": [
        "emp_health"
        ],
    "IndLogist": [
        "emp_mfg_prod",
        "emp_mfg_office",
        "emp_whsle_whs",
        "emp_trans"
        ],
    "Office": [
        "emp_prof_bus_svcs",
        "emp_personal_svcs_office",
        "emp_state_local_gov_white",
        "emp_own_occ_dwell_mgmt",
        "emp_fed_gov_accts",
        "emp_st_lcl_gov_accts",
        "emp_cap_accts"
        ],
    "Other": [
        "emp_const_non_bldg_prod",
        "emp_const_non_bldg_office",
        "emp_utilities_prod",
        "emp_utilities_office",
        "emp_const_bldg_prod",
        "emp_const_bldg_office",
        "emp_prof_bus_svcs_bldg_maint",
        "emp_religious",
        "emp_pvt_hh",
        "emp_scrap_other",
        "emp_fed_non_mil",
        "emp_fed_mil",
        "emp_state_local_gov_blue"
        ],
    "Resource": [
        "emp_ag"
        ],
    "EnrollAdlt": [
        "collegeEnroll",
        "otherCollegeEnroll",
        "AdultSchEnrl"
        ],
    "EnrollK12": [
        "EnrollGradeKto8",
        "EnrollGrade9to12",
        "PrivateEnrollGradeKto8"
        ]
}



# %% FUNCTIONS
def _consolidateCols(df, base_fields, consolidate_cols):
    if isinstance(base_fields, string_types):
        base_fields = [base_fields]

    clean_cols = base_fields + list(consolidate_cols.keys())
    for out_col in consolidate_cols.keys():
        sum_cols = consolidate_cols[out_col]
        df[out_col] = df[sum_cols].sum(axis=1)
    clean_df = df[clean_cols].copy()
    return clean_df


def cleanMAZ(se_table, se_id_field, maz_fc, fc_id_field, out_fc,
             base_fields=None, consolidate_cols={}, overwrite=False):
    """
    Creates a feature class of MAZ features and joins attributes from an
    MAZ-level table of socio-economic and demographic data. Columns in the
    source table can be consolidated using a dictionary of column
    specifications.

    Parameters
    -----------
    se_table: Path
    se_id_field: String
    maz_fc: Path
    fc_id_field: String
    out_fc: String
    base_fields: [String, ...], default=None
    consolidate_cols: {string: [string, ...], ...}, default={}
    overwrite: Boolean, default=False

    Returns
    ---------
    out_fc: Path
    """
    # Check for output
    if arcpy.Exists(out_fc):
        if overwrite:
            print(f"... deleting existing file {out_fc}")
            arcpy.Delete_management(out_fc)
        else:
            raise RuntimeError(f"Output {out_fc} already exists")

    # Copy features
    print("... Creating output feature class")
    out_path, out_name = os.path.split(out_fc)
    arcpy.FeatureClassToFeatureClass_conversion(maz_fc, out_path, out_name)

    # Read MAZ table
    print("... reading MAZ rows")
    maz_df = pd.read_csv(se_table)

    # Clean up columns
    if consolidate_cols:
        if base_fields:
            # Listify if needed
            if isinstance(base_fields, string_types):
                base_fields = [base_fields]
            # Extend list of columns to keep to include the maz_id_field
            bf = [se_id_field] + base_fields
        else:
            bf = []
        clean_df = _consolidateCols(maz_df, bf, consolidate_cols)
    else:
        clean_df = maz_df.copy()

    # Extend table
    print("... joining columns to output features")
    PMT.extendTableDf(out_fc, fc_id_field, clean_df, se_id_field)

    return out_fc


def summarizeMAZtoTAZ(se_table, maz_id_field, taz_id_field, out_table,
                      base_fields=None, consolidate_cols={},
                      overwrite=False):
    """
    Summarizes socio-economic and demographic data in an MAZ-level table
    to the TAZ level, and exports results to a new csv output. Columns
    in the source table can be consolidated using a dictionary of column
    specifications.

    Parameters
    ------------
    se_table: Path
    maz_id_field: String
    taz_id_field: String
    out_table: Path
    base_fields: [String, ...], default=None
    consolidate_cols: {string: [string, ...], ...}, default={}
    overwrite: Boolean, default=False

    Returns
    --------
    out_table: Path
    """
    # Check for output
    if arcpy.Exists(out_table):
        if overwrite:
            print(f"... deleting existing file {out_table}")
            arcpy.Delete_management(out_table)
        else:
            raise RuntimeError(f"Output {out_table} already exists")

    # Read MAZ table
    print("... reading MAZ rows")
    maz_df = pd.read_csv(se_table)

    # Clean up columns
    if consolidate_cols:
        if base_fields:
            # Listify if needed
            if isinstance(base_fields, string_types):
                base_fields = [base_fields]
            # Extend list of columns to keep to include the maz_id_field
            bf = [maz_id_field] + base_fields
        else:
            bf = []
        clean_df = _consolidateCols(maz_df, bf, consolidate_cols)
    else:
        clean_df = maz_df.copy()
    
    # group_by and export
    print("... storing summarized output table")
    sum_df = clean_df.groupby(taz_id_field).sum().reset_index()
    PMT.dfToTable(sum_df, out_table)

    return out_table

def mazToCentroid(maz_fc, fields, out_fc, overwrite=False):
    """
    A simple function to create centroid points from MAZ polygon features,
    retaining the specified fields.

    Parameters
    -----------
    maz_fc: Path
    fields: [String, ...]
    out_fc: Path
    overwrite: Boolean, default=False

    Returns
    -------
    out_fc: Path
    """
    print("... Dumping features to centroids")
    # Check for output
    if arcpy.Exists(out_fc):
        if overwrite:
            print(f"... deleting existing file {out_fc}")
            arcpy.Delete_management(out_fc)
        else:
            raise RuntimeError(f"Output {out_fc} already exists")

    if isinstance(fields, string_types):
        fields = [fields]
    fields.append("SHAPE@XY")
    _fields = list(set(fields))
    # Dump to array
    sr = arcpy.Describe(maz_fc).spatialReference
    a = arcpy.da.FeatureClassToNumPyArray(maz_fc, _fields)
    # Export output
    arcpy.da.NumPyArrayToFeatureClass(
        a, out_fc, "SHAPE@XY", spatial_reference=sr)
    return out_fc

# %% MAIN
if __name__ == "__main__":
    maz_fc = PMT.makePath(PMT.RAW, "SERPM", "SERPM8MAZ_NAD83_170502.shp")
    fc_id_field = "MAZ"
    for year in MODEL_YEARS:
        print(year)
        se_table = PMT.makePath(PMT.RAW, "SERPM", f"maz_data_{year}.csv")
        se_id_field = "mgra"
        # Export MAZ shapes with attributes
        print("-- to shape")
        out_fc = PMT.makePath(PMT.CLEANED, "SERPM", f"maz_{year}.shp")
        base_fields = ["TAZ"]
        cleanMAZ(se_table, se_id_field, maz_fc, fc_id_field, out_fc,
                 base_fields=BASE_FIELDS, consolidate_cols=CONSOLIDATE_COLS,
                 overwrite=True)
        # Summarize MAZs to TAZs with attributes
        print("-- TAZ summary table")
        out_table = PMT.makePath(PMT.CLEANED, "SERPM", f"taz_{year}.dbf")
        summarizeMAZtoTAZ(se_table, se_id_field, "TAZ", out_table,
                          base_fields=BASE_FIELDS,
                          consolidate_cols=CONSOLIDATE_COLS,
                          overwrite=True)
    # Dump MAZ shapes to centroids
    maz_c_fc = out_fc[:]
    out_fc = PMT.makePath(PMT.CLEANED, "SERPM", "maz_centroids.shp")
    mazToCentroid(maz_c_fc, [fc_id_field], out_fc, overwrite=True)



