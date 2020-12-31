"""
Created: December 2020
@Author: Alex Bell


"""


# %% IMPORTS
import PMT
import arcpy
import pandas as pd
import numpy as np
import os
from six import string_types

#%% trip length stats quick run
# in_table = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\SERPM\V7\from_zone_AM_2010.csv"
# usecols=["F_TAZ", "AUTO_TRIPS", "TRAN_TRIPS", "AUTO_DIST_AC", "TRAN_DIST_AC", "avg_au_time", "avg_au_dist", "avg_tr_time", "avg_tr_dist"]
# renames = ["TAZ", "TRIPS_AU", "TRIPS_TR", "VMT", "TRAN_PMT", "AVG_TIME_AU", "AVG_DIST_AU", "AVG_TIME_TR", "AVG_DIST_TR"]
# dtypes = [int, float, float, float, float, float, float, float, float]

# for year in PMT.YEARS:
#     df = pd.read_csv(in_table, usecols=usecols, dtype=dict(zip(usecols,dtypes)))
#     df.rename(columns=dict(zip(usecols, renames)), inplace=True)
#     au_fltr = df.TRIPS_AU == 0
#     tr_fltr = df.TRIPS_TR == 0
#     for mode, fltr in zip(["AU", "TR"], [au_fltr, tr_fltr]):
#         df.loc[fltr, f"AVG_TIME_{mode}"] = -1
#         df.loc[fltr, f"AVG_DIST_{mode}"] = -1
#     out_table = PMT.makePath(PMT.DATA, f"IDEAL_PMT_{year}.gdb", "TripStats_TAZ")
#     PMT.dfToTable(df, out_table, overwrite=True)


# %% GLOBALS
# GENERAL
MODES = ["Auto", "Transit", "Walk", "Bike"]
TIME_BREAKS = [15, 30, 45, 60]
UNITS = "Min"

# SERPM
BASE_YEAR = 2010
HORIZON_YEAR = 2040


# SKIMS
O_FIELD = "OName"
D_FIELD = "DName"
IMP_FIELD = "Minutes"
SKIM_DT = {
    O_FIELD: int,
    D_FIELD: int,
    IMP_FIELD: float
}
D_ACT_FIELDS = ["TotalJobs", "ConsJobs", "HCJobs", "EnrollAdlt", "EnrollK12"]
O_ACT_FIELDS = ["HH"]
# Only ever base and forecast model years
# Walk/bike networks can vary based on osm downloads (initial 5 years all the same)
NET_BY_YEAR = {  ##### PUSH THESE SPECS TO A CONFIG OR REFERENCE TABLE?
    2014: {
        "Auto": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Transit": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Walk": ["OSM_Networks", "q3_2019", "q3_2019"],
        "Bike": ["OSM_Networks", "q3_2019", "q3_2019"]
    },
    2015: {
        "Auto": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Transit": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Walk": ["OSM_Networks", "q3_2019", "q3_2019"],
        "Bike": ["OSM_Networks", "q3_2019", "q3_2019"]
    },
    2016: {
        "Auto": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Transit": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Walk": ["OSM_Networks", "q3_2019", "q3_2019"],
        "Bike": ["OSM_Networks", "q3_2019", "q3_2019"]
    },
    2017: {
        "Auto": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Transit": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Walk": ["OSM_Networks", "q3_2019", "q3_2019"],
        "Bike": ["OSM_Networks", "q3_2019", "q3_2019"]
    },
    2018: {
        "Auto": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Transit": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Walk": ["OSM_Networks", "q3_2019", "q3_2019"],
        "Bike": ["OSM_Networks", "q3_2019", "q3_2019"]
    },
    2019: {
        "Auto": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Transit": ["SERPM\\V7", BASE_YEAR, HORIZON_YEAR],
        "Walk": ["OSM_Networks", "q3_2019", "q3_2019"],
        "Bike": ["OSM_Networks", "q3_2019", "q3_2019"]
    }
}

#LAND USE
MODE_REF = { # Mode: [scale, id_field]
    "Auto": ["taz", "TAZ"],
    "Transit": ["taz", "TAZ"],
    "Walk": ["maz", "MAZ"],
    "Bike": ["maz", "MAZ"]
}

# Need 1 "snapshot" output using base auto/transit
#  - Copy into "trend" and "nearterm"
# Need 1 "longterm" output using foreacst auto/transit

# Need walk/bike for every distinct analysis year
#  - copy to years if already solved
#  - copy latest to "nearterm" and "longterm"


# %% FUNCTIONS
def createZoneFc(source_fc, out_fc, keep_fields=[], overwrite=False):
    """
    """
    # Prepare field mappings
    FM = arcpy.FieldMappings()
    #FM.addTable(clean_parcels)
    for kf in keep_fields:
        fm = arcpy.FieldMap()
        try:
            fm.addInputField(source_fc, kf)
            fm.outputField.name = kf
            fm.outputField.aliasName = kf
            FM.addFieldMap(fm)
        except:
            print(f"-- no input field {kf}")
    # Copy features
    PMT.checkOverwriteOutput(out_fc, overwrite=overwrite)
    out_path, out_name = os.path.split(out_fc)
    if keep_fields:
        arcpy.FeatureClassToFeatureClass_conversion(
            source_fc, out_path, out_name,field_mapping=FM)
    else:
        arcpy.FeatureClassToFeatureClass_conversion(
            source_fc, out_path, out_name)


def summarizeAccess(skim_table, o_field, d_field, imped_field,
                    se_data, id_field, act_fields, imped_breaks,
                    units="minutes", join_by="D", chunk_size=100000,
                    **kwargs):
    """
    Reads an origin-destination skim table, joins activity data,
    and summarizes activities by impedance bins.

    Parameters
    -----------
    skim_table: Path
    o_field: String
    d_field: String
    imped_field: String
    se_data: Path
    id_field: String
    act_fields: [String, ...]
    out_table: Path
    out_fc_field: String
    imped_breaks: [Numeric, ...]
    mode: String
    units: String, default="minutes"
    join_by: String, default="D"
    chunk_size: Int, default=100000
    kwargs:
        Keyword arguments for reading the skim table

    Returns
    --------
    out_table: Path
    """
    # Prep vars
    if isinstance(act_fields, string_types):
        act_fields = [act_fields]
    if join_by == "D":
        left_on = d_field
        gb_field = o_field
    elif join_by == "O":
        left_on = o_field
        gb_field = d_field
    else:
        raise ValueError(
            f"Expected 'D' or 'O' as `join_by` value - got {join_by}")
    bin_field = f"BIN_{units}"
    # Read the activity data
    _a_fields_ = [id_field] + act_fields
    act_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(se_data, _a_fields_)
    )

    # Read the skim table
    out_dfs = []
    use_cols = [o_field, d_field, imped_field]
    print("... ... ... binning skims")
    for chunk in pd.read_csv(
        skim_table, usecols=use_cols, chunksize=chunk_size, **kwargs):
        # Define impedance bins
        
        low = -np.inf
        criteria = []
        labels = []
        for i_break in imped_breaks:
            crit = np.logical_and(
                chunk[imped_field] >= low,
                chunk[imped_field] < i_break
            )
            criteria.append(crit)
            labels.append(f"{i_break}{units}")
            low = i_break
        # Apply categories
        chunk[bin_field] = np.select(
            criteria, labels, f"{i_break}{units}p"
        )
        labels.append(f"{i_break}{units}p")
        # Join the activity data
        join_df = chunk.merge(
            act_df, how="inner", left_on=left_on, right_on=id_field)
        # Summarize
        sum_fields = [gb_field]
        prod_fields = []
        for act_field in act_fields:
            new_field = f"Wtd{units}{act_field}"
            join_df[new_field] = join_df[imped_field] * join_df[act_field]
            sum_fields += [act_field, new_field]
            prod_fields.append(new_field)
        sum_df = join_df.groupby([gb_field, bin_field]).sum().reset_index()
        out_dfs.append(sum_df)
    # Concatenate all
    out_df = pd.concat(out_dfs)
    # Pivot, summarize, and join
    # - Pivot
    print("... ... ... bin columns")
    pivot_fields = [gb_field, bin_field] + act_fields
    pivot = pd.pivot_table(
        out_df[pivot_fields], index=gb_field, columns=bin_field)
    pivot.columns = PMT.colMultiIndexToNames(pivot.columns, separator="")
    # - Summarize
    print("... ... ... average time by activitiy")
    sum_df = out_df[sum_fields].groupby(gb_field).sum()
    avg_fields = []
    for act_field, prod_field in zip(act_fields, prod_fields):
        avg_field = f"Avg{units}{act_field}"
        avg_fields.append(avg_field)
        sum_df[avg_field] = sum_df[prod_field]/sum_df[act_field]
    # - Join
    final_df = pivot.merge(
        sum_df[avg_fields], how="outer", left_index=True, right_index=True)
    
    # # Extend output feature class
    # print("... ... ... adding columns to ouput table")
    # PMT.extendTableDf(out_fc, id_field, final_df.reset_index(), gb_field)
    # return out_fc
    return final_df.reset_index()

# def _listAccessFields(act_fields, time_breaks, units, mode):
#     access_fields = []
#     # Activities by time time bin
#     for act_field in act_fields:
#         for tbreak in time_breaks:
#             field = f"{act_field}{tbreak}{units}{mode[0]}"
#             access_fields.append(field)
#         # Greater than last time break field
#         field = f"{act_field}{tbreak}{UNITS}p{mode[0]}"
#         access_fields.append(field)
#         # Average time to activity field
#         field = f"Avg{units}{act_field}{mode[0]}"
#         access_fields.append(field)

#     return access_fields


# %% MAIN
if __name__ == "__main__":
    for year in PMT.YEARS:
        print(f"Analysis year: {year}")
        for mode in MODES:
            print(f"... {mode}")
            # Get reference info from globals
            folder, base, horizon = NET_BY_YEAR[year][mode]
            scale, id_field = MODE_REF[mode]
            # Look up zone and skim data for each mode
            zone_data = PMT.makePath(
                PMT.CLEANED, "SERPM\\V7", "ZoneData.gdb", f"{scale}_{year}")
            skim_data = PMT.makePath(
                PMT.CLEANED, folder, f"{mode}_Skim_{base}.csv")
            # Analyze access 
            atd_df = summarizeAccess(skim_data, O_FIELD, D_FIELD, IMP_FIELD,
                                     zone_data, id_field, D_ACT_FIELDS, 
                                     TIME_BREAKS, units=UNITS, join_by="D",
                                     dtype=SKIM_DT, chunk_size=100000)
            afo_df = summarizeAccess(skim_data, O_FIELD, D_FIELD, IMP_FIELD,
                                     zone_data, id_field, O_ACT_FIELDS,
                                     TIME_BREAKS, units=UNITS, join_by="O",
                                     dtype=SKIM_DT, chunk_size=100000)
            # Merge tables
            atd_df.rename(columns={O_FIELD: id_field}, inplace=True)
            afo_df.rename(columns={D_FIELD: id_field}, inplace=True)
            full_table = atd_df.merge(afo_df, on=id_field)

            # Export output
            out_table = PMT.makePath(
                PMT.DATA, f"IDEAL_PMT_{year}.gdb", f"Access_{scale}_{mode}"
            )
            PMT.dfToTable(full_table, out_table, overwrite=True)


    # For each model year, make maz and taz features within
    # each PMT analysis year gdb
    # print("OUTPUT FEATURE CLASSES")
    # for year in PMT.YEARS:
    #     print(f"Analysis year: {year}")
    #     out_fd = PMT.makePath(PMT.DATA, f"PMT_{year}.gdb", "SERPM")
    #     for m_year in [BASE_YEAR, HORIZON_YEAR]:
    #         print(f"... model year: {m_year}")
    #         # Copy MAZ features
    #         print("... ... MAZ features")
    #         maz_source = PMT.makePath(
    #             PMT.CLEANED, "SERPM", f"maz_{m_year}.shp")
    #         maz_fc = PMT.makePath(out_fd, f"maz_{m_year}")
    #         createZoneFc(
    #             maz_source, maz_fc, keep_fields=["MAZ", "TAZ"], overwrite=True)
    #         # Dissolve TAZ features
    #         print("... ... TAZ features")
    #         taz_fc = PMT.makePath(out_fd, f"taz_{m_year}")
    #         PMT.checkOverwriteOutput(taz_fc, overwrite=True)
    #         arcpy.Dissolve_management(maz_fc, taz_fc, dissolve_field="TAZ")

    # print("ACCESS SUMMARIES")
    # solved = {} # "mode-year-scale"
    # for year in PMT.YEARS:
    #     print(f"Analysis year: {year}")
    #     out_fd = PMT.makePath(PMT.DATA, f"PMT_{year}.gdb", "SERPM")
    #     # Base condition
    #     for mode in MODES:
    #         print(f"... {mode}")
    #         folder, base, horizon = NET_BY_YEAR[year][mode]
    #         scale, id_field = MODE_REF[mode]
    #         check = f"{mode}-{base}-{scale}"
    #         out_fc = PMT.makePath(out_fd, f"{scale}_{BASE_YEAR}")
    #         try:
    #             # Copy output
    #             source = solved[check]
    #             print("... .... Base conditions - copying fields")
    #             # Access fields
    #             access_fields = [id_field]
    #             access_fields += _listAccessFields(
    #                 D_ACT_FIELDS, TIME_BREAKS, UNITS, mode)
    #             access_fields += _listAccessFields(
    #                 O_ACT_FIELDS, TIME_BREAKS, UNITS, mode)
    #             all_fields = [f.name for f in arcpy.ListFields(source, "*")]
    #             copy_fields = [
    #                 f for f in access_fields if f in all_fields
    #             ]
    #             # print(copy_fields)
    #             copy_ar = arcpy.da.TableToNumPyArray(source, copy_fields)
    #             arcpy.da.ExtendTable(out_fc, id_field, copy_ar, id_field)


    #         except KeyError:
    #             # Run the analysis
    #             zone_data = PMT.makePath(
    #                 PMT.CLEANED, "SERPM", f"{scale}_{BASE_YEAR}.dbf")
    #             skim_data = PMT.makePath(
    #                 PMT.CLEANED, folder, f"{mode}_Skim_{base}.csv")
    #             # Summarize access for this mode's base year
    #             print("... ... Base conditions - dest activities")
    #             d = summarizeAccess(skim_data, O_FIELD, D_FIELD, IMP_FIELD,
    #                                 zone_data, id_field, D_ACT_FIELDS, out_fc,
    #                                 id_field, TIME_BREAKS, mode=mode[0],
    #                                 units=UNITS, join_by="D", dtype=SKIM_DT,
    #                                 chunk_size=100000)
    #             print("... ... Base conditions - origin activities")
    #             o = summarizeAccess(skim_data, O_FIELD, D_FIELD, IMP_FIELD,
    #                                 zone_data, id_field, O_ACT_FIELDS, out_fc,
    #                                 id_field, TIME_BREAKS, mode=mode[0], 
    #                                 units=UNITS, join_by="O", dtype=SKIM_DT,
    #                                 chunk_size=100000)
    #             solved[check] = out_fc







