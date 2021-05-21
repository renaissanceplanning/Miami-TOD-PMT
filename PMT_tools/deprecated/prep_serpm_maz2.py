"""
Estimate each year's MAZ and TAZ jobs and housing units levels
based on parcel data.

- Intersect parcels with MAZ's
- Join se data by parcel
- Consolidate columns as needed
- Summarize intersection by MAZ
- Summarize MAZ level to TAZ level

"""


# %% IMPORTS
from PMT_tools.deprecated.year_to_snapshot import *
from PMT_tools.PMT import makePath, RAW

# %% GLOBALS
MAZ_FC = makePath(RAW, "SERPM", "V7", "SEFlorida_MAZs_2010.shp")
MAZ_FC_ID = "MAZ2010"
MAZ_TAZ = "REG_TAZ"
_KEEP_COLS_ = [MAZ_FC_ID, MAZ_TAZ]
DROP_COLS = [
    f.name for f in arcpy.ListFields(MAZ_FC)if f.name not in _KEEP_COLS_
    ]

# Id field
PAR_ID_FIELD = "FOLIO"
SE_ID_FIELD = "FOLIO"

# Aggregation specs
AGG_COLS = [
    AggColumn("NO_RES_UNTS", rename="HH"),
    AggColumn("Total_Employment", rename="TotalJobs"),
    AggColumn("CNS16", rename="HCJobs"),
    AggColumn("CNS15", rename="EdJobs")
    ]
#  - Consolidations
CONSOLIDATE = [
    Consolidation("RsrcJobs", ["CNS01", "CNS02"]),
    Consolidation("IndJobs", ["CNS05", "CNS06", "CNS08"]),
    Consolidation("ConsJobs", ["CNS07", "CNS17", "CNS18"]),
    Consolidation("OffJobs", ["CNS09", "CNS10", "CNS11",
                              "CNS12", "CNS13", "CNS20"]),
    Consolidation("OthJobs", ["CNS03", "CNS04", "CNS14",
                              "CNS19"])
]
# Base year SE data
MAZ_SE = makePath(RAW, "SERPM", "V7", "maz_data.csv")
MAZ_SE_ID = "mgra"
MAZ_SE_TAZ = "TAZ"
BASE_FIELDS = [MAZ_SE_ID, MAZ_SE_TAZ]
CONSOLIDATE_COLS={
    "HH": [
        "hh"
    ],
    "TotalJobs": [
        "emp_total"
        ],
    "ConsJobs": [
        "emp_retail",
        "emp_amusement",
        "emp_hotel",
        "emp_restaurant_bar",
        "emp_personal_svcs_retail",
        "emp_state_local_gov_ent"
        ],
    "EdJobs": [
        "emp_pvt_ed_k12",
        "emp_pvt_ed_post_k12_oth",
        "emp_public_ed"
        ],
    "HCJobs": [
        "emp_health"
        ],
    "IndJobs": [
        "emp_mfg_prod",
        "emp_mfg_office",
        "emp_whsle_whs",
        "emp_trans"
        ],
    "OffJobs": [
        "emp_prof_bus_svcs",
        "emp_personal_svcs_office",
        "emp_state_local_gov_white",
        "emp_own_occ_dwell_mgmt",
        "emp_fed_gov_accts",
        "emp_st_lcl_gov_accts",
        "emp_cap_accts"
        ],
    "OthJobs": [
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
    "RsrcJobs": [
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


# %% FUNCTION
def consolidateCols(df, base_fields, consolidate_cols):
    """
    """
    if isinstance(base_fields, str):
        base_fields = [base_fields]

    clean_cols = base_fields + list(consolidate_cols.keys())
    for out_col in consolidate_cols.keys():
        sum_cols = consolidate_cols[out_col]
        df[out_col] = df[sum_cols].sum(axis=1)
    clean_df = df[clean_cols].copy()
    return clean_df


def estimateMAZFromParcels(par_fc, par_id_field, maz_fc, maz_id_field,
                           taz_id_field, se_data, se_id_field, agg_cols,
                           consolidations):
    # intersect
    int_fc = intersectFeatures(maz_fc, par_fc)
    # Join
    joinAttributes(int_fc, par_id_field, se_data, se_id_field, "*")
    # Summarize
    gb_cols = [Column(maz_id_field), Column(taz_id_field)]
    df = summarizeAttributes(
        int_fc, gb_cols, agg_cols, consolidations=consolidations)
    return df



# %% MAIN
if __name__ == "__main__":
    out_gdb = PMT.makePath(
        PMT.CLEANED, "SERPM", "V7", "ZoneData.gdb"
    )
    if arcpy.Exists(out_gdb):
        arcpy.Delete_management(out_gdb)
    out_ws, out_name = os.path.split(out_gdb)
    arcpy.CreateFileGDB_management(out_ws, out_name)
    print("Exporting MAZ centroids")
    # save centroids
    pts_fc = PMT.makePath(out_gdb, "maz_centroids")
    PMT.polygons_to_points(MAZ_FC, pts_fc, fields=[MAZ_FC_ID, MAZ_TAZ])

    for year in PMT.YEARS[-1:]:
        print(year)

        # Create output feature class
        print("... exporting MAZ features")
        out_fc = PMT.makePath(out_gdb, f"MAZ_{year}")
        if arcpy.Exists(out_fc):
            arcpy.Delete_management(out_fc)
        PMT.copy_features(MAZ_FC, out_fc, drop_columns=DROP_COLS,
                          rename_columns={MAZ_FC_ID: "MAZ", MAZ_TAZ: "TAZ"})
        
        # Dissolve TAZ features
        print("... dissolving TAZ features")
        taz_fc = PMT.makePath(out_gdb, f"TAZ_{year}")
        if arcpy.Exists(taz_fc):
            arcpy.Delete_management(taz_fc)
        arcpy.Dissolve_management(
            out_fc, taz_fc, "TAZ", multi_part="MULTI_PART")

        # Summarize parcel data to MAZ
        print("... summarizing MAZ activities from parcels")
        gdb = PMT.makePath(PMT.DATA, f"IDEAL_PMT_{year}.gdb")
        par_fc = PMT.makePath(gdb, "Polygons", "Parcels")
        se_data = PMT.makePath(gdb, "EconDemog_parcels")
        par_data = estimateMAZFromParcels(par_fc=par_fc, par_id_field=PAR_ID_FIELD,
                                          maz_fc=out_fc, maz_id_field="MAZ",
                                          taz_id_field="TAZ",
                                          se_data=se_data, se_id_field=SE_ID_FIELD,
                                          agg_cols=AGG_COLS, consolidations=CONSOLIDATE)

        # Fetch MAZ data (enrollments, etc.)
        print("... fetching other base-year MAZ data")
        usecols = [MAZ_SE_ID, MAZ_SE_TAZ]
        usecols += [i for v in CONSOLIDATE_COLS.values() for i in v]
        maz_data = pd.read_csv(MAZ_SE, usecols=usecols)

        # Consolidate
        maz_data = consolidateCols(
            maz_data, BASE_FIELDS, CONSOLIDATE_COLS)
        maz_data.rename(
            columns={MAZ_SE_ID: "MAZ", MAZ_SE_TAZ: "TAZ"}, inplace=True)

        # Patch
        # - where maz data do not overlap with parcels, use MAZ-level data
        patch_fltr = np.in1d(maz_data.MAZ, par_data.MAZ)
        matching_rows =  maz_data[patch_fltr].copy()
        all_par = par_data.merge(matching_rows, how="inner", on="MAZ",
                                 suffixes=("", "_M"))
        drop_cols = [c for c in all_par.columns if "_M" in c]
        if drop_cols:
            all_par.drop(columns=drop_cols, inplace=True)
        all_data = pd.concat([all_par, maz_data[~patch_fltr]])
        
        # Join
        print("... extending MAZ attributes")
        # all_data = par_data.merge(
        #     maz_data, how="left", left_on="MAZ", right_on=MAZ_SE_ID)
        all_data.fillna(0.0, inplace=True)
        join_data = all_data.drop(columns=["TAZ"])
        PMT.extend_table_df(out_fc, "MAZ", join_data, "MAZ")

        # Roll up to TAZ
        print("... extending TAZ attributes")
        all_data.drop(columns=["MAZ"], inplace=True)
        taz_data = all_data.groupby("TAZ").sum().reset_index()
        PMT.extend_table_df(taz_fc, "TAZ", taz_data, "TAZ")



