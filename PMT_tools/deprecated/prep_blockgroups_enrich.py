"""
Created: October 2020
@Author: Brian Froeb & Alex Bell

...
...
"""

# %% IMPORTS
import PMT_tools.PMT as PMT
from PMT_tools.PMT import Comp, And, Or, CLEANED, ROOT, RAW
import arcpy
import pandas as pd
import os

DEBUG = True
# %% GLOBALS
BLOCKGROUP_ID = "GEOID"
PARCELD_ID = "FOLIO"
ACS_ID = "GEOID10"
LODES_ID = "bgrp"
PARCEL_LU_FIELD = "DOR_UC"
PARCEL_BLD_AREA = "TOT_LVG_AREA"
PARCEL_SUMMARY_FIELDS = ["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA"]
LODES_CRITERIA = {
    "CNS_01_par": And([Comp(">=", 50), Comp("<=", 69)]),
    "CNS_02_par": Comp("==", 92),
    "CNS_03_par": Comp("==", 91),
    "CNS_04_par": [Comp("==", 17), Comp("==", 19)],
    "CNS_05_par": [Comp("==", 41), Comp("==", 42)],
    "CNS_06_par": Comp("==", 29),
    "CNS_07_par": And([Comp(">=", 11), Comp("<=", 16)]),
    "CNS_08_par": [Comp("==", 20), Comp("==", 48), Comp("==", 49)],
    "CNS_09_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_10_par": [Comp("==", 23), Comp("==", 24)],
    "CNS_11_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_12_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_13_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_14_par": Comp("==", 89),
    "CNS_15_par": [Comp("==", 72), Comp("==", 83), Comp("==", 84)],
    "CNS_16_par": [Comp("==", 73), Comp("==", 85)],
    "CNS_17_par": [And([Comp(">=", 30), Comp("<=", 38)]), Comp("==", 82)],
    "CNS_18_par": [Comp("==", 21), Comp("==", 22), Comp("==", 33), Comp("==", 39)],
    "CNS_19_par": [Comp("==", 27), Comp("==", 28)],
    "CNS_20_par": And([Comp(">=", 86), Comp("<=", 89)]),
    "RES_par": [And([Comp(">=", 1), Comp("<=", 9)]),
                And([Comp(">=", 100), Comp("<=", 102)])]
}


# %% FUNCTIONS

def enrich_bg_with_parcels(bg_fc, bg_id_field, parcels_fc, par_id_field, out_tbl,
                           par_lu_field, par_bld_area, sum_crit=None, par_sum_fields=None,
                           overwrite=False):
    """
    Relates parcels to block groups based on centroid location and summarizes
    key parcel fields to the block group level, including building floor area
    by potential activity type (residential, jobs by type, e.g.).

    Parameters
    ------------

    bg_fc: String; path
    parcels_fc: String; path
    out_tbl: String; path
    bg_id_field: String, default="GEOID10"
    par_id_field: String, default="PARCELNO"
    par_lu_field: String, default="DOR_UC"
    par_bld_area: String, default="TOT_LVG_AREA"
    sum_crit: dict, mapping of LODES variables to Land Use codes in parcels
    par_sum_fields: List, [String,...], default=["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA]
        If provided, these parcel fields will also be summed to the block-group level.
    overwrite: Bool, set to True will delete and recreate an existing output
    """
    # Prep output
    if sum_crit is None:
        sum_crit = {}
    PMT.checkOverwriteOutput(output=out_tbl, overwrite=overwrite)

    sr = arcpy.Describe(parcels_fc).spatialReference

    # Make parcel feature layer
    parcel_fl = arcpy.MakeFeatureLayer_management(parcels_fc, "__parcels__")
    par_fields = [par_id_field, par_lu_field, par_bld_area]
    par_fields += [psf for psf in par_sum_fields if psf not in par_fields]

    try:
        # Iterate over bg features
        print("--- analyzing block group features")
        bg_fields = ["SHAPE@", bg_id_field]
        bg_stack = []
        with arcpy.da.SearchCursor(
                bg_fc, bg_fields, spatial_reference=sr) as bgc:
            for bgr in bgc:
                bg_poly, bg_id = bgr
                # Select parcels in this BG
                arcpy.SelectLayerByLocation_management(parcel_fl, "HAVE_THEIR_CENTER_IN", bg_poly)
                # Dump selected to data frame
                par_df = PMT.featureclass_to_df(in_fc=parcel_fl, keep_fields=par_fields, null_val=0)
                if len(par_df) == 0:
                    print(f"---  --- no parcels found for BG {bg_id}")
                # Get mean parcel values
                par_grp_fields = [par_id_field] + par_sum_fields
                par_sum = par_df[par_grp_fields].groupby(par_id_field).mean()
                # Summarize totals to BG level
                par_sum[bg_id_field] = bg_id
                bg_grp_fields = [bg_id_field] + par_sum_fields
                bg_sum = par_sum[bg_grp_fields].groupby(bg_id_field).sum()
                # Select and summarize new fields
                for grouping in sum_crit.keys():
                    # Mask based on land use criteria
                    crit = Or(par_df[par_lu_field], sum_crit[grouping])
                    mask = crit.eval()
                    # Summarize masked data
                    #  - Parcel means (to account for multi-poly's)
                    area = par_df[mask].groupby([par_id_field]).mean()[par_bld_area]
                    #  - BG Sums
                    if len(area) > 0:
                        area = area.sum()
                    else:
                        area = 0
                    bg_sum[grouping] = area
                bg_stack.append(bg_sum.reset_index())
        # Join bg sums to outfc
        print("--- joining parcel summaries to block groups")
        bg_df = pd.concat(bg_stack)
        print(f"---  --- {len(bg_df)} block group rows")
        PMT.df_to_table(df=bg_df, out_table=out_tbl)
    except:
        raise
    finally:
        arcpy.Delete_management(parcel_fl)


def is_gz_file(filepath):
    with open(filepath, 'rb') as test_f:
        return test_f.read(2) == b'\x1f\x8b'


def enrich_bg_with_econ_demog(tbl_path, tbl_id_field, join_tbl, join_id_field, join_fields):
    """
    adds data from another raw table as new columns based on teh fields provided.
    Parameters
    ----------
    tbl_path: String; path
    tbl_id_field: String; table primary key
    join_tbl: String; path
    join_id_field: String; join table foreign key
    join_fields: [String, ...]; list of fields to include in update

    Returns
    -------
    None
    """
    # TODO: add checks for join_fields as actual columns in join_tbl
    if is_gz_file(join_tbl):
        tbl_df = pd.read_csv(join_tbl, usecols=join_fields, compression='gzip')
    else:
        tbl_df = pd.read_csv(join_tbl, usecols=join_fields)
    PMT.extend_table_df(in_table=tbl_path, table_match_field=tbl_id_field, df=tbl_df, df_match_field=join_id_field)


# %% MAIN
if __name__ == "__main__":
    # TODO: move this into preparer.py as a task
    if DEBUG:
        import uuid

        ROOT = r"C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT\Data\PROCESSING_TEST"
        RAW = PMT.make_path(ROOT, "RAW")
        CLEANED = PMT.make_path(ROOT, "CLEANED")

    # Define analysis specs
    bg_id_field = "GEOID10"
    par_lu_field = "DOR_UC"
    par_bld_area = "TOT_LVG_AREA"
    sum_crit = LODES_CRITERIA
    par_sum_fields = ["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA"]
    # For all years, enrich block groups with parcels
    for year in PMT.YEARS:
        print(year)
        # Define inputs/outputs
        gdb = PMT.make_path(CLEANED, f"PMT_{year}.gdb")
        fds = PMT.make_path(gdb, "Polygons")
        parcels_fc = PMT.make_path(fds, "Parcels")
        bg_fc = PMT.make_path(fds, "Census_BlockGroups")
        out_tbl = PMT.make_path(gdb, "Enrichment_blockgroups")
        # define table vars
        race_tbl = PMT.make_path(RAW, "CENSUS", f"ACS_{year}_race.csv")
        commute_tbl = PMT.make_path(RAW, "CENSUS", f"ACS_{year}_commute.csv")
        lodes_tbl = PMT.make_path(RAW, "LODES", f"fl_wac_S000_JT00_{year}_bgrp.csv.gz")
        race_fields = [ACS_ID] + \
                      ['Total_Non_Hisp', 'Total_Hispanic', 'White_Non_Hisp', 'Black_Non_Hisp', 'Asian_Non_Hisp',
                       'Multi_Non_Hisp', 'White_Hispanic', 'Black_Hispanic', 'Asian_Hispanic', 'Multi_Hispanic',
                       'Other_Non_Hisp', 'Other_Hispanic']
        commute_fields = [ACS_ID] + \
                         ['Total_Commutes', 'Drove_alone', 'Carpool', 'Transit', 'Taxi',
                          'Motorcycle', 'Bicycle', 'Walk', 'Other', 'Work_From_Home',
                          'Drove', 'NonMotor', 'AllOther', 'SOV_Share', 'HOV_Share',
                          'PT_Share', 'NM_Share', 'Oth_Share', 'WFH_Share']
        lodes_fields = [LODES_ID] + \
                       ['C000', 'CA01', 'CA02', 'CA03', 'CE01', 'CE02', 'CE03',
                        'CNS01', 'CNS02', 'CNS03', 'CNS04', 'CNS05', 'CNS06', 'CNS07', 'CNS08', 'CNS09', 'CNS10',
                        'CNS11', 'CNS12', 'CNS13', 'CNS14', 'CNS15', 'CNS16', 'CNS17', 'CNS18', 'CNS19', 'CNS20',
                        'CR01', 'CR02', 'CR03', 'CR04', 'CR05', 'CR07',
                        'CT01', 'CT02', 'CD01', 'CD02', 'CD03', 'CD04', 'CS01', 'CS02',
                        'CFA01', 'CFA02', 'CFA03', 'CFA04', 'CFA05', 'CFS01', 'CFS02', 'CFS03', 'CFS04', 'CFS05']
        # # Enrich BGs with parcel data
        unique_name = uuid.uuid4().hex
        temp_bg = PMT.make_path("in_memory", f"__blockgroups_{str(unique_name)}__")
        t_path, t_name = os.path.split(temp_bg)
        arcpy.FeatureClassToFeatureClass_conversion(in_features=bg_fc, out_path=t_path, out_name=t_name)
        enrich_bg_with_parcels(bg_fc=temp_bg, bg_id_field=BLOCKGROUP_ID, parcels_fc=parcels_fc, par_id_field=PARCELD_ID,
                               out_tbl=out_tbl, par_lu_field=PARCEL_LU_FIELD, par_bld_area=PARCEL_BLD_AREA,
                               sum_crit=LODES_CRITERIA, par_sum_fields=PARCEL_SUMMARY_FIELDS, overwrite=True)
        for table, tbl_id, fields in zip([race_tbl, commute_tbl, lodes_tbl],
                                         [ACS_ID, ACS_ID, LODES_ID],
                                         [race_fields, commute_fields, lodes_fields]):
            enrich_bg_with_econ_demog(tbl_path=out_tbl, tbl_id_field=BLOCKGROUP_ID,
                                      join_tbl=table, join_id_field=tbl_id, join_fields=fields)
