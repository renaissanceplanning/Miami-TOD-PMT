# -*- coding: utf-8 -*-
"""
Created: October 2020

@Author: Brian Froeb, Aaron Weinstock
"""

# %% IMPORTS
import arcpy
import pandas as pd
import numpy as np
import os

import PMT_tools.PMT as PMT
from PMT_tools.PMT import (Comp, And, Or)
# %% GLOBALS
BLOCK_GROUP_KEY = 'GEOID10'

LODES_ATTRS = [
    'CNS01', 'CNS02', 'CNS03', 'CNS04', 'CNS05', 'CNS06', 'CNS07', 'CNS08', 'CNS09', 'CNS10',
    'CNS11', 'CNS12', 'CNS13', 'CNS14', 'CNS15', 'CNS16', 'CNS17', 'CNS18', 'CNS19', 'CNS20'
]
LODES_CRITERIA = {
    "CNS01": And([Comp(">=", 50), Comp("<=", 69)]),
    "CNS02": Comp("==", 92),
    "CNS03": Comp("==", 91),
    "CNS04": [Comp("==", 17), Comp("==", 19)],
    "CNS05": [Comp("==", 41), Comp("==", 42)],
    "CNS06": Comp("==", 29),
    "CNS07": And([Comp(">=", 11), Comp("<=", 16)]),
    "CNS08": [Comp("==", 20), Comp("==", 48), Comp("==", 49)],
    "CNS09": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS10": [Comp("==", 23), Comp("==", 24)],
    "CNS11": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS12": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS13": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS14": Comp("==", 89),
    "CNS15": [Comp("==", 72), Comp("==", 83), Comp("==", 84)],
    "CNS16": [Comp("==", 73), Comp("==", 85)],
    "CNS17": [And([Comp(">=", 30), Comp("<=", 38)]), Comp("==", 82)],
    "CNS18": [Comp("==", 21), Comp("==", 22), Comp("==", 33), Comp("==", 39)],
    "CNS19": [Comp("==", 27), Comp("==", 28)],
    "CNS20": And([Comp(">=", 86), Comp("<=", 89)]),
    "Population": [And([Comp(">=", 1), Comp("<=", 9)]), And([Comp(">=", 100), Comp("<=", 102)])]
}
DEMOG_ATTRS = [
    'Total_Hispanic', 'White_Hispanic', 'Black_Hispanic', 'Asian_Hispanic', 'Multi_Hispanic', 'Other_Hispanic',
    'Total_Non_Hisp', 'White_Non_Hisp', 'Black_Non_Hisp', 'Asian_Non_Hisp', 'Multi_Non_Hisp', 'Other_Non_Hisp',
]
COMMUTE_ATTRS = [
    'Drove', 'Carpool', 'Transit', 'NonMotor', 'Work_From_Home', 'AllOther'
]
BLOCK_GROUP_ATTRS = [BLOCK_GROUP_KEY] + LODES_ATTRS + DEMOG_ATTRS + COMMUTE_ATTRS

NON_RES_LU_CODES = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 27, 28, 29,
                    30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41, 42, 48, 49, 72, 73, 82,
                    84, 85, 86, 87, 88, 89]

ALL_DEV_LU_CODES = NON_RES_LU_CODES + [1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 101, 102]


# %% FUNCTIONS
# TODO: perform a merge of bg and Jobs (LODES) and Population (RACE, and COMMUTE)
def prep_blockgroup_allocation(parcel_fc, bg_fc, out_gdb, parcels_id="FOLIO",
                               parcel_lu="DOR_UC", parcel_liv_area="TOT_LVG_AREA"):
    """
    Allocate block group data to parcels using relative abundances of
    parcel building square footage
    
    Parameters 
    ----------
    parcel_fc: Path
        path to shape of parcel polygons, containing at a minimum a unique ID
        field, land use field, and total living area field (Florida DOR)
    bg_fc: Path
        path to shape of block group polygons, attributed with job and
        population data for allocation
    out_gdb: Path
        path to location in which the allocated results will be saved. If 
        `bg_for_alloc_path` is in a .gdb, it makes sense to set `save_gdb` to
        that .gdb (or a feature class within that .gdb)
    parcels_id: str
        unique ID field in the parcels shape
        Default is "PARCELNO" for Florida parcels
    parcel_lu: str
        land use code field in the parcels shape
        Default is "DOR_UC" for Florida parcels
    parcel_liv_area: str
        building square footage field in the parcels shape
        Default is "TOT_LVG_AREA" for Florida parcels
        
    
    Returns
    -------
    path of location at which the allocation results are saved. 
    Saving will be completed as part of the function. The allocation estimates
    will be joined to the original parcels shape
    """

    # Add a unique ID field to the parcels called "ProcessID"
    print("...adding a unique ID field for individual parcels")
    # creating a temporary copy of parcels
    temp_parcels = PMT.makePath("in_memory", "temp_parcels")
    arcpy.FeatureClassToFeatureClass_conversion(in_features=parcel_fc, out_path="in_memory", out_name="temp_parcels")
    process_id = PMT.add_unique_id(feature_class=temp_parcels)

    print("Spatial processing for allocation")
    parcel_fields = [parcels_id, parcel_lu, parcel_liv_area, "Shape_Area"]
    intersect_fc = PMT.intersect_features(summary_fc=bg_fc,
                                          disag_fc=temp_parcels, disag_fields=parcel_fields)

    print("... loading data to dataframe")
    intersect_fields = [process_id] + parcel_fields + BLOCK_GROUP_ATTRS
    intersect_df = PMT.featureclass_to_df(in_fc=intersect_fc, keep_fields=intersect_fields)

    # create output dataset keeping only process_id and parcel_id
    print("-- initializing a new feature class for allocation results...")
    # TODO:
    keep = [process_id, parcels_id]
    field_mapper = arcpy.FieldMappings()
    for k in keep:
        fm = arcpy.FieldMap()
        fm.addInputField(temp_parcels, k)
        field_mapper.addFieldMap(fm)
    socio_econ_demog_fc = arcpy.FeatureClassToFeatureClass_conversion(in_features=parcel_fc, out_path=out_gdb,
                                                                      out_name="socioeconomic_and_demographic",
                                                                      field_mapping=field_mapper)[0]

    print("Allocation")

    print("-- formatting block group for allocation data...")
    # set any value below 0 to 0 and set any land use from -1 to NA
    to_clip = LODES_ATTRS + DEMOG_ATTRS + COMMUTE_ATTRS + [parcel_liv_area, "Shape_Area"]
    for var in to_clip:
        intersect_df[f"{var}"] = intersect_df[f"{var}"].clip(lower=0)
    # 2. replace -1 in DOR_UC with NA
    pluf = parcel_lu
    elu = intersect_df[pluf] == -1
    intersect_df.loc[elu, pluf] = None

    # Step 1 in allocation is totaling the living area by activity in each block group. 
    # To do this, we define in advance which activities can go to which land uses

    # First, we set up this process by matching activities to land uses
    print("-- setting up activity-land use matches...")
    lu_mask = {
        'CNS01': ((intersect_df[pluf] >= 50) & (intersect_df[pluf] <= 69)),
        'CNS02': (intersect_df[pluf] == 92),
        'CNS03': (intersect_df[pluf] == 91),
        'CNS04': ((intersect_df[pluf] == 17) | (intersect_df[pluf] == 19)),
        'CNS05': ((intersect_df[pluf] == 41) | (intersect_df[pluf] == 42)),
        'CNS06': (intersect_df[pluf] == 29),
        'CNS07': ((intersect_df[pluf] >= 11) & (intersect_df[pluf] <= 16)),
        'CNS08': ((intersect_df[pluf] == 48) | (intersect_df[pluf] == 49) | (intersect_df[pluf] == 20)),
        'CNS09': ((intersect_df[pluf] == 17) | (intersect_df[pluf] == 18) | (intersect_df[pluf] == 19)),
        'CNS10': ((intersect_df[pluf] == 23) | (intersect_df[pluf] == 24)),
        'CNS11': ((intersect_df[pluf] == 17) | (intersect_df[pluf] == 18) | (intersect_df[pluf] == 19)),
        'CNS12': ((intersect_df[pluf] == 17) | (intersect_df[pluf] == 18) | (intersect_df[pluf] == 19)),
        'CNS13': ((intersect_df[pluf] == 17) | (intersect_df[pluf] == 18) | (intersect_df[pluf] == 19)),
        'CNS14': (intersect_df[pluf] == 89),
        'CNS15': ((intersect_df[pluf] == 72) | (intersect_df[pluf] == 83) | (intersect_df[pluf] == 84)),
        'CNS16': ((intersect_df[pluf] == 73) | (intersect_df[pluf] == 85)),
        'CNS17': (((intersect_df[pluf] >= 30) & (intersect_df[pluf] <= 38)) | (intersect_df[pluf] == 82)),
        'CNS18': ((intersect_df[pluf] == 21) | (intersect_df[pluf] == 22) | (intersect_df[pluf] == 33) | 
                  (intersect_df[pluf] == 39)),
        'CNS19': ((intersect_df[pluf] == 27) | (intersect_df[pluf] == 28)),
        'CNS20': ((intersect_df[pluf] >= 86) & (intersect_df[pluf] <= 89)),
        'Population': (((intersect_df[pluf] >= 1) & (intersect_df[pluf] <= 9)) | 
                       ((intersect_df[pluf] >= 100) & (intersect_df[pluf] <= 102)))
    }

    # Note that our activity-land use matches aren't guaranteed because they are subjectively defined. 
    # To that end, we need backups in case a block group is entirely missing all possible land uses 
    # for an activity. 
    #   - we set up masks for 'all non-res' (all land uses relevant to any non-NAICS-1-or-2 job type) 
    #   - and 'all developed' ('all non-res' + any residential land uses). 
    #  ['all non-res' will be used if a land use isn't present for a given activity; 
    #  [ the 'all developed' will be used if 'all non-res' fails]

    all_non_res = {'NR': (intersect_df[pluf].isin(NON_RES_LU_CODES))}
    all_developed = {'AD': (intersect_df[pluf].isin(ALL_DEV_LU_CODES))}

    # If all else fails, A fourth level we'll use (if we need to) is simply all total living area in the block group, 
    #   but we don't need a mask for that. If this fails (which it rarely should), we revert to land area, 
    #   which we know will work (all parcels have area right?)

    # Next, we'll total parcels by block group (this is just a simple operation
    # to give our living area totals something to join to)
    print("-- initializing living area sums...")
    count_parcels_bg = intersect_df.groupby(['GEOID10'])['GEOID10'].agg(['count'])
    count_parcels_bg.rename(columns={'count': 'NumParBG'}, inplace=True)
    count_parcels_bg = count_parcels_bg.reset_index()

    # Now we can begin totaling living area. We'll start with jobs
    print("-- totaling living area by job type...")
    # 1. Define our jobs variables

    # 2. get count of total living area (w.r.t. land use mask) for each
    # job type
    parcel_liv_area = parcel_liv_area
    pldaf = "Shape_Area"

    for var in LODES_ATTRS:
        # mask by LU, group on GEOID10
        area = intersect_df[lu_mask[var]].groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
        area.rename(columns={'sum': f'{var}_Area'},
                    inplace=True)
        area = area[area[f'{var}_Area'] > 0]
        area = area.reset_index()
        area[f'{var}_How'] = "lu_mask"
        missing = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
        if (len(missing) > 0):
            lev1 = intersect_df[all_non_res["NR"]]
            lev1 = lev1[lev1.GEOID10.isin(missing)]
            area1 = lev1.groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
            area1.rename(columns={'sum': f'{var}_Area'},
                         inplace=True)
            area1 = area1[area1[f'{var}_Area'] > 0]
            area1 = area1.reset_index()
            area1[f'{var}_How'] = "non_res"
            area = pd.concat([area, area1])
            missing1 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
            if (len(missing1) > 0):
                lev2 = intersect_df[all_developed["AD"]]
                lev2 = lev2[lev2.GEOID10.isin(missing1)]
                area2 = lev2.groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
                area2.rename(columns={'sum': f'{var}_Area'},
                             inplace=True)
                area2 = area2[area2[f'{var}_Area'] > 0]
                area2 = area2.reset_index()
                area2[f'{var}_How'] = "all_dev"
                area = pd.concat([area, area2])
                missing2 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
                if (len(missing2) > 0):
                    lev3 = intersect_df[intersect_df.GEOID10.isin(missing2)]
                    area3 = lev3.groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
                    area3.rename(columns={'sum': f'{var}_Area'},
                                 inplace=True)
                    area3 = area3[area3[f'{var}_Area'] > 0]
                    area3 = area3.reset_index()
                    area3[f'{var}_How'] = "living_area"
                    area = pd.concat([area, area3])
                    missing3 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
                    if (len(missing3) > 0):
                        lev4 = intersect_df[intersect_df.GEOID10.isin(missing3)]
                        area4 = lev4.groupby(['GEOID10'])[pldaf].agg(['sum'])
                        area4.rename(columns={'sum': f'{var}_Area'},
                                     inplace=True)
                        area4 = area4.reset_index()
                        area4[f'{var}_How'] = "land_area"
                        area = pd.concat([area, area4])
        area = area.reset_index(drop=True)
        count_parcels_bg = pd.merge(count_parcels_bg, area,
                                    how='left',
                                    on='GEOID10')

    # Repeat the above with population
    print("-- totaling living area for population...")
    area = intersect_df[lu_mask['Population']].groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
    area.rename(columns={'sum': 'Population_Area'},
                inplace=True)
    area = area[area['Population_Area'] > 0]
    area = area.reset_index()
    area['Population_How'] = "lu_mask"
    missing1 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
    if (len(missing1) > 0):
        lev2 = intersect_df[all_developed["AD"]]
        lev2 = lev2[lev2.GEOID10.isin(missing1)]
        area2 = lev2.groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
        area2.rename(columns={'sum': 'Population_Area'},
                     inplace=True)
        area2 = area2[area2['Population_Area'] > 0]
        area2 = area2.reset_index()
        area2['Population_How'] = "all_dev"
        area = pd.concat([area, area2])
        missing2 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
        if (len(missing2) > 0):
            lev3 = intersect_df[intersect_df.GEOID10.isin(missing2)]
            area3 = lev3.groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
            area3.rename(columns={'sum': 'Population_Area'},
                         inplace=True)
            area3 = area3[area3['Population_Area'] > 0]
            area3 = area3.reset_index()
            area3['Population_How'] = "living_area"
            area = pd.concat([area, area3])
            missing3 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
            if (len(missing3) > 0):
                lev4 = intersect_df[intersect_df.GEOID10.isin(missing3)]
                area4 = lev4.groupby(['GEOID10'])[pldaf].agg(['sum'])
                area4.rename(columns={'sum': 'Population_Area'},
                             inplace=True)
                area4 = area4.reset_index()
                area4['Population_How'] = "land_area"
                area = pd.concat([area, area4])
    area = area.reset_index(drop=True)
    count_parcels_bg = pd.merge(count_parcels_bg, area,
                                how='left',
                                on='GEOID10')

    # Now, we format and re-merge with our original parcel data
    print("-- merging living area totals with parcel-level data...")
    # 1. fill table with NAs -- no longer needed because NAs are eliminated
    # by nesting structure
    # tot_bg = tot_bg.fillna(0)
    # 2. merge back to original data
    intersect_df = pd.merge(intersect_df, count_parcels_bg,
                            how='left',
                            on='GEOID10')

    # Step 2 in allocation is taking parcel-level proportions of living area
    # relative to the block group total, and calculating parcel-level
    # estimates of activities by multiplying the block group activity total
    # by the parcel-level proportions

    # For allocation, we need a two step process, depending on how the area
    # was calculated for the activity. If "{var}_How" is land_area, then
    # allocation needs to be relative to land area; otherwise, it needs to be
    # relative to living area. To do this, we'll set up mask dictionaries
    # similar to the land use mask
    print("-- setting up allocation logic...")
    lu = {}
    nr = {}
    ad = {}
    lvg_area = {}
    lnd_area = {}
    for v in lu_mask.keys():
        lu[v] = (intersect_df[f'{v}_How'] == "lu_mask")
        nr[v] = (intersect_df[f'{v}_How'] == "non_res")
        ad[v] = (intersect_df[f'{v}_How'] == "all_dev")
        lvg_area[v] = (intersect_df[f'{v}_How'] == "living_area")
        lnd_area[v] = (intersect_df[f'{v}_How'] == "land_area")

    # First up, we'll allocate jobs
    print("-- allocating jobs and population...")
    # 1. for each job variable, calculate the proportion, then allocate     
    for var in lu_mask.keys():
        # First for lu mask
        intersect_df.loc[lu[var] & lu_mask[var], f'{var}_Par_Prop'] = (
                intersect_df[parcel_liv_area][lu[var] & lu_mask[var]] / intersect_df[f'{var}_Area'][lu[var] & lu_mask[var]]
        )
        # Then for non res
        intersect_df.loc[nr[var] & all_non_res["NR"], f'{var}_Par_Prop'] = (
                intersect_df[parcel_liv_area][nr[var] & all_non_res["NR"]] / intersect_df[f'{var}_Area'][
            nr[var] & all_non_res["NR"]]
        )
        # Then for all dev
        intersect_df.loc[ad[var] & all_developed["AD"], f'{var}_Par_Prop'] = (
                intersect_df[parcel_liv_area][ad[var] & all_developed["AD"]] / intersect_df[f'{var}_Area'][ad[var] & all_developed["AD"]]
        )
        # Then for living area
        intersect_df.loc[lvg_area[var], f'{var}_Par_Prop'] = (
                intersect_df[parcel_liv_area][lvg_area[var]] / intersect_df[f'{var}_Area'][lvg_area[var]]
        )
        # Then for land area
        intersect_df.loc[lnd_area[var], f'{var}_Par_Prop'] = (
                intersect_df[pldaf][lnd_area[var]] / intersect_df[f'{var}_Area'][lnd_area[var]]
        )
        # Now fill NAs with 0 for proportions
        intersect_df[f'{var}_Par_Prop'] = intersect_df[f'{var}_Par_Prop'].fillna(0)

        # Now allocate (note that for pop, we're using the population ratios
        # for all racial subsets)
        if var != "Population":
            intersect_df[f'{var}_PAR'] = intersect_df[f'{var}_Par_Prop'] * intersect_df[var]
        else:
            race_vars = ['Total_Hispanic', 'White_Hispanic', 'Black_Hispanic',
                         'Asian_Hispanic', 'Multi_Hispanic', 'Other_Hispanic',
                         'Total_Non_Hisp', 'White_Non_Hisp', 'Black_Non_Hisp',
                         'Asian_Non_Hisp', 'Multi_Non_Hisp', 'Other_Non_Hisp']
            for rv in race_vars:
                intersect_df[f'{rv}_PAR'] = intersect_df['Population_Par_Prop'] * intersect_df[rv]

    # If what we did worked, all the proportions should sum to 1. This will
    # help us identify if there are any errors
    v = [f'{var}_Par_Prop' for var in LODES_ATTRS + ["Population"]]
    x = intersect_df.groupby(["GEOID10"])[v].apply(lambda x: x.sum())
    x[v].apply(lambda x: [min(x), max(x)])

    # Now we can sum up totals
    print("-- totaling allocated jobs and population...")
    intersect_df['Total_Employment'] = (
            intersect_df['CNS01_PAR'] + intersect_df['CNS02_PAR'] + intersect_df['CNS03_PAR'] +
            intersect_df['CNS04_PAR'] + intersect_df['CNS05_PAR'] + intersect_df['CNS06_PAR'] +
            intersect_df['CNS07_PAR'] + intersect_df['CNS08_PAR'] + intersect_df['CNS09_PAR'] +
            intersect_df['CNS10_PAR'] + intersect_df['CNS11_PAR'] + intersect_df['CNS12_PAR'] +
            intersect_df['CNS13_PAR'] + intersect_df['CNS14_PAR'] + intersect_df['CNS15_PAR'] +
            intersect_df['CNS16_PAR'] + intersect_df['CNS17_PAR'] + intersect_df['CNS18_PAR'] +
            intersect_df['CNS19_PAR'] + intersect_df['CNS20_PAR']
    )
    intersect_df['Total_Population'] = (
            intersect_df['Total_Non_Hisp_PAR'] + intersect_df['Total_Hispanic_PAR']
    )

    # Finally, we'll allocate transportation usage
    print("-- allocating commutes...")
    # Commutes will be allocated relative to total population, so total by
    # the block group and calculate the parcel share
    tp_props = intersect_df.groupby("GEOID10")["Total_Population"].sum().reset_index()
    tp_props.columns = ["GEOID10", "TP_Agg"]
    geoid_edit = tp_props[tp_props.TP_Agg == 0].GEOID10
    intersect_df = pd.merge(intersect_df, tp_props,
                            how='left',
                            on='GEOID10')
    intersect_df["TP_Par_Prop"] = intersect_df['Total_Population'] / intersect_df['TP_Agg']
    # If there are any 0s (block groups with 0 population) replace with
    # the population area population, in case commutes are predicted where
    # population isn't
    intersect_df.loc[intersect_df.GEOID10.isin(geoid_edit), "TP_Par_Prop"] = intersect_df["Population_Par_Prop"][
        intersect_df.GEOID10.isin(geoid_edit)]
    # Now we can allocate commutes
    transit_vars = ['Drove', 'Carpool', 'Transit',
                    'NonMotor', 'Work_From_Home', 'AllOther']
    for var in transit_vars:
        intersect_df[f'{var}_PAR'] = intersect_df["TP_Par_Prop"] * intersect_df[var]

    # And, now we can sum up totals
    print("-- totaling allocated commutes...")
    intersect_df['Total_Commutes'] = (
            intersect_df['Drove_PAR'] + intersect_df['Carpool_PAR'] + intersect_df['Transit_PAR'] +
            intersect_df['NonMotor_PAR'] + intersect_df['Work_From_Home_PAR'] + intersect_df['AllOther_PAR']
    )

    # ------------------------------------------------------------------------

    print("")
    print("Writing results")

    # We don't need all the columns we have, so first we define the columns
    # we want and select them from our data. Note that we don't need to
    # maintain the parcels_id_field here, because our save file has been
    # initialized with this already!
    print("-- selecting columns of interest...")
    to_keep = ["ProcessID",
               parcel_liv_area,
               parcel_lu,
               'GEOID10',
               'Total_Employment',
               'CNS01_PAR', 'CNS02_PAR', 'CNS03_PAR', 'CNS04_PAR',
               'CNS05_PAR', 'CNS06_PAR', 'CNS07_PAR', 'CNS08_PAR',
               'CNS09_PAR', 'CNS10_PAR', 'CNS11_PAR', 'CNS12_PAR',
               'CNS13_PAR', 'CNS14_PAR', 'CNS15_PAR', 'CNS16_PAR',
               'CNS17_PAR', 'CNS18_PAR', 'CNS19_PAR', 'CNS20_PAR',
               'Total_Population',
               'Total_Hispanic_PAR',
               'White_Hispanic_PAR', 'Black_Hispanic_PAR', 'Asian_Hispanic_PAR',
               'Multi_Hispanic_PAR', 'Other_Hispanic_PAR',
               'Total_Non_Hisp_PAR',
               'White_Non_Hisp_PAR', 'Black_Non_Hisp_PAR', 'Asian_Non_Hisp_PAR',
               'Multi_Non_Hisp_PAR', 'Other_Non_Hisp_PAR',
               'Total_Commutes',
               'Drove_PAR', 'Carpool_PAR', 'Transit_PAR',
               'NonMotor_PAR', 'Work_From_Home_PAR', 'AllOther_PAR']
    intersect_df = intersect_df[to_keep]

    # For saving, we join the allocation estimates back to the ID shape we
    # initialized during spatial processing
    print("-- merging allocations back to parcel shape (please be patient, this could take a while)...")
    # 1. convert pandas df to numpy array for use with arcpy ExtendTable
    df_et = np.rec.fromrecords(recList=intersect_df.values,
                               names=intersect_df.dtypes.index.tolist())
    df_et = np.array(df_et)
    # 2. use ExtendTable to modify the parcels data
    allocation_path = os.path.join(out_gdb,
                                   "socioeconomic_and_demographic")
    arcpy.da.ExtendTable(in_table=allocation_path,
                         table_match_field="ProcessID",
                         in_array=df_et,
                         array_match_field="ProcessID")
    # 3. delete the ProcessID field from the allocation feature class
    arcpy.DeleteField_management(in_table=allocation_path,
                                 drop_field="ProcessID")

    # Note that instead of using this "ProcessID" method, we could groupby-sum
    # our df on PARCELNO and join back to the parcels

    # ------------------------------------------------------------------------

    print("")
    print("Done!")
    print("Allocation saved to: " + allocation_path)
    print("")
    return (allocation_path)


# %% MAIN
if __name__ == "__main__":
    for year in [2015, 2016, 2017, 2018, 2019]:
        print("")
        print(year)
        parcels_path = os.path.join("K:/Projects/MiamiDade/PMT/Data/Cleaned",
                                    "Parcels.gdb",
                                    '_'.join(["Miami", str(year)]))
        bg_for_alloc_path = os.path.join("K:/Projects/MiamiDade/PMT/Data",
                                         ''.join(["PMT_", str(year), ".gdb"]),
                                         "BlockGroups/blockgroup_for_alloc")
        save_gdb_location = os.path.join("K:/Projects/MiamiDade/PMT/Data",
                                         ''.join(["PMT_", str(year), ".gdb"]),
                                         "Parcels")
        parcels_id_field = "PARCELNO"
        parcels_land_use_field = "DOR_UC"
        parcels_living_area_field = "TOT_LVG_AREA"
        prep_blockgroup_allocation(parcel_fc=parcels_path,
                                   bg_fc=bg_for_alloc_path,
                                   out_gdb=save_gdb_location,
                                   parcels_id=parcels_id_field,
                                   parcel_lu=parcels_land_use_field,
                                   parcel_liv_area=parcels_living_area_field)
