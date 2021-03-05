# %% IMPORTS
import os
import pandas as pd
import numpy as np
import arcpy

# %% GLOBALS
d = r"K:\Projects\MiamiDade\PMT\Data"
gdbs = ["IDEAL_PMT_Snapshot.gdb", "ref_2015.gdb", "ref_2016.gdb", "ref_2017.gdb", "ref_2018.gdb"]


# %% PARCEL AREA SUMS
# Vacant area
# table = "Polygons\\Parcels"
# new_field = "VAC_AREA"
# new_field_type = "LONG"
# code_block = "def vac_area(gn_va_lu, lnd_sqfoot):\n  if gn_va_lu == 'Vacant/Undeveloped':\n    return lnd_sqfoot\n  else:\n    return 0"
# expr = "vac_area(!GN_VA_LU!, !LND_SQFOOT!)"

# for gdb in gdbs:
#     p = os.path.join(d, gdb, table)
#     arcpy.AddField_management(p, new_field, new_field_type)
#     arcpy.CalculateField_management(p, new_field, expr, "PYTHON", code_block)


# # Res area
# new_field = "RES_AREA"
# new_field_type = "LONG"
# code_block = "def vac_area(res_nres, lnd_sqfoot):\n  if res_nres == 'RES':\n    return lnd_sqfoot\n  else:\n    return 0"
# expr = "vac_area(!RES_NRES!, !LND_SQFOOT!)"

# # Nonres area
# new_field = "NRES_AREA"
# new_field_type = "LONG"
# code_block = "def vac_area(res_nres, lnd_sqfoot):\n  if res_nres == 'NRES':\n    return lnd_sqfoot\n  else:\n    return 0"
# expr = "vac_area(!RES_NRES!, !LND_SQFOOT!)"

#%% PARCEL AREA TO SUMMARY AREA
# # For each summary area, get its VAC_AREA, RES_AREA, NRES_AREA
# def calc_areas(sa_table, sa_by_lu_table, sa_field, cor_field, lu_field, area_field, new_field, new_field_type, crit):
#     # Dump sa by lu table
#     lu_df = pd.DataFrame(arcpy.da.TableToNumPyArray(sa_by_lu_table, [sa_field, cor_field, lu_field, area_field]))
#     fltr = np.logical_or.reduce([lu_df[lu_field] == cr for cr in crit])
#     lu_df = lu_df[fltr].copy()
#     # add field to summary areas
#     arcpy.AddField_management(sa_table, new_field, new_field_type)
#     with arcpy.da.UpdateCursor(sa_table, [sa_field, cor_field, new_field]) as c:
#         for r in c:
#             sa = r[0]
#             cor = r[1]
#             fltr = np.logical_and(lu_df[sa_field] == sa, lu_df[cor_field] == cor)
#             lu_rows = lu_df[fltr]
#             r[-1] = lu_rows[area_field].sum()
#             c.updateRow(r)

# sa_table = "Polygons\\SummaryAreas"
# sa_by_lu_table = "AttrByLandUse"
# sa_field = "Name"
# cor_field = "Corridor"
# lu_field = "GN_VA_LU"
# area_field = "LND_SQFOOT"
# vac_crit = ["Vacant/Undeveloped"]
# res_crit = ["Multifamily", "Single-family"]
# nres_crit = ["Commercial/Retail", "Industrial/Manufacturing", "Office", "Other"]

# new_field = "VAC_AREA"
# new_field_type = "LONG"
# for gdb in gdbs:
#     sa_p = os.path.join(d, gdb, sa_table)
#     lu_p = os.path.join(d, gdb, sa_by_lu_table)
#     calc_areas(sa_p, lu_p, sa_field, cor_field, lu_field, area_field, new_field, new_field_type, vac_crit)
    

# new_field = "RES_AREA"
# new_field_type = "LONG"
# for gdb in gdbs:
#     sa_p = os.path.join(d, gdb, sa_table)
#     lu_p = os.path.join(d, gdb, sa_by_lu_table)
#     calc_areas(sa_p, lu_p, sa_field, cor_field, lu_field, area_field, new_field, new_field_type, res_crit)
    

# new_field = "NRES_AREA"
# new_field_type = "LONG"
# for gdb in gdbs:
#     sa_p = os.path.join(d, gdb, sa_table)
#     lu_p = os.path.join(d, gdb, sa_by_lu_table)
#     calc_areas(sa_p, lu_p, sa_field, cor_field, lu_field, area_field, new_field, new_field_type, nres_crit)


#%% DENSITY (CALCULATED FIELDS)
# table = "Polygons\\SummaryAreas"
# new_field = "RES_DENS"
# new_field_type = "FLOAT"
# expr = "!NO_RES_UNTS!/(!RES_AREA! / 43560.0)"

# for gdb in gdbs:
#     p = os.path.join(d, gdb, table)
#     arcpy.AddField_management(p, new_field, new_field_type)
#     arcpy.CalculateField_management(p, new_field, expr, "PYTHON")

# table = "Polygons\\SummaryAreas"
# new_field = "EMP_DENS"
# new_field_type = "FLOAT"
# expr = "!Total_Employment!/(!RES_AREA! / 43560.0)"

# for gdb in gdbs:
#     p = os.path.join(d, gdb, table)
#     arcpy.AddField_management(p, new_field, new_field_type)
#     arcpy.CalculateField_management(p, new_field, expr, "PYTHON")


# table = "Polygons\\SummaryAreas"
# new_field = "FAR"
# new_field_type = "FLOAT"
# expr = "!TOT_LVG_AREA!/!LND_SQFOOT!"

# for gdb in gdbs:
#     p = os.path.join(d, gdb, table)
#     arcpy.AddField_management(p, new_field, new_field_type)
#     arcpy.CalculateField_management(p, new_field, expr, "PYTHON")


#%% LU MIX (CALCULATED FIELDS)
# table = "Polygons\\SummaryAreas"
# new_field = "JHRatio"
# new_field_type = "FLOAT"
# expr = "!Total_Employment!/!NO_RES_UNTS!"

# for gdb in gdbs:
#     p = os.path.join(d, gdb, table)
#     arcpy.AddField_management(p, new_field, new_field_type)
#     arcpy.CalculateField_management(p, new_field, expr, "PYTHON")


#%% GRID DENSITY (CALCULATED FIELDS)
# def blocks_per_sq_mi(sa_fc, area_field, block_fc, new_field, new_field_type, factor=1.0):
#     # Make output field
#     arcpy.AddField_management(sa_fc, new_field, new_field_type)
#     # Count blocks in each feature
#     blk_lyr = arcpy.MakeFeatureLayer_management(block_fc, "blocks")
#     with arcpy.da.UpdateCursor(sa_fc, ["SHAPE@", area_field, new_field]) as c:
#         for r in c:
#             sa_poly = r[0]
#             sa_area = r[1]
#             arcpy.SelectLayerByLocation_management(blk_lyr, "HAVE_THEIR_CENTER_IN", sa_poly, selection_type="NEW_SELECTION")
#             blk_count = int(arcpy.GetCount_management(blk_lyr)[0])
#             blk_per_area = blk_count/sa_area
#             blk_per_sq_mi = blk_per_area * factor
#             r[-1] = blk_per_sq_mi
#             c.updateRow(r)
            

# sa_fc = "Polygons\\SummaryAreas"
# area_field = "LND_SQFOOT"
# blocks_fc = "Polygons\\Blocks"
# factor = 43560.0 * 640.0 # 640 acres / sq mi, 43560 sq ft / acre
# new_field = "GRID_DENS"
# new_field_type = "FLOAT"


# for gdb in gdbs:
#     sa_p = os.path.join(d, gdb, sa_fc)
#     blk_p = os.path.join(d, gdb, blocks_fc)
#     blocks_per_sq_mi(sa_p, area_field, blk_p, new_field, new_field_type, factor=factor)


#%% NON AUTO MODE SHARE (CALCULATED FIELDS)
# table = "Polygons\\SummaryAreas"
# new_field = "SHR_NONAUTO"
# new_field_type = "FLOAT"
# expr = "(!Transit! + !NonMotor! + !AllOther!)/!Total_Commutes!"

# for gdb in gdbs:
#     p = os.path.join(d, gdb, table)
#     arcpy.AddField_management(p, new_field, new_field_type)
#     arcpy.CalculateField_management(p, new_field, expr, "PYTHON")


#%% ACCESS SUMS - DESTS IN 30 MINS (CALCULATED FIELDS)
# activities = ["TotalJobs", "ConsJobs", "HCJobs", "EnrollAdlt", "EnrollK12"[ #, "HH"] # TODO: summarize access from hh
# modes = ["W", "B", "T", "A"]
# table = "Polygons\\SummaryAreas"

# for mode in modes:
#     for act in activities:
#         new_field = "{}win30".format(act)
#         new_field_type = "FLOAT"
#         expr = "!{0}15Min{1}!+ !{0}30Min{1}!".format(act, mode)
#         for gdb in gdbs:
#             p = os.path.join(d, gdb, table)
#             arcpy.AddField_management(p, new_field, new_field_type)
#             arcpy.CalculateField_management(p, new_field, expr, "PYTHON")


# %% SUMMARIZE PARK ACREAGE 
# def park_acreage(sa_fc, parks_fc, park_acre_fld, new_field, new_field_type):
#     # Setup output field
#     arcpy.AddField_management(sa_fc, new_field, new_field_type)
#     # make feature layer of parks
#     lyr = arcpy.MakeFeatureLayer_management(parks_fc, "parks")
#     # Iterate over summary area features
#     with arcpy.da.UpdateCursor(sa_fc, ["SHAPE@", new_field]) as c:
#         for r in c:
#             poly = r[0]
#             arcpy.SelectLayerByLocation_management(lyr, "INTERSECT", poly, selection_type="NEW_SELECTION")
#             a = arcpy.da.TableToNumPyArray(lyr, park_acre_fld)
#             park_acres = np.sum(a[park_acre_fld])
#             r[-1] = park_acres
#             c.updateRow(r)
#     arcpy.Delete_management(lyr)

# sa_fc = "Polygons\\SummaryAreas"
# parks_fc = "Points\\Park_facilities"
# park_acre_fld = "TOTACRE"
# new_field = "Park_Acres"
# new_field_type = "FLOAT"
# for gdb in gdbs:
#     sa_p = os.path.join(d, gdb, sa_fc)
#     parks_p = os.path.join(d, gdb, parks_fc)
#     park_acreage(sa_p, parks_p, park_acre_fld, new_field, new_field_type)

#%% REGIONAL SHARES (CALCULATED FIELDS)
# def regional_share(sa_fc, parcels_fc, sa_val_field, par_sum_field, new_field, new_field_type):
#     # Dump parcels to data frame, get regional total
#     df = pd.DataFrame(arcpy.da.TableToNumPyArray(parcels_fc, par_sum_field))
#     reg_tot = df[par_sum_field].sum()
#     # Create new field
#     arcpy.AddField_management(sa_fc, new_field, new_field_type)
#     # Iterate over sa features and calcualte sa share of total
#     with arcpy.da.UpdateCursor(sa_fc, [sa_val_field, new_field]) as c:
#         for r in c:
#             sa_tot = r[0]
#             share = sa_tot/reg_tot
#             r[-1] = share
#             c.updateRow(r)


# # Res share
# sa_fc = "Polygons\\SummaryAreas"
# parcels_fc = "Polygons\\Parcels"
# sa_val_field = "NO_RES_UNTS"
# par_sum_field = "NO_RES_UNTS"
# new_field = "SHR_RES_UNTS"
# new_field_type = "FLOAT"
# for gdb in gdbs:
#     sa_p = os.path.join(d, gdb, sa_fc)
#     parcels_p = os.path.join(d, gdb, parcels_fc)
#     regional_share(sa_p, parcels_p, sa_val_field, par_sum_field, new_field, new_field_type)

# # Emp share
# sa_val_field = "Total_Employment"
# par_sum_field = "Total_Employment"
# new_field = "SHR_Employment"
# new_field_type = "FLOAT"
# for gdb in gdbs:
#     sa_p = os.path.join(d, gdb, sa_fc)
#     parcels_p = os.path.join(d, gdb, parcels_fc)
#     regional_share(sa_p, parcels_p, sa_val_field, par_sum_field, new_field, new_field_type)

# # Floor area
# sa_val_field = "TOT_LVG_AREA"
# par_sum_field = "TOT_LVG_AREA"
# new_field = "SHR_LVG_AREA"
# new_field_type = "FLOAT"
# for gdb in gdbs:
#     sa_p = os.path.join(d, gdb, sa_fc)
#     parcels_p = os.path.join(d, gdb, parcels_fc)
#     regional_share(sa_p, parcels_p, sa_val_field, par_sum_field, new_field, new_field_type)


#%% REGIONAL VALUE INDEXES (CALCULATED FIELDS)
# def value_index(sa_fc, sa_val_fld, sa_area_fld, parcels_fc, par_val_fld, par_area_fld, new_field, new_field_type, sa_factor=1.0, par_factor=1.0):
#     # Make data frame of parcels
#     par_fields = [par_val_fld, par_area_fld]
#     df = pd.DataFrame(arcpy.da.TableToNumPyArray(parcels_fc, par_fields, skip_nulls=True))
#     reg_val = df[par_val_fld].sum()
#     reg_area = df[par_area_fld].sum() * par_factor
#     reg_rate = reg_val / reg_area
#     # Create new field
#     arcpy.AddField_management(sa_fc, new_field, new_field_type)
#     # Iterate over sa features and calcualte sa share of total
#     sa_fields = [sa_val_fld, sa_area_fld, new_field]
#     with arcpy.da.UpdateCursor(sa_fc, sa_fields) as c:
#         for r in c:
#             sa_val = r[0]
#             sa_area = r[1] * sa_factor
#             sa_rate = sa_val / sa_area
#             val_idx = sa_rate/reg_rate
#             r[-1] = val_idx
#             c.updateRow(r)

# # Taxable value
# sa_fc = "Polygons\\SummaryAreas"
# parcels_fc = "Polygons\\Parcels"
# sa_val_fld = "TV_NSD"
# sa_area_fld = "LND_SQFOOT"
# par_val_fld = "TV_NSD"
# par_area_fld = "LND_SQFOOT"
# new_field = "TV_IDX"
# new_field_type = "FLOAT"
# for gdb in gdbs:
#     sa_p = os.path.join(d, gdb, sa_fc)
#     parcels_p = os.path.join(d, gdb, parcels_fc)
#     value_index(sa_p, sa_val_fld, sa_area_fld, parcels_p, par_val_fld, par_area_fld, new_field, new_field_type, sa_factor=1.0, par_factor=1.0)

# # Land Value
# # TODO: consider changing to something based on land value
# #  or a more complex metric to indicate "how improved is this area?"
# #  such as JV - LND_VAL/LND_SQFOOT (i.e., value of improvements per sq ft)
# #  perhaps expressed relative to a regional or other benchmark
# sa_val_fld = "JV"
# sa_area_fld = "LND_SQFOOT"
# par_val_fld = "JV"
# par_area_fld = "LND_SQFOOT"
# new_field = "JV_IDX"
# new_field_type = "FLOAT"
# for gdb in gdbs:
#     sa_p = os.path.join(d, gdb, sa_fc)
#     parcels_p = os.path.join(d, gdb, parcels_fc)
#     value_index(sa_p, sa_val_fld, sa_area_fld, parcels_p, par_val_fld, par_area_fld, new_field, new_field_type, sa_factor=1.0, par_factor=1.0)


#%% WALKING DIRECTNESS (CALCULATED AT PARCEL LEVEL, SUMMARIZED TO SUMMARY AREAS)
# # TODO: review methodology - no values should be less than 1, 
# #   but since the search is only out 30 minutes, we can get different sets in each study area

# table = "Polygons\\SummaryAreas"

# # Stations
# new_field = "DirIdxStn_walk"
# new_field_type = "FLOAT"
# expr = "!MinTimeStn_walk!/!MinTimeStn_ideal!"

# for gdb in gdbs:
#     p = os.path.join(d, gdb, table)
#     arcpy.AddField_management(p, new_field, new_field_type)
#     arcpy.CalculateField_management(p, new_field, expr, "PYTHON")

# # Parks
# new_field = "DirIdxPark_walk"
# new_field_type = "FLOAT"
# expr = "!MinTimePark_walk!/!MinTimeParks_ideal!" #TODO: address field naming inconsistence park v parks

# for gdb in gdbs:
#     p = os.path.join(d, gdb, table)
#     arcpy.AddField_management(p, new_field, new_field_type)
#     arcpy.CalculateField_management(p, new_field, expr, "PYTHON")


#%% SHARE OF SA PARCELS W/IN 15 MINS OF STN/PARK (CALCULATED FIELDS)
def prop_within(sa_fc, par_fc, par_sel_expr, new_field, new_field_type):
    # Make pacel feature layer
    lyr = arcpy.MakeFeatureLayer_management(par_fc, "parcels")
    # Add output field
    arcpy.AddField_management(sa_fc, new_field, new_field_type)
    # Iterate over sa features and select/analyze
    with arcpy.da.UpdateCursor(sa_fc, ["SHAPE@", new_field]) as c:
        for r in c:
            sa_poly = r[0]
            arcpy.SelectLayerByLocation_management(lyr, "HAVE_THEIR_CENTER_IN", sa_poly, selection_type="NEW_SELECTION")
            tot_par = float(arcpy.GetCount_management(lyr)[0])
            arcpy.SelectLayerByAttribute_management(lyr, "SUBSET_SELECTION", par_sel_expr)
            sel_par = float(arcpy.GetCount_management(lyr)[0])
            share_within = sel_par / tot_par
            r[-1] = share_within
            c.updateRow(r)
    arcpy.Delete_management(lyr)


sa_fc = "Polygons\\SummaryAreas"
par_fc = "Polygons\\Parcels"

# Stations
new_field = "Prop_Stn15"
new_field_type = "FLOAT"
for gdb in gdbs:
    sa_p = os.path.join(d, gdb, sa_fc)
    par_p = os.path.join(d, gdb, par_fc)
    par_sel_expr = arcpy.AddFieldDelimiters(par_p, "MinTimeStn_walk") + "<= 15.0"
    prop_within(sa_p, par_p, par_sel_expr, new_field, new_field_type)



# Parks
new_field = "Prop_Park15"
new_field_type = "FLOAT"
for gdb in gdbs:
    sa_p = os.path.join(d, gdb, sa_fc)
    par_p = os.path.join(d, gdb, par_fc)
    par_sel_expr = arcpy.AddFieldDelimiters(par_p, "MinTimePark_walk") + "<= 15.0"
    prop_within(sa_p, par_p, par_sel_expr, new_field, new_field_type)