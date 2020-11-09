"""
Created: October 2020
@Author: Alex Bell

...
"""

# %% IMPORTS
import PMT
import arcpy
import numpy as np

# %% GLOBALS
SUM_FIELDS = [
    "JV",
    #"JV_CHNG",
    #"AV_SD",
    #"AV_NSD",
    #"TV_SD",
    #"TV_NSD",
    #"JV_HMSTD",
    #"AV_HMSTD",
    #"JV_NON_HMSTD_RESD",
    #"AV_NON_HMSTD_RESD",
    #"JV_RESD_NON_RESD",
    #"AV_RESD_NON_RESD",
    #"JV_CLASS_USE",
    #"AV_CLASS_USE",
    #"JV_H2O_RECHRGE",
    #"AV_H2O_RECHRGE",
    #"JV_CONSRV_LND",
    #"AV_CONSRV_LND",
    #"JV_HIST_COM_PROP",
    #"AV_HIST_COM_PROP",
    #"JV_HIST_SIGNF",
    #"AV_HIST_SIGNF",
    #"JV_WRKNG_WTRFNT",
    #"AV_WRKNG_WTRFNT",
    #"NCONST_VAL",
    "LND_VAL",
    "LND_SQFOOT",
    #"CONST_CLASS",
    "TOT_LVG_AREA",
    "NO_BULDNG",
    "NO_RES_UNTS"
]

# %% FUNCTIONS
def sumLandUseAndValue(station_polys,
                       corridor_polys,
                       parcels,
                       sum_fields,
                       output_gdb,
                       stn_id_field="Name",
                       corridor_id_field="Name",
                       groupby_fields=["INBOUNDARY", "INSTATION", "INCORRIDOR", "DIV_CLASS"],
                       parcel_id="PARCELNO",
                       *args, **kwargs):
    """
    """
    # Aggregate to corridors
    print("...aggregating to corridors")
    cor_sum_fc = PMT.makePath(output_gdb, "Corridors", "land_use_and_value_cor")
    PMT.sumToAggregateGeo(parcels, sum_fields, groupby_fields, corridor_polys,
                          corridor_id_field, cor_sum_fc, overlap_type="INTERSECT",
                          agg_funcs=np.sum, flatten_disag_id=parcel_id, *args,
                          **kwargs)
    # Aggregate to station areas
    print("...aggegating to station areas")
    stn_sum_fc = PMT.makePath(output_gdb, "StationAreas", "land_use_and_value_sa")
    PMT.sumToAggregateGeo(parcels, sum_fields, groupby_fields, station_polys,
                          stn_id_field, stn_sum_fc, overlap_type="INTERSECT",
                          agg_funcs=np.sum, flatten_disag_id=parcel_id, *args,
                          **kwargs)


#%% MAIN
if __name__ == "__main__":
    station_areas = PMT.makePath(PMT.BASIC_FEATURES, "SMART_Plan_Station_Areas")
    corridors = PMT.makePath(PMT.BASIC_FEATURES, "SMART_Plan_Corridors")
    groupby_fields=["INSTATION", "INCORRIDOR", "RES_NRES"]
    # Summarize trend
    trends_sa = []
    trends_cor = []
    for year in PMT.YEARS:
        print(year)
        out_gdb = PMT.makePath(PMT.ROOT, f"PMT_{year}.gdb")
        parcels = PMT.makePath(out_gdb, "parcels", "land_use_and_value")
        sumLandUseAndValue(station_areas,
                           corridors,
                           parcels,
                           sum_fields=SUM_FIELDS,
                           output_gdb=out_gdb,
                           stn_id_field="Id",
                           corridor_id_field="Corridor",
                           groupby_fields=groupby_fields,
                           parcel_id="PARCELNO")
        stn_sum_fc = PMT.makePath(
            out_gdb, "StationAreas", "land_use_and_value_sa")
        cor_sum_fc = PMT.makePath(
            out_gdb, "Corridors", "land_use_and_value_cor")
        trends_sa.append(stn_sum_fc)
        trends_cor.append(cor_sum_fc)
    
    print("Merging trend tables for station areas")
    trend_sa_fc = PMT.makePath(
        PMT.ROOT, "PMT_Trend.gdb", "StationAreas", "land_use_and_value_sa")
    arcpy.Merge_management(trends_sa, trend_sa_fc)
    
    print("Merging trend tables for corridors")
    trend_cor_fc = PMT.makePath(
        PMT.ROOT, "PMT_Trend.gdb", "Corridors", "land_use_and_value_cor")
    arcpy.Merge_management(trends_cor, trend_cor_fc)
    
    # Summarize near term


    # Summarize long term
