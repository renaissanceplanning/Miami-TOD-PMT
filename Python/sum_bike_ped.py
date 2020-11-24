"""
Created: November 2020
@Author: Charles Rudder

...
"""

# %% IMPORTS
from config.config_project import (
    SCRIPTS, BASIC_FEATURES, YEARS, ROOT
)
import PMT
import arcpy
from pathlib import Path
import numpy as np

# %% Debug setting
GITHUB = True

# %% GLOBALS
SUM_FIELDS = ["INJSEVER", "TRANS_TYPE"]
SUM_AREAS = ["SMART_Plan_Station_Areas", "SMART_Plan_Corridors"]
GROUPBYS = ["INSTATION", "INCORRIDOR", "RES_NRES"]
ID_FIELD = 'NAME'


# %% FUNCTIONS
def sum_bike_ped_crashes(
        station_polys,
        corridor_polys,
        crash_points,
        sum_fields,
        output_gdb,
        stn_id_field="Name",
        corridor_id_field="Name",
        groupby_fields=["MONTH"],
        *args,
        **kwargs,
):
    """"""
    # Aggregate to corridors
    print("...aggregating to corridors")
    cor_sum_fc = str(Path(output_gdb, "Corridors", "bike_peds_crash_sum"))
    PMT.sumToAggregateGeo(
        disag_fc=crash_points,
        sum_fields=sum_fields,
        groupby_fields=groupby_fields,
        agg_fc=corridor_polys,
        agg_id_field=corridor_id_field,
        output_fc=cor_sum_fc,
        overlap_type="INTERSECT",
        agg_funcs=np.sum,
        *args,
        **kwargs,
    )
    # Aggregate to station areas
    print("...aggegating to station areas")
    stn_sum_fc = str(Path(output_gdb, "StationAreas", "bike_peds_crash_sum"))
    PMT.sumToAggregateGeo(
        disag_fc=crash_points,
        sum_fields=sum_fields,
        groupby_fields=groupby_fields,
        agg_fc=station_polys,
        agg_id_field=stn_id_field,
        output_fc=stn_sum_fc,
        overlap_type="INTERSECT",
        agg_funcs=np.sum,
        *args,
        **kwargs,
    )
    return cor_sum_fc, stn_sum_fc


# %% MAIN
if __name__ == "__main__":
    if GITHUB:
        ROOT = r'K:\Projects\MiamiDade\PMT'
        BASIC_FEATURES = Path(ROOT, "Basic_features.gdb", "Basic_features_SPFLE")

    trends_sa = []
    trends_cor = []
    for year in YEARS:
        print(year)
        stn_areas = str(Path(BASIC_FEATURES, "SMART_Plan_Station_Areas"))
        corrs = str(Path(BASIC_FEATURES, "SMART_Plan_Corridors"))
        out_gdb = Path(ROOT, f"PMT_{year}.gdb")
        all_crashes = Path(out_gdb, "SafetySecurity", "bike_ped_crashes")
        cor_sum_fc, stn_sum_fc = sum_bike_ped_crashes(
            station_polys=stn_areas,
            corridor_polys=corrs,
            crash_points=all_crashes,
            sum_fields=SUM_FIELDS,
            output_gdb=out_gdb,
            stn_id_field="Id",
            corridor_id_field="Corridor",
            groupby_fields=GROUPBYS,
        )
        trends_sa.append(stn_sum_fc)
        trends_cor.append(cor_sum_fc)

        print("Merging trend tables for station areas")
        trend_sa_fc = str(Path(
            ROOT, "PMT_Trend.gdb", "StationAreas", "bike_ped_crashes_sa"
        ))
        arcpy.Merge_management(inputs=trends_sa, output=trend_sa_fc)

        print("Merging trend tables for corridors")
        trend_cor_fc = str(Path(
            ROOT, "PMT_Trend.gdb", "Corridors", "bike_ped_crashes_cor"
        ))
        arcpy.Merge_management(inputs=trends_cor, output=trend_cor_fc)
