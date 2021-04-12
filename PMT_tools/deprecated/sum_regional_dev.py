"""
Created: October 2020
@Author: Alex Bell

...
"""


# %% IMPORTS
import arcpy
from PMT_tools.PMT import *


# %% GLOBALS
fields = ["INCORRIDOR", "INSTATION", "RES_NRES", "TOT_LVG_AREA", "NO_RES_UNTS", "JV"]
gb_fields = ["INCORRIDOR", "INSTATION", "RES_NRES"]


# %% FUNCTIONS
def sumDevInStationAreas(parcel_fc, sum_dev_fields, ):
    """
    """
    sumToAggregateGeo(parcel_fc, dev_sum_fields, groupby_fields, agg_fc,
                      agg_id_field, output_fc, overlap_type="INTERSECT",
                      agg_funcs=np.sum, disag_wc=None, agg_wc=None,
                      flatten_disag_id=None, *args, **kwargs)



    stn_results = []
    stn_fields = ["SHAPE@", station_id_field]
    with arcpy.da.SearchCursor(stations_fc, stn_fields) as c:
        for r in c:
            stn_shape, stn_id = r



# %% MAIN
for year in years:
...     print year
...     in_layer = "land_use_and_value{}".format(year)
...     df = pd.DataFrame(
...         arcpy.da.TableToNumPyArray(in_layer, fields, skip_nulls=True)
...     )
...     df_sum = df.groupby(gb_fields).sum().reset_index()
...     out_table = r"K:\Projects\MiamiDade\PMT\Data\Temp\regional_summary.gdb\lands_use_and_value_sum_{}".format(year)
...     dfToTable(df_sum, out_table)
...     



### ARCPY COPY
with arcpy.da.InsertCursor("station_area_floor_area_snapshot", ["SHAPE@", "Id", "Name", "YEAR", "RES_NRES", "JV", "TOT_LVG_AREA", "NO_RES_UNTS"]) as ic:
...     with arcpy.da.SearchCursor("SMART_Plan_Station_Areas", ["SHAPE@", "Id", "Name"]) as c:
...         for row in c:
...             stn_shape, stn_id, stn_name = row
...             out_row = [stn_shape, stn_id, stn_name]
...             for year in years:
...                 sel = "land_use_and_value{}".format(year)
...                 arcpy.SelectLayerByLocation_management(sel, "HAVE_THEIR_CENTER_IN", stn_shape)
...                 df = pd.DataFrame(arcpy.da.TableToNumPyArray(sel, ["RES_NRES", "JV", "TOT_LVG_AREA", "NO_RES_UNTS"], skip_nulls=True))
...                 df_res = df[df.RES_NRES == "RES"].sum()
...                 df_nres = df[df.RES_NRES == "NRES"].sum()
...                 res_row = out_row + [2000 + year, "RES", df_res["JV"], df_res["TOT_LVG_AREA"], df_res["NO_RES_UNTS"]]                
...                 nres_row = out_row + [2000 + year, "NRES", df_nres["JV"], df_nres["TOT_LVG_AREA"], df_nres["NO_RES_UNTS"]]
...                 ic.insertRow(res_row)
...                 ic.insertRow(nres_row)


with arcpy.da.InsertCursor("station_area_permit_sums", ["SHAPE@", "Id", "Name", "RES_NRES", "COST", "UNITS_VAL"]) as ic:
...     with arcpy.da.SearchCursor("SMART_Plan_Station_Areas", ["SHAPE@", "Id", "Name"]) as c:
...         for row in c:
...             stn_shape, stn_id, stn_name = row
...             out_row = [stn_shape, stn_id, stn_name]
...             print stn_name
...             arcpy.SelectLayerByLocation_management("Miami_Dade_BuildingPermits", "HAVE_THEIR_CENTER_IN", stn_shape)
...             df = pd.DataFrame(arcpy.da.TableToNumPyArray("Miami_Dade_BuildingPermits", ["RES_NRES", "COST", "UNITS_VAL"], skip_nulls=True))
...             df_res = df[df.RES_NRES == "RES"].sum()
...             df_nres = df[df.RES_NRES == "NRES"].sum()
...             res_row = out_row + ["RES", df_res["COST"], df_res["UNITS_VAL"]]                
...             nres_row = out_row + ["NRES", df_nres["COST"], df_nres["UNITS_VAL"]]
...             ic.insertRow(res_row)
...             ic.insertRow(nres_row)




with arcpy.da.InsertCursor("regional_sums_by_year", ['YEAR', 'RES_NRES', "JV", "TOT_LVG_AREA", "NO_RES_UNTS"]) as ic:
...     for year in years:
...         print(year)
...         lyr = "land_use_and_value{}".format(year)
...         df = pd.DataFrame(arcpy.da.TableToNumPyArray(lyr, ["RES_NRES", "JV", "TOT_LVG_AREA", "NO_RES_UNTS"], skip_nulls=True))
...         df_res = df[df.RES_NRES == "RES"].sum()
...         df_nres = df[df.RES_NRES == "NRES"].sum()
...         res_row = [2000 + year, "RES", df_res["JV"], df_res["TOT_LVG_AREA"], df_res["NO_RES_UNTS"]]                
...         nres_row =  [2000 + year, "NRES", df_nres["JV"], df_nres["TOT_LVG_AREA"], df_nres["NO_RES_UNTS"]]
...         ic.insertRow(res_row)
...         ic.insertRow(nres_row