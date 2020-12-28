import arcpy
import pandas as pd
import numpy as np

arcpy.env.overwriteOutput = True


years = [2014, 2015, 2016, 2017, 2018, 2019]
in_tables = ["WalkTime_Parcels", "EconDemog_parcels", "LandUseCodes_parces"]
gb_wt = ["FOLIO", "NearestStn_walk", "BinStn_walk", "NearestPark_walk", "BinPark_walk", "NearestStn_ideal",
         "BinStn_ideal", "NearestParks_ideal", "BinParks_ideal"]
gb_ed = ["FOLIO"]
gb_lu = ["FOLIO", "SPC_LU", "GEN_LU", "GN_VA_LU", "DIV_CLASS", "NRES_ENERGY", "RES_NRES"]
gb_set = [gb_wt, gb_ed, gb_lu]
years
methods = ["mean", "sum", "size"]
in_tables = ["WalkTime_Parcels", "EconDemog_parcels", "LandUseCodes_parcels"]


def dfToTable(df, out_table, overwrite=False):
    in_array = np.array(np.rec.fromrecords(df.values, names=df.dtypes.index.tolist()))
    arcpy.da.NumPyArrayToTable(in_array, out_table)
    return out_table


def flatten(in_table, gb_cols, method, out_table):
    in_fields = [f.name for f in arcpy.ListFields(in_table) if not f.required]
    df = pd.DataFrame(arcpy.da.TableToNumPyArray(in_table, in_fields, null_value=0))
    sum_df = df.groupby(gb_cols).agg(method).reset_index()
    if 0 in sum_df.columns:
        sum_df.drop(columns=0, inplace=True)
    dfToTable(sum_df, out_table)


for year in years:
    print(year)
    for in_table, gb_cols, method in zip(in_tables, gb_set, methods):
        _in_ = r"C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT\Data\IDEAL_PMT_{}.gdb\{}".format(year, in_table)
        _out_ = r"C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT\Data\IDEAL_PMT_{}.gdb\{}_flat".format(year, in_table)
        print("\t", in_table)
        if arcpy.Exists(_in_):
            if arcpy.Exists(_out_):
                print('\t\t...deleting existing table')
                arcpy.Delete_management(_out_)
            print('\t\tFLATTENING')
            flatten(_in_, gb_cols, method, _out_)
            print('\t\tFLATTENED')