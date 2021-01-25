import json
import numpy as np
import pandas as pd
import time
import os

from PMT_tools.download.download_helper import validate_directory, validate_geodatabase
from prepare_config import (CRASH_FIELDS_DICT, CRASH_INCIDENT_TYPES,
                            USE_CRASH, DROP_CRASH, IN_CRS, OUT_CRS,
                            CRASH_HARMFUL_CODES, CRASH_SEVERITY_CODES, CRASH_CITY_CODES, )
from prepare_helpers import (geojson_to_featureclass, clean_bike_ped_crashes,
                             clean_park_polys, clean_park_points)
# PMT functions
from PMT_tools.PMT import makePath
# PMT globals
from PMT_tools.PMT import (RAW, CLEANED, YEARS)
import arcpy

arcpy.env.overwriteOutput = True

GITHUB = True

if __name__ == "__main__":
    if GITHUB:
        ROOT = r'C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT\Data'
        RAW = validate_directory(directory=makePath(ROOT, 'DownloadTest', "RAW"))
        CLEANED = validate_directory(directory=makePath(ROOT, 'DownloadTest', "CLEANED"))

    REFERENCE_GDB = validate_geodatabase(gdb_path=makePath(CLEANED, "REFERENCE.gdb"))

    ''' parks '''
    park_polys = [makePath(RAW, "Municipal_Parks.geojson"),
                  makePath(RAW, "Federal_State_Parks.geojson"),
                  makePath(RAW, "County_Parks.geojson")]
    park_points = makePath(RAW, "Park_Facilities.geojson")
    poly_use_cols = [["FOLIO", "NAME", "TYPE"], ["NAME"], ["FOLIO", "NAME", "TYPE"]]
    poly_rename_cols = [{}, {}, {}]
    out_park_polys = makePath(REFERENCE_GDB, "Park_Polys")
    out_park_points = makePath(REFERENCE_GDB, "Park_Points")
    clean_park_polys(in_fcs=park_polys, out_fc=out_park_polys, use_cols=poly_use_cols, rename_dicts=poly_rename_cols)
    clean_park_points(in_fc=park_points, out_fc=out_park_points)

    ''' crashes '''
    crash_json = makePath(RAW, "Safety_Security", "bike_ped.geojson")
    all_features = geojson_to_featureclass(geojson_path=crash_json)
    for year in YEARS:
        if year == 2019:
            out_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_{year}.gdb"))
            FDS = os.path.join(out_gdb, "Points")
            out_name = 'BikePedCrashes'
            year_wc = f'"CALENDAR_YEAR" = {year}'
            clean_bike_ped_crashes(in_fc=all_features, out_path=FDS, out_name=out_name,
                                   where_clause=year_wc, use_cols=USE_CRASH, rename_dict=CRASH_FIELDS_DICT)
