"""
Created: October 2020
@Author: Charles Rudder

Convert data from MD Open data portal related to crashes involving
    pedestrians and bicyclist to a standard format

Sources inlcude:
    - FDOT open data download of CSV copy of data to maintain attributes
    - Florida NonMotorist Fatal and Serious Injuries YYYY found via 
        https://services1.arcgis.com/O1JpcwDW8sjYuddV/arcgis/rest/services/Florida_NonMotorist_Fatal_and_Serious_Injuries_YYYY/FeatureServer
        replace YYYY with year of interest, where data are available for 2014 to 2018, with 2019/2020 data not made available for download yet.
    - request has been made for 19/20 data
"""
# %% IMPORTS
import os
from glob import glob
from os import rename
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from datetime import datetime as dt
from collections import OrderedDict
from crash_config import (
    FIELDS_DICT,
    INCIDENT_TYPES,
    USE,
    DROPS,
    IN_CRS,
    OUT_CRS,
    COUNTY,
    HARMFUL_CODES,
    SEVERITY_CODES,
    CITY_CODES,
)

# %% PATHING
# define pathing
script_path = os.path.realpath(__file__)
script_folder = os.path.dirname(script_path)
project_folder = os.path.dirname(script_folder)
source_folder = os.path.join(
    os.path.dirname(project_folder),
    "Data",
    "Raw",
    "Safety_Security",
    "Crash_Data",
)
output_folder = os.path.join(
    os.path.dirname(project_folder),
    "Data",
    "Cleaned",
    "Safety_Security",
    "Crash_Data",
)


def rename_subset(csv, usecols, rename_dict, county):
    df = pd.read_csv(
        csv,
        usecols=usecols,
    )
    df.rename(columns=rename_dict, inplace=True)
    return df[df["COUNTY"] == county]


def split_date(df, date_field):
    df[date_field] = pd.to_datetime(arg=df[date_field], infer_datetime_format=True)
    df["DAY"] = df[date_field].dt.day
    df["MONTH"] = df[date_field].dt.month
    df["YEAR"] = df[date_field].dt.year
    return df


def combine_incidents(df, type_dict):
    for crash_type, crash_text in type_dict.items():
        mask = df[crash_type] == "Y"
        df.loc[mask, "TRANS_TYPE"] = crash_text


## work


source_files = glob(source_folder + "\*.csv")
gdf_list = []
for sf in source_files:
    # read data and format
    df = rename_subset(csv=sf, usecols=USE, rename_dict=FIELDS_DICT, county=COUNTY)
    split_date(df=df, date_field="DATE")
    combine_incidents(df=df, type_dict=INCIDENT_TYPES)
    # recode city to text value
    df.replace(to_replace={"CITY": CITY_CODES}, inplace=True)
    # recode harmful event to text value
    df.replace(to_replace={"HARM_EVNT": HARMFUL_CODES}, inplace=True)
    # recode crash injury severity
    df.replace(to_replace={"INJSEVER": SEVERITY_CODES}, inplace=True)
    
    # convert to geodataframe
    gdf = gpd.GeoDataFrame(
        df.drop(DROPS, axis=1),
        crs=IN_CRS,
        geometry=[Point(yx) for yx in zip(df["LONG"], df["LAT"])],
    )
    gdf.to_crs(crs=OUT_CRS, inplace=True)
    gdf.to_file(
        filename=os.path.join(output_folder, os.path.basename(sf)[:-4] + ".shp")
    )
    gdf_list.append(gdf)

all = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True), crs=gdf_list[0].crs)
oldest = int(all["YEAR"].min())
newest = int(all["YEAR"].max())
all.to_file(
    os.path.join(
        output_folder,
        f"Miami_Dade_NonMotorist_CrashData_{str(oldest)}-{str(newest)}.shp",
    )
)
