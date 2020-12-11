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
import json
import geopandas as gpd
import pandas as pd
import time
from pathlib import Path
from PMT import makePath
import arcpy
arcpy.env.overwriteOutput = True

from config.config_project import SCRIPTS, DATA, RAW, CLEANED, YEARS

from config.config_crashes import (
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

GITHUB = True

source_folder = makePath(
    RAW,
    "Safety_Security",
    "Crash_Data",
)
output_folder = makePath(
    CLEANED,
    "Safety_Security",
    "Crash_Data",
)


def geojson_to_gdf(geojson, crs, use_cols=USE, rename_dict=FIELDS_DICT):
    """
    reads in geojson, drops unnecessary attributes and renames the kept attributes
    Parameters
    ----------
    geojson: json
        GeoJSON text file consisting of points for bike/ped crashes
    crs:
        EPSG code representing
    use_cols: list
        list of columns to use in formatting
    rename_dict: dict
        dictionary to map existing column names to more readable names
    Returns
    -------
        geodataframe
    """
    with open(str(geojson), "r") as src:
        js = json.load(src)
        gdf = gpd.GeoDataFrame.from_features(js["features"], crs=crs, columns=use_cols)
        gdf.rename(columns=rename_dict, inplace=True)
    return gdf


def split_date(gdf, date_field):
    """
    ingest date attribute and splits it out to DAY, MONTH, YEAR
    Parameters
    ----------
    gdf: geodataframe
        geodataframe with a date field
    date_field: column name
        geodatafram column name containing a well formatted date
    Returns
    -------
        geodataframe reformatted to include split day, month and year
    """
    # convert unix time to da
    gdf[date_field] = gdf[date_field].apply(lambda x: str(x)[:10])
    gdf[date_field] = gdf[date_field].apply(
        lambda x: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(x)))
    )
    gdf[date_field] = pd.to_datetime(arg=gdf[date_field], infer_datetime_format=True)
    gdf["DAY"] = gdf[date_field].dt.day
    gdf["MONTH"] = gdf[date_field].dt.month
    gdf["YEAR"] = gdf[date_field].dt.year
    return gdf


def combine_incidents(gdf, type_dict):
    """
    Combines boolean bike involved, pedestrian involved and bike/ped involved
    columns into a single text attribute
    Parameters
    ----------
    df: pandas dataframe
    type_dict: dictionary
        dictionary mapping 3 types to text representation

    Returns
    -------
    None
        maps boolean incidents types to text representation column
    """
    for crash_type, crash_text in type_dict.items():
        mask = gdf[crash_type] == "Y"
        gdf.loc[mask, "TRANS_TYPE"] = crash_text


def clean_bike_ped_crashes(
    input_path,
    out_path,
    usecols=USE,
    rename_dict=INCIDENT_TYPES,
    in_crs=IN_CRS,
    out_crs=OUT_CRS,
):
    """
    ingests a file path to a raw geojson file to clean and format for use
    Parameters
    ----------
    input_path: String
        string path to geojson file
    out_path: String
        string path to output folder
    out_name: String
        string name of output file
    usecols: List
        list of columns to use in formatting
    rename_dict: dict
        dictionary mapping existing columns to output names
    county: String
        county containing data of interest
    in_crs: pyproj crs
        EPSG code of the input data
    out_crs: pyProj crs
        EPSG code of the output data

    Returns
    -------
    None
    """
    if out_path.exists():
        out_path.unlink()
    bp_gdf = geojson_to_gdf(
        geojson=input_path, crs=in_crs, use_cols=usecols, rename_dict=FIELDS_DICT
    )

    split_date(gdf=bp_gdf, date_field="DATE")
    combine_incidents(gdf=bp_gdf, type_dict=rename_dict)
    # recode integer coded attributes
    bp_gdf["CITY"] = bp_gdf["CITY"].apply(lambda x: CITY_CODES.get(int(x), "None"))
    bp_gdf["HARM_EVNT"] = bp_gdf["HARM_EVNT"].apply(
        lambda x: HARMFUL_CODES.get(int(x), "None")
    )
    bp_gdf["INJSEVER"] = bp_gdf["INJSEVER"].apply(
        lambda x: SEVERITY_CODES.get(int(x), "None")
    )
    # drop unneeded fields
    bp_gdf.drop(columns=DROPS, inplace=True)
    # reproject to project CRS
    bp_gdf.to_crs(epsg=out_crs, inplace=True)
    # write out to shapefile
    bp_gdf.to_file(filename=out_path)


if __name__ == "__main__":
    # if running code from a local github repo branch
    if GITHUB:
        ROOT = r"K:\Projects\MiamiDade\PMT"
        DATA = Path(ROOT, 'Data')
        RAW = Path(DATA, "Raw")
        CLEANED = Path(DATA, "Cleaned")

    data = Path(RAW, "Safety_Security", "Crash_Data", "bike_ped.geojson")
    out_path = Path(CLEANED, "Safety_Security", "Crash_Data")
    out_name = "Miami_Dade_NonMotorist_CrashData.shp"
    cleaned_file = Path(out_path, out_name)
    clean_bike_ped_crashes(input_path=data, out_path=cleaned_file, usecols=USE,
                           rename_dict=INCIDENT_TYPES, in_crs=IN_CRS, out_crs=OUT_CRS, )
    for year in YEARS:
        out_gdb = Path(ROOT, f"PMT_{year}.gdb")
        crashes = Path(out_gdb, "SafetySecurity")
        year_wc = f'{arcpy.AddFieldDelimiters(cleaned_file, "YEAR")} = {year}'
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=str(cleaned_file), out_path=str(crashes), out_name="bike_ped_crashes", where_clause=year_wc)

