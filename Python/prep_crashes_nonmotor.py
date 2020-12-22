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
# import geopandas as gpd
import pandas as pd
import time
import os
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


def clean_crashes_to_GDF(
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


def validate_json(json_file):
    try:
        return json.load(fp=json_file)
    except ValueError as e:
        message = f"invalid json file passed: {e}"
        arcpy.AddMessage(message)
        raise ValueError(message)


def clean_bike_ped_crashes(
        input_geojson, out_fc, where_clause=None,
        use_cols=USE, rename_dict=FIELDS_DICT):
    # validate data are json
    if validate_json(json_file=input_geojson):
        # define out pathing
        out_path, out_fc = os.path.split(out_fc)
        # convert json to temp feature class
        temp_points = r"in_memory\\crash_points"
        all_features = arcpy.JSONToFeatures_conversion(
            in_json_file=input_geojson, out_features=temp_points,
            geometry_type="POINT")
        # reformat attributes and keep only useful
        fields = [f.name for f in arcpy.ListFields(all_features)]
        drop_fields = [f for f in fields if f not in use_cols]
        for drop in drop_fields:
            arcpy.DeleteField_management(
                in_table=all_features, drop_field=drop)
        # rename attributes
        for name, rename in rename_dict.items():
            arcpy.AlterField_management(
                in_table=all_features, field=name,
                new_field_name=rename, new_field_alias=rename)
        # utilize where clause to split up points
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=all_features, out_path=out_path,
            out_name=out_fc, where_clause=where_clause)


if __name__ == "__main__":
    # if running code from a local github repo branch
    if GITHUB:
        ROOT = r"C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT"
        DATA = makePath(ROOT, 'Data')
        RAW = makePath(DATA, "Raw")
        CLEANED = makePath(DATA, "Cleaned")

    data = makePath(RAW, "Safety_Security", "Crash_Data", "bike_ped.geojson")
    # out_path = makePath(CLEANED, "Safety_Security", "Crash_Data")
    # out_name = "Miami_Dade_NonMotorist_CrashData.shp"
    # cleaned_file = makePath(out_path, out_name)
    # clean_bike_ped_crashes(input_path=data, out_path=cleaned_file, use_cols=USE,
    #                        rename_dict=INCIDENT_TYPES, in_crs=IN_CRS, out_crs=OUT_CRS, )
    for year in YEARS:
        if year == 2019:
            out_gdb = makePath(DATA, f"PMT_{year}_ideal.gdb")
            FDS = makePath(out_gdb, "Points")
            out_fc = makePath(out_gdb, FDS, 'bike_ped_crashes')
            year_wc = f"'YEAR' = {year}"
            clean_bike_ped_crashes(input_geojson=data, out_fc=out_fc, where_clause=year_wc,
                                   use_cols=USE, rename_dict=FIELDS_DICT)


