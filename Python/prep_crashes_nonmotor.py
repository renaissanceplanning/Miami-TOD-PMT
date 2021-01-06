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
import numpy as np
import pandas as pd
import time
import os
# from PMT import makePath
import arcpy

arcpy.env.overwriteOutput = True

from config.config_project import RAW, CLEANED, YEARS

from Python.download.download_config import (
    CRASH_FIELDS_DICT,
    CRASH_INCIDENT_TYPES,
    USE_CRASH,
    DROP_CRASH,
    IN_CRS,
    OUT_CRS,
    CRASH_HARMFUL_CODES,
    CRASH_SEVERITY_CODES,
    CRASH_CITY_CODES,
)

GITHUB = True

source_folder = os.path.join(
    RAW,
    "Safety_Security",
    "Crash_Data",
)
output_folder = os.path.join(
    CLEANED,
    "Safety_Security",
    "Crash_Data",
)


# def geojson_to_gdf(geojson, crs, use_cols=USE, rename_dict=FIELDS_DICT):
#     """
#     reads in geojson, drops unnecessary attributes and renames the kept attributes
#     Parameters
#     ----------
#     geojson: json
#         GeoJSON text file consisting of points for bike/ped crashes
#     crs:
#         EPSG code representing
#     use_cols: list
#         list of columns to use in formatting
#     rename_dict: dict
#         dictionary to map existing column names to more readable names
#     Returns
#     -------
#         geodataframe
#     """
#     with open(str(geojson), "r") as src:
#         js = json.load(src)
#         gdf = gpd.GeoDataFrame.from_features(js["features"], crs=crs, columns=use_cols)
#         gdf.rename(columns=rename_dict, inplace=True)
#     return gdf


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


def clean_crashes(
        input_df,
        out_path,
        usecols=USE_CRASH,
        rename_dict=CRASH_INCIDENT_TYPES,
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

    split_date(gdf=input_df, date_field="DATE")
    combine_incidents(gdf=input_df, type_dict=rename_dict)
    # recode integer coded attributes
    input_df["CITY"] = input_df["CITY"].apply(lambda x: CRASH_CITY_CODES.get(int(x), "None"))
    input_df["HARM_EVNT"] = input_df["HARM_EVNT"].apply(lambda x: CRASH_HARMFUL_CODES.get(int(x), "None"))
    input_df["INJSEVER"] = input_df["INJSEVER"].apply(lambda x: CRASH_SEVERITY_CODES.get(int(x), "None"))
    # drop unneeded fields
    input_df.drop(columns=DROP_CRASH, inplace=True)
    # write out to featureclass
    # build array from dataframe
    temp_fc = r"in_memory\temp_points"
    shape_fields = ["LONG", "LAT"]
    from_sr = 4326
    in_array = np.array(
        np.rec.fromrecords(
            input_df.values, names=input_df.dtypes.index.tolist()
        )
    )
    # write to temp feature class
    arcpy.da.NumPyArrayToFeatureClass(
        in_array=in_array,
        out_table=temp_fc,
        shape_fields=shape_fields,
        spatial_reference=from_sr,
    )
    # reproject if needed, otherwise dump to output location
    if from_sr != to_sr:
        arcpy.Project_management(
            in_dataset=temp_fc, out_dataset=out_fc, out_coor_system=to_sr
        )
    else:
        out_path, out_fc = os.path.split(out_fc)
        if overwrite:
            checkOverwriteOutput(output=out_fc, overwrite=overwrite)
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=temp_fc, out_path=out_path, out_name=out_fc
        )
    # clean up temp_fc
    arcpy.Delete_management(in_data=temp_fc)


def clean_and_drop(feature_class, use_cols=USE_CRASH, rename_dict=CRASH_FIELDS_DICT):
    # reformat attributes and keep only useful
    fields = [f.name for f in arcpy.ListFields(feature_class) if not f.required]
    drop_fields = [f for f in fields if f not in list(use_cols) + ['Shape']]
    for drop in drop_fields:
        arcpy.DeleteField_management(in_table=feature_class, drop_field=drop)
    # rename attributes
    for name, rename in rename_dict.items():
        arcpy.AlterField_management(in_table=feature_class, field=name, new_field_name=rename, new_field_alias=rename)


def validate_json(json_file):
    try:
        with open(json_file) as file:
            arcpy.AddMessage('Valid JSON')
            return json.load(file)
    except json.decoder.JSONDecodeError as e:
        message = f"Invalid json file passed: {e}"
        arcpy.AddMessage(message)
        raise ValueError(message)


def clean_bike_ped_crashes(
        in_fc, out_path, out_name, where_clause=None,
        use_cols=USE_CRASH, rename_dict=CRASH_FIELDS_DICT):
        # dump subset to new FC
        out_fc = os.path.join(out_path, out_name)
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=in_fc, out_path=out_path,
            out_name=out_name, where_clause=where_clause)

        # reformat attributes and keep only useful
        clean_and_drop(feature_class=out_fc)

        # update city code/injury severity/Harmful event to text value
        with arcpy.da.UpdateCursor(out_fc, field_names=["CITY", "INJSEVER", "HARM_EVNT"]) as cur:
            for row in cur:
                city, severity, event = row
                if city is not None:
                    row[0] = CRASH_CITY_CODES[int(city)]
                if severity is not None:
                    row[1] = CRASH_SEVERITY_CODES[int(severity)]
                if event is not None:
                    row[2] = CRASH_HARMFUL_CODES[int(event)]
                cur.updateRow(row)

        # combine bike and ped type into single attribute and drop original
        arcpy.AddField_management(in_table=out_fc, field_name="TRANS_TYPE", field_type="TEXT")
        fields = ["TRANS_TYPE", "PED_TYPE", "BIKE_TYPE"]
        with arcpy.da.UpdateCursor(out_fc, field_names=fields) as cur:
            for row in cur:
                both, ped, bike = row
                if ped == "Y":
                    row[0] = "PEDESTRIAN"
                if bike == "Y":
                    row[0] = "BIKE"
                cur.updateRow(row)
        for field in fields[1:]:
            arcpy.DeleteField_management(in_table=out_fc, drop_field=field)


if __name__ == "__main__":

    # if running code from a local github repo branch
    if GITHUB:
        ROOT = r"K:\Projects\MiamiDade\PMT"
        DATA = os.path.join(ROOT, 'Data')
        RAW = os.path.join(DATA, "Raw")
        CLEANED = os.path.join(DATA, "Cleaned")
        TEST = os.path.join(DATA, "Temp")

    data = os.path.join(RAW, "Safety_Security", "Crash_Data", "bike_ped.geojson")
    # out_path = makePath(CLEANED, "Safety_Security", "Crash_Data")
    # out_name = "Miami_Dade_NonMotorist_CrashData.shp"
    # cleaned_file = makePath(out_path, out_name)
    # clean_bike_ped_crashes(input_path=data, out_path=cleaned_file, use_cols=USE,
    #                        rename_dict=INCIDENT_TYPES, in_crs=IN_CRS, out_crs=OUT_CRS, )
    # setup_project.build_year_gdb(TEST)
    # validate data are json
    if validate_json(json_file=data):
        try:
            # convert json to temp feature class
            temp_points = r"in_memory\\crash_points"
            all_features = arcpy.JSONToFeatures_conversion(in_json_file=data, out_features=temp_points,
                                                           geometry_type="POINT")
            # all_df = pd.DataFrame(arcpy.da.TableToNumPyArray(in_table=all_features,
            #                                                  field_names=list(FIELDS_DICT.keys())))
            for year in YEARS:
                if year == 2019:
                    out_gdb = os.path.join(TEST, f"PMT_{year}.gdb")
                    FDS = os.path.join(out_gdb, "Points")
                    out_name = 'BikePedCrashes'
                    year_wc = f'"CALENDAR_YEAR" = {year}'
                    clean_bike_ped_crashes(in_fc=all_features, out_path=FDS, out_name=out_name,
                                           where_clause=year_wc, use_cols=USE_CRASH, rename_dict=CRASH_FIELDS_DICT)
        except Exception as e:
            arcpy.AddMessage(e)
            if arcpy.Exists(all_features):
                arcpy.Delete_management(all_features)

