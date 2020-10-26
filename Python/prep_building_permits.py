"""
Created: October 2020
@Author: Charles Rudder

Convert data from MD RER Road Impact Fee database report to table joinable to Parcels

Sources inlcude
"""
# %% IMPORTS
import json
import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
from PMT import (
    DATA,
    RAW,
    CLEANED,
    makePath,
)
from config.config_permits import (
    FIELDS_DICT,
    USE,
    DROPS,
    IN_CRS,
    OUT_CRS,
    CAT_CODE_PEDOR,
    CAT_CODE_TBL
)

github = True

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
    gdf: GeoDataFrame
        GeoDataFrame with a date field
    date_field: column name
GeoDataFrame    Returns
    -------
        GeoDataFrame reformatted to include split day, month and year
    """
    # convert unix time to date
    # gdf[date_field] = gdf[date_field].apply(
    #     lambda x: str(x)[:10])
    # gdf[date_field] = gdf[date_field].apply(
    #     lambda x: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(x)))
    # )
    gdf[date_field] = pd.to_datetime(
        arg=gdf[date_field],
        infer_datetime_format=True)
    gdf["DAY"] = gdf[date_field].dt.day
    gdf["MONTH"] = gdf[date_field].dt.month
    gdf["YEAR"] = gdf[date_field].dt.year
    return gdf


def csv_to_df(csv_file, use_cols, rename_dict):
    """
    helper function to convert CSV file to pandas dataframe, and drop unnecessary columns
    assumes any strings with comma (,) should have those removed and dtypes infered
    Parameters
    ----------
    csv_file: String
        path to csv file
    use_cols: List
        list of columns to keep from input csv
    rename_dict: dict
        dictionary mapping existing column name to standardized column names

    Returns
        Pandas dataframe
    -------

    """
    df = pd.read_csv(
        filepath_or_buffer=csv_file,
        usecols=use_cols,
        thousands=",")
    df = df.convert_dtypes()
    df.rename(
        columns=rename_dict,
        inplace=True)
    return df


if __name__ == "__main__":
    # if running code from a local github repo branch
    if github:
        ROOT = r"K:\Projects\MiamiDade\PMT\Data"
        RAW = Path(ROOT, "Raw")
        CLEANED = Path(ROOT, "Cleaned")
    data = Path(RAW, "BuildingPermits", "Road Impact Fee Collection Report -- 2019.csv")
    out_path = Path(CLEANED, "BuildingPermits")
    if not out_path.exists():
        Path.mkdir(out_path)
    out_name = "Miami_Dade_BuildingPermits.csv"
    raw_df = csv_to_df(csv_file=data, use_cols=USE, rename_dict=FIELDS_DICT)
    raw_df['PARCEL_NO'] = raw_df['PARCEL_NO'].astype(np.str)
    raw_df['PARCEL_NO'] = raw_df['PARCEL_NO'].apply(lambda x: x.zfill(13))
    raw_df["PED_ORIENTED"] = np.where(raw_df.CAT_CODE.str.contains(CAT_CODE_PEDOR), 1, 0)
    raw_df['CAT_CODE'] = np.where(raw_df.CAT_CODE.str.contains(CAT_CODE_PEDOR),
                                   raw_df.CAT_CODE.str[:-2],
                                   raw_df.CAT_CODE)
    lu_df = pd.read_csv(CAT_CODE_TBL)
    merge = raw_df.merge(right=lu_df, how="inner", on='CAT_CODE')
    merge.to_csv(Path(out_path, out_name))
    print("done")

    #TODO: sort out these use codes --> 832-00 (potentially equates to 932-00), 9999-00, 944-00
    #TODO:  they dont appear in the CAT_CODES Table

