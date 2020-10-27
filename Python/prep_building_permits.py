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
    STATUS_DICT,
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
    parcel_layer = Path(RAW, 'Parcels', 'Miami_2019.shp')

    out_path = Path(CLEANED, "BuildingPermits")
    if not out_path.exists():
        Path.mkdir(out_path)
    out_name = "Miami_Dade_BuildingPermits.shp"

    # read parcels to geodataframe and convert to centroids
    print('- reading in parcel data to points')
    parcels = gpd.read_file(parcel_layer)
    parcel_pts = parcels.copy()
    parcel_pts['geometry'] = parcel_pts['geometry'].centroid

    # read permit data to dataframe and reformat
    print('- reading in permit data')
    raw_df = csv_to_df(csv_file=data, use_cols=USE, rename_dict=FIELDS_DICT)
    raw_df['COST'] = raw_df['CONST_COST'] + raw_df['ADMIN_COST']    # combine cost
    print('...combined cost generated')

    # drop fake data - Keith Richardson of RER informed us that any PROC_NUM/ADDRESS that contains with 'SMPL' or
    # 'SAMPLE' should be ignored as as SAMPLE entry
    ignore_text = ['SMPL', "SAMPLE", ]
    for ignore in ignore_text:
        for col in ['PROC_NUM', 'ADDRESS']:
            raw_df = raw_df[~raw_df[col].str.contains(ignore)]

    # fix parcelno to string of 13 len
    raw_df['PARCELNO'] = raw_df['PARCELNO'].astype(np.str)
    raw_df['PARCELNO'] = raw_df['PARCELNO'].apply(lambda x: x.zfill(13))

    # id project as pedestrain oriented
    raw_df["PED_ORIENTED"] = np.where(raw_df.CAT_CODE.str.contains(CAT_CODE_PEDOR), 1, 0)

    # set landuse codes appropriately accounting for pedoriented dev
    raw_df['CAT_CODE'] = np.where(raw_df.CAT_CODE.str.contains(CAT_CODE_PEDOR),
                                   raw_df.CAT_CODE.str[:-2],
                                   raw_df.CAT_CODE)
    print('...pedestrian oriented projects identified')
    # set project status
    raw_df['STATUS'] = raw_df['STATUS'].map(STATUS_DICT, na_action='NONE')
    print('...project status set')

    # add landuse codes
    lu_df = pd.read_csv(CAT_CODE_TBL)
    permit_df = raw_df.merge(right=lu_df, how="inner", on='CAT_CODE')
    print('...land use codes set')
    permit_df.drop(columns=DROPS, inplace=True)

    # join permits to parcel points MANY-TO-ONE
    print('...merging permit data to parcel points')
    permit_points = permit_df.merge(right=parcel_pts, how="inner", on='PARCELNO')
    permit_points = gpd.GeoDataFrame(permit_points, geometry="geometry")
    permit_points.to_file(Path(out_path, out_name))
    print(f"...data written here {out_path}")


    #TODO: sort out these use codes --> 832-00 (potentially equates to 932-00), 9999-00, 944-00
    #TODO:  they dont appear in the CAT_CODES Table

