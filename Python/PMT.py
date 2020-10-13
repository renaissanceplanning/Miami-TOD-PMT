"""
Created: October 2020
@Author: Alex Bell

A collection of helper functions used throughout the PMT data acquisition,
cleaning, analysis, and summarization processes.
"""

import arcpy
import numpy as np
import urllib3
import geopandas as gpd
import pandas as pd
from shapely.geometry.polygon import Polygon
import os
from pathlib import Path
from six import string_types
import re
import json

# %% CONSTANTS - FOLDERS
SCRIPTS = os.getcwd()
ROOT = Path(SCRIPTS).parents[0]
DATA = os.path.join(ROOT, "Data")
RAW = os.path.join(DATA, "raw")
CLEANED = os.path.join(DATA, "cleaned")
REF = os.path.join(DATA, "reference")
BASIC_FEATURES = os.path.join(DATA, "Basic_features.gdb")


# %% FUNCTIONS
def makePath(in_folder, *subnames):
    """
    Dynamically set a path (e.g., for iteratively referencing
    year-specific geodatabases)

    Parameters
    -----------
    in_folder: String or Path
    subnames:
        A list of arguments to join in making the full path
        `{in_folder}/{subname_1}/.../{subname_n}

    Returns: Path
    """
    return os.path.join(in_folder, *subnames)


def fetchJsonUrl(url, encoding="utf-8", is_spatial=False, crs="epsg:4326"):
    """
    Retrieve a json/geojson file at the given url and convert to a
    data frame or geodataframe.

    PARAMETERS
    -----------
    url: String
    encoding: String, default="uft-8"
    is_spatial: Boolean, default=False
        If True, dump json to geodataframe
    crs: String
    """
    http = urllib3.PoolManager()
    req = http.request("GET", url)
    req_json = json.loads(req.data.decode(encoding))
    gdf = gpd.GeoDataFrame.from_features(
        req_json["features"], crs=crs)
    if is_spatial:
        return gdf
    else:
        return pd.DataFrame(gdf.drop(columns="geometry"))


def copyFeatures(in_fc, out_fc, drop_columns=[], rename_columns=[]):
    """
    Copy features from a raw directory to a cleaned directory.
    During copying, columns may be dropped or renamed.

    Parameters
    ------------
    in_fc: Path
    out_fc: Path
    drop_columns: [String,...]
        A list of column names to drop when copying features.
    rename_columns: {String: String,...}
        A dictionary with keys that reflect raw column names and
        values that assign new names to these columns.
    """
    gdf = gpd.read_file(in_fc)
    if drop_columns:
        gdf.drop(columns=drop_columns, inplace=True)
    if rename_columns:
        gdf.rename(columns=rename_columns, inplace=True)
    gdf.to_file(out_fc)


def mergeFeatures(raw_dir, fc_names, clean_dir, out_fc,
                  drop_columns=[], rename_columns=[]):
    """
    Combine feature classes from a raw folder in a single feature class in
    a clean folder.

    Parameters
    -------------
    raw_dir: Path
        The directory where all raw feature classes are stored.
    fc_names: [String,...]
        A list of feature classes in `raw_dir` to combine into a single
        feature class of all features in `clean_dir`.
    clean_dir: Path
        The directory where the cleaned output feature class will be stored.
    out_fc: String
        The name of the output feature class with combined features.
    drop_columns: [[String,...]]
        A list of lists. Each list corresponds to feature classes listed in
        `fc_names`. Each includes column names to drop when combining
        features.
    rename_columns: [{String: String,...},...]
        A list of dictionaries. Each dictionary corresponds to feature classes
        listed in `fc_names`. Each has keys that reflect raw column names and
        values that assign new names to these columns.
    """
    # Align inputs
    if isinstance(fc_names, string_types):
        fc_names = [fc_names]
    if not drop_columns:
        drop_columns = [[] for _ in fc_names]
    if not rename_columns:
        rename_columns = [{} for _ in fc_names]

    # Iterate over input fc's
    all_features = []
    for fc_name, drop_cols, rename_cols in zip(
            fc_names, drop_columns, rename_columns):
        # Read features
        in_fc = makePath(raw_dir, fc_name)
        gdf = gpd.read_file(in_fc)
        # Drop/rename
        gdf.drop(columns=drop_cols, inplace=True)
        gdf.rename(columns=rename_cols, inplace=True)
        all_features.append(gdf)

    # Concatenate features
    merged = pd.concat(all_features)

    # Save output
    out_file = makePath(clean_dir, out_fc)
    merged.to_file(out_file)


def colMultiIndexToNames(columns, separator="_"):
    """
    For a collection of columns in a data frame, collapse index levels to
    flat column names. Index level values are joined using the provided
    `separator`.

    Parameters
    -----------
    columns: pd.Index
    separator: String

    Returns
    --------
    flat_columns: pd.Index
    """
    if isinstance(columns, pd.MultiIndex):
        columns = columns.to_series().apply(lambda col: separator.join(col))
    return columns


def extendTableDf(in_table, table_match_field, df, df_match_field, **kwargs):
    """
    Use a pandas data frame to extend (add columns to) an existing table based
    through a join on key columns. Key values in the existing table must be
    unique.

    Parameters
    -----------
    in_table: Path, feature layer, or table view
        The existing table to be extended
    table_match_field: String
        The field in `in_table` on which to join values from `df`
    df: DataFrame
        The data frame whose columns will be added to `in_table`
    df_match_field: String
        The field in `df` on which join values to `in_table`
    kwargs:
        Optional keyword arguments to be passed to `arcpy.da.ExtendTable`.

    Returns
    --------
    None
        `in_table` is modified in place
    """
    in_array = np.array(
        np.rec.fromrecords(
            df.values, names=df.dtypes.index.tolist()
        )
    )
    arcpy.da.ExtendTable(in_table=in_table,
                         table_match_field=table_match_field,
                         in_array=in_array,
                         array_match_field=df_match_field,
                         **kwargs)


def multipolygonToPolygon(gdf):
    """
    For a geopandas data frame, convert multipolygon geometries in a single
    row into multiple rows of simply polygon geometries.

    This function is an adaptation of the approach outlined here:
    https://gist.github.com/mhweber/cf36bb4e09df9deee5eb54dc6be74d26

    Parameters
    -----------
    gdf: geodataframe
    """
    # Setup an empty geodataframe as the output container
    poly_df = gpd.GeoDataFrame(columns=gdf.columns)
    # Iterate over input rows
    for idx, row in gdf.iterrows():
        if type(row.geometry) == Polygon:
            # Append existing simple polygons to the output
            poly_df = poly_df.append(row, ignore_index=True)
        else:
            # Explode multi-polygons to simple features and append
            #  - Create a mini-geodataframe to assist
            mult_df = gpd.GeoDataFrame(columns=gdf.columns)
            recs = len(row.geometry)
            #  - Repare the feature many times in the mini-gdf
            mult_df = mult_df.append([row] * recs, ignore_index=True)
            #  - Iterate over rows keeping the i'th geom element as you go
            for geom_i in range(recs):
                mult_df.loc[geom_i, "geometry"] = row.geometry[geom_i]
            #  - Append mini-gdf rows to the output container
            poly_df = poly_df.append(mult_df, ignore_index=True)
    return poly_df


def sumToAggregateGeo(disag_fc, sum_fields, groupby_fields, agg_fc,
                      agg_id_field, output_fc, overlap_type="INTERSECT",
                      agg_funcs=np.sum, disag_wc=None, agg_wc=None,
                      *args, **kwargs):
    """
    Summarizes values for features in an input feature class based on their
    relationship to larger geographic units. For example, summarize dwelling
    units from parcels to station areas.

    Summarizations are recorded for each aggregation feature. If any groupby
    fields are provided or multiple agg funcs are provided, summary values are
    reported in multiple columns corresponding to the groupby values or agg
    function names. Note that special characters in observed values are 
    replaced with underscores. This may result in unexpected name collisions.

    Parameters
    -----------
    disag_fc: String or feature layer
        Path to a feature class or a feature layer object. The layer whose
        features and values will be summarized.
    sum_fields: String or [String,...]
        One or more field names in `disag_fc` whose values will be summarized
        within features in `agg_fc`.
    groupby_fields: String or [String,...]
        One or more field names in `disag_fc` to be used to group features
        prior to aggregation. Unique combinaitons of `groupby_field` values
        will appear in field names in `output_fc`.
    agg_fc: String or feature layer
        The features to which disag_fc will be aggregated and summarized.
        These must be polygons.
    agg_id_field: String
        A field in `agg_fc` that uniquely identifies each aggregation feature.
    output_fc: String
        Path to a new feature class to be created by the function.
    overlap_type: String, default="INTERSECT"
        The spatial relationship by which to associate disag features with
        aggregation features.
    agg_funcs: callable, default=np.sum
        Aggregation functions determine what summary statistics are reported
        for each aggregation feature.
    disag_wc: String, default=None
        A where clause for selecting disag features to include in the
        summarization process.
    agg_wc: String, default=None
        A where clause for selecting aggregation features to include in the
        summarization process.
    args:
        Positional arguments to pass to `agg_funcs`
    kwargs:
        Keyword arguments to pass to `agg_funcs`

    Returns
    --------
    None
        `output_fc` is written to disk.
    """
    # Handle inputs
    #  - Confirm agg features are polygons
    desc = arcpy.Describe(agg_fc)
    if desc.shapeType != u"Polygon":
        raise TypeError("Aggregation features must be polygons")
    sr = desc.spatialReference

    #  - If path to FC is given, make feature layer
    #    Otherwise, let's assume these are already feature layers
    if isinstance(disag_fc, string_types):
        disag_fc = arcpy.MakeFeatureLayer_management(disag_fc, "__disag_fc__")

    if isinstance(agg_fc, string_types):
        agg_fc = arcpy.MakeFeatureLayer_management(agg_fc, "__agg_fc__")

    #  - Apply where clauses to input layers
    if disag_wc:
        disag_fc = arcpy.MakeFeatureLayer_management(
            disag_fc, "__disag_subset__", disag_wc)
    if agg_wc:
        arcpy.SelectLayerByAttribute_management(
            agg_fc, "SUBSET_SELECTION", agg_wc)

    #  - Disag field references
    if isinstance(sum_fields, string_types):
        sum_fields = [sum_fields]
    if isinstance(groupby_fields, string_types):
        groupby_fields = [groupby_fields]
    disag_fields = sum_fields + groupby_fields

    # Set up the output feature class (copy features from agg_fc)
    out_ws, out_fc = os.path.split(output_fc)
    # out_ws, out_fc = output_fc.rsplit(r"\", 1)
    arcpy.FeatureClassToFeatureClass_conversion(agg_fc, out_ws, out_fc)

    # Try, except to rollback
    try:
        sum_rows = []
        # Iterate over agg features
        agg_fields = [agg_id_field, "SHAPE@"]
        with arcpy.da.SearchCursor(output_fc, agg_fields) as agg_c:
            for agg_r in agg_c:
                agg_id, agg_shape = agg_r
                # Select disag features there
                arcpy.SelectLayerByLocation_management(
                    disag_fc, overlap_type, agg_shape,
                    selection_type="NEW_SELECTION")
                # Dump to data frame
                df = pd.DataFrame(
                    arcpy.da.TableToNumPyArray(
                        disag_fc, disag_fields)
                )
                # Groupby and apply agg funcs
                if groupby_fields:
                    gb = df.groupby(groupby_fields)
                    df_sum = gb.agg(agg_funcs, *args, **kwargs)
                    if len(groupby_fields) > 1:
                        df_unstack = df_sum.unstack(groupby_fields).fillna(0)
                    else:
                        df_unstack = df_sum.unstack().fillna(0)
                else:
                    df_sum = df
                    df_unstack = df_sum.unstack().fillna(0)
                df_unstack.index = colMultiIndexToNames(df_unstack.index)
                sum_row = df_unstack.to_frame().T

                # Assign agg feature id
                # TODO: confirm agg_id_field does not collide with sum_row cols
                sum_row[agg_id_field] = agg_id

                # Add row to collection
                sum_rows.append(sum_row)
        # Bind all summary rows
        sum_data = pd.concat(sum_rows).fillna(0)

        # Rename cols to eliminate special characters
        cols = sum_data.columns.to_list()
        sum_data.columns = [re.sub("[^a-zA-Z0-9_]", "_", c) for c in cols]

        # Join to output table
        extendTableDf(output_fc, agg_id_field, sum_data, agg_id_field)

    except:
        arcpy.AddWarning("Error encountered, rolling back changes")
        # delete all temp layers
        arcpy.Delete_management("__disag_fc__")
        arcpy.Delete_management("__agg_fc__")
        arcpy.Delete_management("__disag_subset__")
        # Delete output fc
        arcpy.Delete_management(output_fc)
        raise


if __name__ == "__main__":
    arcpy.env.overwriteOutput = True
    sumToAggregateGeo(
        disag_fc=r"K:\Projects\MiamiDade\PMT\Data\Cleaned\Safety_Security\Crash_Data"
                 r"\Miami_Dade_NonMotorist_CrashData_2012-2020.shp",
        sum_fields=["SPEED_LIM"], groupby_fields=["CITY"],
        agg_fc=r"K:\Projects\MiamiDade\PMT\Basic_features.gdb\Basic_features_SPFLE\SMART_Plan_Station_Areas",
        agg_id_field="Id", output_fc=r"C:\Users\V_RPG\Desktop\bike_speed_agg")
