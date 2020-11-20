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
from shapely.geometry.polygon import Polygon as POLY
import os
from pathlib import Path
from six import string_types
import re
import json

# %% CONSTANTS - FOLDERS
SCRIPTS = Path(r"K:\Projects\MiamiDade\PMT\code")
ROOT = Path(SCRIPTS).parents[0]
DATA = os.path.join(ROOT, "Data")
RAW = os.path.join(DATA, "raw")
CLEANED = os.path.join(DATA, "cleaned")
REF = os.path.join(DATA, "reference")
BASIC_FEATURES = os.path.join(ROOT, "Basic_features.gdb", "Basic_features_SPFLE")
YEARS = [2014, 2015, 2016, 2017, 2018, 2019]
SNAPSHOT_YEAR = 2019


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


def gdfToFeatureClass(gdf, out_fc, sr=4326):
    """
    Creates a feature class or shapefile from a geopandas GeoDataFrame.

    Parameters
    ------------
    gdf: GeoDataFrame
    out_fc: Path
    sr: spatial reference, default=4326
        A spatial reference specification. Authority/factory code, WKT, WKID,
        ESRI name, path to .prj file, etc.

    Returns
    ---------
    out_fc: Path

    SeeAlso
    ---------
    jsonToFeatureClass
    """
    j = json.loads(gdf.to_json())
    jsonToFeatureClass(j, out_fc, sr=sr)


def jsonToFeatureClass(json_obj, out_fc, sr=4326):
    """
    Creates a feature class or shape file from a json object.

    Parameters
    -----------
    json_obj: dict
    out_fc: Path
    sr: spatial reference, default=4326
        A spatial reference specification. Authority/factory code, WKT, WKID,
        ESRI name, path to .prj file, etc.

    Returns
    --------
    out_fc: Path

    See Also
    ---------
    gdfToFeatureClass
    jsonToTable
    """
    # Stack features and attributes
    prop_stack = []
    geom_stack = []
    for ft in json_obj["features"]:
        attr_dict = ft["properties"]
        df = pd.DataFrame([attr_dict.values()], columns=attr_dict.keys())
        prop_stack.append(df)
        geom = arcpy.AsShape(ft["geometry"], False)
        geom_stack.append(geom)

    # Create output fc
    sr = arcpy.SpatialReference(sr)
    geom_type = geom_stack[0].type.upper()
    if arcpy.Exists(out_fc):
        if overwrite:
            arcpy.Delete_management(out_fc)
        else:
            raise RuntimeError(f"output {out_fc} already exists")
    out_path, out_name = os.path.split(out_fc)
    arcpy.CreateFeatureclass_management(out_path, out_name, geom_type,
                                        spatial_reference=sr)
    arcpy.AddField_management(out_fc, "LINEID", "LONG")

    # Add geometries
    with arcpy.da.InsertCursor(out_fc, ["SHAPE@", "LINEID"]) as c:
        for i, geom in enumerate(geom_stack):
            row = [geom, i]
            c.insertRow(row)

    # Create attributes dataframe
    prop_df = pd.concat(prop_stack)
    prop_df["LINEID"] = np.arange(len(prop_df))
    for excl in exclude:
        if excl in prop_df.columns.to_list():
            prop_df.drop(columns=excl, inplace=True)
    if arcpy.Describe(out_fc).dataType.lower() == "shapefile":
        prop_df.fillna(0.0, inplace=True)

    # Extend table
    print([f.name for f in arcpy.ListFields(out_fc)])
    print(prop_df.columns)
    return extendTableDf(out_fc, "LINEID", prop_df, "LINEID")


def jsonToTable(json_obj, out_file):
    """
    Creates an ArcGIS table from a json object.

    Parameters
    -----------
    json_obj: dict
    out_file: Path

    Returns
    --------
    out_file: Path

    SeeAlso
    ---------
    jsonToFeatureClass
    """
    # convert to dataframe
    gdf = gpd.GeoDataFrame.from_features(req_json["features"], crs=crs)
    df = pd.DataFrame(gdf.drop(columns="geometry"))
    return dfToTable(df, out_file)


def fetchJsonUrl(url, out_file, encoding="utf-8", is_spatial=False,
                 crs=4326, overwrite=False):
    """
    Retrieve a json/geojson file at the given url and convert to a
    data frame or geodataframe.

    Parameters
    -----------
    url: String
    out_file: Path
    encoding: String, default="uft-8"
    is_spatial: Boolean, default=False
        If True, dump json to geodataframe
    crs: String
    overwrite: Boolean

    Returns
    ---------
    out_file: Path
    """
    exclude = ["FID", "OID", "ObjectID", "SHAPE_Length", "SHAPE_Area"]
    http = urllib3.PoolManager()
    req = http.request("GET", url)
    req_json = json.loads(req.data.decode(encoding))

    if is_spatial:
        jsonToFeatureClass(json_obj, out_fc, sr=4326)

    else:
        prop_stack = []

        gpd.GeoDataFrame.from_features(req_json["features"], crs=crs)
        return pd.DataFrame(gdf.drop(columns="geometry"))


def copyFeatures(in_fc, out_fc, drop_columns=[], rename_columns={}):
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

    Returns
    ---------
    out_fc: Path
        Path to the file location for the copied features.
    """
    _unmapped_types_ = ["Geometry", "OID", "GUID"]
    field_mappings = arcpy.FieldMappings()
    fields = arcpy.ListFields(in_fc)
    keep_fields = []
    for f in fields:
        if f.name not in drop_columns and f.type not in _unmapped_types_:
            keep_fields.append(f.name)
    for kf in keep_fields:
        fm = arcpy.FieldMap()
        fm.addInputField(in_fc, kf)
        out_field = fm.outputField
        out_fname = rename_columns.get(kf, kf)
        out_field.name = out_fname
        out_field.aliasName = out_fname
        fm.outputField = out_field
        field_mappings.addFieldMap(fm)

    out_path, out_name = os.path.split(out_fc)
    arcpy.FeatureClassToFeatureClass_conversion(in_fc, out_path, out_name,
                                                field_mapping=field_mappings)

    return out_fc


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

    Returns
    --------
    out_file: Path
        Path to the output file (merged features saved to disk)
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

    return out_file


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


def dfToTable(df, out_table):
    """
    Use a pandas data frame to export an arcgis table.

    Parameters
    -----------
    df: DataFrame
    out_table: Path

    Returns
    --------
    out_table: Path
    """
    in_array = np.array(
        np.rec.fromrecords(
            df.values, names=df.dtypes.index.tolist()
        )
    )
    arcpy.da.NumPyArrayToTable(in_array, out_table)
    return out_table

def dfToPoints(df, out_fc, shape_fields, spatial_reference):
    """
    Use a pandas data frame to export an arcgis point feature class.

    Parameters
    -----------
    df: DataFrame
    out_fc: Path
    shape_fields: [String,...]
        Columns to be used as shape fields (x, y)
    spatial_reference: SpatialReference
        A spatial reference to use when creating the output features.

    Returns
    --------
    out_fc: Path
    """
    in_array = np.array(
        np.rec.fromrecords(
            df.values, names=df.dtypes.index.tolist()
        )
    )
    arcpy.da.NumPyArrayToFeatureClass(in_array, out_fc,
                                      shape_fields=shape_fields,
                                      spatial_reference=spatial_reference)
    return out_fc


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
        if type(row.geometry) == POLY:
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


def polygonsToPoints(in_fc, out_fc, fields="*", skip_nulls=False,
                     null_value=0):
    """
    Convenience function to dump polygon features to centroids and
    save as a new feature class.

    Parameters
    -------------
    in_fc: Path
    out_fc: Path
    fields: [String,...], default="*"
    skip_nulls: Boolean, default=False
    null_value: Integer, default=0
    """
    sr = arcpy.Describe(in_fc).spatialReference
    if fields == "*":
        fields = arcpy.ListFields(in_fc)
        fields = [f for f in fields if f.type != "Geometry"]
        fields = [f for f in fields if "shape" not in f.name.lower()]
        fields = [f for f in fields if "objectid" not in f.name.lower()]
        fields = [f.name for f in fields]
    elif isinstance(fields, string_types):
        fields = [fields]
    fields.append("SHAPE@XY")
    a = arcpy.da.FeatureClassToNumPyArray(in_fc, fields, skip_nulls=skip_nulls,
                                          null_value=null_value)
    arcpy.da.NumPyArrayToFeatureClass(a, out_fc, "SHAPE@XY",
                                      spatial_reference=sr)
    return out_fc


def sumToAggregateGeo(disag_fc, sum_fields, groupby_fields, agg_fc,
                      agg_id_field, output_fc, overlap_type="INTERSECT",
                      agg_funcs=np.sum, disag_wc=None, agg_wc=None,
                      flatten_disag_id=None, *args, **kwargs):
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
    flatten_disag_id: String, default=None
        If given, the disag features are assumed to contain redundant data,
        such as multiple rows showing the same parcel. The mean value by
        this field is used prior to summarization to aggregate features.
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
    if flatten_disag_id is not None:
        disag_fields.append(flatten_disag_id)

    # Set up the output feature class (copy features from agg_fc)
    out_ws, out_fc = os.path.split(output_fc)
    # out_ws, out_fc = output_fc.rsplit(r"\", 1)
    if arcpy.Exists(output_fc):
        arcpy.Delete_management(output_fc)
    arcpy.FeatureClassToFeatureClass_conversion(agg_fc, out_ws, out_fc)
    print(output_fc)
    sr = arcpy.Describe(agg_fc).spatialReference.exportToString()

    # Try, except to rollback
    try:
        sum_rows = []
        # shapes = []
        # Iterate over agg features
        agg_fields = [agg_id_field, "SHAPE@"]
        with arcpy.da.SearchCursor(output_fc, agg_fields) as agg_c:
        # with arcpy.da.SearchCursor(agg_fc, agg_fields) as agg_c:
            for agg_r in agg_c:
                agg_id, agg_shape = agg_r
                # shapes.append(
                #     POLY(
                #         [(pt.X, pt.Y) for pt in agg_shape.getPart()[0]]
                #     )
                # )
                # Select disag features there
                arcpy.SelectLayerByLocation_management(
                    disag_fc, overlap_type, agg_shape,
                    selection_type="NEW_SELECTION")
                # Dump to data frame
                df = pd.DataFrame(
                    arcpy.da.TableToNumPyArray(
                        disag_fc, disag_fields, skip_nulls=False, null_value=0)
                )
                df.fillna(0, inplace=True)
                # Flatten
                if flatten_disag_id is not None:
                    gfields = groupby_fields + [flatten_disag_id]
                    df = df.groupby(gfields).mean().reset_index()
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
                #sum_row = df_sum.reset_index()

                # Assign agg feature id
                # TODO: confirm agg_id_field does not collide with sum_row cols
                sum_row[agg_id_field] = agg_id

                # Add row to collection
                sum_rows.append(sum_row)
        # Bind all summary rows
        sum_data = pd.concat(sum_rows).fillna(0)
        # sum_data["geometry"] = shapes
        # Rename cols to eliminate special characters
        cols = sum_data.columns.to_list()
        sum_data.columns = [re.sub(r"[^A-Za-z0-9 ]+", "_", c) for c in cols]

        # Join to output table
        extendTableDf(output_fc, agg_id_field, sum_data, agg_id_field)
        # gdf = gpd.GeoDataFrame(sum_data, geometry="geometry", crs=sr)
        # gdfToFeatureClass(gdf, output_fc, sr=sr)

    except:
        raise
    finally:
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
        agg_id_field="Id", output_fc=r"D:\Users\DE7\Desktop\agg_test.gdb\bike_speed_agg")
