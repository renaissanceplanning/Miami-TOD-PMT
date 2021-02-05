"""
Created: October 2020
@Author: Alex Bell

A collection of helper functions used throughout the PMT data acquisition,
cleaning, analysis, and summarization processes.
"""
# %% imports
import numpy as np
import urllib3
import geopandas as gpd
import pandas as pd
from shapely.geometry.polygon import Polygon as POLY

import os
import tempfile
from pathlib import Path
from six import string_types
from collections.abc import Iterable

import re
import json

# import arcpy last as arc messes with global states on import likely changing globals in a way that doesnt allow
# other libraries to locate their expected resources
import arcpy

# %% CONSTANTS - FOLDERS
SCRIPTS = Path(r"K:\Projects\MiamiDade\PMT\code")
ROOT = Path(SCRIPTS).parents[0]
DATA = os.path.join(ROOT, "Data")
RAW = os.path.join(DATA, "Raw")
CLEANED = os.path.join(DATA, "Cleaned")
REF = os.path.join(DATA, "Reference")
BASIC_FEATURES = os.path.join(DATA, "PMT_BasicFeatures.gdb", "BasicFeatures")
YEARS = [2014, 2015, 2016, 2017, 2018, 2019]
SNAPSHOT_YEAR = 2019

EPSG_LL = 4326
EPSG_FLSPF = 2881
EPSG_WEB_MERC = 3857
SR_WGS_84 = arcpy.SpatialReference(EPSG_LL)
SR_FL_SPF = arcpy.SpatialReference(EPSG_FLSPF)  # Florida_East_FIPS_0901_Feet
SR_WEB_MERCATOR = arcpy.SpatialReference(EPSG_WEB_MERC)


# %% UTILITY CLASSES
''' column and aggregation classes '''
class Column():
    def __init__(self, name, default=0.0, rename=None):
        self.name = name
        self.default = default
        self.rename = rename


class AggColumn(Column):
    def __init__(self, name, agg_method=sum, default=0.0, rename=None):
        Column.__init__(self, name, default, rename)
        self.agg_method = agg_method


class CollCollection(AggColumn):
    def __init__(self, name, input_cols, agg_method=sum, default=0.0):
        AggColumn.__init__(self, name, agg_method, default)
        self.input_cols = input_cols

    def __setattr__(self, name, value):
        if name == "input_cols":
            valid = True
            if isinstance(value, string_types):
                valid = False
            elif not isinstance(value, Iterable):
                valid = False
            elif len(value) <= 1:
                valid = False
            elif not isinstance(value[0], string_types):
                valid = False
            # Set property of raise error
            if valid:
                super().__setattr__(name, value)
            else:
                raise ValueError(
                    f"Expected iterable of column names for `input_cols`")
        else:
            super().__setattr__(name, value)

    def defaultsDict(self):
        if isinstance(self.default, Iterable) and \
                not isinstance(self.default, string_types):
            return dict(zip(self.input_cols, self.default))
        else:
            return dict(
                zip(self.input_cols,
                    [self.default for ic in self.input_cols]
                    )
            )


class Consolidation(CollCollection):
    def __init__(self, name, input_cols, cons_method=sum,
                 agg_method=sum, default=0.0):
        CollCollection.__init__(self, name, input_cols, agg_method, default)
        self.cons_method = cons_method


class MeltColumn(CollCollection):
    def __init__(self, label_col, val_col, input_cols,
                 agg_method=sum, default=0.0):
        CollCollection.__init__(self, val_col, input_cols, agg_method, default)
        self.label_col = label_col
        self.val_col = val_col


class Join(CollCollection):
    def __init__(self, on_col, input_cols, agg_method=sum, default=0.0):
        CollCollection.__init__(self, None, input_cols, agg_method, default)
        self.on_col = on_col


    ''' comparison classes '''
class Comp:
    """
    Comparison methods:
      - __eq__() = equals [==]
      - __ne__() = not equal to [!=]
      - __lt__() = less than [<]
      - __le__() = less than or equal to [<=]
      - __gt__() = greater than [>]
      - __ge__() = greater than or equal to [>=]
    """

    def __init__(self, comp_method, v):
        _comp_methods = {
            "==": "__eq__",
            "!=": "__ne__",
            "<": "__lt__",
            "<=": "__le__",
            ">": "__gt__",
            ">=": "__ge__"
        }
        self.comp_method = _comp_methods[comp_method]
        self.v = v

    def eval(self, val):
        return getattr(val, self.comp_method)(self.v)


class And:
    """
    """

    def __init__(self, criteria):
        self.criteria = criteria

    def __setattr__(self, name, value):
        if name == "criteria":
            criteria = []
            if isinstance(value, Iterable):
                for v in value:
                    if not isinstance(v, Comp):
                        raise TypeError(f"Expected Comp, got {type(v)}")
                    criteria.append(v)
            else:
                if isinstance(value, Comp):
                    criteria.append(value)
                else:
                    raise TypeError(f"Expected Criterion, got {type(value)}")
            super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    def eval(self, *vals):
        """
        """
        # Check
        try:
            v = vals[1]
        except IndexError:
            vals = [vals[0] for _ in self.criteria]
        bools = [c.eval(v) for c, v in zip(self.criteria, vals)]

        return np.logical_and.reduce(bools)


class Or:
    """
    """

    def __init__(self, vector, criteria):
        self.vector = vector
        if isinstance(criteria, Iterable):
            self.criteria = criteria  # TODO: validate criteria
        else:
            self.criteria = [criteria]

    def eval(self):
        return (
            np.logical_or.reduce(
                [c.eval(self.vector) for c in self.criteria]
            )
        )



# %% FUNCTIONS
# TODO: Review all functions here and deprecate as makes sense
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


def checkOverwriteOutput(output, overwrite=False):
    """
    A helper function that checks if an output file exists and
    deletes the file if an overwrite is expected.

    Parameters
    -------------
    output: Path
        The file to be checked/deleted
    overwrite: Boolean
        If True, `output` will be deleted if it already exists.
        If False, raises `RuntimeError`.
    
    Raises
    -------
    RuntimeError:
        If `output` exists and `overwrite` is False.
    """
    if arcpy.Exists(output):
        if overwrite:
            print(f"--- --- deleting existing file {output}")
            arcpy.Delete_management(output)
        else:
            raise RuntimeError(f"Output file {output} already exists")


def intersectFeatures(summary_fc, disag_fc, disag_fields="*"):
    """
        creates a temporary intersected feature class for disaggregation of data
    Parameters
    ----------
    summary_fc: String; path to path to polygon feature class with data to be disaggregated from
    disag_fc: String; path to polygon feature class with data to be disaggregated to
    disag_fields: [String,...]; list of fields to pass over to intersect function
    Returns
    -------
    int_fc: String; path to temp intersected feature class
    """
    # Create a temporary gdb for storing the intersection result
    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(out_folder_path=temp_dir, out_name="Intermediates.gdb")
    int_gdb = makePath(temp_dir, "Intermediates.gdb")
    # Convert disag features to centroids
    disag_full_path = arcpy.Describe(disag_fc).catalogPath
    disag_ws, disag_name = os.path.split(disag_full_path)
    out_fc = makePath(int_gdb, disag_name)
    disag_pts = polygonsToPoints(in_fc=disag_fc, out_fc=out_fc,
                                 fields=disag_fields, skip_nulls=False, null_value=0)
    # Run intersection
    int_fc = makePath(int_gdb, f"int_{disag_name}")
    arcpy.Intersect_analysis(in_features=[summary_fc, disag_pts], out_feature_class=int_fc)

    # return intersect
    return int_fc


def gdfToFeatureClass(gdf, out_fc, new_id_field, exclude, sr=4326, overwrite=False):
    """
    Creates a feature class or shapefile from a geopandas GeoDataFrame.

    Parameters
    ------------
    new_id_field
    gdf: GeoDataFrame
    out_fc: Path
    exclude:
    sr: spatial reference, default=4326
        A spatial reference specification. Authority/factory code, WKT, WKID,
        ESRI name, path to .prj file, etc.
    overwrite:
    Returns
    ---------
    out_fc: Path

    SeeAlso
    ---------
    jsonToFeatureClass
    """
    j = json.loads(gdf.to_json())
    jsonToFeatureClass(json_obj=j, out_fc=out_fc, new_id_field=new_id_field,
                       exclude=exclude, sr=sr, overwrite=overwrite)


def jsonToFeatureClass(json_obj, out_fc, new_id_field='ROW_ID',
                       exclude=None, sr=4326, overwrite=False):
    """
    Creates a feature class or shape file from a json object.

    Parameters
    -----------
    new_id_field
    json_obj: dict
    out_fc: Path
    exclude: List; [String,...] list of columns to exclude
    sr: spatial reference, default=4326
        A spatial reference specification. Authority/factory code, WKT, WKID,
        ESRI name, path to .prj file, etc.
    overwrite: Boolean; True/False whether to overwrite an existing dataset

    Returns
    --------
    out_fc: Path

    See Also
    ---------
    gdfToFeatureClass
    jsonToTable
    """
    # Stack features and attributes
    if exclude is None:
        exclude = []
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
    if overwrite:
        checkOverwriteOutput(output=out_fc, overwrite=overwrite)
    out_path, out_name = os.path.split(out_fc)
    arcpy.CreateFeatureclass_management(
        out_path, out_name, geom_type, spatial_reference=sr
    )
    arcpy.AddField_management(out_fc, new_id_field, "LONG")

    # Add geometries
    with arcpy.da.InsertCursor(out_fc, ["SHAPE@", new_id_field]) as c:
        for i, geom in enumerate(geom_stack):
            row = [geom, i]
            c.insertRow(row)

    # Create attributes dataframe
    prop_df = pd.concat(prop_stack)
    prop_df[new_id_field] = np.arange(len(prop_df))
    exclude = [excl for excl in exclude if excl in prop_df.columns.to_list()]
    prop_df.drop(labels=exclude, axis=1, inplace=True)
    if arcpy.Describe(out_fc).dataType.lower() == "shapefile":
        prop_df.fillna(0.0, inplace=True)

    # Extend table
    print([f.name for f in arcpy.ListFields(out_fc)])
    print(prop_df.columns)
    return extendTableDf(in_table=out_fc, table_match_field=new_id_field,
                         df=prop_df, df_match_field=new_id_field)


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


def fetch_json_to_file(url, out_file, encoding="utf-8", overwrite=False):
    http = urllib3.PoolManager()
    req = http.request("GET", url)
    req_json = json.loads(req.data.decode(encoding))
    if overwrite:
        checkOverwriteOutput(output=out_file, overwrite=overwrite)
    with open(out_file, 'w') as dst:
        json_txt = json.dumps(req_json)
        dst.write(json_txt)


def fetchJsonUrl(
        url, out_file, encoding="utf-8", is_spatial=False, crs=4326, overwrite=False
):
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
        jsonToFeatureClass(req_json, out_file, sr=4326)

    else:
        prop_stack = []
        gdf = gpd.GeoDataFrame.from_features(req_json["features"], crs=crs)
        return pd.DataFrame(gdf.drop(columns="geometry"))


def iterRowsAsChunks(in_table, chunksize=1000):
    """
    A generator to iterate over chunks of a table for arcpy processing.
    This method cannot be reliably applied to a table view of feature
    layer with a current selection as it alters selections as part
    of the chunking process.

    Parameters
    ------------
    in_table: Table View or Feature Layer
    chunksize: Integer, default=1000

    Returns
    --------
    in_table: Table View of Feature Layer
        `in_table` is returned with iterative selections applied
    """
    # Get OID field
    oid_field = arcpy.Describe(in_table).OIDFieldName
    # List all rows by OID
    with arcpy.da.SearchCursor(in_table, "OID@") as c:
        all_rows = [r[0] for r in c]
    # Iterate
    n = len(all_rows)
    for i in range(0, n, chunksize):
        expr_ref = arcpy.AddFieldDelimiters(in_table, oid_field)
        expr = " AND ".join(
            [expr_ref + f">{i}",
             expr_ref + f"<={i + chunksize}"
             ]
        )
        arcpy.SelectLayerByAttribute_management(
            in_table, "NEW_SELECTION", expr
        )
        yield in_table


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
    arcpy.FeatureClassToFeatureClass_conversion(
        in_fc, out_path, out_name, field_mapping=field_mappings
    )

    return out_fc


def mergeFeatures(
        raw_dir, fc_names, clean_dir, out_fc, drop_columns=[], rename_columns=[]
):
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
    for fc_name, drop_cols, rename_cols in zip(fc_names, drop_columns, rename_columns):
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
        columns = columns.to_series().apply(
            lambda col: separator.join(col)
        )
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
    in_array = np.array(np.rec.fromrecords(df.values, names=df.dtypes.index.tolist()))
    arcpy.da.ExtendTable(in_table=in_table, table_match_field=table_match_field, in_array=in_array,
                         array_match_field=df_match_field, **kwargs)


def dfToTable(df, out_table, overwrite=False):
    """
    Use a pandas data frame to export an arcgis table.

    Parameters
    -----------
    df: DataFrame
    out_table: Path
    overwrite: Boolean, default=False

    Returns
    --------
    out_table: Path
    """
    if overwrite:
        checkOverwriteOutput(output=out_table, overwrite=overwrite)
    in_array = np.array(np.rec.fromrecords(df.values, names=df.dtypes.index.tolist()))
    arcpy.da.NumPyArrayToTable(in_array, out_table)
    return out_table


def dfToPoints(df, out_fc, shape_fields,
               from_sr, to_sr, overwrite=False):
    """
    Use a pandas data frame to export an arcgis point feature class.

    Parameters
    -----------
    df: DataFrame
    out_fc: Path
    shape_fields: [String,...]
        Columns to be used as shape fields (x, y)
    from_sr: SpatialReference
        The spatial reference definition for the coordinates listed
        in `shape_field`
    to_sr: SpatialReference
        The spatial reference definition for the output features.
    overwrite: Boolean, default=False

    Returns
    --------
    out_fc: Path
    """
    # set paths
    temp_fc = r"in_memory\temp_points"

    # coerce sr to Spatial Reference object
    # Check if it is a spatial reference already
    try:
        # sr objects have .type attr with one of two values
        check_type = from_sr.type
        type_i = ["Projected", "Geographic"].index(check_type)
    except:
        from_sr = arcpy.SpatialReference(from_sr)
    try:
        # sr objects have .type attr with one of two values
        check_type = to_sr.type
        type_i = ["Projected", "Geographic"].index(check_type)
    except:
        to_sr = arcpy.SpatialReference(to_sr)

    # build array from dataframe
    in_array = np.array(
        np.rec.fromrecords(
            df.values, names=df.dtypes.index.tolist()
        )
    )
    # write to temp feature class
    arcpy.da.NumPyArrayToFeatureClass(in_array=in_array, out_table=temp_fc,
                                      shape_fields=shape_fields, spatial_reference=from_sr,)
    # reproject if needed, otherwise dump to output location
    if from_sr != to_sr:
        arcpy.Project_management(in_dataset=temp_fc, out_dataset=out_fc, out_coor_system=to_sr)
    else:
        out_path, out_fc = os.path.split(out_fc)
        if overwrite:
            checkOverwriteOutput(output=out_fc, overwrite=overwrite)
        arcpy.FeatureClassToFeatureClass_conversion(in_features=temp_fc, out_path=out_path, out_name=out_fc)
    # clean up temp_fc
    arcpy.Delete_management(in_data=temp_fc)
    return out_fc


def featureclass_to_df(in_fc, keep_fields="*", null_val=0):
    """
    converts feature class/feature layer to pandas DataFrame object, keeping only a subset of fields if provided
    - drops all spatial data
    Parameters
    ----------
    in_fc: String; path to a feature class
    keep_fields: List or Tuple; field names to return in the dataframe,
        "*" is default and will return all fields
    null_val: value to be used for nulls found in the data

    Returns
    -------
    pandas Dataframe
    """
    # setup fields
    if keep_fields == "*":
        keep_fields = [f.name for f in arcpy.ListFields(in_fc) if not f.required]
    elif isinstance(keep_fields, string_types):
        keep_fields = [keep_fields]
    # process fc to array
    in_fc_arr = arcpy.da.FeatureClassToNumPyArray(in_table=in_fc, field_names=keep_fields,
                                                  skip_nulls=False, null_value=null_val)
    return pd.DataFrame(in_fc_arr)


def multipolygon_to_polygon_arc(file_path):
    polygon_fcs = makePath("in_memory", "polygons")
    arcpy.MultipartToSinglepart_management(in_features=file_path, out_feature_class=polygon_fcs)
    return polygon_fcs


def multipolygonToPolygon(gdf, in_crs):
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
    poly_df.crs = in_crs
    return poly_df


def polygonsToPoints(in_fc, out_fc, fields="*", skip_nulls=False, null_value=0):
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
        fields = [f.name for f in arcpy.ListFields(in_fc) if not f.required]
    elif isinstance(fields, string_types):
        fields = [fields]
    fields.append("SHAPE@XY")
    a = arcpy.da.FeatureClassToNumPyArray(in_table=in_fc, field_names=fields,
                                          skip_nulls=skip_nulls, null_value=null_value)
    arcpy.da.NumPyArrayToFeatureClass(in_array=a, out_table=out_fc,
                                      shape_fields="SHAPE@XY", spatial_reference=sr)
    return out_fc


def sumToAggregateGeo(
        disag_fc,
        sum_fields,
        groupby_fields,
        agg_fc,
        agg_id_field,
        output_fc,
        overlap_type="INTERSECT",
        agg_funcs=np.sum,
        disag_wc=None,
        agg_wc=None,
        flatten_disag_id=None,
        *args,
        **kwargs,
):
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
    if desc.shapeType != "Polygon":
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
            in_features=disag_fc, out_layer="__disag_subset__", where_clause=disag_wc
        )
    if agg_wc:
        arcpy.SelectLayerByAttribute_management(
            in_layer_or_view=agg_fc,
            selection_type="SUBSET_SELECTION",
            where_clause=agg_wc,
        )

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
                    in_layer=disag_fc,
                    overlap_type=overlap_type,
                    select_features=agg_shape,
                    selection_type="NEW_SELECTION",
                )
                # Dump to data frame
                df = pd.DataFrame(
                    arcpy.da.TableToNumPyArray(
                        disag_fc, disag_fields, skip_nulls=False, null_value=0
                    )
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
                # sum_row = df_sum.reset_index()

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


def add_unique_id(feature_class, new_id_field=None):
    """
    adds a unique incrementing integer value to a feature class and returns that name
    Parameters
    ----------
    feature_class: String; path to a feature class
    new_id_field: String; name of new id field, if none is provided, ProcessID is used

    Returns
    -------
    new_id_field: String; name of new id field
    """
    CODEBLOCK = """
        val = 0 
        def unique_ID(): 
            global val 
            start = 1 
            if (val == 0):  
                val = start
            else:  
                val += 1  
            return val
         """
    if new_id_field is None:
        new_id_field = "ProcessID"
    arcpy.CalculateField_management(in_table=feature_class, field=new_id_field,
                                    expression="unique_ID()", expression_type="PYTHON3",
                                    code_block=CODEBLOCK, field_type="LONG")
    return new_id_field


if __name__ == "__main__":
    arcpy.env.overwriteOutput = True
    sumToAggregateGeo(
        disag_fc=r"K:\Projects\MiamiDade\PMT\Data\Cleaned\Safety_Security\Crash_Data"
                 r"\Miami_Dade_NonMotorist_CrashData_2012-2020.shp",
        sum_fields=["SPEED_LIM"],
        groupby_fields=["CITY"],
        agg_fc=r"K:\Projects\MiamiDade\PMT\Basic_features.gdb\Basic_features_SPFLE\SMART_Plan_Station_Areas",
        agg_id_field="Id",
        output_fc=r"D:\Users\DE7\Desktop\agg_test.gdb\bike_speed_agg",
    )
