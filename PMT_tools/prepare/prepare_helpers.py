# global configurations
# TODO: shuffle global imports to preparer and update functions to take in new variables accordingly
from PMT_tools.download.download_helper import (validate_directory, validate_geodatabase, validate_feature_dataset)
from PMT_tools.config.prepare_config import (CRASH_CODE_TO_STRING, CRASH_CITY_CODES,
                                             CRASH_SEVERITY_CODES, CRASH_HARMFUL_CODES)
from PMT_tools.config.prepare_config import (PERMITS_CAT_CODE_PEDOR, PERMITS_STATUS_DICT, PERMITS_FIELDS_DICT,
                                             PERMITS_USE, PERMITS_DROPS, )
from PMT_tools.prepare.preparer import RIF_CAT_CODE_TBL, DOR_LU_CODE_TBL

from datetime import time
import numpy as np
import pandas as pd
import xlrd

# import geopandas as gpd
from pathlib import Path
import tempfile as tempfile
import zipfile
import fnmatch
import json
from json.decoder import JSONDecodeError
import os
import uuid
import re
from sklearn import linear_model
import scipy
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import arcpy
import dask.dataframe as dd
from six import string_types

from functools import reduce

import PMT_tools.logger as log

import PMT_tools.PMT as PMT  # Think we need everything here
from PMT_tools.PMT import (dfToPoints, extendTableDf, makePath, make_inmem_path, dfToTable, add_unique_id,
                           intersectFeatures, featureclass_to_df, copyFeatures, dbf_to_df)

logger = log.Logger(add_logs_to_arc_messages=True)


# TODO: verify functions generally return python objects (dataframes, e.g.) and leave file writes to `preparer.py`
# general use functions
def is_gz_file(filepath):
    with open(filepath, 'rb') as test_f:
        return test_f.read(2) == b'\x1f\x8b'


def validate_json(json_file, encoding='utf8'):
    with open(json_file, encoding=encoding) as file:
        try:
            return json.load(file)
        except JSONDecodeError:
            logger.log("Invalid JSON file passed")
            logger.log_error()


def update_field_values(in_fc, fields, mappers):
    # ensure number of fields and dictionaries is the same
    try:
        if len(fields) == len(mappers):
            for attribute, mapper in zip(fields, mappers):
                # check that bother input types are as expected
                if isinstance(attribute, str) and isinstance(mapper, dict):
                    with arcpy.da.UpdateCursor(in_fc, field_names=attribute) as cur:
                        for row in cur:
                            code = row[0]
                            if code is not None:
                                if mapper.get(int(code)) in mapper:
                                    row[0] = mapper.get(int(code))
                                else:
                                    row[0] = "None"
                            cur.updateRow(row)
    except ValueError:
        logger.log_msg("either attributes (String) or mappers (dict) are of the incorrect type")
        logger.log_error()


# TODO: mapping isnt working as expected, needs debugging
def field_mapper(in_fcs, use_cols, rename_dicts):
    """
    create a field mapping for one or more feature classes
    Parameters
    ----------
    in_fcs - list (string), list of feature classes
    use_cols - list (string), list or tuple of lists of column names to keep
    rename_dict - list of dict(s), dict or tuple of dicts to map field names

    Returns
    --- --- -
    arcpy.FieldMapings object
    """
    _unmapped_types_ = ["Geometry", "OID", "GUID"]
    # check to see if we have only one use or rename and handle for zip
    if isinstance(in_fcs, str):
        in_fcs = [in_fcs]
    if not any(isinstance(el, list) for el in use_cols):
        use_cols = [use_cols]
    if isinstance(rename_dicts, dict):
        rename_dicts = [rename_dicts]
    # create field mappings object and add/remap all necessary fields
    field_mappings = arcpy.FieldMappings()
    for in_fc, use, rename in zip(in_fcs, use_cols, rename_dicts):
        # only keep the fields that we want and that are mappable
        fields = [f.name for f in arcpy.ListFields(in_fc)
                  if f.name in use
                  and f.type not in _unmapped_types_]
        for field in fields:
            fm = arcpy.FieldMap()
            fm.addInputField(in_fc, field)
            out_field = fm.outputField
            out_fname = rename.get(field, field)
            out_field.name = out_fname
            out_field.aliasName = out_fname
            fm.outputField = out_field
            field_mappings.addFieldMap(fm)
    return field_mappings


def geojson_to_feature_class_arc(geojson_path, geom_type, encoding='utf8'):
    if validate_json(json_file=geojson_path, encoding=encoding):
        try:
            # convert json to temp feature class
            temp_feature = make_inmem_path()
            arcpy.JSONToFeatures_conversion(in_json_file=geojson_path, out_features=temp_feature,
                                            geometry_type=geom_type)
            return temp_feature
        except:
            logger.log_msg("something went wrong converting geojson to feature class")
            logger.log_error()


def csv_to_df(csv_file, use_cols, rename_dict):
    """
    helper function to convert CSV file to pandas dataframe, and drop unnecessary columns
    assumes any strings with comma (,) should have those removed and dtypes infered
    Parameters
    --- --- --- -
    csv_file: String
        path to csv file
    use_cols: List
        list of columns to keep from input csv
    rename_dict: dict
        dictionary mapping existing column name to standardized column names

    Returns: Pandas dataframe
    --- --- -

    """
    df = pd.read_csv(filepath_or_buffer=csv_file, usecols=use_cols, thousands=",")
    df = df.convert_dtypes()
    df.rename(columns=rename_dict, inplace=True)
    return df


def split_date(df, date_field, unix_time=False):
    """
    ingest date attribute and splits it out to DAY, MONTH, YEAR
    Parameters
    --- --- --- -
    unix_time
    df: DataFrame; DataFrame with a date field
    date_field: column name

    Returns
    --- --- -
        df: DataFrame reformatted to include split day, month and year
    """
    # convert unix time to date
    if unix_time:
        df[date_field] = df[date_field].apply(lambda x: str(x)[:10])
        df[date_field] = df[date_field].apply(
            lambda x: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(x)))
        )
    # otherwise infer
    else:
        df[date_field] = pd.to_datetime(arg=df[date_field], infer_datetime_format=True)
    df["DAY"] = df[date_field].dt.day
    df["MONTH"] = df[date_field].dt.month
    df["YEAR"] = df[date_field].dt.year
    return df


# DEPRECATED
# def geojson_to_gdf(geojson, crs, use_cols=None, rename_dict=None):
#     """
#     reads in geojson, drops unnecessary attributes and renames the kept attributes
#     Parameters
#     --- --- --- -
#     geojson: json
#         GeoJSON text file consisting of points for bike/ped crashes
#     crs:
#         EPSG code representing
#     use_cols: list
#         list of columns to use in formatting
#     rename_dict: dict
#         dictionary to map existing column names to more readable names
#     Returns
#     --- --- -
#         geodataframe
#     """
#     if rename_dict is None:
#         rename_dict = []
#     if use_cols is None:
#         use_cols = []
#     with open(str(geojson), "r") as src:
#         js = json.load(src)
#         gdf = gpd.GeoDataFrame.from_features(js["features"], crs=crs, columns=use_cols)
#         gdf.rename(columns=rename_dict, inplace=True)
#     return gdf


def polygon_to_points_arc(in_fc, id_field=None, point_loc="INSIDE"):
    # TODO: replace usages with PMT.polygonsToPoints
    try:
        # convert json to temp feature class
        unique_name = str(uuid.uuid4().hex)
        temp_feature = PMT.makePath("in_memory", f"_{unique_name}")
        arcpy.FeatureToPoint_management(in_features=in_fc, out_feature_class=temp_feature,
                                        point_location=point_loc)
        if id_field:
            clean_and_drop(feature_class=temp_feature, use_cols=[id_field])
        return temp_feature
    except:
        logger.log_msg("something went wrong converting polygon to points")
        logger.log_error()


# DEPRECATED
# def polygon_to_points_gpd(poly_fc, id_field=None):
#     # TODO: add validation and checks
#     poly_df = gpd.read_file(poly_fc)
#     if id_field:
#         columns = poly_df.columns
#         drops = [col for col in columns if col != id_field]
#         poly_df = poly_df.drop(columns=drops)
#     pts_df = poly_df.copy()
#     pts_df['geometry'] = pts_df['geometry'].centroid
#     return pts_df


def add_xy_from_poly(poly_fc, poly_key, table_df, table_key):
    pts = polygon_to_points_arc(in_fc=poly_fc, id_field=poly_key)
    pts_sdf = pd.DataFrame.spatial.from_featureclass(pts)

    esri_ids = ["OBJECTID", "FID"]
    if set(esri_ids).issubset(set(pts_sdf.columns.to_list())):
        pts_sdf.drop(labels=esri_ids, axis=1, inplace=True, errors='ignore')
    # join permits to parcel points MANY-TO-ONE
    print('--- merging polygon data to tabular')
    pts = table_df.merge(right=pts_sdf, how="inner", on=table_key)
    return pts
    # with tempfile.TemporaryDirectory() as temp_dir:
    #     t_geoj = PMT.makePath(temp_dir, "temp.geojson")
    #     arcpy.FeaturesToJSON_conversion(in_features=pts, out_json_file=t_geoj,
    #                                     geoJSON="GEOJSON", outputToWGS84="WGS84")
    #     with open(t_geoj, "r") as j_file:
    #         pts_gdf = gpd.read_file(j_file)
    # esri_ids = ["OBJECTID", "FID"]
    # if esri_ids.issubset(pts_gdf.columns).any():
    #     pts_gdf.drop(labels=esri_ids, axis=1, inplace=True, errors='ignore')
    # # join permits to parcel points MANY-TO-ONE
    # logger.log_msg('--- merging polygon data to tabular')
    # pts = table_df.merge(right=pts_gdf, how="inner", on=table_key)
    # return gpd.GeoDataFrame(pts, geometry="geometry")


def clean_and_drop(feature_class, use_cols=None, rename_dict=None):
    # reformat attributes and keep only useful
    if rename_dict is None:
        rename_dict = {}
    if use_cols is None:
        use_cols = []
    if use_cols:
        fields = [f.name for f in arcpy.ListFields(feature_class) if not f.required]
        drop_fields = [f for f in fields if f not in list(use_cols) + ['Shape']]
        for drop in drop_fields:
            arcpy.DeleteField_management(in_table=feature_class, drop_field=drop)
    # rename attributes
    if rename_dict:
        for name, rename in rename_dict.items():
            arcpy.AlterField_management(in_table=feature_class, field=name, new_field_name=rename,
                                        new_field_alias=rename)


def _merge_df_(x_specs, y_specs, on=None, left_on=None, right_on=None,
               **kwargs):
    df_x, suffix_x = x_specs
    df_y, suffix_y = y_specs
    merge_df = df_x.merge(df_y, suffixes=(suffix_x, suffix_y), **kwargs)
    # Must return a tuple to support reliably calling from `reduce`
    return (merge_df, suffix_y)


def combine_csv_dask(merge_fields, out_table, *tables, suffixes=None, **kwargs):
    ddfs = [dd.read_csv(t, **kwargs) for t in tables]
    if suffixes is None:
        df = reduce(lambda this, next: this.merge(next, on=merge_fields), ddfs)
    else:
        # for odd lenghts, the last suffix will be lost because there will
        # be no collisions (applies to well-formed data only - inconsistent
        # field naming could result in field name collisions prompting a
        # suffix
        if len(ddfs) % 2 != 0:
            # Force rename suffix
            cols = [
                (c, f"{c}_{suffixes[-1]}")
                for c in ddfs[-1].columns
                if c not in merge_fields
            ]
            renames = dict(cols)
            ddfs[-1] = ddfs[-1].rename(columns=renames)
        # zip ddfs and suffixes
        specs = zip(ddfs, suffixes)
        df = reduce(
            lambda this, next: _merge_df_(this, next, on=merge_fields), specs
        )
    df.to_csv(out_table, single_file=True)


# basic features functions
def _listifyInput(input):
    if isinstance(input, string_types):
        return input.split(";")
    else:
        return list(input)


def _stringifyList(input):
    return ";".join(input)


def makeBasicFeatures(bf_gdb, stations_fc, stn_diss_fields, stn_corridor_fields,
                      alignments_fc, align_diss_fields, stn_buff_dist="2640 Feet",
                      align_buff_dist="2640 Feet", stn_areas_fc="Station_Areas",
                      corridors_fc="Corridors", long_stn_fc="Stations_Long",
                      rename_dict={}, overwrite=False):
    """In a geodatabase with basic features (station points and corridor alignments),
        create polygon feature classes used for standard mapping and summarization.
        The output feature classes include:
         - buffered corridors,
         - buffered station areas,
         - and a long file of station points, where each station/corridor combo is represented
                as a separate feature.

    Args:
        bf_gdb (str): Path to a geodatabase with key basic features, including stations and
            alignments
        stations_fc (str): A point feature class in`bf_gdb` with station locations and columns
            indicating belonging in individual corridors (i.e., the column names reflect corridor
            names and flag whether the station is served by that corridor).
        stn_diss_fields (list): [String,...] Field(s) on which to dissolve stations when buffering
            station areas. Stations that reflect the same location by different facilities may
            be dissolved by name or ID, e.g. This may occur at intermodal locations.
            For example, where metro rail meets commuter rail - the station points may represent
            the same basic station but have slightly different geolocations.
        stn_corridor_fields (list): [String,...] The columns in `stations_fc` that flag each
            stations belonging in various corridors.
        alignments_fc (str): Path to a line feature class in `bf_gdb` reflecting corridor alignments
        align_diss_fields (list): [String,...] Field(s) on which to dissolve alignments when buffering
            corridor areas.
        stn_buff_dist (str): Linear Unit, [default="2640 Feet"] A linear unit by which to buffer
            station points to create station area polygons.
        align_buff_dist (str): Linear Unit, [default="2640 Feet"] A linear unit by which to buffer
            alignments to create corridor polygons
        stn_areas_fc (str): [default="Station_Areas"] The name of the output feature class to
            hold station area polygons
        corridors_fc (str): [default="Corridors"], The name of the output feature class to hold corridor polygons
        long_stn_fc (str): [default="Stations_Long"], The name of the output feature class to hold station features,
            elongated based on corridor belonging (to support dashboard menus)
        rename_dict (dict): [default={}], If given, `stn_corridor_fields` can be relabeled before pivoting
            to create `long_stn_fc`, so that the values reported in the output "Corridor" column are not
            the column names, but values mapped on to the column names
            (changing "EastWest" column to "East-West", e.g.)
        overwrite (bool): default=False
    """
    stn_diss_fields = _listifyInput(stn_diss_fields)
    stn_corridor_fields = _listifyInput(stn_corridor_fields)
    align_diss_fields = _listifyInput(align_diss_fields)

    old_ws = arcpy.env.workspace
    arcpy.env.workspace = bf_gdb

    # Buffer features
    #  - stations (station areas, unique)
    print("--- buffering station areas")
    PMT.checkOverwriteOutput(stn_areas_fc, overwrite)
    _diss_flds_ = _stringifyList(stn_diss_fields)
    arcpy.Buffer_analysis(stations_fc, stn_areas_fc, stn_buff_dist,
                          dissolve_option="LIST", dissolve_field=_diss_flds_)
    #  - alignments (corridors, unique)
    print("--- buffering corridor areas")
    PMT.checkOverwriteOutput(corridors_fc, overwrite)
    _diss_flds_ = _stringifyList(align_diss_fields)
    arcpy.Buffer_analysis(alignments_fc, corridors_fc, align_buff_dist,
                          dissolve_option="LIST", dissolve_field=_diss_flds_)

    # Add (All corridors) to `corridors_fc`
    # - Setup field outputs
    upd_fields = align_diss_fields + ["SHAPE@"]
    fld_objs = arcpy.ListFields(corridors_fc)
    fld_objs = [f for f in fld_objs if f.name in align_diss_fields]
    # TODO: Smarter method to set values for (All corridors)
    # This just sets all string fields to '(All corridors)' and
    # any non-string fields to  <NULL>
    upd_vals = ["(All corridors)" if f.type == "String" else None for f in fld_objs]
    with arcpy.da.SearchCursor(corridors_fc, "SHAPE@") as c:
        # Merge all polys
        all_cors_poly = reduce(lambda x, y: x.union(y), [r[0] for r in c])
    upd_vals.append(all_cors_poly)
    with arcpy.da.InsertCursor(corridors_fc, upd_fields) as c:
        c.insertRow(upd_vals)

    # Elongate stations by corridor (for dashboard displays, selectors)
    print("--- elongating station features")
    # - dump to data frame
    fields = stn_diss_fields + stn_corridor_fields + ["SHAPE@X", "SHAPE@Y"]
    sr = arcpy.Describe(stations_fc).spatialReference
    fc_path = PMT.makePath(bf_gdb, stations_fc)
    stn_df = pd.DataFrame(
        arcpy.da.FeatureClassToNumPyArray(in_table=fc_path, field_names=fields)
    )
    # Rename columns if needed
    if rename_dict:
        stn_df.rename(columns=rename_dict, inplace=True)
        _cor_cols_ = [rename_dict.get(c, c) for c in stn_corridor_fields]
    else:
        _cor_cols_ = stn_corridor_fields
    # Melt to gather cols
    id_vars = stn_diss_fields + ["SHAPE@X", "SHAPE@Y"]
    long_df = stn_df.melt(id_vars=id_vars, value_vars=_cor_cols_,
                          var_name="Corridor", value_name="InCor")
    sel_df = long_df[long_df.InCor != 0].copy()
    long_out_fc = PMT.makePath(bf_gdb, long_stn_fc)
    PMT.checkOverwriteOutput(long_out_fc, overwrite)
    PMT.dfToPoints(df=sel_df, out_fc=long_out_fc, shape_fields=["SHAPE@X", "SHAPE@Y"],
                   from_sr=sr, to_sr=sr, overwrite=True)

    arcpy.env.workspace = old_ws


def makeSummaryFeatures(bf_gdb, long_stn_fc, corridors_fc, cor_name_field,
                        out_fc, stn_buffer_meters=804.672,
                        stn_name_field="Name", stn_cor_field="Corridor",
                        overwrite=False):
    """Creates a single feature class for data summarization based on station
        area and corridor geographies. The output feature class includes each
        station area, all combined station areas, the entire corridor area,
        and the portion of the corridor that is outside station areas.

    Args:
        bf_gdb (str): Path to basic features gdb
        long_stn_fc (str): path to long station points feature class
        corridors_fc (str): path to corridors feature class
        cor_name_field (str): name field for corridor feature class
        out_fc (str): path to output feature class
        stn_buffer_meters (num): Numeric, default=804.672 (1/2 mile)
        stn_name_field (str): station name field default="Name"
        stn_cor_field (str): corridor field, default="Corridor
        overwrite (bool): overwrite existing copy, default=False
    """

    old_ws = arcpy.env.workspace
    arcpy.env.workspace = bf_gdb

    sr = arcpy.Describe(long_stn_fc).spatialReference
    mpu = float(sr.metersPerUnit)
    buff_dist = stn_buffer_meters / mpu

    # Make output container - polygon with fields for Name, Corridor
    print(f"--- creating output feature class {out_fc}")
    PMT.checkOverwriteOutput(out_fc, overwrite)
    out_path, out_name = os.path.split(out_fc)
    arcpy.CreateFeatureclass_management(
        out_path, out_name, "POLYGON", spatial_reference=sr)
    # - Add fields
    arcpy.AddField_management(out_fc, "Name", "TEXT", field_length=80)
    arcpy.AddField_management(out_fc, "Corridor", "TEXT", field_length=80)
    arcpy.AddField_management(out_fc, "RowID", "LONG")

    # Add all corridors with name="(Entire corridor)", corridor=cor_name_field
    print("--- adding corridor polygons")
    out_fields = ["SHAPE@", "Name", "Corridor", "RowID"]
    cor_fields = ["SHAPE@", cor_name_field]
    cor_polys = {}
    i = 0
    with arcpy.da.InsertCursor(out_fc, out_fields) as ic:
        with arcpy.da.SearchCursor(corridors_fc, cor_fields) as sc:
            for sr in sc:
                i += 1
                # Add row for the whole corridor
                poly, corridor = sr
                out_row = [poly, "(Entire corridor)", corridor, i]
                ic.insertRow(out_row)
                # Keep the polygons in a dictionary for use later
                cor_polys[corridor] = poly

    # Add all station areas with name= stn_name_field, corridor=stn_cor_field
    print("--- adding station polygons by corridor")
    stn_fields = ["SHAPE@", stn_name_field, stn_cor_field]
    cor_stn_polys = {}
    with arcpy.da.InsertCursor(out_fc, out_fields) as ic:
        with arcpy.da.SearchCursor(long_stn_fc, stn_fields) as sc:
            for sr in sc:
                i += 1
                # Add row for each station/corridor combo
                point, stn_name, corridor = sr
                poly = point.buffer(buff_dist)
                out_row = [poly, stn_name, corridor, i]
                ic.insertRow(out_row)
                # Merge station polygons by corridor in a dict for later use
                cor_poly = cor_stn_polys.get(corridor, None)
                if cor_poly is None:
                    cor_stn_polys[corridor] = poly
                else:
                    cor_stn_polys[corridor] = cor_poly.union(poly)

    # Add dissolved areas with name = (All stations), corridor=stn_cor_field
    # Add difference area with name = (Outside station areas), corridor=stn_cor_field
    print("--- adding corridor in-station/non-station polygons")
    with arcpy.da.InsertCursor(out_fc, out_fields) as ic:
        for corridor in cor_stn_polys.keys():
            # Combined station areas
            i += 1
            all_stn_poly = cor_stn_polys[corridor]
            out_row = [all_stn_poly, "(All stations)", corridor, i]
            ic.insertRow(out_row)
            # Non-station areas
            i += 1
            cor_poly = cor_polys[corridor]
            non_stn_poly = cor_poly.difference(all_stn_poly)
            out_row = [non_stn_poly, "(Outside station areas)", corridor, i]
            ic.insertRow(out_row)

    arcpy.env.workspace = old_ws


# crash functions
def update_crash_type(feature_class, data_fields, update_field):
    arcpy.AddField_management(in_table=feature_class, field_name=update_field, field_type="TEXT")
    with arcpy.da.UpdateCursor(feature_class, field_names=[update_field] + data_fields) as cur:
        for row in cur:
            both, ped, bike = row
            if ped == "Y":
                row[0] = "PEDESTRIAN"
            if bike == "Y":
                row[0] = "BIKE"
            cur.updateRow(row)
    for field in data_fields:
        arcpy.DeleteField_management(in_table=feature_class, drop_field=field)


def prep_bike_ped_crashes(in_fc, out_path, out_name, where_clause=None):
    # dump subset to new FC
    out_fc = PMT.makePath(out_path, out_name)
    arcpy.FeatureClassToFeatureClass_conversion(in_features=in_fc, out_path=out_path,
                                                out_name=out_name, where_clause=where_clause)
    # update city code/injury severity/Harmful event to text value
    update_field_values(in_fc=out_fc, fields=CRASH_CODE_TO_STRING,
                        mappers=[CRASH_CITY_CODES, CRASH_SEVERITY_CODES, CRASH_HARMFUL_CODES])

    # combine bike and ped type into single attribute and drop original
    update_crash_type(feature_class=out_fc, data_fields=["PED_TYPE", "BIKE_TYPE"], update_field="TRANS_TYPE")


# parks functions
def prep_park_polys(in_fcs, geom, out_fc, use_cols=None, rename_dicts=None):
    # Align inputs
    if rename_dicts is None:
        rename_dicts = [{} for _ in in_fcs]
    if use_cols is None:
        use_cols = [[] for _ in in_fcs]
    if isinstance(in_fcs, str):
        in_fcs = [in_fcs]

    # handle the chance of input raw data being geojson
    if any(fc.endswith("json") for fc in in_fcs):
        in_fcs = [geojson_to_feature_class_arc(fc, geom_type=geom)
                  if fc.endswith("json")
                  else fc for fc in in_fcs]

    # merge into one feature class temporarily
    fm = field_mapper(in_fcs=in_fcs, use_cols=use_cols, rename_dicts=rename_dicts)
    arcpy.Merge_management(inputs=in_fcs, output=out_fc, field_mappings=fm)


def prep_feature_class(in_fc, geom, out_fc, use_cols=None, rename_dict=None):
    if rename_dict is None:
        rename_dict = {}
    if use_cols is None:
        use_cols = []
    if in_fc.endswith("json"):
        in_fc = geojson_to_feature_class_arc(in_fc, geom_type=geom)
    out_dir, out_name = os.path.split(out_fc)
    fms = field_mapper(in_fcs=in_fc, use_cols=use_cols, rename_dicts=rename_dict)
    arcpy.FeatureClassToFeatureClass_conversion(in_features=in_fc, out_path=out_dir,
                                                out_name=out_name, field_mapping=fms)


# permit functions
def clean_permit_data(permit_csv, poly_features, permit_key, poly_key, out_file, out_crs):
    """
        reformats and cleans RER road impact permit data, specific to the PMT
    Args:
        permit_csv:
        poly_features:
        permit_key:
        poly_key:
        out_file:
        out_crs:

    Returns:

    """
    # TODO: add validation
    # read permit data to dataframe
    permit_df = csv_to_df(csv_file=permit_csv, use_cols=PERMITS_USE,
                          rename_dict=PERMITS_FIELDS_DICT)

    # clean up and concatenate data where appropriate
    #   fix parcelno to string of 13 len
    permit_df[permit_key] = permit_df[permit_key].astype(np.str)
    permit_df[poly_key] = permit_df[permit_key].apply(lambda x: x.zfill(13))
    permit_df['COST'] = permit_df['CONST_COST'] + permit_df['ADMIN_COST']
    #   id project as pedestrain oriented
    permit_df["PED_ORIENTED"] = np.where(permit_df.CAT_CODE.str.contains(PERMITS_CAT_CODE_PEDOR), 1, 0)
    # drop fake data - Keith Richardson of RER informed us that any PROC_NUM/ADDRESS that contains with 'SMPL' or
    #   'SAMPLE' should be ignored as as SAMPLE entry
    ignore_text = ['SMPL', "SAMPLE", ]
    for ignore in ignore_text:
        for col in ['PROC_NUM', 'ADDRESS']:
            permit_df = permit_df[~permit_df[col].str.contains(ignore)]
    #   set landuse codes appropriately accounting for pedoriented dev
    permit_df['CAT_CODE'] = np.where(permit_df.CAT_CODE.str.contains(PERMITS_CAT_CODE_PEDOR),
                                     permit_df.CAT_CODE.str[:-2],
                                     permit_df.CAT_CODE)
    #   set project status
    permit_df['STATUS'] = permit_df['STATUS'].map(PERMITS_STATUS_DICT, na_action='NONE')
    #   add landuse codes
    lu_df = pd.read_csv(RIF_CAT_CODE_TBL)
    dor_df = pd.read_csv(DOR_LU_CODE_TBL)
    lu_df = lu_df.merge(right=dor_df, how="inner", on="DOR_UC")
    permit_df = permit_df.merge(right=lu_df, how="inner", on='CAT_CODE')
    #   drop unnecessary columns
    permit_df.drop(columns=PERMITS_DROPS, inplace=True)

    # convert to points
    permit_sdf = add_xy_from_poly(poly_fc=poly_features, poly_key=poly_key,
                                  table_df=permit_df, table_key=permit_key)
    permit_sdf.fillna(0.0, inplace=True)
    permit_sdf.spatial.to_featureclass(out_file)
    # PMT.gdfToFeatureClass(gdf=permit_sdf, out_fc=out_file, new_id_field="ROW_ID",
    #                       exclude=['OBJECTID'], sr=out_crs, overwrite=True)


# Urban Growth Boundary
def udbLineToPolygon(udb_fc, county_fc, out_fc):
    """
        Uses the urban development boundary line to bisect the county boundary
        and generate two polygon output features.

        During processing the UDB line features are dissolved into a single
        feature - this assumes all polylines in the shape file touch one another
        such that a single cohesive polyline feature results.

        This function also assumes that the UDB will only define a simple
        bi-section of the county boundary. If the UDB geometry becomes more
        complex over time, modifications to this function may be needed.

        Parameters
        -----------
        udb_fc: Path
            The udb line features.
        county_fc: Path
            The county boundary polygon. This is expected to only include a
            single polygon encompassing the entire county.
        out_fc: Path
            The location to store the output feature class.

        Returns
        --------
        out_fc: Path
    """
    sr = arcpy.Describe(udb_fc)
    # Prepare ouptut feature class
    out_path, out_name = os.path.split(out_fc)
    arcpy.CreateFeatureclass_management(out_path=out_path, out_name=out_name,
                                        geometry_type="POLYGON", spatial_reference=sr)
    arcpy.AddField_management(in_table=out_fc, field_name="IN_UDB", field_type="LONG")

    # Get geometry objects
    temp_udb = PMT.makePath("in_memory", "UDB_dissolve")
    diss_line = arcpy.Dissolve_management(in_features=udb_fc, out_feature_class=temp_udb)
    with arcpy.da.SearchCursor(diss_line, "SHAPE@", spatial_reference=sr) as c:
        for r in c:
            udb_line = r[0]
    with arcpy.da.SearchCursor(county_fc, "SHAPE@", spatial_reference=sr) as c:
        for r in c:
            county_poly = r[0]
    county_line = arcpy.Polyline(county_poly.getPart(0))

    # Get closest point on county boundary to each udb end point
    udb_start = arcpy.PointGeometry(udb_line.firstPoint)
    udb_end = arcpy.PointGeometry(udb_line.lastPoint)
    start_connector = county_line.snapToLine(udb_start)
    end_connector = county_line.snapToLine(udb_end)

    # Cut the county boundary based on the extended UDB line
    cutter_points = [p for p in udb_line.getPart(0)]
    cutter_points.insert(0, start_connector.centroid)
    cutter_points.append(end_connector.centroid)
    cutter = arcpy.Polyline(arcpy.Array(cutter_points))
    cuts = county_poly.cut(cutter.projectAs(sr))

    # Tag the westernmost feature as outside the UDB
    x_mins = []
    for cut in cuts:
        x_min = min([pt.X for pt in cut.getPart(0)])
        x_mins.append(x_min)
    in_udb = [1 for _ in x_mins]
    min_idx = np.argmin(x_mins)
    in_udb[min_idx] = 0

    # Write cut polygons to output feature class
    with arcpy.da.InsertCursor(out_fc, ["SHAPE@", "IN_UDB"]) as c:
        for cut, b in zip(cuts, in_udb):
            c.insertRow([cut, b])

    return out_fc


# Transit Ridership
def read_transit_xls(xls_path, sheet=None, head_row=None, rename_dict=None):
    """XLS File Desc: Sheet 1 contains header and max rows for a sheet (65536),
                    data continue on subsequent sheets without header
        reads in the provided xls file and due to odd formatting concatenates
        the sheets into a single data frame. The
    Args:
        xls_path: str; String path to xls file
        sheet: str, int, list, or None, default None as we need to read the entire file
        head_row: int, list of int, default None as wee need to read the entire file
        rename_dict: dict; dictionary to map existing column names to more readable names
    Returns:
        pd.Dataframe; pandas dataframe of data from xls
    """
    # TODO: add logic to handle files that dont match existing format
    # TODO:     - example check may be to read all pages and see if row 0
    #               is the same or pages are empty
    # read the xls into dict
    wb = xlrd.open_workbook(filename=xls_path, logfile=open(os.devnull, 'w'))
    xls_dict = pd.read_excel(io=wb, engine="xlrd", sheet_name=sheet, header=head_row)
    # concatenate all sheets and set columns from sheet1:row1
    df = pd.concat(xls_dict.values())
    df.columns = df.iloc[0]
    df = df[df.index > 0]
    df = df.infer_objects()
    # rename columns
    if rename_dict:
        df.rename(columns=rename_dict, inplace=True)
        drop_cols = [col for col in df.columns if col not in rename_dict.values()]
        df.drop(columns=drop_cols, axis=1, inplace=True)
    return df


def read_transit_file_tag(file_path):
    """
    extracts a tag from the file path that is used in the naming of "ON" and "OFF"
    counts per stop
    Parameters
    ----------
    file_path: Path; string path of transit ridership file

    Returns
    -------
    tag: String
    """
    # returns "time_year_month" tag for ridership files
    # ex: AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1411_2015_APR_standard_format.XLS
    #       ==> "1411_2015_APR"
    _, file_name = os.path.split(file_path)
    return "_".join(file_name.split("_")[7:10])


def update_dict(d, values, tag):
    """adds new key:value pairs to dictionary given a set of values and
        tags in the keys --- only valuable in the Transit ridership context
    Args:
        d: dict
        values: [String,...]; output value names
        tag: String; unique tag found in a column
    Returns:
        d: dict; updated with new key value pairs
    """
    for value in values:
        d[f"{value}_{tag}"] = value
    return d


def prep_transit_ridership(in_table, rename_dict, shape_fields, from_sr, to_sr, out_fc):
    """converts transit ridership data to a feature class and reformats attributes
    Args:
        in_table (str): xls file path
        rename_dict (dict): dictionary of {existing: new attribute} names
        shape_fields (list): [String,...] columns to be used as shape field (x,y)
        from_sr (SpatialReference): the spatial reference definition for coordinates listed in 'shape_fields'
        to_sr (SpatialReference): the spatial reference definition for output features
        out_fc (str): path to the output feature class
    Returns:
        out_fc: Path
    """
    # on/off data are tagged by the date the file was created
    file_tag = read_transit_file_tag(file_path=in_table)
    rename_dict = update_dict(d=rename_dict, values=["ON", "OFF", "TOTAL"], tag=file_tag)
    # read the transit file to dataframe
    transit_df = read_transit_xls(xls_path=in_table, rename_dict=rename_dict)
    # reduce precision to collapse points
    for shp_fld in shape_fields:
        transit_df[shp_fld] = transit_df[shp_fld].round(decimals=4)
    transit_df = transit_df.groupby(shape_fields +
                                    ["TIME_PERIOD"]).agg({"ON": "sum",
                                                          "OFF": "sum",
                                                          "TOTAL": "sum"}).reset_index()
    transit_df["TIME_PERIOD"] = np.where(
        transit_df["TIME_PERIOD"] == "2:00 AM - 2:45 AM",
        "EARLY AM 02:45AM-05:59AM",
        transit_df["TIME_PERIOD"]
    )
    # convert table to points
    return PMT.dfToPoints(df=transit_df, out_fc=out_fc, shape_fields=shape_fields,
                          from_sr=from_sr, to_sr=to_sr, overwrite=True)


# Parcel data
def clean_parcel_geometry(in_features, fc_key_field, new_fc_key, out_features=None):
    """cleans parcel geometry and sets common key to user supplied new_fc_key
    Args:
        in_features:
        fc_key_field:
        new_fc_key:
        out_features:
        overwrite (bool):
    Returns:

    """
    try:
        if out_features is None:
            raise ValueError
        else:
            temp_fc = make_inmem_path()
            arcpy.CopyFeatures_management(in_features=in_features, out_feature_class=temp_fc)
            # Repair geom and remove null geoms
            logger.log_msg("--- repair geometry")
            arcpy.RepairGeometry_management(in_features=temp_fc, delete_null="DELETE_NULL")
            # Alter field
            arcpy.AlterField_management(in_table=temp_fc, field=fc_key_field,
                                        new_field_name=new_fc_key, new_field_alias=new_fc_key)
            # Dissolve polygons
            logger.log_msg(f"--- dissolving parcel polygons on {new_fc_key}, and add count of polygons")
            arcpy.Dissolve_management(in_features=temp_fc, out_feature_class=out_features,
                                      dissolve_field=new_fc_key,
                                      statistics_fields="{} COUNT".format(new_fc_key), multi_part="MULTI_PART")
            return out_features
    except ValueError:
        logger.log_msg("output feature class path must be provided")


def prep_parcel_land_use_tbl(parcels_fc, parcel_lu_field, parcel_fields,
                             lu_tbl, tbl_lu_field, dtype_map=None, **kwargs):
    """Join parcels with land use classifications based on DOR use codes.
    Args:
        parcels_fc (str): path to parcel feature class
        parcel_lu_field (str): String; The column in `parcels_fc` with each parcel's DOR use code.
        parcel_fields: [String, ...]; Other columns in `parcels_fc` (such as an ID field, e.g.) to retain
            alongside land use codes.
        lu_tbl: Path; A csv table of land use groupings to associated with each parcel,
            keyed by DOR use code.
        tbl_lu_field: String; The column in `lu_tbl` with DOR use codes.
        dtype_map: {string: type}; Data type for data or columns. If any data type specifications are needed
            to properly parse `lu_tbl` provide them as a dictionary.
        **kwargs:
            Any other keyword arguments given are passed to the `pd.read_csv` method when reading `lu_tbl`.

    Returns:
        par_df: DataFrame
    """
    if dtype_map is None:
        dtype_map = {}
    # Dump parcels_fc to data frame
    if isinstance(parcel_fields, string_types):
        parcel_fields = [parcel_fields]
    par_fields = parcel_fields + [parcel_lu_field]
    par_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(parcels_fc, par_fields)  # TODO: null values
    )
    # Read in the land use reference table
    ref_table = pd.read_csv(lu_tbl, dtype=dtype_map, **kwargs)
    # Join
    return par_df.merge(ref_table, how="left", left_on=parcel_lu_field, right_on=tbl_lu_field)


# TODO: remove default values and be sure to import globals in preparer.py and insert there
def enrich_bg_with_parcels(bg_fc, parcels_fc, sum_crit=None, bg_id_field="GEOID10",
                           par_id_field="PARCELNO", par_lu_field="DOR_UC", par_bld_area="TOT_LVG_AREA",
                           par_sum_fields=["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA"]
                           ):
    """Relates parcels to block groups based on centroid location and summarizes
        key parcel fields to the block group level, including building floor area
        by potential activity type (residential, jobs by type, e.g.).
    Args:
        bg_fc (str): path to block group feature class
        parcels_fc (str): path to parcel feature class
        sum_crit (dict): Dictionary whose keys reflect column names to be generated to hold sums
            of parcel-level data in the output block group data frame, and whose
            values consist of at least one PMT comparator class (`Comp`, `And`).
            These are used to map parcel land use codes to LODES variables, e.g.
            An iterable of comparators in a value implies an `Or` operation.
        par_sum_fields (list): [String, ...], default=["LND_VAL", "LND_SQFOOT", "JV",
                                                       "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA]
            If provided, these parcel fields will also be summed to the block-group level.
        bg_id_field (str): block group primary key attribute default="GEOID"
        par_id_field (str): parcel primary key attribute, default="PARCELNO"
        par_lu_field (str): parcel land use attribute, default="DOR_UC"
        par_bld_area (str): parcel building area attribute, default="TOT_LVG_AREA"
    Returns:
        bg_df (pd.DataFrame): DataFrame of block group ids and related/summarized parcel data
    """
    # Prep output
    if sum_crit is None:
        sum_crit = {}
    # PMT.checkOverwriteOutput(output=out_tbl, overwrite=overwrite)
    sr = arcpy.Describe(parcels_fc).spatialReference

    # Make parcel feature layer
    parcel_fl = arcpy.MakeFeatureLayer_management(parcels_fc, "__parcels__")
    par_fields = [par_id_field, par_lu_field, par_bld_area]
    if isinstance(par_sum_fields, string_types):
        par_sum_fields = [par_sum_fields]
    par_fields += [psf for psf in par_sum_fields if psf not in par_fields]

    try:
        # Iterate over bg features
        print("--- analyzing block group features")
        bg_fields = ["SHAPE@", bg_id_field]
        bg_stack = []
        with arcpy.da.SearchCursor(
                bg_fc, bg_fields, spatial_reference=sr) as bgc:
            for bgr in bgc:
                bg_poly, bg_id = bgr
                # Select parcels in this BG
                arcpy.SelectLayerByLocation_management(parcel_fl, "HAVE_THEIR_CENTER_IN", bg_poly)
                # Dump selected to data frame
                par_df = PMT.featureclass_to_df(in_fc=parcel_fl, keep_fields=par_fields, null_val=0)
                if len(par_df) == 0:
                    print(f"---  --- no parcels found for BG {bg_id}")  # TODO: convert to warning?
                # Get mean parcel values # TODO: this assumes single part features, might not be needed now?
                par_grp_fields = [par_id_field] + par_sum_fields
                par_sum = par_df[par_grp_fields].groupby(par_id_field).mean()
                # Summarize totals to BG level
                par_sum[bg_id_field] = bg_id
                bg_grp_fields = [bg_id_field] + par_sum_fields
                bg_sum = par_sum[bg_grp_fields].groupby(bg_id_field).sum()
                # Select and summarize new fields
                for grouping in sum_crit.keys():
                    # Mask based on land use criteria
                    crit = PMT.Or(par_df[par_lu_field], sum_crit[grouping])
                    mask = crit.eval()
                    # Summarize masked data
                    #  - Parcel means (to account for multi-poly's)
                    area = par_df[mask].groupby([par_id_field]).mean()[par_bld_area]
                    #  - BG Sums
                    if len(area) > 0:
                        area = area.sum()
                    else:
                        area = 0
                    bg_sum[grouping] = area
                bg_stack.append(bg_sum.reset_index())
        # Join bg sums to outfc
        print("--- joining parcel summaries to block groups")
        bg_df = pd.concat(bg_stack)
        print(f"---  --- {len(bg_df)} block group rows")
        return bg_df
        # PMT.dfToTable(df=bg_df, out_table=out_tbl)
    except:
        raise
    finally:
        arcpy.Delete_management(parcel_fl)


def get_field_dtype(in_table, field):
    arc_to_pd = {"Double": np.float, "Integer": np.int, "String": np.str}
    f_dtype = [f.type for f in arcpy.ListFields(in_table) if f.name == field]
    return arc_to_pd.get(f_dtype[0])


def enrich_bg_with_econ_demog(tbl_path, tbl_id_field, join_tbl, join_id_field, join_fields):
    """Adds data from another raw table as new columns based on teh fields provided.
    Args:
        tbl_path (str): path table being updated
        tbl_id_field (str): table primary key
        join_tbl (str): path to table being joined with tbl_path
        join_id_field (str): join table foreign key
        join_fields (list): [String, ...]; list of fields to include in update
    Returns:
        None
    """
    try:
        if is_gz_file(join_tbl):
            tbl_df = pd.read_csv(join_tbl, usecols=join_fields, compression='gzip')
        else:
            tbl_df = pd.read_csv(join_tbl, usecols=join_fields)
        tbl_id_type = get_field_dtype(in_table=tbl_path, field=tbl_id_field)
        tbl_df[join_id_field] = tbl_df[join_id_field].astype(tbl_id_type)
        if all(item in tbl_df.columns.values.tolist() for item in join_fields):
            PMT.extendTableDf(in_table=tbl_path, table_match_field=tbl_id_field,
                              df=tbl_df, df_match_field=join_id_field)
        else:
            raise ValueError
    except ValueError:
        print("--- --- fields provided are not in join table (join_tbl)")


# TODO: remove default values for fc_key_field and tbl_key_field and ensure these are globals in preparer.py
def prep_parcels(in_fc, in_tbl, out_fc, fc_key_field="PARCELNO", new_fc_key_field="FOLIO",
                 tbl_key_field="PARCEL_ID", tbl_renames=None, **kwargs):
    """
        Starting with raw parcel features and raw parcel attributes in a table,
        clean features by repairing invalid geometries, deleting null geometries,
        and dissolving common parcel ID's. Then join attribute data based on
        the parcel ID field, managing column names in the process.

    Args:
        in_fc: Path or feature layer; A collection of raw parcel features (shapes)
        in_tbl: String; path to a table of raw parcel attributes.
        out_fc: Path
            The path to the output feature class that will contain clean parcel
            geometries with attribute columns joined.
        fc_key_field: String; default="PARCELNO"
            The field in `in_fc` that identifies each parcel feature.
        new_fc_key_field: String; parcel common key used throughout downstream processing
        tbl_key_field: String; default="PARCEL_ID"
            The field in `in_csv` that identifies each parcel feature.
        tbl_renames: dict; default={}
            Dictionary for renaming columns from `in_csv`. Keys are current column
            names; values are new column names.
        overwrite (bool): true/false value to determine whether to overwrite an exisiting parcel layer
        kwargs:
            Keyword arguments for reading csv data into pandas (dtypes, e.g.)
    Returns:
        updates parcel data
    """
    # prepare geometry data
    logger.log_msg("--- cleaning geometry")
    if tbl_renames is None:
        tbl_renames = {}
    out_fc = clean_parcel_geometry(in_features=in_fc, fc_key_field=fc_key_field,
                                   new_fc_key=new_fc_key_field, out_features=out_fc)
    # Read tabular files
    _, ext = os.path.splitext(in_tbl)
    if ext == ".csv":
        logger.log_msg("--- read csv tables")
        par_df = pd.read_csv(filepath_or_buffer=in_tbl, **kwargs)
    elif ext == ".dbf":
        logger.log_msg("--- read dbf tables")
        par_df = dbf_to_df(dbf_file=in_tbl, upper=False)
    else:
        logger.log_msg("input parcel tabular data must be 'dbf' or 'csv'")
    # ensure key is 12 characters with leading 0's for join to geo data
    par_df[tbl_key_field] = par_df[tbl_key_field].map(lambda x: f'{x:0>12}')
    # Rename columns if needed
    logger.log_msg("--- renaming columns")
    tbl_renames[tbl_key_field] = new_fc_key_field
    par_df.rename(mapper=tbl_renames, axis=1, inplace=True)

    # Add columns to dissolved features
    print("--- joining attributes to features")
    PMT.extendTableDf(in_table=out_fc, table_match_field=new_fc_key_field,
                      df=par_df, df_match_field=new_fc_key_field)


# impervious surface
def unzip_data_to_temp(zipped_file):
    temp_dir = tempfile.TemporaryDirectory()

    return temp_dir.name


def get_raster_file(folder):
    # Get the name of the raster from within the zip (the .img file), there should be only one
    rast_files = []
    raster_formats = [".img", ".tif"]
    logger.log_msg(f"--- finding all raster files of type {raster_formats}")
    try:
        for file in os.listdir(folder):
            for extension in raster_formats:
                if fnmatch.fnmatch(file, f"*{extension}"):
                    rast_files.append(PMT.makePath(folder, file))

        if len(rast_files) == 1:
            return rast_files[0]
        else:
            raise ValueError
    except ValueError:
        logger.log_msg("More than one Raster/IMG file is present in the zipped folder")


def prep_imperviousness(zip_path, clip_path, out_dir, transform_crs=None):
    """
    Clean a USGS impervious surface raster by Clipping to the bounding box of a study area
        and transforming the clipped raster to a desired CRS

    Args:
        zip_path: Path
            .zip folder of downloaded imperviousness raster (see the
            `dl_imperviousness` function)
        clip_path: Path
            path of study area polygon(s) whose bounding box will be used to clip
            the raster
        out_dir: Path
            save location for the clipped and transformed raster
        transform_crs: Anything accepted by arcpy.SpatialReference(), optional
            Identifier of spatial reference to which to transform the clipped
            raster; can be any form accepted by arcpy.SpatialReference()

    Returns:
        File will be clipped, transformed, and saved to the save directory; the
        save path will be returned upon completion
    """
    # temp_unzip_folder = unzip_data_to_temp(zipped_file=zip_path)
    with tempfile.TemporaryDirectory() as temp_unzip_folder:
        logger.log_msg("--- unzipping imperviousness raster in temp directory")
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(temp_unzip_folder)
        raster_file = get_raster_file(folder=temp_unzip_folder)

        # define the output file from input file
        rast_name, ext = os.path.splitext(os.path.split(raster_file)[1])
        clipped_raster = PMT.makePath(temp_unzip_folder, f"clipped{ext}")

        logger.log_msg("--- checking if a transformation of the clip geometry is necessary")
        # Transform the clip geometry if necessary
        raster_sr = arcpy.Describe(raster_file).spatialReference
        clip_sr = arcpy.Describe(clip_path).spatialReference
        if raster_sr != clip_sr:
            logger.log_msg("--- reprojecting clipping geometry to match raster")
            project_file = PMT.makePath(temp_unzip_folder, "Project.shp")
            arcpy.Project_management(in_dataset=clip_path, out_dataset=project_file, out_coor_system=raster_sr)
            clip_path = project_file

        # Grab the bounding box of the clipping file
        logger.log_msg("--- clipping raster data to project extent")
        bbox = arcpy.Describe(clip_path).Extent
        arcpy.Clip_management(in_raster=raster_file, rectangle="", out_raster=clipped_raster,
                              in_template_dataset=bbox.polygon, clipping_geometry="ClippingGeometry")

        # Transform the clipped raster
        logger.log_msg("--- copying/reprojecting raster out to project CRS")
        transform_crs = arcpy.SpatialReference(transform_crs)
        out_raster = PMT.makePath(out_dir, f"{rast_name}_clipped{ext}")
        if transform_crs != raster_sr:
            arcpy.ProjectRaster_management(in_raster=clipped_raster, out_raster=out_raster,
                                           out_coor_system=transform_crs, resampling_type="NEAREST")
        else:
            arcpy.CopyRaster_management(in_raster=clipped_raster, out_rasterdataset=out_raster)
    return out_raster


def analyze_imperviousness(impervious_path, zone_geometries_path, zone_geometries_id_field):
    """
        Summarize percent impervious surface cover in each of a collection of zones
    Args:
        impervious_path: Path
            path to clipped/transformed imperviousness raster (see the
            `prep_imperviousness` function)
        zone_geometries_path: Path
            path to polygon geometries to which imperviousness will be summarized
        zone_geometries_id_field: str
            id field in the zone geometries
    Returns:
        pandas dataframe; table of impervious percent within the zone geometries
    """

    logger.log_msg("--- matching imperviousness to zone geometries")
    logger.log_msg("--- --- setting up an intermediates gdb")
    with tempfile.TemporaryDirectory() as temp_dir:
        arcpy.CreateFileGDB_management(out_folder_path=temp_dir, out_name="Intermediates.gdb")
        intmd_gdb = makePath(temp_dir, "Intermediates.gdb")

        # Convert raster to point (and grabbing cell size)
        logger.log_msg("--- --- converting raster to point")
        rtp_path = makePath(intmd_gdb, "raster_to_point")
        arcpy.RasterToPoint_conversion(in_raster=impervious_path,
                                       out_point_features=rtp_path)

        # Intersect raster with zones
        logger.log_msg("--- --- matching raster points to zones")
        intersection_path = makePath(intmd_gdb, "intersection")
        arcpy.Intersect_analysis(in_features=[rtp_path, zone_geometries_path],
                                 out_feature_class=intersection_path)

        # Load the intersection data
        logger.log_msg("--- --- loading raster/zone data")
        load_fields = [zone_geometries_id_field, "grid_code"]
        df = arcpy.da.FeatureClassToNumPyArray(in_table=intersection_path,
                                               field_names=load_fields)
        df = pd.DataFrame(df)

    # Values with 127 are nulls -- replace with 0
    logger.log_msg("--- --- replacing null impervious values with 0")
    df['grid_code'] = df['grid_code'].replace(127, 0)

    logger.log_msg("--- summarizing zonal imperviousness statistics")
    logger.log_msg("--- grabbing impervious raster cell size")
    cellx = arcpy.GetRasterProperties_management(in_raster=impervious_path, property_type="CELLSIZEX")
    celly = arcpy.GetRasterProperties_management(in_raster=impervious_path, property_type="CELLSIZEY")
    cs = float(cellx.getOutput(0)) * float(celly.getOutput(0))

    # Groupby-summarise the variables of interest
    logger.log_msg("--- --- calculating zonal summaries")
    zonal = df.groupby(zone_geometries_id_field)["grid_code"].agg(
        [("IMP_PCT", np.mean),
         ("TotalArea", lambda x: x.count() * cs),
         ("NonDevArea", lambda x: x[x == 0].count() * cs),
         ("DevOSArea", lambda x: x[x.between(1, 19)].count() * cs),
         ("DevLowArea", lambda x: x[x.between(20, 49)].count() * cs),
         ("DevMedArea", lambda x: x[x.between(50, 79)].count() * cs),
         ("DevHighArea", lambda x: x[x >= 80].count() * cs)])
    return zonal.reset_index()


def analyze_blockgroup_model(bg_enrich_path, bg_key, fields="*", acs_years=None, lodes_years=None, save_directory=None):
    """
        fit linear models to block group-level total employment, population, and
        commutes at the block group level, and save the model coefficients for
        future prediction

    Args:
        bg_enrich_path : str
            path to enriched block group data, with a fixed-string wild card for
            year (see Notes)
        acs_years : list of int
            years for which ACS variables (population, commutes) are present in
            the data
        lodes_years : list of int
            years for which LODES variables (employment) are present in the data
        save_directory : str
            directory to which to save the model coefficients (results will be
            saved as a csv)

    Notes:
        in `bg_enrich_path`, replace the presence of a year with the string
        "{year}". For example, if your enriched block group data for 2010-2015 is
        stored at "Data_2010.gdb/enriched", "Data_2010.gdb/enriched", ..., then
        `bg_enrich_path = "Data_{year}.gdb/enriched"`.

    Returns:
        save_path : str
            path to a table of model coefficients
    """

    logger.log_msg("--- reading input data (block group)")
    df = []
    years = np.unique(np.concatenate([acs_years, lodes_years]))
    for year in years:
        logger.log_msg(' '.join(["----> Loading", str(year)]))
        # Read
        load_path = re.sub("YEAR", str(year), bg_enrich_path, flags=re.IGNORECASE)
        # fields = [f.name for f in arcpy.ListFields(load_path)]
        tab = pd.DataFrame(
            arcpy.da.FeatureClassToNumPyArray(
                in_table=load_path, field_names=fields, null_value=0
            )
        )
        # Edit
        tab["Year"] = year
        tab["Since_2013"] = year - 2013
        tab["Total_Emp_Area"] = (
                tab["CNS_01_par"] + tab["CNS_02_par"] + tab["CNS_03_par"] +
                tab["CNS_04_par"] + tab["CNS_05_par"] + tab["CNS_06_par"] +
                tab["CNS_07_par"] + tab["CNS_08_par"] + tab["CNS_09_par"] +
                tab["CNS_10_par"] + tab["CNS_11_par"] + tab["CNS_12_par"] +
                tab["CNS_13_par"] + tab["CNS_14_par"] + tab["CNS_15_par"] +
                tab["CNS_16_par"] + tab["CNS_17_par"] + tab["CNS_18_par"] +
                tab["CNS_19_par"] + tab["CNS_20_par"]
        )
        if year in lodes_years:
            tab["Total_Employment"] = (
                    tab["CNS01"] + tab["CNS02"] + tab["CNS03"] + tab["CNS04"] +
                    tab["CNS05"] + tab["CNS06"] + tab["CNS07"] + tab["CNS08"] +
                    tab["CNS09"] + tab["CNS10"] + tab["CNS11"] + tab["CNS12"] +
                    tab["CNS13"] + tab["CNS14"] + tab["CNS15"] + tab["CNS16"] +
                    tab["CNS17"] + tab["CNS18"] + tab["CNS19"] + tab["CNS20"]
            )
        if year in acs_years:
            tab["Total_Population"] = (
                    tab["Total_Non_Hisp"] + tab["Total_Hispanic"]
            )
        # Store
        df.append(tab)

    # Bind up the table, filling empty rows
    df = pd.concat(df, ignore_index=True)

    # 2. Model
    # --------

    # Variable setup: defines our variables of interest for modeling
    independent_variables = ["LND_VAL", "LND_SQFOOT", "JV", "TOT_LVG_AREA",
                             "NO_BULDNG", "NO_RES_UNTS", "RES_par",
                             "CNS_01_par", "CNS_02_par", "CNS_03_par",
                             "CNS_04_par", "CNS_05_par", "CNS_06_par",
                             "CNS_07_par", "CNS_08_par", "CNS_09_par",
                             "CNS_10_par", "CNS_11_par", "CNS_12_par",
                             "CNS_13_par", "CNS_14_par", "CNS_15_par",
                             "CNS_16_par", "CNS_17_par", "CNS_18_par",
                             "CNS_19_par", "CNS_20_par", "Total_Emp_Area",
                             "Since_2013"]
    response = {"Total_Employment": lodes_years,
                "Total_Population": acs_years,
                "Total_Commutes": acs_years}

    # Step 1: Overwrite NA values with 0 (where we should have data but don't)
    # -- parcel-based variables should be there every time: fill all with 0
    # -- job variables should be there for `lodes_years`: fill these with 0
    # -- dem variables should be there for `acs_years`: fill these with 0
    logger.log_msg("--- replacing missing values")
    parcel_na_dict = {iv: 0 for iv in independent_variables}
    df.fillna(parcel_na_dict,
              inplace=True)
    df.loc[(df.Total_Employment.isna()) & df.Year.isin(lodes_years), "Total_Employment"] = 0
    df.loc[(df.Total_Population.isna()) & df.Year.isin(acs_years), "Total_Population"] = 0
    df.loc[(df.Total_Commutes.isna()) & df.Year.isin(acs_years), "Total_Commutes"] = 0
    keep_cols = [bg_key, "Year"] + independent_variables + list(response.keys())
    df = df[keep_cols]

    # Step 2: conduct modeling by extracting a correlation matrix between candidate
    # explanatories and our responses, identifying explanatories with significant
    # correlations to our response, and fitting a MLR using these explanatories
    logger.log_msg("--- fitting and applying models")
    fits = []
    for key, value in response.items():
        logger.log_msg(' '.join(["---->", key]))
        # Subset to relevant years for relevant years
        mdf = df[df.Year.isin(value)][independent_variables + [key]]
        n = len(mdf.index)
        # Correlation of all explanatories with response
        corr_mat = mdf.corr()
        cwr = corr_mat[key]
        cwr.drop(key, inplace=True)
        cwr = cwr[~cwr.isna()]
        # Calculate t statistic and p-value for correlation test
        t_stat = cwr * np.sqrt((n - 2) / (1 - cwr ** 2))
        p_values = pd.Series(scipy.stats.t.sf(t_stat, n - 2) * 2,
                             index=t_stat.index)
        # Variables for the model
        mod_vars = []
        cutoff = 0.05
        while len(mod_vars) == 0:
            mod_vars = p_values[p_values.le(cutoff)].index.tolist()
            cutoff += 0.05
        # Fit a multiple linear regression
        regr = linear_model.LinearRegression()
        regr.fit(X=mdf[mod_vars],
                 y=mdf[[key]])
        # Save the model coefficients
        fits.append(pd.Series(regr.coef_[0],
                              index=mod_vars,
                              name=key))

    # Step 3: combine results into a single df
    logger.log_msg("--- formatting model coefficients into a single table")
    coefs = pd.concat(fits, axis=1).reset_index()
    coefs.rename(columns={"index": "Variable"},
                 inplace=True)
    coefs.fillna(0,
                 inplace=True)

    # 3. Write
    # --------

    logger.log_msg("--- writing results")
    save_path = makePath(save_directory,
                         "block_group_model_coefficients.csv")
    coefs.to_csv(save_path,
                 index=False)

    # Done
    # ----
    return save_path


def analyze_blockgroup_apply(year, bg_enrich_path, bg_geometry_path, bg_id_field,
                             model_coefficients_path, save_gdb_location, shares_from=None):
    """
        predict block group-level total employment, population, and commutes using
        pre-fit linear models, and apply a shares-based approach to subdivide
        totals into relevant subgroups

    Args:
        year : int
            year of the `bg_enrich` data
        bg_enrich_path : str
            path to enriched block group data; this is the data to which the
            models will be applied
        bg_geometry_path : str
            path to geometry of block groups underlying the data
        model_coefficients_path : str
            path to table of model coefficients
        save_gdb_location : str
            gdb location to which to save the results (results will be saved as a
            table)
        shares_from : dict, optional
            if the year of interest does not have observed data for either LODES
            or ACS, provide other files from which subgroup shares can be
            calculated (with the keys "LODES" and "ACS", respectively).
            For example, imagine that you are applying the models to a year where
            ACS data was available but LODES data was not. Then,
            `shares_from = {"LODES": "path_to_most_recent_bg_enrich_file_with_LODES"}.
            A separate file does not need to be referenced for ACS because the
            data to which the models are being applied already reflects shares for
            ACS variables.
            The default is None, which assumes LODES and ACS data are available
            for the year of interest in the provided `bg_enrich` file

    Returns:
        save_path : str
            path to a table of model application results
    """

    logger.log_msg("--- reading input data (block group)")
    fields = [f.name for f in arcpy.ListFields(bg_enrich_path)]
    df = pd.DataFrame(
        arcpy.da.FeatureClassToNumPyArray(in_table=bg_enrich_path, field_names=fields, null_value=0)
    )

    df["Since_2013"] = year - 2013
    df["Total_Emp_Area"] = (
            df["CNS_01_par"] + df["CNS_02_par"] + df["CNS_03_par"] +
            df["CNS_04_par"] + df["CNS_05_par"] + df["CNS_06_par"] +
            df["CNS_07_par"] + df["CNS_08_par"] + df["CNS_09_par"] +
            df["CNS_10_par"] + df["CNS_11_par"] + df["CNS_12_par"] +
            df["CNS_13_par"] + df["CNS_14_par"] + df["CNS_15_par"] +
            df["CNS_16_par"] + df["CNS_17_par"] + df["CNS_18_par"] +
            df["CNS_19_par"] + df["CNS_20_par"]
    )
    # Fill na
    independent_variables = ["LND_VAL", "LND_SQFOOT", "JV", "TOT_LVG_AREA",
                             "NO_BULDNG", "NO_RES_UNTS", "RES_par",
                             "CNS_01_par", "CNS_02_par", "CNS_03_par",
                             "CNS_04_par", "CNS_05_par", "CNS_06_par",
                             "CNS_07_par", "CNS_08_par", "CNS_09_par",
                             "CNS_10_par", "CNS_11_par", "CNS_12_par",
                             "CNS_13_par", "CNS_14_par", "CNS_15_par",
                             "CNS_16_par", "CNS_17_par", "CNS_18_par",
                             "CNS_19_par", "CNS_20_par", "Total_Emp_Area",
                             "Since_2013"]
    parcel_na_dict = {iv: 0 for iv in independent_variables}
    df.fillna(parcel_na_dict, inplace=True)

    # 2. Apply models
    # ---------------
    logger.log_msg("--- applying models to predict totals")
    # Load the coefficients
    coefs = pd.read_csv(model_coefficients_path)
    # Predict using matrix multiplication
    mod_inputs = df[coefs["Variable"]]
    coef_values = coefs.drop(columns="Variable")
    preds = np.matmul(mod_inputs.to_numpy(), coef_values.to_numpy())
    preds = pd.DataFrame(data=preds)
    preds.columns = coef_values.columns.tolist()
    pwrite = pd.concat([df[[bg_id_field]], preds], axis=1)
    # If any prediction is below 0, turn it to 0
    pwrite.loc[pwrite.Total_Employment < 0, "Total_Employment"] = 0
    pwrite.loc[pwrite.Total_Population < 0, "Total_Population"] = 0
    pwrite.loc[pwrite.Total_Commutes < 0, "Total_Commutes"] = 0

    # 3. Shares
    # ---------

    # Variable setup: defines our variables of interest for modeling
    dependent_variables_emp = ["CNS01", "CNS02", "CNS03", "CNS04", "CNS05",
                               "CNS06", "CNS07", "CNS08", "CNS09", "CNS10",
                               "CNS11", "CNS12", "CNS13", "CNS14", "CNS15",
                               "CNS16", "CNS17", "CNS18", "CNS19", "CNS20"]
    dependent_variables_pop_tot = ["Total_Hispanic", "Total_Non_Hisp"]
    dependent_variables_pop_sub = ["White_Hispanic", "Black_Hispanic",
                                   "Asian_Hispanic", "Multi_Hispanic",
                                   "Other_Hispanic", "White_Non_Hisp",
                                   "Black_Non_Hisp", "Asian_Non_Hisp",
                                   "Multi_Non_Hisp", "Other_Non_Hisp"]
    dependent_variables_trn = ["Drove", "Carpool", "Transit",
                               "NonMotor", "Work_From_Home", "AllOther"]
    acs_vars = dependent_variables_pop_tot + dependent_variables_pop_sub + dependent_variables_trn

    # Pull shares variables from appropriate sources, recognizing that they
    # may not all be the same!
    logger.log_msg("--- formatting shares data")
    # Format
    if shares_from is not None:
        if "LODES" in shares_from.keys():
            lodes = PMT.featureclass_to_df(in_fc=shares_from["LODES"],
                                           keep_fields=[bg_id_field] + dependent_variables_emp, null_val=0.0)
        else:
            lodes = df[[bg_id_field] + dependent_variables_emp]
        if "ACS" in shares_from.keys():
            acs = PMT.featureclass_to_df(in_fc=shares_from["ACS"],
                                         keep_fields=[bg_id_field] + acs_vars, null_val=0.0)
        else:
            acs = df[[bg_id_field] + acs_vars]
    else:
        lodes = df[[bg_id_field] + dependent_variables_emp]
        acs = df[[bg_id_field] + acs_vars]
    # merge and replace NA
    shares_df = pd.merge(lodes, acs, on=bg_id_field, how="left")
    shares_df.fillna(0, inplace=True)

    # Step 2: Calculate shares relative to total
    # This is done relative to the "Total" variable for each group
    logger.log_msg("--- calculating shares")
    shares_dict = {}
    for name, vrs in zip(["Emp", "Pop_Tot", "Pop_Sub", "Comm"],
                         [dependent_variables_emp,
                          dependent_variables_pop_tot,
                          dependent_variables_pop_sub,
                          dependent_variables_trn]):
        sdf = shares_df[vrs]
        sdf["TOTAL"] = sdf.sum(axis=1)
        for d in vrs:
            sdf[d] = sdf[d] / sdf["TOTAL"]
        sdf[bg_id_field] = shares_df[bg_id_field]
        sdf.drop(columns="TOTAL",
                 inplace=True)
        shares_dict[name] = sdf

    # Step 3: some rows have NA shares because the total for that class of
    # variables was 0. For these block groups, take the average share of all
    # block groups that touch that one
    logger.log_msg("--- estimating missing shares")
    # What touches what? # TODO: add make_inmem
    temp = PMT.make_inmem_path()
    arcpy.PolygonNeighbors_analysis(in_features=bg_geometry_path, out_table=temp, in_fields=bg_id_field)

    touch = pd.DataFrame(
        arcpy.da.FeatureClassToNumPyArray(
            in_table=temp, field_names=[f"src_{bg_id_field}", f"nbr_{bg_id_field}"]
        )
    )
    touch.rename(columns={f"src_{bg_id_field}": bg_id_field, f"nbr_{bg_id_field}": "Neighbor"}, inplace=True)
    # Loop filling of NA by mean of adjacent non-NAs
    ctf = 1
    i = 1
    while (ctf > 0):
        # First, identify cases where we need to fill NA
        to_fill = []
        for key, value in shares_dict.items():
            f = value[value.isna().any(axis=1)]
            f = f[[bg_id_field]]
            f["Fill"] = key
            to_fill.append(f)
        to_fill = pd.concat(to_fill, ignore_index=True)
        # Create a neighbors table
        nt = pd.merge(to_fill, touch, how="left", on=bg_id_field)
        nt.rename(columns={bg_id_field: "Source", "Neighbor": bg_id_field}, inplace=True)
        # Now, merge in the shares data for appropriate rows
        fill_by_touching = {}
        nrem = []
        for key, value in shares_dict.items():
            fill_df = pd.merge(nt[nt.Fill == key], value, how="left", on=bg_id_field)
            nv = fill_df.groupby("Source").mean()
            nv["RS"] = nv.sum(axis=1)
            data_cols = [c for c in nv.columns.tolist() if c != bg_id_field]
            for d in data_cols:
                nv[d] = nv[d] / nv["RS"]
            nv.drop(columns="RS", inplace=True)
            nv = nv.reset_index()
            nv.rename(columns={"Source": bg_id_field}, inplace=True)
            not_replaced = value[~value[bg_id_field].isin(nv[bg_id_field])]
            replaced = pd.concat([not_replaced, nv])
            fill_by_touching[key] = replaced
            nrem.append(len(replaced[replaced.isna().any(axis=1)].index))
        # Now, it's possible that some block group/year combos to be filled had
        # 0 block groups in that year touching them that had data. If this happened,
        # we're goisadfsdfsdfng to repeat the process. Check by summing nrem
        # and initialize by resetting the shares dict
        ctf = sum(nrem)
        i += 1
        shares_dict = fill_by_touching

    # Step 4: merge and format the shares
    logger.log_msg("--- merging and formatting shares")
    filled_shares = [df.set_index(bg_id_field) for df in shares_dict.values()]
    cs_shares = pd.concat(filled_shares, axis=1).reset_index()
    cs_shares.rename(columns={"index": bg_id_field}, inplace=True)

    # 4. Block group estimation
    # -------------------------

    logger.log_msg("--- estimating variable levels using model estimates and shares")
    # Now, our allocations are simple multiplication problems! Hooray!
    # So, all we have to do is multiply the shares by the appropriate column
    # First, we'll merge our estimates and shares
    alloc = pd.merge(pwrite,
                     cs_shares,
                     on=bg_id_field)
    # We'll do employment first
    for d in dependent_variables_emp:
        alloc[d] = alloc[d] * alloc.Total_Employment
    # Now population
    for d in dependent_variables_pop_tot:
        alloc[d] = alloc[d] * alloc.Total_Population
    for d in dependent_variables_pop_sub:
        alloc[d] = alloc[d] * alloc.Total_Population
    # Finally commutes
    for d in dependent_variables_trn:
        alloc[d] = alloc[d] * alloc.Total_Commutes

    # 5. Writing
    # ----------

    logger.log_msg("--- writing outputs")
    # Here we write block group for allocation
    save_path = makePath(save_gdb_location,
                         "Modeled_blockgroups")
    dfToTable(df=alloc,
              out_table=save_path)

    # Done
    # --- -
    return save_path


def analyze_blockgroup_allocate(out_gdb, bg_modeled, bg_geom, bg_id_field,
                                parcel_fc, parcels_id="FOLIO", parcel_lu="DOR_UC", parcel_liv_area="TOT_LVG_AREA"):
    """Allocate block group data to parcels using relative abundances of
        parcel building square footage
    Args:
        parcel_fc: Path
            path to shape of parcel polygons, containing at a minimum a unique ID
            field, land use field, and total living area field (Florida DOR)
        bg_modeled: Path
            path to table of modeled block group job, populatiom, and commute
            data for allocation
        bg_geom: Path
            path to feature class of block group polygons
        out_gdb: Path
            path to location in which the allocated results will be saved
        parcels_id: str
            unique ID field in the parcels shape
            Default is "PARCELNO" for Florida parcels
        parcel_lu: str
            land use code field in the parcels shape
            Default is "DOR_UC" for Florida parcels
        parcel_liv_area: str
            building square footage field in the parcels shape
            Default is "TOT_LVG_AREA" for Florida parcels
    Returns:
        path of location at which the allocation results are saved.
        Saving will be completed as part of the function. The allocation estimates
        will be joined to the original parcels shape

    """

    # Organize constants for allocation
    lodes_attrs = ['CNS01', 'CNS02', 'CNS03', 'CNS04', 'CNS05',
                   'CNS06', 'CNS07', 'CNS08', 'CNS09', 'CNS10',
                   'CNS11', 'CNS12', 'CNS13', 'CNS14', 'CNS15',
                   'CNS16', 'CNS17', 'CNS18', 'CNS19', 'CNS20']
    demog_attrs = ['Total_Hispanic', 'White_Hispanic', 'Black_Hispanic',
                   'Asian_Hispanic', 'Multi_Hispanic', 'Other_Hispanic',
                   'Total_Non_Hisp', 'White_Non_Hisp', 'Black_Non_Hisp',
                   'Asian_Non_Hisp', 'Multi_Non_Hisp', 'Other_Non_Hisp']
    commute_attrs = ['Drove', 'Carpool', 'Transit',
                     'NonMotor', 'Work_From_Home', 'AllOther']
    block_group_attrs = [bg_id_field] + lodes_attrs + demog_attrs + commute_attrs

    # Initialize spatial processing by intersecting
    print("--- intersecting blocks and parcels")
    temp_spatial = make_inmem_path()
    copyFeatures(in_fc=bg_geom, out_fc=temp_spatial)
    arcpy.JoinField_management(in_data=temp_spatial, in_field=bg_id_field,
                               join_table=bg_modeled, join_field=bg_id_field)
    parcel_fields = [parcels_id, parcel_lu, parcel_liv_area, "Shape_Area"]
    intersect_fc = intersectFeatures(summary_fc=temp_spatial, disag_fc=parcel_fc, disag_fields=parcel_fields)
    intersect_fields = parcel_fields + block_group_attrs
    intersect_df = featureclass_to_df(in_fc=intersect_fc, keep_fields=intersect_fields)

    # Format data for allocation
    logger.log_msg("--- formatting block group for allocation data")
    # set any value below 0 to 0 and set any land use from -1 to NA
    to_clip = lodes_attrs + demog_attrs + commute_attrs + [parcel_liv_area, "Shape_Area"]
    for var in to_clip:
        intersect_df[f"{var}"] = intersect_df[f"{var}"].clip(lower=0)
    # 2. replace -1 in DOR_UC with NA
    pluf = parcel_lu
    elu = intersect_df[pluf] == -1
    intersect_df.loc[elu, pluf] = None

    # Step 1 in allocation is totaling the living area by activity in each block group.
    # To do this, we define in advance which activities can go to which land uses

    # First, we set up this process by matching activities to land uses
    print("--- setting up activity-land use matches...")
    lu_mask = {
        'CNS01': ((intersect_df[pluf] >= 50) & (intersect_df[pluf] <= 69)),
        'CNS02': (intersect_df[pluf] == 92),
        'CNS03': (intersect_df[pluf] == 91),
        'CNS04': ((intersect_df[pluf] == 17) | (intersect_df[pluf] == 19)),
        'CNS05': ((intersect_df[pluf] == 41) | (intersect_df[pluf] == 42)),
        'CNS06': (intersect_df[pluf] == 29),
        'CNS07': ((intersect_df[pluf] >= 11) & (intersect_df[pluf] <= 16)),
        'CNS08': ((intersect_df[pluf] == 48) | (intersect_df[pluf] == 49) | (intersect_df[pluf] == 20)),
        'CNS09': ((intersect_df[pluf] == 17) | (intersect_df[pluf] == 18) | (intersect_df[pluf] == 19)),
        'CNS10': ((intersect_df[pluf] == 23) | (intersect_df[pluf] == 24)),
        'CNS11': ((intersect_df[pluf] == 17) | (intersect_df[pluf] == 18) | (intersect_df[pluf] == 19)),
        'CNS12': ((intersect_df[pluf] == 17) | (intersect_df[pluf] == 18) | (intersect_df[pluf] == 19)),
        'CNS13': ((intersect_df[pluf] == 17) | (intersect_df[pluf] == 18) | (intersect_df[pluf] == 19)),
        'CNS14': (intersect_df[pluf] == 89),
        'CNS15': ((intersect_df[pluf] == 72) | (intersect_df[pluf] == 83) | (intersect_df[pluf] == 84)),
        'CNS16': ((intersect_df[pluf] == 73) | (intersect_df[pluf] == 85)),
        'CNS17': (((intersect_df[pluf] >= 30) & (intersect_df[pluf] <= 38)) | (intersect_df[pluf] == 82)),
        'CNS18': ((intersect_df[pluf] == 21) | (intersect_df[pluf] == 22) | (intersect_df[pluf] == 33) |
                  (intersect_df[pluf] == 39)),
        'CNS19': ((intersect_df[pluf] == 27) | (intersect_df[pluf] == 28)),
        'CNS20': ((intersect_df[pluf] >= 86) & (intersect_df[pluf] <= 89)),
        'Population': (((intersect_df[pluf] >= 1) & (intersect_df[pluf] <= 9)) |
                       ((intersect_df[pluf] >= 100) & (intersect_df[pluf] <= 102)))
    }

    # Note that our activity-land use matches aren't guaranteed because they are subjectively defined.
    # To that end, we need backups in case a block group is entirely missing all possible land uses
    # for an activity.
    #   - we set up masks for 'all non-res' (all land uses relevant to any non-NAICS-1-or-2 job type)
    #   - and 'all developed' ('all non-res' + any residential land uses).
    #  ['all non-res' will be used if a land use isn't present for a given activity;
    #  [ the 'all developed' will be used if 'all non-res' fails]
    non_res_lu_codes = [11, 12, 13, 14, 15, 16, 17, 18, 19,
                        20, 21, 22, 23, 24, 27, 28, 29,
                        30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
                        41, 42, 48, 49,
                        72, 73,
                        82, 84, 85, 86, 87, 88, 89]
    all_dev_lu_codes = non_res_lu_codes + [1, 2, 3, 4, 5, 6, 7, 8, 9,
                                           100, 101, 102]
    all_non_res = {'NR': (intersect_df[pluf].isin(non_res_lu_codes))}
    all_developed = {'AD': (intersect_df[pluf].isin(all_dev_lu_codes))}

    # If all else fails, A fourth level we'll use (if we need to) is simply all total living area in the block group,
    #   but we don't need a mask for that. If this fails (which it rarely should), we revert to land area,
    #   which we know will work (all parcels have area right?)

    # Next, we'll total parcels by block group (this is just a simple operation
    # to give our living area totals something to join to)
    logger.log_msg("--- initializing living area sums")
    count_parcels_bg = intersect_df.groupby([bg_id_field])[bg_id_field].agg(['count'])
    count_parcels_bg.rename(columns={'count': 'NumParBG'}, inplace=True)
    count_parcels_bg = count_parcels_bg.reset_index()

    # Now we can begin totaling living area. We'll start with jobs
    logger.log_msg("--- totaling living area by job type")
    # 1. get count of total living area (w.r.t. land use mask) for each
    # job type
    pldaf = "Shape_Area"
    for var in lodes_attrs:
        # mask by LU, group on bg_id_field
        area = intersect_df[lu_mask[var]].groupby([bg_id_field])[parcel_liv_area].agg(['sum'])
        area.rename(columns={'sum': f'{var}_Area'},
                    inplace=True)
        area = area[area[f'{var}_Area'] > 0]
        area = area.reset_index()
        area[f'{var}_How'] = "lu_mask"
        missing = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
        if (len(missing) > 0):
            lev1 = intersect_df[all_non_res["NR"]]
            lev1 = lev1[lev1[bg_id_field].isin(missing)]
            area1 = lev1.groupby([bg_id_field])[parcel_liv_area].agg(['sum'])
            area1.rename(columns={'sum': f'{var}_Area'},
                         inplace=True)
            area1 = area1[area1[f'{var}_Area'] > 0]
            area1 = area1.reset_index()
            area1[f'{var}_How'] = "non_res"
            area = pd.concat([area, area1])
            missing1 = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
            if (len(missing1) > 0):
                lev2 = intersect_df[all_developed["AD"]]
                lev2 = lev2[lev2[bg_id_field].isin(missing1)]
                area2 = lev2.groupby([bg_id_field])[parcel_liv_area].agg(['sum'])
                area2.rename(columns={'sum': f'{var}_Area'},
                             inplace=True)
                area2 = area2[area2[f'{var}_Area'] > 0]
                area2 = area2.reset_index()
                area2[f'{var}_How'] = "all_dev"
                area = pd.concat([area, area2])
                missing2 = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
                if (len(missing2) > 0):
                    lev3 = intersect_df[intersect_df[bg_id_field].isin(missing2)]
                    area3 = lev3.groupby([bg_id_field])[parcel_liv_area].agg(['sum'])
                    area3.rename(columns={'sum': f'{var}_Area'},
                                 inplace=True)
                    area3 = area3[area3[f'{var}_Area'] > 0]
                    area3 = area3.reset_index()
                    area3[f'{var}_How'] = "living_area"
                    area = pd.concat([area, area3])
                    missing3 = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
                    if (len(missing3) > 0):
                        lev4 = intersect_df[intersect_df[bg_id_field].isin(missing3)]
                        area4 = lev4.groupby([bg_id_field])[pldaf].agg(['sum'])
                        area4.rename(columns={'sum': f'{var}_Area'},
                                     inplace=True)
                        area4 = area4.reset_index()
                        area4[f'{var}_How'] = "land_area"
                        area = pd.concat([area, area4])
        area = area.reset_index(drop=True)
        count_parcels_bg = pd.merge(count_parcels_bg, area,
                                    how='left',
                                    on=bg_id_field)

    # Repeat the above with population
    logger.log_msg("--- totaling living area for population")
    area = intersect_df[lu_mask['Population']].groupby([bg_id_field])[parcel_liv_area].agg(['sum'])
    area.rename(columns={'sum': 'Population_Area'},
                inplace=True)
    area = area[area['Population_Area'] > 0]
    area = area.reset_index()
    area['Population_How'] = "lu_mask"
    missing1 = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
    if (len(missing1) > 0):
        lev2 = intersect_df[all_developed["AD"]]
        lev2 = lev2[lev2[bg_id_field].isin(missing1)]
        area2 = lev2.groupby([bg_id_field])[parcel_liv_area].agg(['sum'])
        area2.rename(columns={'sum': 'Population_Area'},
                     inplace=True)
        area2 = area2[area2['Population_Area'] > 0]
        area2 = area2.reset_index()
        area2['Population_How'] = "all_dev"
        area = pd.concat([area, area2])
        missing2 = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
        if (len(missing2) > 0):
            lev3 = intersect_df[intersect_df[bg_id_field].isin(missing2)]
            area3 = lev3.groupby([bg_id_field])[parcel_liv_area].agg(['sum'])
            area3.rename(columns={'sum': 'Population_Area'},
                         inplace=True)
            area3 = area3[area3['Population_Area'] > 0]
            area3 = area3.reset_index()
            area3['Population_How'] = "living_area"
            area = pd.concat([area, area3])
            missing3 = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
            if (len(missing3) > 0):
                lev4 = intersect_df[intersect_df[bg_id_field].isin(missing3)]
                area4 = lev4.groupby([bg_id_field])[pldaf].agg(['sum'])
                area4.rename(columns={'sum': 'Population_Area'},
                             inplace=True)
                area4 = area4.reset_index()
                area4['Population_How'] = "land_area"
                area = pd.concat([area, area4])
    area = area.reset_index(drop=True)
    count_parcels_bg = pd.merge(count_parcels_bg, area,
                                how='left',
                                on=bg_id_field)

    # Now, we format and re-merge with our original parcel data
    logger.log_msg("--- merging living area totals with parcel-level data")
    # 1. fill table with NAs -- no longer needed because NAs are eliminated
    # by nesting structure
    # tot_bg = tot_bg.fillna(0)
    # 2. merge back to original data
    intersect_df = pd.merge(intersect_df, count_parcels_bg,
                            how='left',
                            on=bg_id_field)

    # Step 2 in allocation is taking parcel-level proportions of living area
    # relative to the block group total, and calculating parcel-level
    # estimates of activities by multiplying the block group activity total
    # by the parcel-level proportions

    # For allocation, we need a two step process, depending on how the area
    # was calculated for the activity. If "{var}_How" is land_area, then
    # allocation needs to be relative to land area; otherwise, it needs to be
    # relative to living area. To do this, we'll set up mask dictionaries
    # similar to the land use mask
    logger.log_msg("setting up allocation logic --- ")
    lu = {}
    nr = {}
    ad = {}
    lvg_area = {}
    lnd_area = {}
    for v in lu_mask.keys():
        lu[v] = (intersect_df[f'{v}_How'] == "lu_mask")
        nr[v] = (intersect_df[f'{v}_How'] == "non_res")
        ad[v] = (intersect_df[f'{v}_How'] == "all_dev")
        lvg_area[v] = (intersect_df[f'{v}_How'] == "living_area")
        lnd_area[v] = (intersect_df[f'{v}_How'] == "land_area")

    # First up, we'll allocate jobs
    logger.log_msg("--- allocating jobs and population")
    # 1. for each job variable, calculate the proportion, then allocate
    for var in lu_mask.keys():
        # First for lu mask
        intersect_df.loc[lu[var] & lu_mask[var], f'{var}_Par_Prop'] = (
                intersect_df[parcel_liv_area][lu[var] & lu_mask[var]] / intersect_df[f'{var}_Area'][
            lu[var] & lu_mask[var]]
        )
        # Then for non res
        intersect_df.loc[nr[var] & all_non_res["NR"], f'{var}_Par_Prop'] = (
                intersect_df[parcel_liv_area][nr[var] & all_non_res["NR"]] / intersect_df[f'{var}_Area'][
            nr[var] & all_non_res["NR"]]
        )
        # Then for all dev
        intersect_df.loc[ad[var] & all_developed["AD"], f'{var}_Par_Prop'] = (
                intersect_df[parcel_liv_area][ad[var] & all_developed["AD"]] / intersect_df[f'{var}_Area'][
            ad[var] & all_developed["AD"]]
        )
        # Then for living area
        intersect_df.loc[lvg_area[var], f'{var}_Par_Prop'] = (
                intersect_df[parcel_liv_area][lvg_area[var]] / intersect_df[f'{var}_Area'][lvg_area[var]]
        )
        # Then for land area
        intersect_df.loc[lnd_area[var], f'{var}_Par_Prop'] = (
                intersect_df[pldaf][lnd_area[var]] / intersect_df[f'{var}_Area'][lnd_area[var]]
        )
        # Now fill NAs with 0 for proportions
        intersect_df[f'{var}_Par_Prop'] = intersect_df[f'{var}_Par_Prop'].fillna(0)

        # Now allocate (note that for pop, we're using the population ratios
        # for all racial subsets)
        if var != "Population":
            intersect_df[f'{var}_PAR'] = intersect_df[f'{var}_Par_Prop'] * intersect_df[var]
        else:
            race_vars = ['Total_Hispanic', 'White_Hispanic', 'Black_Hispanic',
                         'Asian_Hispanic', 'Multi_Hispanic', 'Other_Hispanic',
                         'Total_Non_Hisp', 'White_Non_Hisp', 'Black_Non_Hisp',
                         'Asian_Non_Hisp', 'Multi_Non_Hisp', 'Other_Non_Hisp']
            for rv in race_vars:
                intersect_df[f'{rv}_PAR'] = intersect_df['Population_Par_Prop'] * intersect_df[rv]

    # If what we did worked, all the proportions should sum to 1. This will
    # help us identify if there are any errors
    # v = [f'{var}_Par_Prop' for var in lodes_attrs + ["Population"]]
    # x = intersect_df.groupby([bg_id_field])[v].apply(lambda x: x.sum())
    # x[v].apply(lambda x: [min(x), max(x)])

    # Now we can sum up totals
    logger.log_msg("--- totaling allocated jobs and population")
    intersect_df['Total_Employment'] = (
            intersect_df['CNS01_PAR'] + intersect_df['CNS02_PAR'] + intersect_df['CNS03_PAR'] +
            intersect_df['CNS04_PAR'] + intersect_df['CNS05_PAR'] + intersect_df['CNS06_PAR'] +
            intersect_df['CNS07_PAR'] + intersect_df['CNS08_PAR'] + intersect_df['CNS09_PAR'] +
            intersect_df['CNS10_PAR'] + intersect_df['CNS11_PAR'] + intersect_df['CNS12_PAR'] +
            intersect_df['CNS13_PAR'] + intersect_df['CNS14_PAR'] + intersect_df['CNS15_PAR'] +
            intersect_df['CNS16_PAR'] + intersect_df['CNS17_PAR'] + intersect_df['CNS18_PAR'] +
            intersect_df['CNS19_PAR'] + intersect_df['CNS20_PAR']
    )
    intersect_df['Total_Population'] = (
            intersect_df['Total_Non_Hisp_PAR'] + intersect_df['Total_Hispanic_PAR']
    )

    # Finally, we'll allocate transportation usage
    logger.log_msg("--- allocating commutes")
    # Commutes will be allocated relative to total population, so total by
    # the block group and calculate the parcel share
    tp_props = intersect_df.groupby(bg_id_field)["Total_Population"].sum().reset_index()
    tp_props.columns = [bg_id_field, "TP_Agg"]
    geoid_edit = tp_props[tp_props.TP_Agg == 0][bg_id_field]
    intersect_df = pd.merge(intersect_df, tp_props,
                            how='left',
                            on=bg_id_field)
    intersect_df["TP_Par_Prop"] = intersect_df['Total_Population'] / intersect_df['TP_Agg']
    # If there are any 0s (block groups with 0 population) replace with
    # the population area population, in case commutes are predicted where
    # population isn't
    intersect_df.loc[intersect_df[bg_id_field].isin(geoid_edit), "TP_Par_Prop"] = intersect_df["Population_Par_Prop"][
        intersect_df[bg_id_field].isin(geoid_edit)]
    # Now we can allocate commutes
    transit_vars = ['Drove', 'Carpool', 'Transit',
                    'NonMotor', 'Work_From_Home', 'AllOther']
    for var in transit_vars:
        intersect_df[f'{var}_PAR'] = intersect_df["TP_Par_Prop"] * intersect_df[var]

    # And, now we can sum up totals
    logger.log_msg("--- totaling allocated commutes")
    intersect_df['Total_Commutes'] = (
            intersect_df['Drove_PAR'] + intersect_df['Carpool_PAR'] + intersect_df['Transit_PAR'] +
            intersect_df['NonMotor_PAR'] + intersect_df['Work_From_Home_PAR'] + intersect_df['AllOther_PAR']
    )

    # Now we're ready to write

    # We don't need all the columns we have, so first we define the columns
    # we want and select them from our data. Note that we don't need to
    # maintain the parcels_id_field here, because our save file has been
    # initialized with this already!
    logger.log_msg("--- selecting columns of interest")
    to_keep = [parcels_id,
               parcel_liv_area,
               parcel_lu,
               bg_id_field,
               'Total_Employment',
               'CNS01_PAR', 'CNS02_PAR', 'CNS03_PAR', 'CNS04_PAR',
               'CNS05_PAR', 'CNS06_PAR', 'CNS07_PAR', 'CNS08_PAR',
               'CNS09_PAR', 'CNS10_PAR', 'CNS11_PAR', 'CNS12_PAR',
               'CNS13_PAR', 'CNS14_PAR', 'CNS15_PAR', 'CNS16_PAR',
               'CNS17_PAR', 'CNS18_PAR', 'CNS19_PAR', 'CNS20_PAR',
               'Total_Population',
               'Total_Hispanic_PAR',
               'White_Hispanic_PAR', 'Black_Hispanic_PAR', 'Asian_Hispanic_PAR',
               'Multi_Hispanic_PAR', 'Other_Hispanic_PAR',
               'Total_Non_Hisp_PAR',
               'White_Non_Hisp_PAR', 'Black_Non_Hisp_PAR', 'Asian_Non_Hisp_PAR',
               'Multi_Non_Hisp_PAR', 'Other_Non_Hisp_PAR',
               'Total_Commutes',
               'Drove_PAR', 'Carpool_PAR', 'Transit_PAR',
               'NonMotor_PAR', 'Work_From_Home_PAR', 'AllOther_PAR']
    intersect_df = intersect_df[to_keep]

    # For saving, we join the allocation estimates back to the ID shape we
    # initialized during spatial processing
    logger.log_msg("--- writing table of allocation results")
    sed_path = makePath(out_gdb,
                        "EconDemog_parcels")
    dfToTable(intersect_df, sed_path)
    print("\n")
    # Then we done!
    return sed_path


# MAZ/TAZ data prep helpers
def estimate_maz_from_parcels(par_fc, par_id_field, maz_fc, maz_id_field,
                              taz_id_field, se_data, se_id_field, agg_cols,
                              consolidations):
    """
    Estimate jobs, housing, etc. at the MAZ level based on underlying parcel
    data.

    Parameters
    --- --- --- ---
    par_fc: Path
        Parcel features
    par_id_field: String
        Field identifying each parcel feature
    maz_fc: Path
        MAZ features
    maz_id_field: String
        Field identifying each MAZ feature
    taz_id_field: String
        Field in `maz_fc` that defines which TAZ the MAZ feature is in.
    se_data: Path
        A gdb table containing parcel-level socio-economic/demographic
        estimates.
    se_id_field: String
        Field identifying each parcel in `se_data`
    agg_cols: [PMT.AggColumn, ...]
        Columns to summarize to MAZ level
    consolidations: [PMT.Consolidation, ...]
        Columns to consolidated into a single statistic and then summarize
        to TAZ level.

    Returns
    --- --- --
    DataFrame

    See Also
    --- --- --
    PMT.AggColumn
    PMT.Consolidation
    """
    # intersect
    int_fc = PMT.intersectFeatures(maz_fc, par_fc)
    # Join
    PMT.joinAttributes(int_fc, par_id_field, se_data, se_id_field, "*")
    # Summarize
    gb_cols = [PMT.Column(maz_id_field), PMT.Column(taz_id_field)]
    df = PMT.summarizeAttributes(
        int_fc, gb_cols, agg_cols, consolidations=consolidations)
    return df


# Consolidate MAZ data (for use in areas outside the study area)
# TODO: This could become a more generalized method
def consolidate_cols(df, base_fields, consolidations):
    """
    Use the `PMT.Consolidation` class to combine columns and
    return a clean data frame.

    Parameters
    --- --- --- --
    df: DataFrame
    base_fields: [String, ...]
        Field(s) in `df` that are not subject to consolidation but which
        are to be retained in the returned data frame.
    consolidations: [Consolidation, ...]
        Specifications for output columns that consolidate columns
        found in `df`.

    Returns
    --- --- --
    clean_df: DataFrame
        A new data frame with columns reflecting `base_field` and
        `consolidations`.

    See Also
    --- --- --- -
    PMT.Consolidation
    """
    if isinstance(base_fields, str):
        base_fields = [base_fields]

    clean_cols = base_fields + [c.name for c in consolidations]
    for c in consolidations:
        df[c.name] = df[c.input_cols].agg(c.cons_method, axis=1)

    clean_df = df[clean_cols].copy()
    return clean_df


def patch_local_regional_maz(maz_par_df, maz_par_key, maz_df, maz_key):
    """
    Create a region wide MAZ socioeconomic/demographic data frame based
    on parcel-level and MAZ-level data. Where MAZ features do not overlap
    with parcels, use MAZ-level data.
    Args:
        maz_par_df:
        maz_par_key:
        maz_df:
        maz_key:

    Returns:

    """
    # Create a filter to isolate MAZ features having parcel overlap
    patch_fltr = np.in1d(maz_df[maz_key], maz_par_df[maz_par_key])
    matching_rows = maz_df[patch_fltr].copy()
    # Join non-parcel-level data (school enrollments, e.g.) to rows with
    #  other columns defined by parcel level data, creating a raft of
    #  MAZ features with parcel overlap that have all desired columns
    all_par = maz_par_df.merge(
        matching_rows, how="inner", on=maz_par_key, suffixes=("", "_M"))
    # Drop extraneous columns generated by the join
    drop_cols = [c for c in all_par.columns if c[-2:] == "_M"]
    if drop_cols:
        all_par.drop(columns=drop_cols, inplace=True)
    # MAZ features with no parcel overlap already have all attributes
    #  and can be combined with the `all_par` frame created above
    return pd.concat([all_par, maz_df[~patch_fltr]])


def clean_skim(in_csv, o_field, d_field, imp_fields, out_csv,
               chunksize=100000, rename={}, **kwargs):
    """
    A simple function to read rows from a skim table (csv file), select
    key columns, and save to an ouptut csv file. Keyword arguments can be
    given to set column types, etc.

    Parameters
    -------------
    in_csv: Path
    o_field: String
    d_field: String
    imp_fields: [String, ...]
    out_csv: Path
    chunksize: Integer, default=1000000
    rename: Dict, default={}
        A dictionary to rename columns with keys reflecting existing column
        names and values new column names.
    kwargs:
        Keyword arguments parsed by the pandas `read_csv` method.
    """
    # Manage vars
    if isinstance(imp_fields, string_types):
        imp_fields = [imp_fields]
    # Read chunks
    mode = "w"
    header = True
    usecols = [o_field, d_field] + imp_fields
    for chunk in pd.read_csv(
            in_csv, usecols=usecols, chunksize=chunksize, **kwargs):
        if rename:
            chunk.rename(columns=rename, inplace=True)
        # write output
        chunk.to_csv(out_csv, header=header, mode=mode, index=False)
        header = False
        mode = "a"


def copy_net_result(source_fds, target_fds, fc_names):
    # TODO: Generalize function name and docstring, as this now just copies one or more fcs across fds's
    """
    Since some PMT years use the same OSM network, a solved network analysis
    can be copied from one year to another to avoid redundant processing.

    This function is a helper function called by PMT wrapper functions. It
    is not intended to be run indepenently.

    Parameters
    ------------
    source_fds: Path, FeatureDataset
    target_fds: Path, FeatureDataset
    fc_names: [String, ...]
        The feature class(es) to be copied from an already-solved
        analysis. Provide the names only (not paths).

    Returns
    --------
    None
        Copies network service area feature classes to the target year
        output from a source year using the same OSM data. Any existing
        features in the feature dataset implied by the target year are
        overwritten.
    """
    # Coerce fcs to list
    if isinstance(fc_names, string_types):
        fc_names = [fc_names]

    # Copy feature classes
    print(f"- copying results from {source_fds} to {target_fds}")
    for fc_name in fc_names:
        print(f" - - {fc_name}")
        src_fc = PMT.makePath(source_fds, fc_name)
        tgt_fc = PMT.makePath(target_fds, fc_name)
        if arcpy.Exists(tgt_fc):
            arcpy.Delete_management(tgt_fc)
        arcpy.FeatureClassToFeatureClass_conversion(
            src_fc, target_fds, fc_name)

    # TODO: these may not exist when just copying centrality results
    # for mode in ["walk", "bike"]:
    #     for dest_grp in ["stn", "parks"]:
    #         for run in ["MERGE", "NO_MERGE", "OVERLAP", "NON_OVERLAP"]:
    #             fc_name = f"{mode}_to_{dest_grp}_{run}"


def lines_to_centrality(line_features, impedance_attribute):
    """
    Using the "lines" layer output from an OD matrix problem, calculate
    node centrality statistics and store results in a csv table.

    Parameters
    -----------
    line_features: ODMatrix/Lines feature layer
    impedance_attribute: String
    """
    imp_field = f"Total_{impedance_attribute}"
    # Dump to df
    df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(line_features, ["Name", imp_field]))
    names = ["N", "Node"]
    df[names] = df["Name"].str.split(" - ", n=1, expand=True)
    # Summarize
    sum_df = df.groupby("Node").agg(
        {"N": "size", imp_field: sum}
    ).reset_index()
    # Calculate centrality
    sum_df["centrality"] = (sum_df.N - 1) / sum_df[imp_field]
    # Add average length
    sum_df["AvgLength"] = 1 / sum_df.centrality
    # Add centrality index
    sum_df["CentIdx"] = sum_df.N / sum_df.AvgLength
    return sum_df


def network_centrality(in_nd, in_features, net_loader,
                       name_field="OBJECTID", impedance_attribute="Length",
                       cutoff="1609", restrictions="", chunksize=1000):
    """
    Uses Network Analyst to create and iteratively solve an OD matrix problem
    to assess connectivity among point features.

    The evaluation analyses how many features can reach a given feature and
    what the total and average travel impedances are. Results are reported
    for traveling TO each feature (i.e. features as destinations), which may
    be significant if oneway or similar restrictions are honored.

    in_nd: Path, NetworkDataset
    in_features: Path, Feature Class or Feature Layer
        A point feature class or feature layer that will serve as origins and
        destinations in the OD matrix
    net_loader: NetLoader
        Provide network location loading preferences using a NetLoader
        instance.
    name_field: String, default="OBJECTID"
        A field in `in_features` that identifies each feature. Generally this
        should be a unique value.
    impedance_attribute: String, default="Length"
        The attribute in `in_nd` to use when solving shortest paths among
        features in `in_features`.
    cutoff: String, default="1609"
        A number (as a string) that establishes the search radius for
        evaluating node centrality. Counts and impedances from nodes within
        this threshold are summarized. Units are implied by the
        `impedance_attribute`.
    restrictions: String, default=""
        If `in_nd` includes restriction attributes, provide a
        semi-colon-separated string listing which restrictions to honor
        in solving the OD matrix.
    chunksize: Integer, default=1000
        Destination points from `in_features` are loaded iteratively in chunks
        to manage memory. The `chunksize` determines how many features are
        analyzed simultaneously (more is faster but consumes more memory).

    Returns
    -------
    centrality_df: DataFrame
    """
    results = []
    # Step 1: OD problem
    print("Make OD Problem")
    arcpy.MakeODCostMatrixLayer_na(
        in_network_dataset=in_nd,
        out_network_analysis_layer="OD Cost Matrix",
        impedance_attribute=impedance_attribute,
        default_cutoff=cutoff,
        default_number_destinations_to_find="",
        accumulate_attribute_name="",
        UTurn_policy="ALLOW_UTURNS",
        restriction_attribute_name=restrictions,
        hierarchy="NO_HIERARCHY",
        hierarchy_settings="",
        output_path_shape="NO_LINES",
        time_of_day=""
    )
    # Step 2 - add all origins
    print("Load all origins")
    # in_features = in_features
    arcpy.AddLocations_na(
        in_network_analysis_layer="OD Cost Matrix",
        sub_layer="Origins",
        in_table=in_features,
        field_mappings=f"Name {name_field} #",
        search_tolerance=net_loader.search_tolerance,
        sort_field="",
        search_criteria=net_loader.search_criteria,
        match_type=net_loader.match_type,
        append=net_loader.append,
        snap_to_position_along_network=net_loader.snap,
        snap_offset=net_loader.offset,
        exclude_restricted_elements=net_loader.exclude_restricted,
        search_query=net_loader.search_query
    )
    # Step 3 - iterate through destinations
    print("Iterate destinations and solve")
    # Use origin field maps to expedite loading
    fm = "Name Name #;CurbApproach CurbApproach 0;SourceID SourceID #;SourceOID SourceOID #;PosAlong PosAlong #;SideOfEdge SideOfEdge #"
    dest_src = arcpy.MakeFeatureLayer_management(
        "OD Cost Matrix\Origins", "DEST_SOURCE")
    for chunk in PMT.iterRowsAsChunks(dest_src, chunksize=chunksize):
        # printed dots track progress over chunks
        print(".", end="")
        arcpy.AddLocations_na(
            in_network_analysis_layer="OD Cost Matrix",
            sub_layer="Destinations",
            in_table=chunk,
            field_mappings=fm,
            append="CLEAR"
        )
        # Solve OD Matrix
        arcpy.Solve_na("OD Cost Matrix", "SKIP", "CONTINUE")
        # Dump to df
        line_features = "OD Cost Matrix\Lines"
        temp_df = lines_to_centrality(line_features, impedance_attribute)
        # Stack df results
        results.append(temp_df)
    print(f"All solved ({len(results)} chunks")

    # Delete temp layers
    arcpy.Delete_management(dest_src)
    arcpy.Delete_management("OD Cost Matrix")

    return pd.concat(results, axis=0)


def parcel_walk_time_bin(in_table, bin_field, time_field, code_block):
    """
    Adds a field to create travel time categories in a new `bin_field`
    based on the walk times recorded in a `time_field` and an extended
    if/else `code_block` that defines a simple function `assignBin()`.

    Parameters
    -----------
    in_table: path
    bin_field: String
    time_field: String
    code_block: String
        Defines a python function `assignBin()` with if/else statements
        to group times in `time_field` into bins to be stored as string
        values in `bin_field`.
    """
    arcpy.AddField_management(in_table, bin_field, "TEXT", field_length=20)
    arcpy.CalculateField_management(in_table=in_table, field=bin_field,
                                    expression=f"assignBin(!{time_field}!)",
                                    expression_type="PYTHON3", code_block=code_block)


def parcel_walk_times(parcel_fc, parcel_id_field, ref_fc, ref_name_field,
                      ref_time_field, target_name):
    """
    For features in a parcel feature class, summarize walk times reported
    in a reference features class of service area lines. Generates fields
    recording the nearest reference feature, walk time to the nearest
    reference feature, number of reference features within the service
    area walk time cutoff, and a minimum walk time "category" field.

    Parameters
    -----------
    parcel_fc: Path
        The parcel features to which walk time estimates will be appended.
    parcel_id_field: String
        The field in `parcel_fc` that uniquely identifies each feature.
    ref_fc: Path
        A feature class of line features with travel time estimates from/to
        key features (stations, parks, etc.)
    ref_name_field: String
        A field in `ref_fc` that identifies key features (which station, e.g.)
    ref_time_field: String
        A field in `ref_fc` that reports the time to walk from each line
        feature from/to key features.
    target_name: String
        A string suffix included in output field names.

    Returns
    --------
    walk_time_df: DataFrame
        A data frame with columns storing walk time data:
        `nearest_{target_name}`, `min_time_{target_name}`,
        `n_{target_name}`

    See Also
    ---------
    parcel_ideal_walk_time
    """
    sr = arcpy.Describe(ref_fc).spatialReference
    # Name time fields
    min_time_field = f"min_time_{target_name}"
    nearest_field = f"nearest_{target_name}"
    number_field = f"n_{target_name}"
    # Intersect layers
    print("--- intersecting parcels and network outputs")
    int_fc = "in_memory\\par_wt_sj"
    int_fc = arcpy.SpatialJoin_analysis(parcel_fc, ref_fc, int_fc,
                                        join_operation="JOIN_ONE_TO_MANY",
                                        join_type="KEEP_ALL",
                                        match_option="WITHIN_A_DISTANCE",
                                        search_radius="80 Feet")
    # Summarize
    print(f"--- summarizing by {parcel_id_field}, {ref_name_field}")
    sum_tbl = "in_memory\\par_wt_sj_sum"
    statistics_fields = [[ref_time_field, "MIN"], [ref_time_field, "MEAN"]]
    case_fields = [parcel_id_field, ref_name_field]
    sum_tbl = arcpy.Statistics_analysis(
        int_fc, sum_tbl, statistics_fields, case_fields)
    # Delete intersect features
    arcpy.Delete_management(int_fc)

    # Dump sum table to data frame
    print("--- converting to data frame")
    sum_fields = [f"MEAN_{ref_time_field}"]
    dump_fields = [parcel_id_field, ref_name_field] + sum_fields
    int_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(sum_tbl, dump_fields)
    )
    int_df.columns = [parcel_id_field, ref_name_field, ref_time_field]
    # Delete summary table
    arcpy.Delete_management(sum_tbl)

    # Summarize
    print("--- summarizing times")
    int_df = int_df.set_index(ref_name_field)
    gb = int_df.groupby(parcel_id_field)
    which_name = gb.idxmin()
    min_time = gb.min()
    number = gb.size()

    # Export output table
    print("--- saving walk time results")
    walk_time_df = pd.concat(
        [which_name, min_time, number], axis=1).reset_index()
    renames = [parcel_id_field, nearest_field, min_time_field, number_field]
    walk_time_df.columns = renames
    return walk_time_df


def parcel_ideal_walk_time(parcels_fc, parcel_id_field, target_fc,
                           target_name_field, radius, target_name,
                           overlap_type="HAVE_THEIR_CENTER_IN",
                           sr=None, assumed_mph=3):
    """
    Estimate walk time between parcels and target features (stations, parks,
    e.g.) based on a straight-line distance estimate and an assumed walking
    speed.

    Parameters
    ------------
    parcels_fc: Path
    parcel_id_field: String
        A field that uniquely identifies features in `parcels_fc`
    target_fc: Path
    target_name_field: String
        A field that uniquely identifies features in `target_fc`
    radius: String
        A "linear unit" string for spatial selection ('5280 Feet', e.g.)
    target_name: String
        A string suffix included in output field names.
    overlap_type: String, default="HAVE_THEIR_CENTER_IN"
        A string specifying selection type (see ArcGIS )
    sr: SpatialReference, default=None
        A spatial reference code, string, or object to ensure parcel and
        target features are projected consistently. If `None`, the spatial
        reference from `parcels_fc` is used.
    assumed_mph: numeric, default=3
        The assumed average walk speed expressed in miles per hour.

    Returns
    --------
    walk_time_fc: DataFrame
        A data frame with columns storing ideal walk time data:
        `nearest_{target_name}`, `min_time_{target_name}`,
        `n_{target_name}`
    """
    # output_fields
    nearest_field = f"nearest_{target_name}"
    min_time_field = f"min_time_{target_name}"
    n_field = f"n_{target_name}"
    # Set spatial reference
    if sr is None:
        sr = arcpy.Describe(parcels_fc).spatialReference
    else:
        sr = arcpy.SpatialReference(sr)
    mpu = float(sr.metersPerUnit)
    # Make feature layers
    par_lyr = arcpy.MakeFeatureLayer_management(parcels_fc, "parcels")
    tgt_lyr = arcpy.MakeFeatureLayer_management(target_fc, "target")
    try:
        print("--- estimating ideal times")
        tgt_results = []
        # Iterate over targets
        tgt_fields = [target_name_field, "SHAPE@"]
        par_fields = [parcel_id_field, "SHAPE@X", "SHAPE@Y"]
        out_fields = [parcel_id_field, target_name_field, "minutes"]
        with arcpy.da.SearchCursor(
                tgt_lyr, tgt_fields, spatial_reference=sr) as tgt_c:
            for tgt_r in tgt_c:
                tgt_name, tgt_feature = tgt_r
                tgt_x = tgt_feature.centroid.X
                tgt_y = tgt_feature.centroid.Y
                # select parcels in target buffer
                arcpy.SelectLayerByLocation_management(
                    par_lyr, overlap_type, tgt_feature, search_distance=radius)
                # dump to df
                par_df = pd.DataFrame(
                    arcpy.da.FeatureClassToNumPyArray(
                        par_lyr, par_fields, spatial_reference=sr)
                )
                par_df[target_name_field] = tgt_name
                # estimate distances
                par_df["dx"] = par_df["SHAPE@X"] - tgt_x
                par_df["dy"] = par_df["SHAPE@Y"] - tgt_y
                par_df["meters"] = np.sqrt(par_df.dx ** 2 + par_df.dy ** 2) * mpu
                par_df["minutes"] = (par_df.meters * 60) / (assumed_mph * 1609.344)
                # store in mini df output
                tgt_results.append(par_df[out_fields].copy())
        # Bind up results
        print("--- binding results")
        bind_df = pd.concat(tgt_results).set_index(target_name_field)
        # Group by/summarize
        print("--- summarizing times")
        gb = bind_df.groupby(parcel_id_field)
        par_min = gb.min()
        par_count = gb.size()
        par_nearest = gb["minutes"].idxmin()
        walk_time_df = pd.concat([par_nearest, par_min, par_count], axis=1)
        walk_time_df.columns = [nearest_field, min_time_field, n_field]
        walk_time_df.reset_index(inplace=True)
        return walk_time_df
    except:
        raise
    finally:
        arcpy.Delete_management(par_lyr)
        arcpy.Delete_management(tgt_lyr)


def summarizeAccess(skim_table, o_field, d_field, imped_field,
                    se_data, id_field, act_fields, imped_breaks,
                    units="minutes", join_by="D", chunk_size=100000,
                    **kwargs):
    """
    Reads an origin-destination skim table, joins activity data,
    and summarizes activities by impedance bins.

    Parameters
    -----------
    skim_table: Path
    o_field: String
    d_field: String
    imped_field: String
    se_data: Path
    id_field: String
    act_fields: [String, ...]
    out_table: Path
    out_fc_field: String
    imped_breaks: [Numeric, ...]
    mode: String
    units: String, default="minutes"
    join_by: String, default="D"
    chunk_size: Int, default=100000
    kwargs:
        Keyword arguments for reading the skim table

    Returns
    --------
    out_table: Path
    """
    # Prep vars
    if isinstance(act_fields, string_types):
        act_fields = [act_fields]
    if join_by == "D":
        left_on = d_field
        gb_field = o_field
    elif join_by == "O":
        left_on = o_field
        gb_field = d_field
    else:
        raise ValueError(
            f"Expected 'D' or 'O' as `join_by` value - got {join_by}")
    bin_field = f"BIN_{units}"
    # Read the activity data
    _a_fields_ = [id_field] + act_fields
    act_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(se_data, _a_fields_)
    )

    # Read the skim table
    out_dfs = []
    use_cols = [o_field, d_field, imped_field]
    print("--- --- --- binning skims")
    for chunk in pd.read_csv(
            skim_table, usecols=use_cols, chunksize=chunk_size, **kwargs):
        # Define impedance bins
        low = -np.inf
        criteria = []
        labels = []
        for i_break in imped_breaks:
            crit = np.logical_and(
                chunk[imped_field] >= low,
                chunk[imped_field] < i_break
            )
            criteria.append(crit)
            labels.append(f"{i_break}{units}")
            low = i_break
        # Apply categories
        chunk[bin_field] = np.select(
            criteria, labels, f"{i_break}{units}p"
        )
        labels.append(f"{i_break}{units}p")
        # Join the activity data
        join_df = chunk.merge(
            act_df, how="inner", left_on=left_on, right_on=id_field)
        # Summarize
        sum_fields = [gb_field]
        prod_fields = []
        for act_field in act_fields:
            new_field = f"Wtd{units}{act_field}"
            join_df[new_field] = join_df[imped_field] * join_df[act_field]
            sum_fields += [act_field, new_field]
            prod_fields.append(new_field)
        sum_df = join_df.groupby([gb_field, bin_field]).sum().reset_index()
        out_dfs.append(sum_df)
    # Concatenate all
    out_df = pd.concat(out_dfs)
    # Pivot, summarize, and join
    # - Pivot
    print("--- --- --- bin columns")
    pivot_fields = [gb_field, bin_field] + act_fields
    pivot = pd.pivot_table(
        out_df[pivot_fields], index=gb_field, columns=bin_field)
    pivot.columns = PMT.colMultiIndexToNames(pivot.columns, separator="")
    # - Summarize
    print("--- --- --- average time by activitiy")
    sum_df = out_df[sum_fields].groupby(gb_field).sum()
    avg_fields = []
    for act_field, prod_field in zip(act_fields, prod_fields):
        avg_field = f"Avg{units}{act_field}"
        avg_fields.append(avg_field)
        sum_df[avg_field] = sum_df[prod_field] / sum_df[act_field]
    # - Join
    final_df = pivot.merge(
        sum_df[avg_fields], how="outer", left_index=True, right_index=True)

    return final_df.reset_index()


def genODTable(origin_pts, origin_name_field, dest_pts, dest_name_field,
               in_nd, imped_attr, cutoff, net_loader, out_table,
               restrictions=None, use_hierarchy=False, uturns="ALLOW_UTURNS",
               o_location_fields=None, d_location_fields=None,
               o_chunk_size=None):
    """
    Creates and solves an OD Matrix problem for a collection of origin and
    destination points using a specified network dataset. Results are
    exported as a csv file.

    Parameters
    ----------
    origin_pts: Path
    origin_name_field: String
    dest_pts: Path
    dest_name_field: String
    in_nd: Path
    imped_attr: String
    cutoff: numeric
    net_loader: NetLoader
    out_table: Path
    restrictions: [String, ...], default=None
    use_hierarchy: Boolean, default=False
    uturns: String, default="ALLOW_UTURNS"
    o_location_fields: [String, ...], default=None
    d_location_fields: [String, ...], default=None
    o_chunk_size: Integer, default=None
    """
    if use_hierarchy:
        hierarchy = "USE_HIERARCHY"
    else:
        hierarchy = "NO_HIERARCHY"
    # accum = _listAccumulationAttributes(in_nd, imped_attr)

    print("--- ---OD MATRIX: create network problem")
    net_layer = arcpy.MakeODCostMatrixLayer_na(
        # Setup
        in_network_dataset=in_nd,
        out_network_analysis_layer="__od__",
        impedance_attribute=imped_attr,
        default_cutoff=cutoff,
        accumulate_attribute_name=None,
        UTurn_policy=uturns,
        restriction_attribute_name=restrictions,
        output_path_shape="NO_LINES",
        hierarchy=hierarchy,
        time_of_day=None
    )
    net_layer_ = net_layer.getOutput(0)

    try:
        PMT._loadLocations(net_layer_, "Destinations", dest_pts, dest_name_field,
                           net_loader, d_location_fields)
        # Iterate solves as needed
        if o_chunk_size is None:
            o_chunk_size = arcpy.GetCount_management(origin_pts)[0]
        write_mode = "w"
        header = True
        for o_pts in PMT.iterRowsAsChunks(origin_pts, chunksize=o_chunk_size):
            # TODO: update printing: too many messages when iterating
            PMT._loadLocations(net_layer_, "Origins", o_pts, origin_name_field,
                               net_loader, o_location_fields)
            s = PMT._solve(net_layer_)
            print("--- --- solved, dumping to data frame")
            # Get output as a data frame
            sublayer_names = arcpy.na.GetNAClassNames(net_layer_)
            extend_lyr_name = sublayer_names["ODLines"]
            try:
                extend_sublayer = net_layer_.listLayers(extend_lyr_name)[0]
            except:
                extend_sublayer = arcpy.mapping.ListLayers(
                    net_layer, extend_lyr_name)[0]
            out_fields = ["Name", f"Total_{imped_attr}"]
            columns = ["Name", imped_attr]
            # out_fields += [f"Total_{attr}" for attr in accum]
            # columns += [c for c in accum]
            df = pd.DataFrame(
                arcpy.da.TableToNumPyArray(extend_sublayer, out_fields)
            )
            df.columns = columns
            # Split outputs
            if len(df) > 0:
                names = ["OName", "DName"]
                df[names] = df["Name"].str.split(" - ", n=1, expand=True)

                # Save
                df.to_csv(
                    out_table, index=False, mode=write_mode, header=header)

                # Update writing params
                write_mode = "a"
                header = False
    except:
        raise
    finally:
        print("--- ---deleting network problem")
        arcpy.Delete_management(net_layer)


def taz_travel_stats(skim_table, trip_table, o_field, d_field, dist_field, ):
    """


    Parameters
    -----------

    Returns
    --------
    taz_stats_df: DataFrame
    """

    # Read skims, trip tables
    skims = pd.read_csv()

    # Summarize trips, total trip mileage by TAZ
    #

    return taz_stats_df


# TODO: verify functions generally return python objects (dataframes, e.g.) and leave file writes to `preparer.py`
# TODO: verify functions generally return python objects (dataframes, e.g.) and leave file writes to `preparer.py`
def generate_chunking_fishnet(template_fc, out_fishnet_name, chunks=20):
    """ generates a fishnet feature class that minimizes the rows and columns based on
        number of chunks and template_fc proportions
    Args:
        template_fc (str): path to template feature class
        out_fishnet_name (str): name of output file
        chunks (int): number of chunks to be used for processing
    Returns:
        quadrat_fc (str): path to fishnet generated for chunking
    """
    desc = arcpy.Describe(template_fc)
    ext = desc.extent
    xmin, xmax, ymin, ymax = (ext.XMin, ext.XMax, ext.YMin, ext.YMax)
    hw_ratio = (ymax - ymin) / (xmax - xmin)
    candidate_orientations = [[i, chunks // i]
                              for i in range(1, chunks + 1)
                              if chunks % i == 0]

    orientation_matching = np.argmin([abs(orientation[0] / orientation[1] - hw_ratio)
                                      for orientation in candidate_orientations])
    orientation = candidate_orientations[orientation_matching]
    quadrat_nrows = orientation[0]
    quadrat_ncols = orientation[1]

    # With the extent information and rows/columns, we can create our quadrats
    # by creating a fishnet over the parcels
    quadrat_origin = ' '.join([str(xmin), str(ymin)])
    quadrat_ycoord = ' '.join([str(xmin), str(ymin + 10)])
    quadrat_corner = ' '.join([str(xmax), str(ymax)])
    quadrats_fc = make_inmem_path(file_name=out_fishnet_name)
    arcpy.CreateFishnet_management(out_feature_class=quadrats_fc, origin_coord=quadrat_origin,
                                   y_axis_coord=quadrat_ycoord, number_rows=quadrat_nrows, number_columns=quadrat_ncols,
                                   corner_coord=quadrat_corner, template=ext, geometry_type="POLYGON")
    return quadrats_fc


def symmetric_difference(in_fc, diff_fc, out_fc_name):
    # TODO: add capability to define output location instead of in_memory space
    """if Advanced arcpy license not available this will calculate the
        symmetrical difference of two sets of polygons
    Args:
        in_fc:
        diff_fc:
        out_fc_name:
    Returns:
        out_fc (str): path to output file in in_memory space
    """
    _, diff_name = os.path.split(os.path.splitext(diff_fc)[0])
    # union datasets
    out_fc = make_inmem_path(file_name=out_fc_name)
    arcpy.Union_analysis(in_features=[in_fc, diff_fc], out_feature_class=out_fc)
    # select features from the diff fc uisng the -1 FID code to ID
    where = arcpy.AddFieldDelimiters(diff_fc, f"FID_{diff_name}") + " <> -1"
    temp = arcpy.MakeFeatureLayer_management(in_features=out_fc, out_layer="_temp_", where_clause=where)
    if int(arcpy.GetCount_management(temp)[0]) > 0:
        arcpy.DeleteRows_management(temp)
    return out_fc


def validate_weights(weights):
    if type(weights) == str:
        weights = weights.lower()
        if weights == "rook":
            return dict({"top_left": 0, "top_center": 1, "top_right": 0,
                         "middle_left": 1, "self": 1, "middle_right": 1,
                         "bottom_left": 0, "bottom_center": 1, "bottom_right": 0})
        elif weights == "queen":
            return dict({"top_left": 1, "top_center": 2, "top_right": 1,
                         "middle_left": 2, "self": 1, "middle_right": 2,
                         "bottom_left": 1, "bottom_center": 2, "bottom_right": 1})
        elif weights == "nn":
            return dict({"top_left": 1, "top_center": 1, "top_right": 1,
                         "middle_left": 1, "self": 1, "middle_right": 1,
                         "bottom_left": 1, "bottom_center": 1, "bottom_right": 1})
        else:
            raise ValueError("Invalid string specification for 'weights'; "
                             "'weights' can only take 'rook', 'queen', or 'nn' as a string\n")
    elif type(weights) == dict:
        k = weights.keys()
        missing = list({"top_left", "top_center", "top_right",
                        "middle_left", "self", "middle_right",
                        "bottom_left", "bottom_center", "bottom_right"} - set(k))
        if len(missing) != 0:
            raise ValueError(f'Necessary keys missing from "weights"; '
                             f'missing keys include: '
                             f'{", ".join([str(m) for m in missing])}')
    else:
        raise ValueError(''.join(["'weights' must be a string or dictionary; ",
                                  "if string, it must be 'rook', 'queen', or 'nn', and "
                                  "if dictionary, it must have keys 'top_left','top_center','top_right',"
                                  "'middle_left','self','middle_right','bottom_left','bottom_center',"
                                  "'bottom_right'\n"]))


def contiguity_index(quadrats_fc, parcels_fc, buildings_fc, parcels_id_field,
                     cell_size=40, weights="nn"):
    """calculate contiguity of developable area
    Args:
        quadrats_fc (str): path to fishnet of chunks for processing
        parcels_copy (str): path to parcel polygons; contiguity will be calculated relative to this
        buildings_fc (str): path to building polygons;
        parcels_id_field (str): name of parcel primary key field
        cell_size (int): cell size for raster over which contiguity will be calculated.
            (in the units of the input data crs) Default 40 (works for PMT)
        weights (str or dict): weights for neighbors in contiguity calculation. (see notes for how to specify weights)
            Default "nn", all neighbors carry the same weight, regardless of orientation
    Returns:
        pandas dataframe; table of polygon-level (sub-parcel) contiguity indices
    Notes:
        Weights can be provided in one of two ways:
            1. one of three defaults: "rook", "queen", or "nn".
            "rook" weights give all horizontal/vertical neighbors a weight of 1,
                and all diagonal neighbors a weight of 0
            "queen" weights give all horizontal/vertical neighbors a weight of 2,
                and all diagonal neighbors a weight of 1
            "nn" (nearest neighbor) weights give all neighbors a weight of 1,
                regardless of orientation
            For developable area, "nn" makes the most sense to describe contiguity,
                and thus is the recommended option for weights in this function

            2. a dictionary of weights for each of 9 possible neighbors. This dictionary must have the keys
                ["top_left", "top_center", "top_right",
                "middle_left", "self", "middle_right",
                "bottom_left", "bottom_center", "bottom_right"].
                If providing weights as a dictionary, a good strategy is to set "self"=1,
                and then set other weights according to a perceived relative importance to the cell itself.
                It is recommended, however, to use one of the default weighting options; the dictionary
                option should only be used in rare cases.
    Raises:
        ValueError:
            if weights are an invalid string or a dictionary with invalid keys (see Notes)
    """

    # Weights setup
    logger.log_msg("--- checking weights")
    weights = validate_weights(weights)

    logger.log_msg("--- --- Creating temporary processing workspace")
    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(out_folder_path=temp_dir, out_name="Intermediates.gdb")
    intmd_gdb = makePath(temp_dir, "Intermediates.gdb")

    logger.log_msg("--- --- copying the parcels feature class to avoid overwriting")
    fmap = arcpy.FieldMappings()
    fmap.addTable(parcels_fc)
    fields = {f.name: f for f in arcpy.ListFields(parcels_fc)}
    for fname, fld in fields.items():
        if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
            if fname != parcels_id_field:
                fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
    parcels_copy = makePath(intmd_gdb, "parcels")
    p_path, p_name = os.path.split(parcels_copy)
    arcpy.FeatureClassToFeatureClass_conversion(in_features=parcels_fc, out_path=p_path,
                                                out_name=p_name, field_mapping=fmap)

    logger.log_msg("--- --- tagging parcels with a chunk ID")
    arcpy.AddField_management(in_table=parcels_copy, field_name="ChunkID", field_type="LONG")
    p_layer = arcpy.MakeFeatureLayer_management(in_features=parcels_copy, out_layer="_p_layer")
    chunks = []
    with arcpy.da.SearchCursor(quadrats_fc, ["OID@", "SHAPE@"]) as search:
        for srow in search:
            chunk_id, geom = srow
            arcpy.SelectLayerByLocation_management(in_layer=p_layer, overlap_type="HAVE_THEIR_CENTER_IN",
                                                   select_features=geom)
            arcpy.CalculateField_management(in_table=p_layer, field="ChunkID", expression=chunk_id)
            chunks.append(chunk_id)
    # difference parcels and buildings
    logger.log_msg("--- --- differencing parcels and buildings")
    difference_fc = symmetric_difference(in_fc=parcels_copy, diff_fc=buildings_fc, out_fc_name="difference")

    logger.log_msg("--- --- converting difference to singlepart polygons")
    diff_fc = make_inmem_path(file_name="diff")
    arcpy.MultipartToSinglepart_management(in_features=difference_fc, out_feature_class=diff_fc)

    logger.log_msg("--- --- adding a unique ID field for individual polygons")
    PMT.add_unique_id(feature_class=diff_fc, new_id_field="PolyID")

    logger.log_msg("--- --- extracting a polygon-parcel ID reference table")
    ref_df = featureclass_to_df(in_fc=diff_fc, keep_fields=[parcels_id_field, "PolyID"], null_val=-1.0)
    arcpy.Delete_management(difference_fc)

    # loop through the chunks to calculate contiguity:
    logger.log_msg("--- chunk processing contiguity and developable area")
    contiguity_stack = []
    diff_lyr = arcpy.MakeFeatureLayer_management(in_features=diff_fc, out_layer="_diff_lyr_")
    for i in chunks:
        logger.log_msg(f"--- --- chunk {str(i)} of {str(len(chunks))}")

        logger.log_msg("--- --- --- selecting chunk")
        selection = f'"ChunkID" = {str(i)}'
        parcel_chunk = arcpy.SelectLayerByAttribute_management(in_layer_or_view=diff_lyr,
                                                               selection_type="NEW_SELECTION",
                                                               where_clause=selection)

        logger.log_msg("--- --- --- rasterizing chunk")
        rp = make_inmem_path()
        arcpy.FeatureToRaster_conversion(in_features=parcel_chunk, field="PolyID",
                                         out_raster=rp, cell_size=cell_size)

        logger.log_msg("--- --- --- loading chunk raster")
        ras_array = arcpy.RasterToNumPyArray(in_raster=rp, nodata_to_value=-1)
        arcpy.Delete_management(rp)

        # calculate total developable area
        logger.log_msg("--- --- --- calculating developable area by polygon")
        poly_ids, counts = np.unique(ras_array, return_counts=True)
        area = pd.DataFrame.from_dict({"PolyID": poly_ids, "Count": counts})
        area = area[area.PolyID != -1]
        area["Developable_Area"] = area.Count * (cell_size ** 2) / 43560
        # ASSUMES FEET IS THE INPUT CRS, MIGHT WANT TO MAKE THIS AN
        # ACTUAL CONVERSION IF WE USE THIS OUTSIDE OF PMT. SEE THE
        # LINEAR UNITS CODE/NAME BOOKMARKS
        # spatial_reference.linearUnitName and .linearUnitCode
        area = area.drop(columns="Count")


        npolys = len(area.index)
        if npolys == 0:
            logger.log_msg("*** no polygons in this quadrat, proceeding to next chunk ***")
        else:
            logger.log_msg("--- --- --- initializing cell neighbor identification")
            ras_dim = ras_array.shape
            nrow = ras_dim[0]
            ncol = ras_dim[1]

            id_tab_self = pd.DataFrame({"Row": np.repeat(np.arange(nrow), ncol),
                                        "Col": np.tile(np.arange(ncol), nrow),
                                        "ID": ras_array.flatten()})
            id_tab_neighbor = pd.DataFrame({"NRow": np.repeat(np.arange(nrow), ncol),
                                            "NCol": np.tile(np.arange(ncol), nrow),
                                            "NID": ras_array.flatten()})

            logger.log_msg("--- --- --- identifying non-empty cells")
            row_oi = id_tab_self[id_tab_self.ID != -1].Row.to_list()
            col_oi = id_tab_self[id_tab_self.ID != -1].Col.to_list()

            logger.log_msg("--- --- --- identifying neighbors of non-empty cells")
            row_basic = [np.arange(x - 1, x + 2) for x in row_oi]
            col_basic = [np.arange(x - 1, x + 2) for x in col_oi]

            meshed = [np.array(np.meshgrid(x, y)).reshape(2, 9).T
                      for x, y in zip(row_basic, col_basic)]
            meshed = np.concatenate(meshed, axis=0)
            meshed = pd.DataFrame(meshed, columns=["NRow", "NCol"])

            meshed.insert(1, "Col", np.repeat(col_oi, 9))
            meshed.insert(0, "Row", np.repeat(row_oi, 9))

            logger.log_msg("--- --- --- filtering to valid neighbors by index")
            meshed = meshed[(meshed.NRow >= 0) & (meshed.NRow < nrow)
                            & (meshed.NCol >= 0) & (meshed.NCol < ncol)]

            logger.log_msg("--- --- --- tagging cells and their neighbors with polygon IDs")
            meshed = pd.merge(meshed, id_tab_self,
                              left_on=["Row", "Col"], right_on=["Row", "Col"], how="left")
            meshed = pd.merge(meshed, id_tab_neighbor,
                              left_on=["NRow", "NCol"], right_on=["NRow", "NCol"], how="left")

            logger.log_msg("--- --- --- fitering to valid neighbors by ID")
            meshed = meshed[meshed.ID == meshed.NID]
            meshed = meshed.drop(columns="NID")

            # With neighbors identified, we now need to define cell weights for contiguity calculations.
            # These are based off the specifications in the 'weights' inputs to the function. So, we tag each
            # cell-neighbor pair in 'valid_neighbors' with a weight.
            logger.log_msg("--- --- --- tagging cells and neighbors with weights")
            conditions = [(np.logical_and(meshed["NRow"] == meshed["Row"] - 1, meshed["NCol"] == meshed["Col"] - 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] - 1, meshed["NCol"] == meshed["Col"])),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] - 1, meshed["NCol"] == meshed["Col"] + 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"], meshed["NCol"] == meshed["Col"] - 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"], meshed["NCol"] == meshed["Col"])),
                          (np.logical_and(meshed["NRow"] == meshed["Row"], meshed["NCol"] == meshed["Col"] + 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] + 1, meshed["NCol"] == meshed["Col"] - 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] + 1, meshed["NCol"] == meshed["Col"])),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] + 1, meshed["NCol"] == meshed["Col"] + 1))]
            choices = ["top_left", "top_center", "top_right", "middle_left", "self", "middle_right", "bottom_left",
                       "bottom_center", "bottom_right"]
            meshed["Type"] = np.select(conditions, choices)
            meshed["Weight"] = [weights[key] for key in meshed["Type"]]

            # To initialize the contiguity calculation, we sum weights by cell.
            # We lose the ID in the groupby though, which we need to get to contiguity,
            # so we need to merge back to our cell-ID table
            logger.log_msg("--- --- --- summing weight by cell")
            weight_tbl = meshed.groupby(["Row", "Col"])[["Weight"]].agg("sum").reset_index()
            weight_tbl = pd.merge(weight_tbl, id_tab_self, left_on=["Row", "Col"], right_on=["Row", "Col"], how="left")

            # We are now finally at the point of calculating contiguity! It's a pretty simple function,
            # which we apply over our IDs. This is the final result of our chunk process, so we'll also rename our "ID"
            # field to "PolyID", because this is the proper name for the ID over which we've calculated contiguity.
            # This will make our life easier when chunk processing is complete, and we move into data formatting
            # and writing
            logger.log_msg("--- --- --- calculating contiguity by polygon")
            weight_max = sum(weights.values())
            contiguity = weight_tbl.groupby("ID").apply(
                lambda x: (sum(x.Weight) / len(x.Weight) - 1) / (weight_max - 1)).reset_index(name="Contiguity")
            contiguity.columns = ["PolyID", "Contiguity"]

            # For reporting results, we'll merge the contiguity and developable
            # area tables
            logger.log_msg("--- --- --- merging contiguity and developable area information")
            contiguity = pd.merge(contiguity, area, left_on="PolyID", right_on="PolyID", how="left")

            # We're done chunk processing -- we'll put the resulting data frame
            # in our chunk results list as a final step
            logger.log_msg("--- --- --- appending chunk results to master list")
            contiguity_stack.append(contiguity)

    # Contiguity results formatting
    logger.log_msg("--- formatting polygon-level results")
    logger.log_msg("--- --- combining chunked results into table format")
    contiguity_df = pd.concat(contiguity_stack, axis=0)

    logger.log_msg("--- --- filling table with missing polygons")
    contiguity_df = pd.merge(ref_df, contiguity_df, left_on="PolyID", right_on="PolyID", how="left")

    logger.log_msg("--- overwriting missing values with 0")
    contiguity_df = contiguity_df.fillna(value={"Contiguity": 0, "Developable_Area": 0})

    # clean up in_memory space and temporary data
    arcpy.Delete_management(parcels_copy)
    arcpy.Delete_management(intmd_gdb)
    arcpy.Delete_management(diff_fc)
    return contiguity_df


def contiguity_index_dep(parcels_fc, buildings_fc, parcels_id_field,
                         chunks=20, cell_size=40, weights="nn"):
    """calculate contiguity of developable area
    Args:
        parcels_fc: String; path to parcel polygons; contiguity will be calculated relative to this
        buildings_fc: String; path to building polygons;
        parcels_id_field: String; name of a field used to identify the parcels in the future summarized
            parcel results
        chunks: int;  number of chunks in which you want to process contiguity. necessary because of memory issues
            with rasterization of large feature classes
        cell_size: int; cell size for raster over which contiguity will be calculated.
            (in the units of the input data crs) Default 40 (works for PMT)
        weights: str or dict;  weights for neighbors in contiguity calculation. (see notes for how to specify weights)
            Default "nn", all neighbors carry the same weight, regardless of orientation

    Returns:
        pandas dataframe; table of polygon-level (sub-parcel) contiguity indices

    Notes:
        Weights can be provided in one of two ways:
            1. one of three defaults: "rook", "queen", or "nn".
            "rook" weights give all horizontal/vertical neighbors a weight of 1,
                and all diagonal neighbors a weight of 0
            "queen" weights give all horizontal/vertical neighbors a weight of 2,
                and all diagonal neighbors a weight of 1
            "nn" (nearest neighbor) weights give all neighbors a weight of 1,
                regardless of orientation
            For developable area, "nn" makes the most sense to describe contiguity,
                and thus is the recommended option for weights in this function

            2. a dictionary of weights for each of 9 possible neighbors. This
            dictionary must have the keys ["top_left", "top_center", "top_right",
            "middle_left", "self", "middle_right", "bottom_left", "bottom_center",
            "bottom_right"]. If providing weights as a dictionary, a good strategy
            is to set "self"=1, and then set other weights according to a
            perceived relative importance to the cell itself. It is recommended,
            however, to use one of the default weighting options; the dictionary
            option should only be used in rare cases.

    Raises:
        ValueError:
            if weights are an invalid string or a dictionary with invalid keys (see Notes)
    """

    # Weights setup
    # -------------

    logger.log_msg("--- checking weights")

    # Before anything else, we need to make sure the weights are set up
    # properly; if not, we need to kill the function. We'll do that through
    # a series of logicals

    if type(weights) == str:
        weights = weights.lower()
        if weights == "rook":
            weights = dict({"top_left": 0, "top_center": 1, "top_right": 0,
                            "middle_left": 1, "self": 1, "middle_right": 1,
                            "bottom_left": 0, "bottom_center": 1, "bottom_right": 0})
        elif weights == "queen":
            weights = dict({"top_left": 1, "top_center": 2, "top_right": 1,
                            "middle_left": 2, "self": 1, "middle_right": 2,
                            "bottom_left": 1, "bottom_center": 2, "bottom_right": 1})
        elif weights == "nn":
            weights = dict({"top_left": 1, "top_center": 1, "top_right": 1,
                            "middle_left": 1, "self": 1, "middle_right": 1,
                            "bottom_left": 1, "bottom_center": 1, "bottom_right": 1})
        else:
            raise ValueError(''.join(["Invalid string specification for 'weights'; ",
                                      "'weights' can only take 'rook', 'queen', or 'nn' as a string\n"]))
    elif type(weights) == dict:
        k = weights.keys()
        missing = list({"top_left", "top_center", "top_right",
                        "middle_left", "self", "middle_right",
                        "bottom_left", "bottom_center", "bottom_right"} - set(k))
        if len(missing) != 0:
            raise ValueError(''.join(["Necessary keys missing from 'weights'; ",
                                      "missing keys include: ",
                                      ', '.join([str(m) for m in missing]),
                                      "\n"]))
    else:
        raise ValueError(''.join(["'weights' must be a string or dictionary; ",
                                  "if string, it must be 'rook', 'queen', or 'nn', and "
                                  "if dictionary, it must have keys 'top_left','top_center','top_right',"
                                  "'middle_left','self','middle_right','bottom_left','bottom_center',"
                                  "'bottom_right'\n"]))

    # After this, we can be confident that our weights are properly formatted
    # for how we plan to use them in contiguity

    # Chunking setup
    # --------------

    logger.log_msg("--- set up for chunk processing of contiguity")

    # Before anything, recognizing this is going to create a LOT of data,
    # we need to set up a location for intermediate files.
    logger.log_msg("--- --- setting up an intermediates gdb")

    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(out_folder_path=temp_dir,
                                   out_name="Intermediates.gdb")
    intmd_gdb = makePath(temp_dir, "Intermediates.gdb")

    # First, we're going to create our quadrats for chunking. To do this,
    # we need to start with the extent of our parcels
    logger.log_msg("--- --- extracting parcels extent")
    desc = arcpy.Describe(parcels_fc)
    parcels_extent = desc.extent

    # Next, we find the ratio of dimensions for our parcels. This will inform
    # how our quadrats get structured -- we'll pick the orientation that most
    # closely matches our height/width ratio
    logger.log_msg("--- --- determining parcels dimension ratio")
    xmin = parcels_extent.XMin
    xmax = parcels_extent.XMax
    ymin = parcels_extent.YMin
    ymax = parcels_extent.YMax
    hw_ratio = (ymax - ymin) / (xmax - xmin)

    # Now, we define out the orientation of our quadrats by identifying the
    # one that is closest to 'hw_ratio'. This gives us the number of rows
    # and columns for our quadrats
    logger.log_msg("--- --- defining row/column orientation for quadrats")
    candidate_ontns = [[i, chunks // i]
                       for i in range(1, chunks + 1)
                       if chunks % i == 0]

    ontn_matching = [abs(o[0] / o[1] - hw_ratio) for o in candidate_ontns]
    orientation = candidate_ontns[np.argmin(ontn_matching)]
    quadrat_nrows = orientation[0]
    quadrat_ncols = orientation[1]

    # With the extent information and rows/columns, we can create our quadrats
    # by creating a fishnet over the parcels
    logger.log_msg("--- --- creating quadrats")
    quadrat_origin = ' '.join([str(xmin), str(ymin)])
    quadrat_ycoord = ' '.join([str(xmin), str(ymin + 10)])
    quadrat_corner = ' '.join([str(xmax), str(ymax)])
    quadrats_fc = make_inmem_path(file_name="quadrats")
    arcpy.CreateFishnet_management(out_feature_class=quadrats_fc, origin_coord=quadrat_origin,
                                   y_axis_coord=quadrat_ycoord, number_rows=quadrat_nrows, number_columns=quadrat_ncols,
                                   corner_coord=quadrat_corner, template=parcels_extent, geometry_type="POLYGON")

    # The next step is identifying the quadrat in which each parcel falls.
    # This will give us a "chunk ID", which we can use to process the parcels
    # in chunks. We'll identify quadrat ownership using parcel centroids
    # because a point-polygon intersection will be a lot less expensive, but
    # ultimately we want to merge back to parcel polygons. To do this, we'll
    # need to set up a unique ID field in the polygons that we can carry over
    # to the centroids, then use to merge chunk IDs back to the polygons.

    # We start by extracting parcel centroids, maintaining the ID field for a
    # future merge back to the polygons
    # logger.log_msg("--- --- extracting parcel centroids")
    # parcel_fields = [parcels_id_field, "SHAPE@X", "SHAPE@Y"]
    # centroids_fc = makePath(intmd_gdb, "centroids")
    # PMT.polygonsToPoints(in_fc=parcels_path, out_fc=centroids_fc, fields=parcel_fields,
    #                      skip_nulls=False, null_value=-1)

    # Next, we intersect the parcels centroids with the quadrats to
    # identify quadrat ownership -- the parcels will be enriched with the
    # quadrat FIDs, which can be used for chunk identification. We're now
    # done with the quadrats and centroids, so we can delete them
    logger.log_msg("--- --- identifying parcel membership in quadrats")
    intersect_fc = intersectFeatures(summary_fc=quadrats_fc, disag_fc=parcels_fc, disag_fields=parcels_id_field)
    # arcpy.Intersect_analysis(in_features=[centroids_fc, quadrats_fc], out_feature_class=intersect_fc)
    # arcpy.Delete_management(quadrats_fc)
    # arcpy.Delete_management(centroids_fc)

    # Now that we've identified membership, we pull the parcels ID and the
    # chunk ID (stored as "FID_quadrats" by default from the create fishnet
    # function), and merge back to the parcel polygons to give us the
    # necessary chunk attribution. We'll rename "FID_quadrats" to
    # "ChunkID" for legibility. Also, we're now done with the intersect,
    # so we can delete it

    # First, we'll need a copy of the original parcels (this way we don't
    # have to modify the parcels as they are and potentially mess stuff up)
    logger.log_msg("--- --- copying the parcels feature class to avoid overwriting")
    fmap = arcpy.FieldMappings()
    fmap.addTable(parcels_fc)
    fields = {f.name: f for f in arcpy.ListFields(parcels_fc)}
    for fname, fld in fields.items():
        if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
            if fname != parcels_id_field:
                fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
    arcpy.FeatureClassToFeatureClass_conversion(in_features=parcels_fc, out_path=intmd_gdb, out_name="parcels",
                                                field_mapping=fmap)
    parcels_fc = makePath(intmd_gdb, "parcels")
    # Now we add the chunk ID to the copied parcels (see the above comment
    # chunk for additional information)
    logger.log_msg("--- --- tagging parcels with a chunk ID")
    itsn_fields = [parcels_id_field, "FID__quadrats"]
    itsn_array = arcpy.da.FeatureClassToNumPyArray(in_table=intersect_fc, field_names=itsn_fields, null_value=-1)
    itsn_array.dtype.names = (parcels_id_field, "ChunkID")
    arcpy.da.ExtendTable(in_table=parcels_fc, table_match_field=parcels_id_field,
                         in_array=itsn_array, array_match_field=parcels_id_field)

    # This completes our chunking setup -- next, we need to take our chunked parcels and difference
    # them with buildings to define developable area

    # Differencing buildings and parcels
    # Contiguity is assessed in terms of parcel area that is not already developed.
    # To do this, we'll need a spatial difference of parcel polygons and building polygons.
    # First, we process the difference
    logger.log_msg("--- --- differencing parcels and buildings")
    difference_fc = symmetric_difference(in_fc=parcels_fc, diff_fc=buildings_fc, out_fc_name="difference")

    # _, buildings_name = os.path.split(buildings_fc)
    # arcpy.SymDiff_analysis(in_features=parcels_fc, update_features=buildings_fc, out_feature_class=difference_fc)

    # union_fc = makePath(intmd_gdb, "union")
    # arcpy.Union_analysis(in_features=[parcels_fc, buildings_fc], out_feature_class=union_fc)
    # where = arcpy.AddFieldDelimiters(buildings_fc, f"FID_{buildings_name}") + "<> -1"
    # difference = arcpy.SelectLayerByAttribute_management(in_layer_or_view=union_fc, selection_type="NEW_SELECTION",
    #                                                      where_clause=where)
    # difference_fc = makePath(intmd_gdb, "difference")
    # arcpy.CopyFeatures_management(in_features=difference, out_feature_class=difference_fc)
    # arcpy.Delete_management(union_fc)
    # arcpy.Delete_management(difference)

    # When we completed the differencing, we may have split some parcels
    # into 2. This is a problem for reporting, because contiguity of
    # developable area is the relevant for singlepart polygons only. For a
    # multipart result, we'd want to calculate contiguity in each part,
    # and then use a summary function to get a contiguity for the parcel
    # as a whole. So, we need to split the difference result into single
    # part polygons
    logger.log_msg("--- --- converting difference to singlepart polygons")
    difference_sp_fc = os.path.join(intmd_gdb, "difference_sp")
    arcpy.MultipartToSinglepart_management(in_features=difference_fc, out_feature_class=difference_sp_fc)
    arcpy.Delete_management(difference_fc)

    # Now, we want an ID to identify each unique polygon, as well be
    # calculating contiguity on a polygon basis. We can create this variable
    # using the same methods as the ProcessID, but we'll call it "PolyID"
    logger.log_msg("--- --- adding a unique ID field for individual polygons")
    PMT.add_unique_id(feature_class=difference_sp_fc, new_id_field="PolyID")

    # Finally, we can delete every field from 'difference_sp' except
    # ProcessID, PolyID, and ChunkID -- we do this because we're going to
    # be eating a LOT of memory in our contiguity calculations, so every
    # bit counts!
    # Thanks to: https://gis.stackexchange.com/questions/229187/copying-only-certain-fields-columns-from-shapefile-into-new-shapefile-using-mode
    logger.log_msg("--- --- formatting the singlepart difference")
    fkeep = [parcels_id_field, "PolyID", "ChunkID"]
    fmap = arcpy.FieldMappings()
    fmap.addTable(difference_sp_fc)
    fields = {f.name: f for f in arcpy.ListFields(difference_sp_fc)}
    for fname, fld in fields.items():
        if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
            if fname not in fkeep:
                fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
    arcpy.FeatureClassToFeatureClass_conversion(in_features=difference_sp_fc, out_path=intmd_gdb,
                                                out_name="diff", field_mapping=fmap)
    arcpy.Delete_management(difference_sp_fc)
    diff_fc = makePath(intmd_gdb, "diff")

    # To match contiguity back to our polygons, we'll use the relationship
    # between PolyID and ParcelID. So, we'll create a table of PolyID and
    # ProcessID to merge our contiguity results to. Once we have our results,
    # we can summarize over ProcessID and merge back to the parcel polygons
    logger.log_msg("--- --- extracting a polygon-parcel ID reference table")
    ref_df = pd.DataFrame(
        arcpy.da.FeatureClassToNumPyArray(
            in_table=diff_fc,
            field_names=[parcels_id_field, "PolyID"],
            null_value=-1
        )
    )

    # This completes our differencing -- we now are ready to calculate
    # contiguity, which we will do on "diff" relative to "PolyID". But,
    # because we want to take care of as much spatial processing in this
    # function as possible, we'll initialize a save feature class for the
    # future summarized results first

    # Now, we're good to move into the meat of this function: calculating
    # contiguity (and developable area)

    # Chunk processing of contiguity
    # ------------------------------

    logger.log_msg("--- chunk processing contiguity and developable area")

    # Chunks are processed in a loop over the chunk IDs, which are simply
    # 1, 2, ..., chunks. So, we need our chunk IDs, and a place to store
    # the results of each chunk

    ctgy = []

    # Now, we loop through the chunks to calculate contiguity:
    for i in range(0, chunks + 1):
        logger.log_msg(f"--- --- chunk {str(i)} of {str(chunks)}")

        # First, we need to select our chunk of interest, which we'll do
        # using select by attribute
        logger.log_msg("--- --- --- selecting chunk")
        selection = f'"ChunkID" = {str(i)}'
        parcel_chunk = arcpy.SelectLayerByAttribute_management(
            in_layer_or_view=diff_fc,
            selection_type="NEW_SELECTION",
            where_clause=selection
        )

        # Contiguity is calculated over a raster, so we need to rasterize
        # our chunk for processing
        logger.log_msg("--- --- --- rasterizing chunk")
        rp = makePath(intmd_gdb, ''.join(["chunk_raster_", str(i)]))
        arcpy.FeatureToRaster_conversion(in_features=parcel_chunk,
                                         field="PolyID",
                                         out_raster=rp,
                                         cell_size=cell_size)

        # Now we can load the data as a numpy array for processing. This is
        # also the end of spatial processing within the chunk loop -- we deal
        # exclusively with the numpy array from here out in the loop
        logger.log_msg("--- --- --- loading chunk raster")
        ras = arcpy.RasterToNumPyArray(in_raster=rp,
                                       nodata_to_value=-1)
        # arcpy.Delete_management(rp)

        # In addition to calculating contiguity, this rasterization gives
        # us an opportunity to calculate total developable area. This area
        # can be defined as the number of cells with a given ID times the
        # cell size squared. Cell size is fixed of course, and we can grab
        # unique values and counts using numpy functions. We'll remove the
        # information regarding the amount of empty space because we don't
        # care about that
        logger.log_msg("--- --- --- calculating developable area by polygon")
        poly_ids, counts = np.unique(ras, return_counts=True)
        area = pd.DataFrame.from_dict({"PolyID": poly_ids,
                                       "Count": counts})
        area = area[area.PolyID != -1]
        area["Developable_Area"] = area.Count * (cell_size ** 2) / 43560
        # ASSUMES FEET IS THE INPUT CRS, MIGHT WANT TO MAKE THIS AN
        # ACTUAL CONVERSION IF WE USE THIS OUTSIDE OF PMT. SEE THE
        # LINEAR UNITS CODE/NAME BOOKMARKS
        # spatial_reference.linearUnitName and .linearUnitCode
        area = area.drop(columns="Count")

        # If the area dataframe is empty, this means we have no polygons
        # in the quadrat. This can happen because the quadrats are built
        # relative to the parcel *extent*, so not all quadrats will
        # necessarily have parcels in them. If this is the case, there's no
        # need to calculate contiguity, so we skip the rest of the iteration
        npolys = len(area.index)
        if npolys == 0:
            logger.log_msg("*** no polygons in this quadrat, proceeding to next chunk ***")
        else:
            # If there is at least one polygon, though, we're on to contiguity.
            # Contiguity is based off a polygons' raster cells' relationship to
            # neighboring cells, particularly those of the same polygon. So, to
            # get at contiguity, we first need to know what polygons are
            # represented in each cell. We'll organize these into two copies of
            # the same table: one will initialize cell ID organization, and the
            # other will initialize neighboring cell ID organization
            logger.log_msg("--- --- --- initializing cell neighbor identification")
            ras_dim = ras.shape
            nrow = ras_dim[0]
            ncol = ras_dim[1]

            id_tab_self = pd.DataFrame({"Row": np.repeat(np.arange(nrow), ncol),
                                        "Col": np.tile(np.arange(ncol), nrow),
                                        "ID": ras.flatten()})
            id_tab_neighbor = pd.DataFrame({"NRow": np.repeat(np.arange(nrow), ncol),
                                            "NCol": np.tile(np.arange(ncol), nrow),
                                            "NID": ras.flatten()})

            # A lot of these cells represent either empty space or building space,
            # which we don't care about. And because neighbor identification is
            # an expensive process, we don't want to calculate neighbors if we
            # don't have to. So, prior to neighbor identification, we'll isolate
            # the cells for which we'll actually calculate contiguity
            logger.log_msg("--- --- --- identifying non-empty cells")
            row_oi = id_tab_self[id_tab_self.ID != -1].Row.to_list()
            col_oi = id_tab_self[id_tab_self.ID != -1].Col.to_list()

            # To know what polygons are represented in a cell's neighbors, we
            # need to know what cells actually are the neighbors (i.e. their
            # indices). Thankfully, rasters are rectangular, so if we know the
            # index of a cell, we can calculate the index of all its neighbors.
            # That is our next step: we'll organize cells and neighbors into
            # a dataframe
            logger.log_msg("--- --- --- identifying neighbors of non-empty cells")
            row_basic = [np.arange(x - 1, x + 2) for x in row_oi]
            col_basic = [np.arange(x - 1, x + 2) for x in col_oi]

            meshed = [np.array(np.meshgrid(x, y)).reshape(2, 9).T
                      for x, y in zip(row_basic, col_basic)]
            meshed = np.concatenate(meshed, axis=0)
            meshed = pd.DataFrame(meshed, columns=["NRow", "NCol"])

            meshed.insert(1, "Col", np.repeat(col_oi, 9))
            meshed.insert(0, "Row", np.repeat(row_oi, 9))

            # When building our neighbor table, we assumed each cell had 9
            # neighbors to rely on easy repeating structure. However, if a cell
            # is on an edge or corner, it has fewer than 9 neighbors. So we now
            # want to remove any neighbors we might have identified that aren't
            # valid cells
            logger.log_msg("--- --- --- filtering to valid neighbors by index")
            meshed = meshed[(meshed.NRow >= 0)
                            & (meshed.NRow < nrow)
                            & (meshed.NCol >= 0)
                            & (meshed.NCol < ncol)]

            # Now that we've identified neighbors for each cell of interest,
            # we want to know what the polygon is represented in the cell and
            # what polygons are represented in the neighbors. To do this, we can
            # merge back to our initialized cell-ID and neighbor-ID tables
            logger.log_msg("--- --- --- tagging cells and their neighbors with polygon IDs")
            meshed = pd.merge(meshed,
                              id_tab_self,
                              left_on=["Row", "Col"],
                              right_on=["Row", "Col"],
                              how="left")
            meshed = pd.merge(meshed,
                              id_tab_neighbor,
                              left_on=["NRow", "NCol"],
                              right_on=["NRow", "NCol"],
                              how="left")

            # We now have one more level of filtering to do to complete our
            # neighbor table. We only want "valid" neighbors: ones where the cell
            # ID and the neighbor ID match (i.e. the cell and neighbor are from
            # the same polygon). We'll complete that filtering here, and then
            # drop the neighbor ID (for legibility)
            logger.log_msg("--- --- --- fitering to valid neighbors by ID")
            meshed = meshed[meshed.ID == meshed.NID]
            meshed = meshed.drop(columns="NID")

            # With neighbors identified, we now need to define cell weights
            # for contiguity calculations. These are based off the specifications
            # in the 'weights' inputs to the function. So, we tag each
            # cell-neighbor pair in 'valid_neighbors' with a weight.
            logger.log_msg("--- --- --- tagging cells and neighbors with weights")
            conditions = [(np.logical_and(meshed["NRow"] == meshed["Row"] - 1, meshed["NCol"] == meshed["Col"] - 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] - 1, meshed["NCol"] == meshed["Col"])),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] - 1, meshed["NCol"] == meshed["Col"] + 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"], meshed["NCol"] == meshed["Col"] - 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"], meshed["NCol"] == meshed["Col"])),
                          (np.logical_and(meshed["NRow"] == meshed["Row"], meshed["NCol"] == meshed["Col"] + 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] + 1, meshed["NCol"] == meshed["Col"] - 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] + 1, meshed["NCol"] == meshed["Col"])),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] + 1, meshed["NCol"] == meshed["Col"] + 1))]
            choices = ["top_left", "top_center", "top_right", "middle_left", "self", "middle_right", "bottom_left",
                       "bottom_center", "bottom_right"]
            meshed["Type"] = np.select(conditions, choices)
            meshed["Weight"] = [weights[key] for key in meshed["Type"]]

            # To initialize the contiguity calculation, we sum weights by cell.
            # We lose the ID in the groupby though, which we need to get to
            # contiguity, so we need to merge back to our cell-ID table
            logger.log_msg("--- --- --- summing weight by cell")
            wtab = meshed.groupby(["Row", "Col"])[["Weight"]].agg("sum").reset_index()
            wtab = pd.merge(wtab, id_tab_self, left_on=["Row", "Col"], right_on=["Row", "Col"], how="left")

            # We are now finally at the point of calculating contiguity! It's a
            # pretty simple function, which we apply over our IDs. This is the
            # final result of our chunk process, so we'll also rename our "ID"
            # field to "PolyID", because this is the proper name for the ID over
            # which we've calculated contiguity. This will make our life easier
            # when chunk processing is complete, and we move into data formatting
            # and writing
            logger.log_msg("--- --- --- calculating contiguity by polygon")
            weight_max = sum(weights.values())
            contiguity = wtab.groupby("ID").apply(
                lambda x: (sum(x.Weight) / len(x.Weight) - 1) / (weight_max - 1)).reset_index(name="Contiguity")
            contiguity.columns = ["PolyID", "Contiguity"]

            # For reporting results, we'll merge the contiguity and developable
            # area tables
            logger.log_msg("--- --- --- merging contiguity and developable area information")
            contiguity = pd.merge(contiguity, area, left_on="PolyID", right_on="PolyID", how="left")

            # We're done chunk processing -- we'll put the resulting data frame
            # in our chunk results list as a final step
            logger.log_msg("--- --- --- appending chunk results to master list")
            ctgy.append(contiguity)

    # Contiguity results formatting
    # -----------------------------

    logger.log_msg("--- formatting polygon-level results")

    # The results of our chunks are stored in a list after the loop -- our
    # first formatting step is thus to merge these into a single dataframe
    logger.log_msg("--- --- combining chunked results into table format")
    ctgy = pd.concat(ctgy, axis=0)

    # Recall that we calculated contiguity relative to ~polygon~, not
    # necessarily the ~parcel~. But, we want our results to be parcel-level.
    # We can merge our contiguity results with our 'ref_df' -- containing
    # ProcessID (parcel) and PolyID (polygon) -- to achieve this
    logger.log_msg("--- --- filling table with missing polygons")
    ctgy = pd.merge(ref_df, ctgy, left_on="PolyID", right_on="PolyID", how="left")

    # It's possible that a polygon winds up not represented because
    # (1) a building covers the whole polygon or (2) the polygon's developable
    # area was not caught by the cell configuration of the raster. Either way,
    # this results in missing data we'll want to fill for completeness.
    # We fill both contiguity and developable area with 0, because these are
    # the values for no contiguity and no area, respectively
    logger.log_msg("--- overwriting missing values with 0")
    ctgy = ctgy.fillna(value={"Contiguity": 0, "Developable_Area": 0})

    arcpy.Delete_management(quadrats_fc)
    return ctgy


def contiguity_summary(full_results_df, parcels_id_field,
                       summary_funcs=["min", "max", "median", "mean"], area_scaling=True):
    """summarize contiguity/developable area results from
        `analyze_contiguity_index` from sub-parcel to parcel

    Args:
        full_results_df: pandas dataframe
            dataframe output of `analyze_contiguity_index`
        parcels_id_field: str
            name of a field used to identify the parcels in the summarized
            parcel results
        summary_funcs: list of strs
            functions to be used to summarize contiguity to the parcel; available
            options include min, max, mean, and median
            Default is all options
        area_scaling: boolean
            should a scaled version of developable area be calculated? If `True`,
            a "scaled_area" statistic will be calculated as developable area times
            contiguity index (at the parcel level). The default is True
    Returns:
        pandas dataframe
            a table of summarized results attributed with:
                1. A parcel identifier (as specified in `analyze_contiguity_index`
                when the featur class was initialized)
                2. Parcel developable area (summed to the parcel)
                3. {fun}-summarized contiguity, for each function in `summary_funs`
                4. {fun}-scaled area, for each of {fun}-summarized contiguity, if
                `area_scaling = True`
    """

    # Summarizing up to the parcel
    # ----------------------------

    logger.log_msg("--- summarizing contiguity and developable area to the parcels")

    # We want to summarize contiguity to the parcel. We'll do that using
    # every function in 'summary_funs'.
    logger.log_msg("--- --- summarizing contiguity to the parcels")
    ctgy_summary = []
    ctgy_variables = []
    for func in summary_funcs:
        logger.log_msg(f"--- --- --- {func}")
        var_name = f'{func.title()}_Contiguity'
        ci = full_results_df.groupby(parcels_id_field).agg({"Contiguity": getattr(np, func)}).reset_index()
        ci.columns = [parcels_id_field, var_name]
        ctgy_summary.append(ci)
        ctgy_variables.append(var_name)

    # The results for each function are stored in a separate table, so we now
    # merge them into a single table
    logger.log_msg("--- ---formatting contiguity summary results")
    ctgy_summary = [df.set_index(parcels_id_field) for df in ctgy_summary]
    ctgy_summary = pd.concat(ctgy_summary, axis=1)
    ctgy_summary = ctgy_summary.reset_index()

    # The only way to summarize developable area is by sum, so we'll take
    # care of that now.
    logger.log_msg("--- ---summarizing developable area to the parcels")
    area_summary = full_results_df.groupby(parcels_id_field)[["Developable_Area"]].agg("sum").reset_index()

    # The final summary step is then merging the contiguity and developable
    # area summary results
    logger.log_msg("--- ---merging contiguity and developable area summaries")
    df = pd.merge(area_summary, ctgy_summary,
                  left_on=parcels_id_field, right_on=parcels_id_field, how="left")

    # If an area scaling is requested (area_scaling = True), that means
    # we need to create a combined statistic for contiguity and area. To do
    # this, we simply multiply contiguity by developable area (essentially,
    # we're weighting developable area by how contiguous it is). We do this
    # for all contiguity summaries we calculated
    if area_scaling == True:
        logger.log_msg("--- --- calculating combined contiguity-developable area statistics")
        for i in ctgy_variables:
            var_name = i.replace("Contiguity", "Scaled_Area")
            df[var_name] = df["Developable_Area"] * df[i]

    return df


def lu_diversity(parcels_path,
                 parcels_id_field,
                 parcels_land_use_field,
                 land_use_recode_path=None,
                 land_use_recode_field=None,
                 on_field=None,
                 aggregate_geometry_path=None,
                 aggregate_geometry_id_field=None,
                 buffer_diversity=0,
                 relevant_land_uses=["auto", "civic", "education",
                                     "entertainment", "grocery",
                                     "healthcare", "industrial",
                                     "lodging", "mf", "office",
                                     "restaurant", "sf", "shopping"],
                 how=["simpson", "shannon", "berger-parker",
                      "enp", "chi-squared"],
                 chisq_props=None,
                 regional_adjustment=True,
                 regional_constants=None):
    """
    calculates land use diversity within aggregate geometries using parcels

    Parameters
    ----------
    parcels_path: Path
        path to parcels [which provide the attributes for diversity calcs]
    parcels_id_field: str
        id field for the parcels
    parcels_land_use_field: str
        land use field for the parcels
    land_use_recode_path: Path, optional
        a path to a table containing a generalized or otherwise modified land
        use field on which diversity should be assessed. must be joinable to
        `parcels` by `parcels_land_use_field`. The default is `None`,
        uses `parcels_land_use_field` for diversity assessment
    land_use_recode_field: str, optional
        field in `land_use_recode` over which to assess diversity; should be
        provided if `land_use_recode_path` is. The default is `None`
    on_field: str, optional
        field in the parcels over which diversity is assessed (e.g. land area,
        building square footage, etc.). The default is `None`, diversity will
        be assessed relative to a parcel count
    aggregate_geometry_path: Path, optional
        path to an aggregate geometry within which diversity will be calculated
        (e.g. neighborhoods, street buffers, etc.). It is highly recommended
        that this is provided. The default is `None`, parcels themselves will
        act as the aggregate geometry
    aggregate_geometry_id_field: str, optional
        id field for the aggregate geometry. must be provided if
        `aggregate_geometry_path` is provided. The default is `None`, this
        means the parcels are the aggregate geometry, so the `parcels_id_field`
        acts as the `aggregate_geometry_id_field`
    buffer_diversity: int, optional
        radius (in units of the CRS of the aggregate geometry) to buffer the
        aggregate geometry for calculation of diversity. Not recommended if
        an `aggregate_geometry_path` is provided; recommended if
        `aggregate_geometry_path` is not provided (i.e. the parcels are the
        aggregate geometry). The default is `0`, no buffer
    relevant_land_uses: list of str, optional
        land uses that should be considered for a diversity calculation. must
        reflect land uses in `parcels_land_use_field` if
        `land_use_recode_field == None`, or `land_use_recode_field` if
        `land_use_recode_field` is provided. The default list removes "vacant",
        "ag", "misc", and "other" from consideration in diversity calculations
    how: list of str, optional
        diversity metrics to calculate. may include any or all of "simpson",
        "shannon", "berger-parker", "enp", and "chi-squared". See notes for
        a description of metrics. The default list includes all options, so all
        5 metrics will be calculated for each feature in the aggregate geometry
    chisq_props: dict of floats, optional
        if "chi-squared" is in `how`, this parameter allows a user to set an
        optimal distribution of land uses to be used in the calculation of
        chi-squared statistics. The keys must match `relevant_land_uses`
        exactly, and the values must sum to 1. Will be ignored if "chi-squared"
        is not in `how`. The default `None`, the optimal distribution of land
        uses is assumed to be an equal abundance of all `relevant_land_uses`
    regional_adjustment: bool, optional
        should a regional adjustment be performed? If so, each diversity metric
        for each feature of the aggregate geometry will be divided by the
        regional (across all parcels) score for that diversity metric, to give
        a sense how that feature performs relative to the whole area. The
        default is `True`, complete a regional adjustment
    regional_constants: dict of floats, optional
        if `regional_adjustment` is `True`, this parameter allows a user to set
        constants to which to compare the aggregate geometry diversity
        metrics (as opposed to comparing to the calculated regional scores).
        The keys must match `how` exactly, and the values must be between
        0 and 1 (as all scores are adjusted to a 0-1 scale). The default is
        `None`, complete the regional adjustment by calculating regional scores

    Notes
    -----
    The diversity measures are defined as followed:
    1. Simpson index: mathematically, the probability that a random draw of
       one unit of land use A would be followed by a random draw of one unit
       of land use B. Ranges from 0 (only one land use present)
       to 1 (all land uses present in equal abundance)
    2. Shannon index: borrowing from information theory, Shannon quantifies
       the uncertainty in predicting the land use of a random one unit draw.
       The higher the uncertainty, the higher the diversity. Ranges from 0
       (only one land use present) to -log(1/|land uses|) (all land uses
       present in equal abundance)
    3. Berger-Parker index: the maximum proportional abundance, giving a
       measure of dominance. Ranges from 1 (only one land use present) to
       1/|land uses| (all land uses present in equal abundance). Lower values
       indicate a more even spread, while high values indicate the dominance
       of one land use.
    4. Effective number of parties (ENP): a count of land uses, as weighted
       by their proportional abundance. A land use contributes less to ENP if
       it is relatively rare, and more if it is relatively common. Ranges from
       1 (only one land use present) to |land uses| (all land uses present in
       equal abunance)
    5. Chi-squared goodness of fit: the ratio of an observed chi-squared
       goodness of fit test statistic to a "worst case scenario" chi-squared
       goodness of fit test statistic. The goodness of fit test requires the
       definition of an "optimal" land use distribution ("optimal" is assumed
       to be equal abundance of all land uses, but can be specified by the
       user). The "worst case scenario" defines the highest possible
       chi-squared statistic that could be observed under the optimal land use
       distribution. In practice, this "worst case scenario" is the equivalent
       of the least likely land use [according to the optimal distribution]
       comprising the entire area. Ranges from 0 (all land uses present
       in equal abundance) to 1 (only one land use present)

    Returns
    -------
    dict
        a dictionary with:
            an entry pair for a table of diversity results at the aggregate
            geometry level. The key will be the name of the aggregate geometry
            file
            if `regional_adjustment == True` and `regional_constants == None`,
            a key/value pair for a table of regional diversity metrics, with
            the key of "region"

    """

    # Spatial processing
    # ------------------

    logger.log_msg("---spatial processing for diversity")

    # First, we have to set up the process with a few input variables
    logger.log_msg("--- ---setting up inputs for spatial processing")

    # 1. field names we want to keep from parcels. if 'on_field' is None, that
    # means we're going to do a count based diversity. for this, we'll create
    # the field ourselves, so we don't call it from the parcels
    parcel_fields = [parcels_land_use_field,
                     "SHAPE@X",
                     "SHAPE@Y"]
    if on_field is not None:
        parcel_fields = [on_field] + parcel_fields

    # 2. parcel spatial reference (for explicit definition of spatial
    # reference in arcpy operations
    sr = arcpy.Describe(parcels_path).spatialReference

    # 3. are the parcels the aggregate geometry?
    if aggregate_geometry_path is None:
        logger.log_msg("** NOTE: parcels will act as the aggregate geometry **")
        aggregate_geometry_id_field = parcels_id_field
        aggregate_geometry_path = parcels_path

    # Now we're ready for the true spatial processing. We start by extracting
    # parcel centroids to numpy array, then converting array to feature class
    logger.log_msg("--- ---converting parcel polygons to parcel centroid points")
    centroids_fc = makePath("in_memory", "centroids")
    PMT.polygonsToPoints(in_fc=parcels_path,
                         out_fc=centroids_fc,
                         fields=parcel_fields,
                         skip_nulls=False,
                         null_value=-1)

    # Next, if a buffer is requested, this means diversity will be
    # calculated within a buffered version of each feature in the aggregate
    # geometry (which, remember, may be the parcels). I.e., our aggregate
    # geometry becomes a buffered aggregate geometry. So, if requested, we
    # create the buffer, and reset our aggregate geometry path
    if buffer_diversity > 0:
        print("--- ---buffering the aggregate geometry")
        arcpy.Buffer_analysis(in_features=aggregate_geometry_path,
                              out_features="in_memory\\buffer",
                              buffer_distance_or_field=buffer_diversity)
        aggregate_geometry_path = "in_memory\\buffer"

    # Now we need to identify parcels within each feature of the aggregate
    # geometry. To do this, we intersect the parcel centroids with the
    # aggregate geometry. This has the effect of tagging the parcels with the
    # unique ProcessID, as well as filtering parcels that don't fall in any
    # feature of the aggregate geometry
    logger.log_msg("--- ---matching parcels to aggregate geometries")
    arcpy.Intersect_analysis(in_features=["in_memory\\centroids",
                                          aggregate_geometry_path],
                             out_feature_class="in_memory\\intersect")

    # Finally, we have to select out the fields we want to work with for
    # diversity calculations. At this point, we can easily create our data
    # for summarization. Note that if we have no "on_field", this means
    # we're working off a parcel count, so we add a field of 1s to simulate
    # a count when we complete our summarizations
    logger.log_msg("--- ---loading data for diversity calculations")
    ret_fields = [aggregate_geometry_id_field,
                  parcels_land_use_field]
    if on_field is not None:
        ret_fields = ret_fields + [on_field]
    div_array = arcpy.da.FeatureClassToNumPyArray(in_table="in_memory\\intersect",
                                                  field_names=ret_fields,
                                                  spatial_reference=sr,
                                                  null_value=-1)
    df = pd.DataFrame(div_array)
    if on_field is None:
        on_field = "count_field"
        df[on_field] = 1

    # Now we have our data for allocation prepped -- great!
    # The last step in spatial processing is now deleting the intermediates
    # we created along the way
    logger.log_msg("--- ---deleting intermediates")
    arcpy.Delete_management("in_memory\\centroids")
    arcpy.Delete_management("in_memory\\intersect")
    if arcpy.Exists("in_memory\\buffer"):
        arcpy.Delete_management("in_memory\\buffer")

    # Diversity calculations
    # ----------------------

    logger.log_msg("2. Diversity calculations")

    # First, we want to do a little formatting
    logger.log_msg("2.1 formatting the input data")

    # 1. Field name resetting. We do this to make our lives a little easier,
    # because we allow user input for nearly all of our fields
    df = df.rename(columns={parcels_land_use_field: "LU_DEL",
                            on_field: "ON"})

    # 2. Remove the cells that have no land use (i.e. the ones filled with -1)
    # and the ones where our on field is 0 or null (i.e. the ones with value
    # < 0, because it could be an observed 0 or a filled -1)
    df = df[df.LU_DEL != -1]
    df = df[df.ON > 0]

    # 3. Now we merge in our new land use definitions (if any):
    if land_use_recode_path is not None:
        lu_rc = pd.read_csv(land_use_recode_path)
        lu_rc = lu_rc[[parcels_land_use_field, land_use_recode_field]]
        lu_rc = lu_rc.rename(columns={parcels_land_use_field: "LU_DEL",
                                      land_use_recode_field: "LU"})
        df = df.merge(lu_rc, how="left")
        df = df.drop(columns="LU_DEL")
    else:
        df = df.rename(columns={"LU_DEL": "LU"})

    # 4. Finally, we filter to only the land uses of interest
    if relevant_land_uses is not None:
        df = df[df.LU.isin(relevant_land_uses)]

    # Now we'll do a bit of pre-summarization to give us the components we
    # need for the diversity calculations. These include a "total" (sum of
    # all 'on_field' in the aggregate geometry) and a "percent" (proportion
    # of 'on_field' in each land use in the aggregate geometry)
    logger.log_msg("--- ---calculating summary values for aggregate geometries")

    divdf = df.groupby([aggregate_geometry_id_field, "LU"])[["ON"]].agg("sum").reset_index()
    tot = divdf.groupby(aggregate_geometry_id_field)[["ON"]].agg("sum").reset_index().rename(columns={"ON": "Total"})
    divdf = divdf.merge(tot, how="left")
    divdf = divdf.assign(Percent=divdf["ON"] / divdf["Total"])

    # We can now reference this table to calculate our diversity metrics
    print("--- ---calculating diversity metrics")
    diversity_metrics = []
    nlu = len(relevant_land_uses)

    # 1. Simpson
    if "simpson" in how:
        logger.log_msg("--- --- ---Simpson")
        mc = divdf.assign(SIN=divdf["ON"] * (divdf["ON"] - 1))
        mc = mc.assign(SID=mc["Total"] * (mc["Total"] - 1))
        diversity = mc.groupby(aggregate_geometry_id_field).apply(
            lambda x: sum(x.SIN) / np.unique(x.SID)[0]).reset_index()
        diversity.columns = [aggregate_geometry_id_field, "Simpson"]
        # Adjust to 0-1 scale
        diversity["Simpson"] = 1 - diversity["Simpson"]
        diversity_metrics.append(diversity)

    # 2. Shannon
    if "shannon" in how:
        logger.log_msg("--- --- ---Shannon")
        mc = divdf.assign(PLP=divdf["Percent"] * np.log(divdf["Percent"]))
        diversity = mc.groupby(aggregate_geometry_id_field).apply(lambda x: sum(x.PLP) * -1).reset_index()
        diversity.columns = [aggregate_geometry_id_field, "Shannon"]
        # Adjust to 0-1 scale
        diversity["Shannon"] = diversity["Shannon"] / (-1 * np.log(1 / nlu))
        diversity_metrics.append(diversity)

    # 3. Berger-Parker
    if "berger-parker" in how:
        logger.log_msg("--- --- ---Berger-Parker")
        diversity = divdf.groupby(aggregate_geometry_id_field).apply(lambda x: max(x.Percent)).reset_index()
        diversity.columns = [aggregate_geometry_id_field, "BergerParker"]
        # Adjust to 0-1 scale
        diversity["BergerParker"] = 1 - diversity["BergerParker"]
        diversity_metrics.append(diversity)

    # 4. ENP
    if "enp" in how:
        logger.log_msg("--- --- ---Effective number of parties (ENP)")
        mc = divdf.assign(P2=divdf["Percent"] ** 2)
        diversity = mc.groupby(aggregate_geometry_id_field).apply(lambda x: 1 / sum(x.P2)).reset_index()
        diversity.columns = [aggregate_geometry_id_field, "ENP"]
        # Adjust to 0-1 scale
        diversity["ENP"] = (diversity["ENP"] - 1) / (nlu - 1)
        diversity_metrics.append(diversity)

    # 5. Chi-squared goodness of fit
    if "chi-squared" in how:
        logger.log_msg("--- --- ---Chi-squared goodness of fit")
        if chisq_props is not None:
            props = pd.DataFrame({"LU": list(chisq_props.keys()),
                                  "ChiP": list(chisq_props.values())})
        else:
            chisq_props = dict()
            for lu in relevant_land_uses:
                chisq_props[lu] = 1 / nlu
            props = pd.DataFrame({"LU": list(chisq_props.keys()),
                                  "ChiP": list(chisq_props.values())})
        d = dict()
        ub = np.unique(divdf[aggregate_geometry_id_field])
        d[aggregate_geometry_id_field] = np.repeat(ub, len(relevant_land_uses))
        d["LU"] = relevant_land_uses * len(ub)
        lu_dummies = pd.DataFrame(d)
        on = divdf[[aggregate_geometry_id_field, "LU", "ON"]].drop_duplicates()
        totals = divdf[[aggregate_geometry_id_field, "Total"]].drop_duplicates()
        mc = lu_dummies.merge(on, how="left").merge(totals, how="left")
        mc = mc.fillna({"ON": 0})
        mc = mc.merge(props, how="left")
        mc = mc.assign(EXP=mc["ChiP"] * mc["Total"])
        mc = mc.assign(Chi2=(mc["ON"] - mc["EXP"]) ** 2 / mc["EXP"])
        mc = mc.assign(WCS=(mc["Total"] - mc["EXP"]) ** 2 / mc["EXP"] - mc["EXP"])
        diversity = mc.groupby(aggregate_geometry_id_field).apply(
            lambda x: sum(x.Chi2) / (sum(x.EXP) + max(x.WCS))).reset_index()
        diversity.columns = [aggregate_geometry_id_field, "ChiSquared"]
        # Adjust to 0-1 scale
        diversity["ChiSquared"] = 1 - diversity["ChiSquared"]
        diversity_metrics.append(diversity)

    # Now that we've calculated all our metrics, we just need to merge
    # into a single data frame for reporting
    logger.log_msg("--- ---formatting diversity results")
    diversity_metrics = [df.set_index(aggregate_geometry_id_field) for df in diversity_metrics]
    diversity_metrics = pd.concat(diversity_metrics, axis=1)
    diversity_metrics = diversity_metrics.reset_index()

    # Regional comparisons ---------------------------------------------------

    # Do we want the region score across ALL parcels?
    # Or across parcels within our aggregate geometries?
    # For now, we use the first... i.e. the "region adjustment" is relative
    # to all area of the context of the aggregate geometries

    # If regional comparison is requested, we calculate each diversity index
    # at the regional level, and adjust the aggregate geometry scores by
    # a ratio of geom score : region score. If regional constants are provided,
    # we do the same sort of adjustment, but use the provided constants as
    # opposed to doing the calculations here.
    if regional_adjustment == True:
        logger.log_msg("---regional adjustment to diversity")

        if regional_constants is not None:
            # Set our adjustment dictionary to the provided constants if
            # constants are provided
            area_div = regional_constants
        else:
            # We need to do our own calculations since no constants are given.
            # Like before, we first need to get summary values. But now, we're
            # calculating them over the whole region, not the individual
            # aggregate geometries. NOTE THAT THE "WHOLE REGION" HERE MEANS
            # ALL PARCELS, so we reference back to the parcels_array from
            # spatial processing
            print("--- ---calculating summary values for region")

            # We'll need to format and summarize the parcels in the same
            # way we did with those in our aggregate geometries
            pdf = pd.DataFrame(parcels_array)
            pdf = pdf.rename(columns={on_field: "ON",
                                      parcels_land_use_field: "LU_DEL"})
            pdf = pdf[["ON", "LU_DEL"]]
            pdf = pdf[pdf.LU_DEL != -1]
            pdf = pdf[pdf.ON > 0]
            pdf = pdf.merge(lu_rc, how="left")
            pdf = pdf.drop(columns="LU_DEL")
            if relevant_land_uses is not None:
                pdf = pdf[pdf.LU.isin(relevant_land_uses)]

            # Now we can summarize
            reg = pdf.groupby("LU")[["ON"]].agg("sum").reset_index()
            reg["Total"] = sum(reg.ON)
            reg = reg.assign(Percent=reg["ON"] / reg["Total"])

            # Now, we calculate each diversity metric for the whole region
            logger.log_msg("--- ---calculating regional diversity")
            area_div = dict()

            # 1. Simpson
            if "simpson" in how:
                logger.log_msg("--- --- ---Simpson")
                area_div["Simpson"] = sum(reg.ON * (reg.ON - 1)) / (reg.Total[0] * (reg.Total[0] - 1))
                area_div["Simpson"] = 1 - area_div["Simpson"]

            # 2. Shannon
            if "shannon" in how:
                logger.log_msg("--- --- ---Shannon")
                area_div["Shannon"] = -1 * sum(reg.Percent * np.log(reg.Percent))
                area_div["Shannon"] = area_div["Shannon"] / (-1 * np.log(1 / nlu))

            # 3. Berger-Parker
            if "berger-parker" in how:
                logger.log_msg("--- --- ---Berger-Parker")
                area_div["BergerParker"] = max(reg.Percent)
                area_div["BergerParker"] = 1 - area_div["BergerParker"]

            # 4. ENP
            if "enp" in how:
                logger.log_msg("--- --- ---Effective number of parties (ENP)")
                area_div["ENP"] = 1 / sum(reg.Percent ** 2)
                area_div["ENP"] = (area_div["ENP"] - 1) / (nlu - 1)

            # 5. Chi-squared goodness of fit
            if "chi-squared" in how:
                logger.log_msg("--- --- ---Chi-squared goodness of fit")
                csr = props.merge(reg, how="left")
                csr = csr.assign(EXP=csr["Total"] * csr["ChiP"])
                csr = csr.assign(Chi2=(csr["ON"] - csr["EXP"]) ** 2 / csr["EXP"])
                csr = csr.assign(WCS=(csr["Total"] - csr["EXP"]) ** 2 / csr["EXP"] - csr["EXP"])
                area_div["ChiSquared"] = sum(csr.Chi2) / (sum(csr.EXP) + max(csr.WCS))
                area_div["ChiSquared"] = 1 - area_div["ChiSquared"]

        # Now, we can calculate our "adjusted" diversities by dividing the
        # aggregate geometry value for a metric by the regional value for
        # a metric
        logger.log_msg("---adjusting diversity by regional score")
        for key in area_div.keys():
            value = area_div[key]
            name = '_'.join([key, "Adj"])
            diversity_metrics[name] = diversity_metrics[key] / value

        # Finally, if we did a regional adjustment, we'll want to write
        # out the region results as well. We'll do this as a simple table,
        # so just create a dataframe
        area_div = pd.DataFrame(area_div, index=[0]).reset_index(drop=True)

    # Done
    # ----

    agg_name = os.path.split(aggregate_geometry_path)[1].lower()
    div_frames = {agg_name: diversity_metrics}
    if regional_adjustment == True and regional_constants is None:
        div_frames["region"] = area_div
    return div_frames


def prep_permits_units_reference(parcels_path, permits_path,
                                 lu_match_field, parcels_living_area_field,
                                 permits_units_field, permits_living_area_name,
                                 units_match_dict={}):
    """creates a reference table by which to convert various units provided in
        the Florida permits_df data to building square footage

    Args:
        parcels_path (str): path to MOST RECENT parcels_df data (see notes)
        permits_path (str): path to the building permits_df data
        lu_match_field (str): field name for a land use field present in BOTH the parcels_df and permits_df data
        parcels_living_area_field (str): field name in the parcels_df for the total living area (building area)
        permits_units_field (str): field name in the permits_df for the unit of measurement for permit types
        permits_living_area_name (str): unit name for building area in the `permits_units_field`
        units_match_dict (dict): a dictionary of the format `{unit_name: parcel_field, ...}`, where the
            `unit_name` is one of the unit names present in the permits_df data's `permits_units_field`, and
            the `parcel_field` is the field name in the parcels_df corresponding to that unit. It should
            be used to identify non-building area fields in the parcels_df for which we can calculate
            a building area/unit for the multiplier. `parcel_field` can also take the form of a basic
            function (+,-,/,*) of a column, see Notes for specifications

    Notes:
        The most up-to-date parcels_df data available should be used, because units multipliers for the
        short term should be based on the most current data

        To specify a function for the units_match_field, use the format "{field} {function sign} {number}".
        So, for example, to map an 'acre' unit in the permits_df to a 'land_square_foot' field in the parcels_df,
        you'd use the dictionary entry `'acre': 'land_square_foot' / 43560`

    Returns:
        pandas dataframe: a table of units multipliers/overwrites by land use
    """

    # Loading data
    # To set up for loading the parcels_df, we'll need to identify desired unit
    # fields that are not building square footage. These are provided in the
    # values of the units_match_dict; however, some of them might be given
    # as functions (i.e. to match to a unit, a function of a parcels_df field is
    # required). So, we need to isolate the field names and the functional
    # components
    logger.log_msg("--- identifying relevant parcel fields")

    match_fields = []
    match_functions = []

    for key in units_match_dict.keys():
        # Field
        field = re.findall("^(.*[A-Za-z])", units_match_dict[key])[0]
        if field not in match_fields:
            match_fields.append(field)
        # Function
        fun = units_match_dict[key].replace(field, "")
        if fun != "":
            fun = ''.join(['parcels_df["', field, '"] = parcels_df["', field, '"]', fun])
            if field not in match_functions:
                match_functions.append(fun)
                # Overwrite value
        units_match_dict[key] = field

    # Parcels: we only need to keep a few fields from the parcels_df: the lu_match_field,
    # the parcels_living_area_field, and any values in the  match_fields (calculated above
    # from units_match_dict). If we have any  functions to apply (again, calculated above from units_match_dict),
    # we'll do that too.
    logger.log_msg("--- reading/formatting parcels_df")
    parcels_fields = [lu_match_field, parcels_living_area_field] + match_fields
    parcels_df = PMT.featureclass_to_df(in_fc=parcels_path, keep_fields=parcels_fields, null_val=0.0)
    for fun in match_functions:
        exec(fun)

    # Permits: like with the parcels_df, we only need to keep a few fields: the lu_match_field, and the units_field.
    # Also, we only need to keep unique rows of this frame (otherwise we'd just be repeating calculations!)
    logger.log_msg("--- reading/formatting permits_df")
    permits_fields = [lu_match_field, permits_units_field]
    permits_df = PMT.featureclass_to_df(in_fc=permits_path, keep_fields=permits_fields, null_val=0.0)
    permits_df = permits_df.drop_duplicates().reset_index(drop=True)

    # Multipliers and overwrites
    # Now, we loop through the units to calculate multipliers. We'll actually have two classes: multipliers
    # and overwrites. Multipliers imply that the unit can be converted to square footage using some function
    # of the unit; overwrites imply that this conversion unavailable

    # To do this, we loop over the rows of permits_df. There are 3 possible paths for each row
    # 1. If the unit of the row is already square footage, we don't need any additional processing
    #   Multiplier: 1 [used to mitigate null values in df to make table]
    #   Overwrite: -1 [used to mitigate null values in df to make table]
    # 2. If the unit is one of the keys in units_match_dict, this means we can calculate a
    # square footage / unit from the parcels_df. We do this relative to all parcels_df with that row's land use
    #   Multiplier: median(square footage / unit)
    #   Overwrite: -1
    # 3. If the unit is NOT one of the keys in units_match_dict, this means  we have to rely on average square
    # footage. This is an overwrite, not a multiplier, and is calculated relative to all parcels_df with that row's
    # land use
    #   Multiplier: -1
    #   Overwrite: median(square footage)
    logger.log_msg("--- calculating multipliers and overwrites")
    rows = np.arange(len(permits_df.index))
    units_multipliers = []
    units_overwrites = []

    for row in rows:
        # Unit and land use for the row
        unit = permits_df[permits_units_field][row]
        lu = permits_df[lu_match_field][row]
        # Parcels of the row's land use
        plu = parcels_df[parcels_df[lu_match_field] == lu]

        # Case (1)
        if unit == permits_living_area_name:
            units_multipliers.append(1.0)
            units_overwrites.append(-1.0)

        # Cases (2) and (3)
        else:
            # Case (2)
            if unit in units_match_dict.keys():
                sqft_per_unit = plu[parcels_living_area_field] / plu[units_match_dict[unit]]
                median_value = np.nanmedian(sqft_per_unit)
                units_multipliers.append(median_value)
                units_overwrites.append(-1.0)

            # Case (3)
            else:
                sq_ft = plu[parcels_living_area_field]
                if len(sq_ft) > 0:
                    median_value = np.nanmedian(sq_ft)
                else:
                    median_value = -1.0
                units_multipliers.append(-1.0)
                units_overwrites.append(median_value)

    # Since our results are lists, we can just attach them back to the permits_df as new columns
    logger.log_msg("--- binding results to the permits_df data")
    permits_df["Multiplier"] = units_multipliers
    permits_df["Overwrite"] = units_overwrites

    # Done
    return permits_df


def build_short_term_parcels(parcels_path, permits_path, permits_ref_df,
                             parcels_id_field, parcels_lu_field,
                             parcels_living_area_field, parcels_land_value_field,
                             parcels_total_value_field, parcels_buildings_field,
                             permits_id_field, permits_lu_field, permits_units_field,
                             permits_values_field, permits_cost_field,
                             save_gdb_location, units_field_match_dict={}):
    # First, we need to initialize a save feature class. The feature class
    # will be a copy of the parcels with a unique ID (added by the function)
    logger.log_msg("--- initializing a save feature class")

    # Add a unique ID field to the parcels called "ProcessID"
    logger.log_msg("--- --- adding a unique ID field for individual parcels")
    # creating a temporary copy of parcels
    temp_parcels = make_inmem_path()
    temp_dir, temp_name = os.path.split(temp_parcels)
    # temp_dir = tempfile.TemporaryDirectory()
    # temp_gdb = arcpy.CreateFileGDB_management(out_folder_path=temp_dir.name, out_name="temp_data.gdb")
    # temp_parcels = arcpy.FeatureClassToFeatureClass_conversion(in_features=parcels_path,
    #                                                            out_path=temp_gdb, out_name="temp_parcels")
    arcpy.FeatureClassToFeatureClass_conversion(in_features=parcels_path, out_path=temp_dir, out_name=temp_name)
    process_id_field = PMT.add_unique_id(feature_class=temp_parcels)

    logger.log_msg("--- --- reading/formatting parcels")
    # read in all of our data
    #   - read the parcels (after which we'll remove the added unique ID from the original data).
    parcels_fields = [f.name for f in arcpy.ListFields(temp_parcels)
                      if f.name not in ["OBJECTID", "Shape", "Shape_Length", "Shape_Area"]]
    parcels_df = PMT.featureclass_to_df(in_fc=temp_parcels, keep_fields=parcels_fields, null_val=0.0)

    # create output dataset keeping only process_id and delete temp file
    logger.log_msg("--- --- creating save feature class")
    fmap = arcpy.FieldMappings()
    fm = arcpy.FieldMap()
    fm.addInputField(temp_parcels, 'ProcessID')
    fmap.addFieldMap(fm)
    short_term_parcels = arcpy.FeatureClassToFeatureClass_conversion(
        in_features=temp_parcels, out_path=save_gdb_location, out_name="Parcels", field_mapping=fmap
    )[0]
    # temp_dir.cleanup()

    # Now we're ready to process the permits to create the short term parcels data
    logger.log_msg("--- creating short term parcels")

    # First we read the permits
    logger.log_msg("--- --- reading/formatting permits_df")
    permits_fields = [permits_id_field, permits_lu_field, permits_units_field, permits_values_field, permits_cost_field]
    permits_df = PMT.featureclass_to_df(in_fc=permits_path, keep_fields=permits_fields, null_val=0.0)
    permits_df = permits_df[permits_df[permits_values_field] >= 0]

    # Merge the permits_df and permits_df reference
    #   - join the two together on the permits_lu_field and permits_units_field
    logger.log_msg("--- --- merging permits_df and permits_df reference")
    permits_df = pd.merge(left=permits_df, right=permits_ref_df,
                          left_on=[permits_lu_field, permits_units_field],
                          right_on=[permits_lu_field, permits_units_field], how="left")

    # Now we add to permits_df the field matches to the parlces (which will be
    # helpful come the time of updating parcels from the permits_df)
    if units_field_match_dict is not None:
        logger.log_msg("--- --- joining units-field matches")
        ufm = pd.DataFrame.from_dict(units_field_match_dict, orient="index").reset_index()
        ufm.columns = [permits_units_field, "Parcel_Field"]
        permits_df = pd.merge(left=permits_df, right=ufm,
                              left_on=permits_units_field, right_on=permits_units_field, how="left")

    # calculate the new building square footage for parcel in the permit features
    # using the reference table multipliers and overwrites
    logger.log_msg("--- --- applying unit multipliers and overwrites")
    new_living_area = []
    for value, multiplier, overwrite in zip(
            permits_df[permits_values_field],
            permits_df["Multiplier"],
            permits_df["Overwrite"]):
        if (overwrite == -1.0) and (multiplier != -1.0):
            new_living_area.append(value * multiplier)
        elif (multiplier == -1.0) and (overwrite != -1.0):
            new_living_area.append(overwrite)
        else:
            new_living_area.append(0)

    logger.log_msg("--- --- appending new living area values to permits_df data")
    permits_df["UpdTLA"] = new_living_area
    permits_df.drop(columns=["Multiplier", "Overwrite"], inplace=True)

    # update the parcels with the info from the permits_df
    #   - match building permits_df to the parcels using id_match_field,
    #   - overwrite parcel LU, building square footage, and anything specified in the match dict
    #   - add replacement flag
    logger.log_msg("--- --- collecting updated parcel data")
    pids = np.unique(permits_df[permits_id_field])
    update = []
    for i in pids:
        df = permits_df[permits_df[permits_id_field] == i]
        if len(df.index) > 1:
            pid = pd.Series(i, index=[permits_id_field])
            # Living area by land use
            total_living_area = df.groupby(permits_lu_field)["UpdTLA"].agg("sum").reset_index()
            # Series for land use [with most living area]
            land_use = pd.Series(total_living_area[permits_lu_field][np.argmax(total_living_area["UpdTLA"])],
                                 index=[permits_lu_field])
            # Series for living area (total across all land uses)
            ba = pd.Series(sum(total_living_area["UpdTLA"]), index=[parcels_living_area_field])
            # Series for other fields (from units-field match)
            others = df.groupby("Parcel_Field")[permits_values_field].agg("sum")
            # Series for cost
            cost = pd.Series(sum(df[permits_cost_field]), index=[permits_cost_field])
            # Bind
            df = pd.DataFrame(pd.concat([pid, land_use, ba, others, cost], axis=0)).T
        else:
            # Rename columns to match parcels
            df.rename(columns={"UpdTLA": parcels_living_area_field,
                               permits_values_field: df.Parcel_Field.values[0]},
                      inplace=True)
            # Drop unnecessary columns (including nulls from units-field match)
            df.drop(columns=[permits_units_field, "Parcel_Field"], inplace=True)
            df = df.loc[:, df.columns.notnull()]
        # Append the results to our storage list
        update.append(df)

    # Now we just merge up the rows. We'll also add 2 columns:
    #    - number of buildings = 1 (a constant assumption, unless TLA == 0)
    #    - a column to indicate that these are update parcels from the permits_df
    # We'll also name our columns to match the parcels
    update = pd.concat(update, axis=0).reset_index(drop=True)
    update.fillna(0, inplace=True)
    update[parcels_buildings_field] = 1
    update.loc[update[parcels_living_area_field] == 0, parcels_buildings_field] = 0
    update["PERMIT"] = 1
    update.rename(columns={permits_id_field: parcels_id_field,
                           permits_lu_field: parcels_lu_field},
                  inplace=True)

    # Finally, we want to update the value field. To do this, we take the
    # max of previous value and previous land value + cost of new development
    logger.log_msg("--- ---estimating parcel value after permit development")
    pv = parcels_df[parcels_df[parcels_id_field].isin(pids)]
    pv = pv.groupby(parcels_id_field)[[parcels_land_value_field, parcels_total_value_field]].sum().reset_index()
    update = pd.merge(update, pv, on=parcels_id_field, how="left")
    update["NV"] = update[parcels_land_value_field] + update[permits_cost_field]
    update[parcels_total_value_field] = np.maximum(update["NV"], update[parcels_total_value_field])
    update.drop(columns=["NV", parcels_land_value_field, "COST"],
                inplace=True)

    # make the replacements. - drop all the rows from the parcels whose IDs are in the permits_df, - add all the rows
    # for the data we just collected. and retain the process ID from the parcels we're dropping for the sake of joining
    logger.log_msg("--- --- replacing parcel data with updated information")
    to_drop = parcels_df[parcels_df[parcels_id_field].isin(pids)]
    process_ids = to_drop.groupby(parcels_id_field)["ProcessID"].min().reset_index()
    update = pd.merge(update, process_ids, on=parcels_id_field, how="left")
    parcel_update = parcels_df[~parcels_df[parcels_id_field].isin(pids)]
    parcel_update["PERMIT"] = 0
    final_update = pd.concat([parcel_update, update], axis=0).reset_index(drop=True)

    # Now we just write!
    logger.log_msg("\nWriting results")
    # join to initialized feature class using extend table (and delete the created ID when its all over)
    logger.log_msg("--- --- joining results to save feature class (be patient, this will take a while)")
    PMT.extendTableDf(in_table=short_term_parcels, table_match_field=process_id_field,
                      df=final_update, df_match_field="ProcessID")
    arcpy.DeleteField_management(in_table=short_term_parcels, drop_field=process_id_field)
    logger.log_msg("\nDone!")
    return short_term_parcels

# DEPRECATED
# def prep_parcel_energy_consumption_tbl(in_parcel_lu_tbl, energy_use_field,
#                                        res_use_tbl, res_use_btu_field,
#                                        nres_use_tbl, nres_use_btu_field):
#     """
#     applies energy
#     Parameters
#     ----------
#     in_parcel_lu_tbl
#     energy_use_field
#     res_use_tbl
#     res_use_btu_field
#     nres_use_tbl
#     nres_use_btu_field
#
#     Returns
#     -------
#
#     """
#     pass
#
#
# def estimate_parcel_nres_consumption(energy_df, energy_lu_field, energy_sqft_field,
#                                      parcel_fc, parcel_lu_field, parcel_sqft_field,
#                                      parcel_id_field, out_table, out_id_field):
#     """
#     Using a table of non-residential energy consumption rates and a parcel
#     feature class, estimate non-residential energy consumption based on
#     parcel land use and building area.
#
#     Parameters
#     -----------
#     energy_df: DataFrame; A csv file containing energy consumption rates
#                         (btu per square foot) based on building type.
#     energy_lu_field: String; The field in `energy_table` that defines building
#                         types or land uses.
#     energy_sqft_field: String; The field in `energy_table` that records BTU
#                         per square foot rates for each building type.
#     parcel_fc: Path;  A feature class of parcels
#     parcel_lu_field: String; The field in `parcel_fc` that defines each parcel's
#                     land use (values correspond to those in `energy_lu_field`).
#     parcel_sqft_field: String; The field in `parcel_fc` that records the total
#                     floor area of buildings on the parcel.
#     parcel_id_field: String
#     out_table: Path
#     out_id_field: String
#
#     Returns
#     --------
#     parcel_fc: Path
#         `parcel_fc` is modified in place such that a new field `NRES_BTU` is
#         added and populated based on building type and floor area.
#     """
#     # # Read in csv table
#     # energy = pd.read_csv(energy_table)
#     # energy[energy_lu_field] = energy[energy_lu_field].str.strip()
#
#     # # Add NRES energy output fields
#     # arcpy.AddField_management(parcel_fc, "NRES_BTU", "DOUBLE")
#     # par_fields = [parcel_lu_field, parcel_sqft_field, "NRES_BTU"]
#     par_fields = [parcel_id_field, parcel_lu_field, parcel_sqft_field]
#     df_cols = [parcel_id_field, "NRES_BTU"]
#
#     # Update parcels
#     out_rows = []
#     with arcpy.da.SearchCursor(parcel_fc, par_fields) as c:
#         for r in c:
#             par_id, lu, sqft = r
#             if lu is None or sqft is None:
#                 out_rows.append((par_id, 0))
#                 continue
#             # Get btu multiplier for this lu
#             fltr = energy_df[energy_lu_field] == lu
#             sel = energy_df[fltr][energy_sqft_field]
#             try:
#                 factor = sel.iloc[0]
#             except IndexError:
#                 factor = 0
#             BTU = sqft * factor
#             out_rows.append((par_id, BTU))
#
#     df = pd.DataFrame(out_rows, columns=df_cols)
#     PMT.extendTableDf(out_table, out_id_field, df, parcel_id_field)
#     return out_table
