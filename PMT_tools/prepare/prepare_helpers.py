# global configurations
from PMT_tools.config.prepare_config import (CRASH_CODE_TO_STRING, CRASH_CITY_CODES,
                                             CRASH_SEVERITY_CODES, CRASH_HARMFUL_CODES)
from PMT_tools.config.prepare_config import (PERMITS_CAT_CODE_PEDOR, PERMITS_STATUS_DICT, PERMITS_FIELDS_DICT,
                                             PERMITS_USE, PERMITS_DROPS, )
from PMT_tools.prepare.preparer import RIF_CAT_CODE_TBL, DOR_LU_CODE_TBL
from PMT_tools.config.prepare_config import PARCEL_COMMON_KEY
from PMT_tools.config.prepare_config import MAZ_COMMON_KEY, TAZ_COMMON_KEY

from PMT_tools.PMT import PMT as PMT # Think we need everything here
from PMT import gdfToFeatureClass, dfToPoints, extendTableDf, makePath, CLEANED
from PMT import Comp, And, Or
from PMT import _loadLocations, _solve, _rowsToCsv


import PMT_tools.logger as log

from datetime import time
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
import tempfile as tempfile
import zipfile
import fnmatch
import json
from json.decoder import JSONDecodeError
import os
import uuid
import arcpy
from six import string_types

logger = log.Logger(add_logs_to_arc_messages=True)


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
    -------
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


def geojson_to_feature_class(geojson_path, geom_type, encoding='utf8'):
    if validate_json(json_file=geojson_path, encoding=encoding):
        try:
            # convert json to temp feature class
            unique_name = str(uuid.uuid4().hex)
            temp_feature = makePath("in_memory", f"_{unique_name}")
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
    df = pd.read_csv(filepath_or_buffer=csv_file, usecols=use_cols, thousands=",")
    df = df.convert_dtypes()
    df.rename(columns=rename_dict, inplace=True)
    return df


def split_date(gdf, date_field, unix_time=False):
    """
    ingest date attribute and splits it out to DAY, MONTH, YEAR
    Parameters
    ----------
    unix_time
    gdf: GeoDataFrame
        GeoDataFrame with a date field
    date_field: column name
    GeoDataFrame    Returns
    -------
        GeoDataFrame reformatted to include split day, month and year
    """
    # convert unix time to date
    if unix_time:
        gdf[date_field] = gdf[date_field].apply(lambda x: str(x)[:10])
        gdf[date_field] = gdf[date_field].apply(
            lambda x: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(x)))
        )
    # otherwise infer
    else:
        gdf[date_field] = pd.to_datetime(arg=gdf[date_field], infer_datetime_format=True)
    gdf["DAY"] = gdf[date_field].dt.day
    gdf["MONTH"] = gdf[date_field].dt.month
    gdf["YEAR"] = gdf[date_field].dt.year
    return gdf


def geojson_to_gdf(geojson, crs, use_cols=None, rename_dict=None):
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
    if rename_dict is None:
        rename_dict = []
    if use_cols is None:
        use_cols = []
    with open(str(geojson), "r") as src:
        js = json.load(src)
        gdf = gpd.GeoDataFrame.from_features(js["features"], crs=crs, columns=use_cols)
        gdf.rename(columns=rename_dict, inplace=True)
    return gdf


def polygon_to_points_arc(in_fc, id_field=None, point_loc="INSIDE"):
    try:
        # convert json to temp feature class
        unique_name = str(uuid.uuid4().hex)
        temp_feature = makePath("in_memory", f"_{unique_name}")
        arcpy.FeatureToPoint_management(in_features=in_fc, out_feature_class=temp_feature,
                                        point_location=point_loc)
        if id_field:
            clean_and_drop(feature_class=temp_feature, use_cols=[id_field])
        return temp_feature
    except:
        logger.log_msg("something went wrong converting polygon to points")
        logger.log_error()


def polygon_to_points_gpd(poly_fc, id_field=None):
    # TODO: add validation and checks
    poly_df = gpd.read_file(poly_fc)
    if id_field:
        columns = poly_df.columns
        drops = [col for col in columns if col != id_field]
        poly_df = poly_df.drop(columns=drops)
    pts_df = poly_df.copy()
    pts_df['geometry'] = pts_df['geometry'].centroid
    return pts_df


def add_xy_from_poly(poly_fc, poly_key, table_df, table_key):
    pts = polygon_to_points_arc(in_fc=poly_fc, id_field=poly_key)
    with tempfile.TemporaryDirectory() as temp_dir:
        t_geoj = makePath(temp_dir, "temp.geojson")
        arcpy.FeaturesToJSON_conversion(in_features=pts, out_json_file=t_geoj,
                                        geoJSON="GEOJSON", outputToWGS84="WGS84")
        with open(t_geoj, "r") as j_file:
            pts_gdf = gpd.read_file(j_file)
    esri_ids = ["OBJECTID", "FID"]
    if esri_ids.issubset(pts_gdf.columns).any():
        pts_gdf.drop(labels=esri_ids, axis=1, inplace=True, errors='ignore')
    # join permits to parcel points MANY-TO-ONE
    logger.log_msg('...merging polygon data to tabular')
    pts = table_df.merge(right=pts_gdf, how="inner", on=table_key)
    return gpd.GeoDataFrame(pts, geometry="geometry")


def clean_and_drop(feature_class, use_cols=[], rename_dict={}):
    # reformat attributes and keep only useful
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
    """
    In a geodatabase with basic features (station points and corridor
    alignments), create polygon feature classes used for standard mapping
    and summarization. The output feature classes include buffered corridors,
    buffered station areas, and a long file of station points, where each
    station/corridor combo is represented as a separate feature.
    
    Parameters
    -----------
    bf_gdb: Path
        A geodatabase with key basic features, including stations and
        alignments
    stations_fc: String
        A point feature class in`bf_gdb` with station locations and columns
        indicating belonging in individual corridors (i.e., the colum names
        reflect corridor names and flag whether the station is served by that
        corridor).
    stn_diss_fields: [String,...]
        Field(s) on which to dissovle stations when buffering station areas.
        Stations that reflect the same location by different facilities may
        be dissolved by name or ID, e.g. This may occur at intermodal locations.
        For example, where metro rail meets commuter rail - the station points
        may represent the same basic station but have slightly different
        geolocations.
    stn_corridor_fields: [String,...]
        The columns in `stations_fc` that flag each stations belonging in
        various corridors.
    alignments_fc: String
        A line feature class in `bf_gdb` reflecting corridor alignments
    align_diss_fields: [String,...]
        Field(s) on which to dissovle alignments when buffering corridor
        areas.
    stn_buff_dist: Linear Unit, default="2640 Feet"
        A linear unit by which to buffer station points to create station
        area polygons.
    align_buff_dist: Linear Unit, default="2640 Feet"
        A linear unit by which to buffer alignments to create corridor
        polygons
    stn_areas_fc: String, default="Station_Areas"
        The name of the output feature class to hold station area polygons
    corridors_fc: String, default="Corridors"
        The name of the output feature class to hold corridor polygons
    long_stn_fc: String, default="Stations_Long"
        The name of the output feature class to hold station features,
        elongated based on corridor belonging (to support dashboard menus)
    rename_dict: dict, default={}
        If given, `stn_corridor_fields` can be relabeled before pivoting
        to create `long_stn_fc`, so that the values reported in the output
        "Corridor" column are not the column names, but values mapped on
        to the column names (chaging "EastWest" column to "East-West", e.g.)
    overwrite: Boolean, default=False
    
    """
    # TODO: include (All) corridors with Name = (All stations), (Entire corridors), (Outside station areas)
    stn_diss_fields = _listifyInput(stn_diss_fields)
    stn_corridor_fields = _listifyInput(stn_corridor_fields)
    align_diss_fields = _listifyInput(align_diss_fields)

    old_ws = arcpy.env.workspace
    arcpy.env.workspace = bf_gdb

    # Buffer features
    #  - stations (station areas, unique)
    print("... buffering station areas")
    PMT.checkOverwriteOutput(stn_areas_fc, overwrite)
    _diss_flds_ = _stringifyList(stn_diss_fields)
    arcpy.Buffer_analysis(stations_fc, stn_areas_fc, stn_buff_dist,
                          dissolve_option="LIST", dissolve_field=_diss_flds_)
    #  - alignments (corridors, unique)
    print("... buffering corridor areas")
    PMT.checkOverwriteOutput(corridors_fc, overwrite)
    _diss_flds_ = _stringifyList(align_diss_fields)
    arcpy.Buffer_analysis(alignments_fc, corridors_fc, align_buff_dist,
                          dissolve_option="LIST", dissolve_field=_diss_flds_)

    # Elongate stations by corridor (for dashboard displays, selectors)
    print("... elongating station features")
    # - dump to data frame
    fields = stn_diss_fields + stn_corridor_fields
    sr = arcpy.Describe(stations_fc).spatialReference
    fc_path = PMT.makePath(bf_gdb, stations_fc)
    stn_df = pd.DataFrame(
        arcpy.da.FeatureClassToNumPyArray(fc_path, fields + ["SHAPE@X", "SHAPE@Y"])
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
    PMT.dfToPoints(sel_df, long_out_fc, ["SHAPE@X", "SHAPE@Y"],
                   from_sr=sr, to_sr=sr, overwrite=True)

    arcpy.env.workspace = old_ws


def makeSummaryFeatures(bf_gdb, long_stn_fc, corridors_fc, cor_name_field,
                        out_fc, stn_buffer_meters=804.672,
                        stn_name_field="Name", stn_cor_field="Corridor",
                        overwrite=False):
    """
    Creates a single feature class for data summarization based on station
    area and corridor geographies. The output feature class includes each
    station area, all combined station areas, the entire corridor area,
    and the portion of the corridor that is outside station areas.

    Parameters
    --------------
    bf_gdb: Path
    long_stn_fc: String
    corridors_fc: String
    cor_name_field: String
    out_fc: String
    stn_buffer_meters: Numeric, default=804.672 (1/2 mile)
    stn_name_field: String, default="Name"
    stn_cor_field: String, default="Corridor
    overwrite: Boolean, default=False
    """

    old_ws = arcpy.env.workspace
    arcpy.env.workspace = bf_gdb

    sr = arcpy.Describe(long_stn_fc).spatialReference
    mpu = float(sr.metersPerUnit)
    buff_dist = stn_buffer_meters / mpu

    # Make output container - polygon with fields for Name, Corridor
    print(f"... creating output feature class {out_fc}")
    PMT.checkOverwriteOutput(out_fc, overwrite)
    out_path, out_name = os.path.split(out_fc)
    arcpy.CreateFeatureclass_management(
        out_path, out_name, "POLYGON", spatial_reference=sr)
    # - Add fields    
    arcpy.AddField_management(out_fc, "Name", "TEXT", field_length=80)
    arcpy.AddField_management(out_fc, "Corridor", "TEXT", field_length=80)
    arcpy.AddField_management(out_fc, "RowID", "LONG")

    # Add all corridors with name="(Entire corridor)", corridor=cor_name_field
    print("... adding corridor polygons")
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
    print("... adding station polygons by corridor")
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
    print("... adding corridor in-station/non-station polygons")
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
    out_fc = makePath(out_path, out_name)
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
        in_fcs = [geojson_to_feature_class(fc, geom_type=geom)
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
        in_fc = geojson_to_feature_class(in_fc, geom_type=geom)
    out_dir, out_name = os.path.split(out_fc)
    fms = field_mapper(in_fcs=in_fc, use_cols=use_cols, rename_dicts=rename_dict)
    arcpy.FeatureClassToFeatureClass_conversion(in_features=in_fc, out_path=out_dir,
                                                out_name=out_name, field_mapping=fms)


# permit functions
def clean_permit_data(permit_csv, poly_features, permit_key, poly_key, out_file, out_crs):
    """
    reformats and cleans RER road impact permit data, specific to the PMT
    Parameters
    ----------
    permit_csv
    poly_features
    out_file

    Returns
    -------

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
    permit_gdf = add_xy_from_poly(poly_fc=poly_features, poly_key=poly_key,
                                  table_df=permit_df, table_key=permit_key)
    gdfToFeatureClass(gdf=permit_gdf, out_fc=out_file, new_id_field="ROW_ID",
                      exclude=['OBJECTID'], sr=out_crs, overwrite=True)


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
    temp_udb = makePath("in_memory", "UDB_dissolve")
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
    """
    XLS File Desc: Sheet 1 contains header and max rows for a sheet (65536),
                    data continue on subsequent sheets without header
    reads in the provided xls file and due to odd formatting concatenates
    the sheets into a single data frame. The
    Parameters
    ----------
        xls_path: str; String path to xls file
        sheet: str, int, list, or None, default None as we need to read the entire file
        head_row: int, list of int, default None as wee need to read the entire file
        rename_dict: dict; dictionary to map existing column names to more readable names
    Returns
    -------
        pd.Dataframe; pandas dataframe of data from xls
    """
    # TODO: add logic to handle files that dont match existing format
    # TODO:     - example check may be to read all pages and see if row 0
    #               is the same or pages are empty
    # read the xls into dict
    xls_dict = pd.read_excel(xls_path, sheet_name=sheet, header=head_row)
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
    """
    adds new key:value pairs to dictionary given a set of values and
    tags in the keys
    - only valuable in the Transit ridership context
    Parameters
    ----------
    d: dict
    values: [String,...]; output value names
    tag: String; unique tag found in a column

    Returns
    -------
    d: dict
    """
    for value in values:
        d[f"{value}_{tag}"] = value
    return d


def prep_transit_ridership(in_table, rename_dict, shape_fields, from_sr, to_sr, out_fc):
    """
    converts transit ridership data to a feature class and reformats attributes
    Parameters
    ----------
    in_table: Path; string xls file path
    rename_dict: dict; dictionary of existing: new attribute names
    shape_fields: [String,...]; columns to be used as shape field (x,y)
    from_sr: SpatialReference; the spatial reference definition for coordinates listed in 'shape_fields'
    to_sr: SpatialReference; the spatial reference definition for output features
    out_fc: Path

    Returns
    -------
    out_fc: Path
    """
    # on/off data are tagged by the date the file was created
    file_tag = read_transit_file_tag(file_path=in_table)
    rename_dict = update_dict(d=rename_dict, values=["ON", "OFF"], tag=file_tag)
    # read the transit file to dataframe
    transit_df = read_transit_xls(xls_path=in_table, rename_dict=rename_dict)
    # convert table to points
    return dfToPoints(df=transit_df, out_fc=out_fc, shape_fields=shape_fields,
                      from_sr=from_sr, to_sr=to_sr, overwrite=True)


# Parcel data
def clean_parcel_geometry(in_features, fc_key_field, out_features=None):
    try:
        if out_features is None:
            raise ValueError
        else:
            temp_fc = r"in_memory\\temp_fc"
            arcpy.CopyFeatures_management(in_features=in_features, out_feature_class=temp_fc)
            # Repair geom and remove null geoms
            logger.log_msg("...repair geometry")
            arcpy.RepairGeometry_management(in_features=temp_fc, delete_null="DELETE_NULL")
            # Alter field
            arcpy.AlterField_management(in_table=temp_fc, field=fc_key_field,
                                        new_field_name=PARCEL_COMMON_KEY, new_field_alias=PARCEL_COMMON_KEY)
            # Dissolve polygons
            logger.log_msg("...dissolve parcel polygons, and add count of polygons")
            arcpy.Dissolve_management(in_features=temp_fc, out_feature_class=out_features,
                                      dissolve_field=PARCEL_COMMON_KEY,
                                      statistics_fields="{} COUNT".format(PARCEL_COMMON_KEY), multi_part="MULTI_PART")
            return out_features
    except ValueError:
        logger.log_msg("output feature class path must be provided")


def prep_parcel_land_use_tbl(parcels_fc, parcel_lu_field, parcel_fields,
                             lu_tbl, tbl_lu_field, dtype={}, **kwargs):
    """
    Join parcels with land use classifications based on DOR use codes.
    

    Parameters
    ----------
    parcels_fc: Path
    parcel_lu_field: String
        The column in `parcels_fc` with each parcel's DOR use code.
    parcel_fields: [String, ...]
        Other columns in `parcels_fc` (such as an ID field, e.g.) to retain
        alongside land use codes.
    lu_tbl: Path
        A csv table of land use groupings to associated with each parcel,
        keyed by DOR use code.
    tbl_lu_field: String
        The column in `lu_tbl` with DOR use codes.
    dtype: {string: type}
        If any data type specifications are needed to properly parse
        `lu_tbl` provide them as a dictionary.
    **kwargs:
        Any other keyword arguments given are passed to the
        `pd.read_csv` method when reading `lu_tbl`.
    
    Returns
    -------
    par_df: DataFrame
    """
    # Dump parcels_fc to data frame
    if isinstance(parcel_fields, string_types):
        parcel_fields = [parcel_fields]
    par_fields = parcel_fields + [parcel_lu_field]
    par_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(parcels_fc, par_fields) # TODO: null values
    )
    # Read in the land use reference table
    ref_table = pd.read_csv(lu_tbl, dtype=dtype, **kwargs)

    # Join
    return par_df.merge(
        ref_table, how="left", left_on=par_lu_field, right_on=tbl_lu_field)


def enrich_bg_with_parcels(bg_fc, parcels_fc, sum_crit, bg_id_field="GEOID10",
                           par_id_field="PARCELNO", par_lu_field="DOR_UC",
                           par_bld_area="TOT_LVG_AREA", sum_crit=None,
                           par_sum_fields=["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA],
                           ):
    """
    Relates parcels to block groups based on centroid location and summarizes
    key parcel fields to the block group level, including building floor area
    by potential activity type (residential, jobs by type, e.g.).

    Parameters
    ------------
    bg_fc: String; path
    parcels_fc: String; path
    sum_crit: dict
        Dictionary whose keys reflect column names to be generated to hold sums
        of parcel-level data in the output block group data frame, and whose
        values consist of at least one PMT comparator class (`Comp`, `And`).
        These are used to map parcel land use codes to LODES variables, e.g.
        An iterable of comparators in a value implies an `Or` operation.
    par_sum_fields: [String, ...], default=["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA]
        If provided, these parcel fields will also be summed to the block-group level.
    bg_id_field: String, default="GEOID10"
    par_id_field: String, default="PARCELNO"
    par_lu_field: String, default="DOR_UC"
    par_bld_area: String, default="TOT_LVG_AREA"

    Returns
    --------
    bg_df: DataFrame
    """
    # Prep output
    if sum_crit is None:
        sum_crit = {}
    #PMT.checkOverwriteOutput(output=out_tbl, overwrite=overwrite)
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
                    print(f"---  --- no parcels found for BG {bg_id}") #TODO: convert to warning?
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
                    crit = Or(par_df[par_lu_field], sum_crit[grouping])
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
        #PMT.dfToTable(df=bg_df, out_table=out_tbl)
    except:
        raise
    finally:
        arcpy.Delete_management(parcel_fl)


def enrich_bg_with_econ_demog(tbl_path, tbl_id_field, join_tbl, join_id_field, join_fields):
    """
    Adds data from another raw table as new columns based on teh fields provided.
    Parameters
    ----------
    tbl_path: String; path
    tbl_id_field: String; table primary key
    join_tbl: String; path
    join_id_field: String; join table foreign key
    join_fields: [String, ...]; list of fields to include in update

    Returns
    -------
    None
    """
    # TODO: add checks for join_fields as actual columns in join_tbl
    if is_gz_file(join_tbl):
        tbl_df = pd.read_csv(
            join_tbl, usecols=join_fields, compression='gzip')
    else:
        tbl_df = pd.read_csv(join_tbl, usecols=join_fields)
    PMT.extendTableDf(
        in_table=tbl_path,
        table_match_field=tbl_id_field,
        df=tbl_df,
        df_match_field=join_id_field
        )


def prep_parcel_energy_consumption_tbl(in_parcel_lu_tbl, energy_use_field,
                                       res_use_tbl, res_use_btu_field,
                                       nres_use_tbl, nres_use_btu_field):
    """
    applies energy
    Parameters
    ----------
    in_parcel_lu_tbl
    energy_use_field
    res_use_tbl
    res_use_btu_field
    nres_use_tbl
    nres_use_btu_field

    Returns
    -------

    """
    pass


def estimate_parcel_nres_consumption(energy_df, energy_lu_field, energy_sqft_field,
                                     parcel_fc, parcel_lu_field, parcel_sqft_field,
                                     parcel_id_field, out_table, out_id_field):
    """
    Using a table of non-residential energy consumption rates and a parcel
    feature class, estimate non-residential energy consumption based on
    parcel land use and building area.

    Parameters
    -----------
    energy_df: DataFrame; A csv file containing energy consumption rates
                        (btu per square foot) based on building type.
    energy_lu_field: String; The field in `energy_table` that defines building
                        types or land uses.
    energy_sqft_field: String; The field in `energy_table` that records BTU
                        per square foot rates for each building type.
    parcel_fc: Path;  A feature class of parcels
    parcel_lu_field: String; The field in `parcel_fc` that defines each parcel's
                    land use (values correspond to those in `energy_lu_field`).
    parcel_sqft_field: String; The field in `parcel_fc` that records the total
                    floor area of buildings on the parcel.
    parcel_id_field: String
    out_table: Path
    out_id_field: String

    Returns
    --------
    parcel_fc: Path
        `parcel_fc` is modified in place such that a new field `NRES_BTU` is
        added and populated based on building type and floor area.
    """
    # # Read in csv table
    # energy = pd.read_csv(energy_table)
    # energy[energy_lu_field] = energy[energy_lu_field].str.strip()

    # # Add NRES energy output fields
    # arcpy.AddField_management(parcel_fc, "NRES_BTU", "DOUBLE")
    # par_fields = [parcel_lu_field, parcel_sqft_field, "NRES_BTU"]
    par_fields = [parcel_id_field, parcel_lu_field, parcel_sqft_field]
    df_cols = [parcel_id_field, "NRES_BTU"]

    # Update parcels
    out_rows = []
    with arcpy.da.SearchCursor(parcel_fc, par_fields) as c:
        for r in c:
            par_id, lu, sqft = r
            if lu is None or sqft is None:
                out_rows.append((par_id, 0))
                continue
            # Get btu multiplier for this lu
            fltr = energy_df[energy_lu_field] == lu
            sel = energy_df[fltr][energy_sqft_field]
            try:
                factor = sel.iloc[0]
            except IndexError:
                factor = 0
            BTU = sqft * factor
            out_rows.append((par_id, BTU))

    df = pd.DataFrame(out_rows, columns=df_cols)
    extendTableDf(out_table, out_id_field, df, parcel_id_field)
    return out_table


def prep_parcels(in_fc, in_tbl, out_fc, fc_key_field="PARCELNO",
                 tbl_key_field="PARCEL_ID", tbl_renames=None, **kwargs):
    """
        Starting with raw parcel features and raw parcel attributes in a table,
        clean features by repairing invalid geometries, deleting null geometries,
        and dissolving common parcel ID's. Then join attribute data based on
        the parcel ID field, managing column names in the process.

        Parameters
        ------------
        in_fc: Path or feature layer
            A collection of raw parcel features (shapes)
        in_tbl: Path
            A table of raw parcel attributes.
        out_fc: Path
            The path to the output feature class that will contain clean parcel
            geometries with attribute columns joined.
        fc_key_field: String, default="PARCELNO"
            The field in `in_fc` that identifies each parcel feature.
        tbl_key_field: String, default="PARCEL_ID"
            The field in `in_csv` that identifies each parcel feature.
        tbl_renames: dict, default={}
            Dictionary for renaming columns from `in_csv`. Keys are current column
            names; values are new column names.
        kwargs:
            Keyword arguments for reading csv data into pandas (dtypes, e.g.)
    """
    # prepare geometry data
    logger.log_msg("...cleaning geometry")
    if tbl_renames is None:
        tbl_renames = {}
    out_fc = clean_parcel_geometry(in_features=in_fc, fc_key_field=fc_key_field,
                                   out_features=out_fc)
    # Read tabular files
    _, ext = os.path.splitext(in_tbl)
    if ext == ".csv":
        logger.log_msg("...read csv tables")
        par_df = pd.read_csv(filepath_or_buffer=in_tbl, **kwargs)
    elif ext == ".dbf":
        logger.log_msg("...read dbf tables")
        par_df = pd.DataFrame(gpd.read_file(filename=in_tbl))
    else:
        logger.log_msg("input parcel tabular data must be 'dbf' or 'csv'")
    # ensure key is 12 characters with leading 0's for join to geo data
    par_df[tbl_key_field] = par_df[tbl_key_field].map(lambda x: f'{x:0>12}')
    # Rename columns if needed
    logger.log_msg("...renaming columns")
    tbl_renames[tbl_key_field] = PARCEL_COMMON_KEY
    par_df.rename(mapper=tbl_renames, axis=1, inplace=True)

    # Add columns to dissolved features
    print("...joining attributes to features")
    print(par_df.columns)
    extendTableDf(in_table=out_fc, table_match_field=PARCEL_COMMON_KEY,
                  df=par_df, df_match_field=PARCEL_COMMON_KEY)


# impervious surface
def unzip_data_to_temp(zipped_file):
    temp_dir = tempfile.TemporaryDirectory()

    return temp_dir.name


def get_raster_file(folder):
    # Get the name of the raster from within the zip (the .img file), there should be only one
    rast_files = []
    raster_formats = [".img", ".tif"]
    logger.log_msg(f"...finding all raster files of type {raster_formats}")
    try:
        for file in os.listdir(folder):
            for extension in raster_formats:
                if fnmatch.fnmatch(file, f"*{extension}"):
                    rast_files.append(makePath(folder, file))

        if len(rast_files) == 1:
            return rast_files[0]
        else:
            raise ValueError
    except ValueError:
        logger.log_msg("More than one Raster/IMG file is present in the zipped folder")


def prep_imperviousness(zip_path, clip_path, out_dir, out_sr=None):
    '''
    Clean a USGS impervious surface raster by:
        1. Clipping to the bounding box of a study area
        2. Transforming the clipped raster to a desired CRS

    Parameters
    ----------
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

    Returns
    -------
    File will be clipped, transformed, and saved to the save directory; the
    save path will be returned upon completion
    '''
    # temp_unzip_folder = unzip_data_to_temp(zipped_file=zip_path)
    with tempfile.TemporaryDirectory() as temp_unzip_folder:
        logger.log_msg("...unzipping imperviousness raster in temp directory")
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(temp_unzip_folder)
        raster_file = get_raster_file(folder=temp_unzip_folder)

        # define the output file from input file
        rast_name, ext = os.path.splitext(os.path.split(raster_file)[1])
        clipped_raster = makePath(temp_unzip_folder, f"clipped{ext}")

        logger.log_msg("...checking if a transformation of the clip geometry is necessary")
        # Transform the clip geometry if necessary
        raster_sr = arcpy.Describe(raster_file).spatialReference
        clip_sr = arcpy.Describe(clip_path).spatialReference
        if raster_sr != clip_sr:
            logger.log_msg("...reprojecting clipping geometry to match raster")
            project_file = makePath(temp_unzip_folder, "Project.shp")
            arcpy.Project_management(in_dataset=clip_path, out_dataset=project_file, out_coor_system=raster_sr)
            clip_path = project_file

        # Grab the bounding box of the clipping file
        logger.log_msg("...clipping raster data to project extent")
        bbox = arcpy.Describe(clip_path).Extent
        arcpy.Clip_management(in_raster=raster_file, rectangle="", out_raster=clipped_raster,
                              in_template_dataset=bbox.polygon, clipping_geometry="ClippingGeometry")

        # Transform the clipped raster
        logger.log_msg("...copying/reprojecting raster out to project CRS")
        out_sr = arcpy.SpatialReference(out_sr)
        out_raster = makePath(out_dir, f"{rast_name}_clipped{ext}")
        if out_sr != raster_sr:
            arcpy.ProjectRaster_management(in_raster=clipped_raster, out_raster=out_raster,
                                           out_coor_system=out_sr, resampling_type="NEAREST")
        else:
            arcpy.CopyRaster_management(in_raster=clipped_raster, out_rasterdataset=out_raster)
    return out_raster

# MAZ/TAZ data prep helpers
def estimate_maz_from_parcels(par_fc, par_id_field, maz_fc, maz_id_field,
                              taz_id_field, se_data, se_id_field, agg_cols,
                              consolidations):
    """
    Estimate jobs, housing, etc. at the MAZ level based on underlying parcel
    data. 

    Parameters
    ------------
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
    --------
    DataFrame

    See Also
    --------
    PMT.AggColumn
    PMT.Consolidation
    """
    # intersect
    int_fc = PMT.intersectFeatures(maz_fc, par_fc)
    # Join
    PMT.joinAttributes(int_fc, par_id_field, se_data, se_id_field, "*")
    # Summarize
    gb_cols = [Column(maz_id_field), Column(taz_id_field)]
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
    -----------
    df: DataFrame
    base_fields: [String, ...]
        Field(s) in `df` that are not subject to consolidation but which 
        are to be retained in the returned data frame.
    consolidations: [Consolidation, ...]
        Specifications for output columns that consolidate columns
        found in `df`.

    Returns
    --------
    clean_df: DataFrame
        A new data frame with columns reflecting `base_field` and
        `consolidations`.
    
    See Also
    ----------
    PMT.Consolidation
    """
    if isinstance(base_fields, str):
        base_fields = [base_fields]

    clean_cols = base_fields + [c.name for c in consolidations]
    for c in consolidations:
        df[c.name] = df[c.input_cols].agg(c.cons_method, axis=1)

    clean_df = df[clean_cols].copy()
    return clean_df


def patch_local_regional_maz(maz_par_df, maz_df):
    """
    Create a regionwide MAZ socioeconomic/demographic data frame based
    on parcel-level and MAZ-level data. Where MAZ features do not overlap
    with parcels, use MAZ-level data.

    Parameters
    -----------
    
    """
    # Create a filter to isolate MAZ features having parcel overlap
    patch_fltr = np.in1d(maz_df[MAZ_COMMON_KEY], maz_par_df[MAZ_COMMON_KEY])
    matching_rows =  maz_df[patch_fltr].copy()
    # Join non-parcel-level data (school enrollments, e.g.) to rows with
    #  other columns defined by parcel level data, creating a raft of
    #  MAZ features with parcel overlap that have all desired columns
    all_par = maz_par_data.merge(
        matching_rows, how="inner", on=MAZ_COMMON_KEY, suffixes=("", "_M"))
    # Drop extraneous columns generated by the join
    drop_cols = [c for c in all_par.columns if c[-2:] == "_M"]
    if drop_cols:
        all_par.drop(columns=drop_cols, inplace=True)
    # MAZ features with no parcel overlap already have all attributes
    #  and can be combined with the `all_par` frame created above
    return pd.concat([all_par, maz_data[~patch_fltr]])


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


def copy_net_result(net_by_year, target_year, solved_years, fc_names):
    """
    Since some PMT years use the same OSM network, a solved network analysis
    can be copied from one year to another to avoid redundant processing.
    
    This function is a helper function called by PMT wrapper functions. It
    is not intended to be run indepenently.
    
    Parameters
    ------------
    net_by_year: dict
        A dictionary with keys reflecting PMT analysis years and values
        reflecting the OSM network vintage to apply to each year.
    target_year: Var
        The PMT analysis year being analyzed
    solved_years: [Var, ...]
        A list of PMT anlaysis years that have already been solved
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
    # Set a source to copy network analysis results from based on net_by_year
    target_net = net_by_year[year]
    source_year = None
    for solved_year in solved_years:
        solved_net = net_by_year[solved_year]
        if solved_net == target_net:
            source_year = solved_year
            break
    source_fds = makePath(CLEANED, f"PMT_{source_year}.gdb", "Networks")
    target_fds = makePath(CLEANED, f"PMT_{target_year}.gdb", "Networks")
    # Copy feature classes
    print(f"- copying results from {copy_fd} to {fd}")
    for fc_name in fc_names:
        print(f" - - {fc_name}")
        src_fc = makePath(source_fds, fc_name)
        tgt_fc = makePath(target_fds, fc_name)
        if arcpy.Exists(tgt_fc):
            arcpy.Delete_management(tgt_fc)
        arcpy.FeatureClassToFeatureClass_conversion(
            src_fc, target_fds, fc_name)


    for mode in ["walk", "bike"]:
        for dest_grp in ["stn", "parks"]:
            for run in ["MERGE", "NO_MERGE", "OVERLAP", "NON_OVERLAP"]:
                fc_name = f"{mode}_to_{dest_grp}_{run}"
                
                


def lines_to_centrality(line_feaures, impedance_attribute):
    """
    Using the "lines" layer output from an OD matrix problem, calculate
    node centrality statistics and store results in a csv table.

    Parameters
    -----------
    line_features: ODMatrix/Lines feature layer
    impedance_attribute: String
    out_csv: Path
    header: Boolean
    mode: String
    """
    imp_field = f"Total_{impedance_attribute}"
    # Dump to df
    df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(
            line_features, ["Name", imp_field]
        )
    )
    names = ["N", "Node"]
    df[names] = df["Name"].str.split(" - ", n=1, expand=True)
    # Summarize
    sum_df = df.groupby("Node").agg(
        {"N": "size", imp_field: sum}
        ).reset_index()
    # Calculate centrality
    sum_df["centrality"] = (sum_df.N - 1)/sum_df[imp_field]
    # Add average length
    sum_df["AvgLength"] = 1/sum_df.centrality
    # Add centrality index
    sum_df["CentIdx"] = sum_df.N/sum_df.AvgLength
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
    #Step 2 - add all origins
    print("Load all origins")
    in_features = in_features
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
        snap_offset=net_loader.snap_offset,
        exclude_restricted_elements=net_loader.exclude_restricted,
        search_query=net_loader.search_query
        )
    # Step 3 - iterate through destinations
    print("Iterate destinations and solve")
    # Use origin field maps to expedite loading
    fm = "Name Name #;CurbApproach CurbApproach 0;SourceID SourceID #;SourceOID SourceOID #;PosAlong PosAlong #;SideOfEdge SideOfEdge #"
    for chunk in PMT.iterRowsAsChunks(
        "OD Cost Matrix\Origins", chunksize=chunksize):
        # printed dots track progress over chunks
        print(".", end="")
        arcpy.AddLocations_na(
            in_network_analysis_layer="OD Cost Matrix",
            sub_layer="Destinations",
            in_table=chunk,
            field_mappings=fm
            )
        # Solve OD Matrix
        arcpy.Solve_na("OD Cost Matrix", "SKIP", "CONTINUE")
        # Dump to df
        line_features = "OD Cost Matrix\Lines"
        temp_df = lines_to_centrality(line_features, impedance_attribute)
        # Stack df results
        results.append(temp_df)
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
    arcpy.AddField_management(
        in_table, bin_field, "TEXT", field_length=20)
    arcpy.CalculateField_management(
        in_table, bin_field, f"assignBin(!{time_field}!)",
        expression_type="PYTHON3", code_block=code_block
    )


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
    print("... intersecting parcels and network outputs")
    int_fc = "in_memory\\par_wt_sj"
    int_fc = arcpy.SpatialJoin_analysis(parcel_fc, ref_fc, int_fc,
                                        join_operation="JOIN_ONE_TO_MANY",
                                        join_type="KEEP_ALL",
                                        match_option="WITHIN_A_DISTANCE",
                                        search_radius="80 Feet")
    # Summarize
    print(f"... summarizing by {parcel_id_field}, {ref_name_field}")
    sum_tbl = "in_memory\\par_wt_sj_sum"
    statistics_fields = [[ref_time_field, "MIN"], [ref_time_field, "MEAN"]]
    case_fields = [parcel_id_field, ref_name_field]
    sum_tbl = arcpy.Statistics_analysis(
        int_fc, sum_tbl, statistics_fields, case_fields)
    # Delete intersect features
    arcpy.Delete_management(int_fc)

    # Dump sum table to data frame
    print("... converting to data frame")
    sum_fields = [f"MEAN_{ref_time_field}"]
    dump_fields = [parcel_id_field, ref_name_field] + sum_fields
    int_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(sum_tbl, dump_fields)
    )
    int_df.columns = [parcel_id_field, ref_name_field, ref_time_field]
    # Delete summary table
    arcpy.Delete_management(sum_tbl)

    # Summarize
    print("... summarizing times")
    int_df = int_df.set_index(ref_name_field)
    gb = int_df.groupby(parcel_id_field)
    which_name = gb.idxmin()
    min_time = gb.min()
    number = gb.size()

    # Export output table
    print("... saving walk time results")
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
        print("... estimating ideal times")
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
                par_df["minutes"] = (par_df.meters * 60)/(assumed_mph * 1609.344)
                # store in mini df output
                tgt_results.append(par_df[out_fields].copy())
        # Bind up results
        print("... binding results")
        bind_df = pd.concat(tgt_results).set_index(target_name_field)
        # Group by/summarize
        print("... summarizing times")
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
    print("... ... ... binning skims")
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
    print("... ... ... bin columns")
    pivot_fields = [gb_field, bin_field] + act_fields
    pivot = pd.pivot_table(
        out_df[pivot_fields], index=gb_field, columns=bin_field)
    pivot.columns = PMT.colMultiIndexToNames(pivot.columns, separator="")
    # - Summarize
    print("... ... ... average time by activitiy")
    sum_df = out_df[sum_fields].groupby(gb_field).sum()
    avg_fields = []
    for act_field, prod_field in zip(act_fields, prod_fields):
        avg_field = f"Avg{units}{act_field}"
        avg_fields.append(avg_field)
        sum_df[avg_field] = sum_df[prod_field]/sum_df[act_field]
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

    print("... ...OD MATRIX: create network problem")
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
        _loadLocations(net_layer_, "Destinations", dest_pts, dest_name_field,
                       net_loader, d_location_fields)
        # Iterate solves as needed
        if o_chunk_size is None:
            o_chunk_size = arcpy.GetCount_management(origin_pts)[0]
        write_mode = "w"
        header = True
        for o_pts in PMT.iterRowsAsChunks(origin_pts, chunksize=o_chunk_size):
            _loadLocations(net_layer_, "Origins", o_pts, origin_name_field,
                           net_loader, o_location_fields)
            s = _solve(net_layer_)
            print("... ... solved, dumping to data frame")
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
        print("... ...deleting network problem")
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
    # Summarize trips, total trip mileage by TAZ
    #

    return taz_stats_df


# TODO: verify functions generally return python objects (dataframes, e.g.) and leave file writes to `preparer.py`