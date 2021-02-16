# global configurations
from PMT_tools.config.prepare_config import (CRASH_CODE_TO_STRING, CRASH_CITY_CODES,
                                             CRASH_SEVERITY_CODES, CRASH_HARMFUL_CODES)
from PMT_tools.config.prepare_config import (PERMITS_CAT_CODE_PEDOR, PERMITS_STATUS_DICT, PERMITS_FIELDS_DICT,
                                             PERMITS_USE, PERMITS_DROPS, )
from PMT_tools.prepare.preparer import RIF_CAT_CODE_TBL, DOR_LU_CODE_TBL
from PMT_tools.config.prepare_config import PARCEL_COMMON_KEY

from PMT_tools.PMT import gdfToFeatureClass, dfToPoints, extendTableDf, makePath, dfToTable, add_unique_id, intersectFeatures, featureclass_to_df, copyFeatures

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
import re
from sklearn import linear_model
import scipy
import arcpy

logger = log.Logger(add_logs_to_arc_messages=True)


# general use functions
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


def prep_parcel_land_use_tbl(in_fc, fc_lu_field, lu_tbl, tbl_lu_field):
    """
    join parcels with dor use codes and output table
    Parameters
    ----------
    in_fc
    fc_lu_field
    lu_tbl
    tbl_lu_field
    Returns
    -------

    """
    pass


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


def analyze_blockgroup_model(bg_enrich_path,
                             acs_years,
                             lodes_years,
                             save_directory):
    '''
    fit linear models to block group-level total employment, population, and
    commutes at the block group level, and save the model coefficients for 
    future prediction

    Parameters
    ----------
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
        
    Notes
    -----
    in `bg_enrich_path`, replace the presence of a year with the string 
    "{year}". For example, if your enriched block group data for 2010-2015 is 
    stored at "Data_2010.gdb/enriched", "Data_2010.gdb/enriched", ..., then 
    `bg_enrich_path = "Data_{year}.gdb/enriched"`.

    Returns
    -------
    save_path : str
        path to a table of model coefficients

    '''
    
    # 1. Read 
    # -------
    
    logger.log_msg("...reading input data (block group)")
    df = []
    years = np.unique(np.concatenate([acs_years, lodes_years]))
    for y in years:
        logger.log_msg(' '.join(["----> Loading", str(y)]))
        # Read
        load_path = re.sub("{year}", str(y), bg_enrich_path,
                           flags = re.IGNORECASE)
        fields = [f.name for f in arcpy.ListFields(load_path)]
        tab = arcpy.da.FeatureClassToNumPyArray(in_table = load_path,
                                                field_names = fields,
                                                null_value = 0)
        tab = pd.DataFrame(tab)
        # Edit
        tab["Year"] = y
        tab["Since_2013"] = y - 2013
        tab["Total_Emp_Area"] = (
            tab["CNS_01_par"] + tab["CNS_02_par"] + tab["CNS_03_par"] + 
            tab["CNS_04_par"] + tab["CNS_05_par"] + tab["CNS_06_par"] + 
            tab["CNS_07_par"] + tab["CNS_08_par"] + tab["CNS_09_par"] + 
            tab["CNS_10_par"] + tab["CNS_11_par"] + tab["CNS_12_par"] + 
            tab["CNS_13_par"] + tab["CNS_14_par"] + tab["CNS_15_par"] + 
            tab["CNS_16_par"] + tab["CNS_17_par"] + tab["CNS_18_par"] + 
            tab["CNS_19_par"] + tab["CNS_20_par"]
        )
        if y in lodes_years:
            tab["Total_Employment"] = (
                tab["CNS01"] + tab["CNS02"] + tab["CNS03"] + tab["CNS04"] + 
                tab["CNS05"] + tab["CNS06"] + tab["CNS07"] + tab["CNS08"] + 
                tab["CNS09"] + tab["CNS10"] + tab["CNS11"] + tab["CNS12"] + 
                tab["CNS13"] + tab["CNS14"] + tab["CNS15"] + tab["CNS16"] + 
                tab["CNS17"] + tab["CNS18"] + tab["CNS19"] + tab["CNS20"]
            )
        if y in acs_years:
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
    independent_variables = ["LND_VAL", "LND_SQFOOT", "JV", "TOT_LVG_AREA" ,
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
    logger.log_msg("...replacing missing values")
    parcel_na_dict = {iv: 0 for iv in independent_variables}
    df.fillna(parcel_na_dict,
              inplace=True)
    df.loc[(df.Total_Employment.isna()) & df.Year.isin(lodes_years), "Total_Employment"] = 0
    df.loc[(df.Total_Population.isna()) & df.Year.isin(acs_years), "Total_Population"] = 0
    df.loc[(df.Total_Commutes.isna()) & df.Year.isin(acs_years), "Total_Commutes"] = 0
    keep_cols = ["GEOID10", "Year"] + independent_variables + list(response.keys())
    df = df[keep_cols]
    
    # Step 2: conduct modeling by extracting a correlation matrix between candidate
    # explanatories and our responses, identifying explanatories with significant
    # correlations to our response, and fitting a MLR using these explanatories
    logger.log_msg("...fitting and applying models")
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
        t_stat = cwr * np.sqrt((n-2) / (1-cwr**2))
        p_values = pd.Series(scipy.stats.t.sf(t_stat, n-2) * 2,
                             index = t_stat.index)
        # Variables for the model
        mod_vars = []
        cutoff = 0.05
        while len(mod_vars) == 0:
            mod_vars = p_values[p_values.le(cutoff)].index.tolist()
            cutoff += 0.05
        # Fit a multiple linear regression
        regr = linear_model.LinearRegression()
        regr.fit(X = mdf[mod_vars],
                 y = mdf[[key]])
        # Save the model coefficients
        fits.append(pd.Series(regr.coef_[0],
                              index = mod_vars,
                              name = key))
        
    # Step 3: combine results into a single df
    logger.log_msg("...formatting model coefficients into a single table")
    coefs = pd.concat(fits, axis=1).reset_index()
    coefs.rename(columns = {"index": "Variable"},
                 inplace=True)
    coefs.fillna(0,
                 inplace=True)
    
    # 3. Write
    # --------
    
    logger.log_msg("...writing results")
    save_path = makePath(save_directory, 
                         "block_group_model_coefficients.csv")
    coefs.to_csv(save_path,
                 index = False)
    
    # Done
    # ----
    return save_path 

    
def analyze_blockgroup_apply(year,
                             bg_enrich_path,
                             bg_geometry_path,
                             model_coefficients_path,
                             save_gdb_location,
                             shares_from = None):
    '''
    predict block group-level total employment, population, and commutes using
    pre-fit linear models, and apply a shares-based approach to subdivide
    totals into relevant subgroups

    Parameters
    ----------
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

    Returns
    -------
    save_path : str
        path to a table of model application results

    '''
    
    # 1. Read 
    # -------
    
    logger.log_msg("...reading input data (block group)")
    # Load
    fields = [f.name for f in arcpy.ListFields(bg_enrich_path)]
    df = arcpy.da.FeatureClassToNumPyArray(in_table = bg_enrich_path,
                                            field_names = fields,
                                            null_value = 0)
    df = pd.DataFrame(df)
    # Edit
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
    independent_variables = ["LND_VAL", "LND_SQFOOT", "JV", "TOT_LVG_AREA" ,
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
    df.fillna(parcel_na_dict,
              inplace=True)
    
    # 2. Apply models
    # ---------------
    
    logger.log_msg("...applying models to predict totals")
    # Load the coefficients
    coefs = pd.read_csv(model_coefficients_path)
    # Predict using matrix multiplication
    mod_inputs = df[coefs["Variable"]]
    coef_values = coefs.drop(columns = "Variable")
    preds = np.matmul(mod_inputs.to_numpy(), coef_values.to_numpy())
    preds = pd.DataFrame(preds)
    preds.columns = coef_values.columns.tolist()
    pwrite = pd.concat([df[["GEOID10"]], preds], axis=1)
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
    logger.log_msg("...formatting shares data")
    # Format
    if shares_from is not None:
        if "LODES" in shares_from.keys():
            lodes = arcpy.da.FeatureClassToNumPyArray(in_table = shares_from["LODES"],
                                                      field_names = ["GEOID10"] + dependent_variables_emp,
                                                      null_value = 0)
            lodes = pd.DataFrame(lodes)
        else:
            lodes = df[["GEOID10"] + dependent_variables_emp]
        if "ACS" in shares_from.keys():
            acs = arcpy.da.FeatureClassToNumPyArray(in_table = shares_from["ACS"],
                                                    field_names = ["GEOID10"] + acs_vars,
                                                    null_value = 0)
            acs = pd.DataFrame(acs)
        else:
            acs = df[["GEOID10"] + acs_vars]
    # Merge and replace NA
    shares_df = pd.merge(lodes, acs, on="GEOID10", how="left")
    shares_df.fillna(0,
                     inplace=True)
    
    # Step 2: Calculate shares relative to total
    # This is done relative to the "Total" variable for each group 
    logger.log_msg("...calculating shares")
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
        sdf["GEOID10"] = shares_df["GEOID10"]
        sdf.drop(columns = "TOTAL", 
                 inplace=True)
        shares_dict[name] = sdf
    
    # Step 3: some rows have NA shares because the total for that class of
    # variables was 0. For these block groups, take the average share of all
    # block groups that touch that one
    logger.log_msg("...estimating missing shares")
    # What touches what?    
    arcpy.PolygonNeighbors_analysis(in_features = bg_geometry_path,
                                    out_table = "in_memory\\neighbors", 
                                    in_fields = "GEOID10")
    touch = arcpy.da.FeatureClassToNumPyArray(in_table = "in_memory\\neighbors",
                                              field_names = ["src_GEOID10","nbr_GEOID10"])
    touch = pd.DataFrame(touch)
    touch.rename(columns = {"src_GEOID10": "GEOID10",
                            "nbr_GEOID10": "Neighbor"},
                 inplace=True)
    # Loop filling of NA by mean of adjacent non-NAs
    ctf = 1
    i = 1
    while(ctf > 0):
        # First, identify cases where we need to fill NA        
        to_fill = []
        for key, value in shares_dict.items():
            f = value[value.isna().any(axis=1)]
            f = f[["GEOID10"]]
            f["Fill"] = key
            to_fill.append(f)
        to_fill = pd.concat(to_fill, ignore_index=True)
        # Create a neighbors table
        nt = pd.merge(to_fill,
                      touch,
                      how = "left",
                      on = "GEOID10")
        nt.rename(columns = {"GEOID10": "Source",
                             "Neighbor": "GEOID10"},
                  inplace=True)
        # Now, merge in the shares data for appropriate rows
        fill_by_touching = {}
        nrem = []
        for key, value in shares_dict.items():
            fill_df = pd.merge(nt[nt.Fill == key],
                               value,
                               how = "left",
                               on = "GEOID10")
            nv = fill_df.groupby("Source").mean()
            nv["RS"] = nv.sum(axis=1)
            data_cols = [c for c in nv.columns.tolist() if c != "GEOID10"]
            for d in data_cols:
                nv[d] = nv[d] / nv["RS"]
            nv.drop(columns = "RS",
                    inplace=True)
            nv = nv.reset_index()
            nv.rename(columns = {"Source":"GEOID10"},
                      inplace=True)
            not_replaced = value[~value.GEOID10.isin(nv.GEOID10)]
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
    logger.log_msg("...merging and formatting shares")
    filled_shares = [df.set_index("GEOID10") for df in shares_dict.values()]
    cs_shares = pd.concat(filled_shares, axis=1).reset_index()
    cs_shares.rename(columns = {"index":"GEOID10"},
                     inplace=True)
    
    # 4. Block group estimation
    # -------------------------
    
    logger.log_msg("...estimating variable levels using model estimates and shares")
    # Now, our allocations are simple multiplication problems! Hooray!
    # So, all we have to do is multiply the shares by the appropriate column
    # First, we'll merge our estimates and shares
    alloc = pd.merge(pwrite, 
                     cs_shares, 
                     on = "GEOID10")
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
    
    logger.log_msg("...writing outputs")
    # Here we write block group for allocation
    save_path = makePath(save_gdb_location,
                         "Modeled_blockgroups")
    dfToTable(df = alloc, 
              out_table = save_path)
        
    # Done
    # ----
    return save_path


def analyze_blockgroup_allocate(parcel_fc, bg_modeled, bg_geom, out_gdb,
                                parcels_id="FOLIO", parcel_lu="DOR_UC", parcel_liv_area="TOT_LVG_AREA"):
    """
    Allocate block group data to parcels using relative abundances of
    parcel building square footage
    
    Parameters 
    ----------
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
        
    
    Returns
    -------
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
    block_group_attrs = ["GEOID10"] + lodes_attrs + demog_attrs + commute_attrs
    
    # Initialize spatial processing by intersecting
    print("...intersecting blocks and parcels")
    bg_spatial = makePath("in_memory", "bg_spatial")
    copyFeatures(bg_geom, bg_spatial)
    arcpy.AddJoin_management(bg_spatial, "GEOID10", bg_modeled, "GEOID10")
    parcel_fields = [parcels_id, parcel_lu, parcel_liv_area, "Shape_Area"]
    intersect_fc = intersectFeatures(summary_fc=bg_spatial,
                                     disag_fc=parcel_fc, disag_fields=parcel_fields)
    intersect_fields = parcel_fields + block_group_attrs
    intersect_df = featureclass_to_df(in_fc=intersect_fc, keep_fields=intersect_fields)

    # Format data for allocation
    logger.log_msg("...formatting block group for allocation data")
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
    print("-- setting up activity-land use matches...")
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
    logger.log_msg("...initializing living area sums")
    count_parcels_bg = intersect_df.groupby(['GEOID10'])['GEOID10'].agg(['count'])
    count_parcels_bg.rename(columns={'count': 'NumParBG'}, inplace=True)
    count_parcels_bg = count_parcels_bg.reset_index()

    # Now we can begin totaling living area. We'll start with jobs
    logger.log_msg("...totaling living area by job type")
    # 1. get count of total living area (w.r.t. land use mask) for each
    # job type
    pldaf = "Shape_Area"
    for var in lodes_attrs:
        # mask by LU, group on GEOID10
        area = intersect_df[lu_mask[var]].groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
        area.rename(columns={'sum': f'{var}_Area'},
                    inplace=True)
        area = area[area[f'{var}_Area'] > 0]
        area = area.reset_index()
        area[f'{var}_How'] = "lu_mask"
        missing = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
        if (len(missing) > 0):
            lev1 = intersect_df[all_non_res["NR"]]
            lev1 = lev1[lev1.GEOID10.isin(missing)]
            area1 = lev1.groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
            area1.rename(columns={'sum': f'{var}_Area'},
                         inplace=True)
            area1 = area1[area1[f'{var}_Area'] > 0]
            area1 = area1.reset_index()
            area1[f'{var}_How'] = "non_res"
            area = pd.concat([area, area1])
            missing1 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
            if (len(missing1) > 0):
                lev2 = intersect_df[all_developed["AD"]]
                lev2 = lev2[lev2.GEOID10.isin(missing1)]
                area2 = lev2.groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
                area2.rename(columns={'sum': f'{var}_Area'},
                             inplace=True)
                area2 = area2[area2[f'{var}_Area'] > 0]
                area2 = area2.reset_index()
                area2[f'{var}_How'] = "all_dev"
                area = pd.concat([area, area2])
                missing2 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
                if (len(missing2) > 0):
                    lev3 = intersect_df[intersect_df.GEOID10.isin(missing2)]
                    area3 = lev3.groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
                    area3.rename(columns={'sum': f'{var}_Area'},
                                 inplace=True)
                    area3 = area3[area3[f'{var}_Area'] > 0]
                    area3 = area3.reset_index()
                    area3[f'{var}_How'] = "living_area"
                    area = pd.concat([area, area3])
                    missing3 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
                    if (len(missing3) > 0):
                        lev4 = intersect_df[intersect_df.GEOID10.isin(missing3)]
                        area4 = lev4.groupby(['GEOID10'])[pldaf].agg(['sum'])
                        area4.rename(columns={'sum': f'{var}_Area'},
                                     inplace=True)
                        area4 = area4.reset_index()
                        area4[f'{var}_How'] = "land_area"
                        area = pd.concat([area, area4])
        area = area.reset_index(drop=True)
        count_parcels_bg = pd.merge(count_parcels_bg, area,
                                    how='left',
                                    on='GEOID10')

    # Repeat the above with population
    logger.log_msg("...totaling living area for population")
    area = intersect_df[lu_mask['Population']].groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
    area.rename(columns={'sum': 'Population_Area'},
                inplace=True)
    area = area[area['Population_Area'] > 0]
    area = area.reset_index()
    area['Population_How'] = "lu_mask"
    missing1 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
    if (len(missing1) > 0):
        lev2 = intersect_df[all_developed["AD"]]
        lev2 = lev2[lev2.GEOID10.isin(missing1)]
        area2 = lev2.groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
        area2.rename(columns={'sum': 'Population_Area'},
                     inplace=True)
        area2 = area2[area2['Population_Area'] > 0]
        area2 = area2.reset_index()
        area2['Population_How'] = "all_dev"
        area = pd.concat([area, area2])
        missing2 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
        if (len(missing2) > 0):
            lev3 = intersect_df[intersect_df.GEOID10.isin(missing2)]
            area3 = lev3.groupby(['GEOID10'])[parcel_liv_area].agg(['sum'])
            area3.rename(columns={'sum': 'Population_Area'},
                         inplace=True)
            area3 = area3[area3['Population_Area'] > 0]
            area3 = area3.reset_index()
            area3['Population_How'] = "living_area"
            area = pd.concat([area, area3])
            missing3 = list(set(count_parcels_bg.GEOID10) - set(area.GEOID10))
            if (len(missing3) > 0):
                lev4 = intersect_df[intersect_df.GEOID10.isin(missing3)]
                area4 = lev4.groupby(['GEOID10'])[pldaf].agg(['sum'])
                area4.rename(columns={'sum': 'Population_Area'},
                             inplace=True)
                area4 = area4.reset_index()
                area4['Population_How'] = "land_area"
                area = pd.concat([area, area4])
    area = area.reset_index(drop=True)
    count_parcels_bg = pd.merge(count_parcels_bg, area,
                                how='left',
                                on='GEOID10')

    # Now, we format and re-merge with our original parcel data
    logger.log_msg("...merging living area totals with parcel-level data")
    # 1. fill table with NAs -- no longer needed because NAs are eliminated
    # by nesting structure
    # tot_bg = tot_bg.fillna(0)
    # 2. merge back to original data
    intersect_df = pd.merge(intersect_df, count_parcels_bg,
                            how='left',
                            on='GEOID10')

    # Step 2 in allocation is taking parcel-level proportions of living area
    # relative to the block group total, and calculating parcel-level
    # estimates of activities by multiplying the block group activity total
    # by the parcel-level proportions

    # For allocation, we need a two step process, depending on how the area
    # was calculated for the activity. If "{var}_How" is land_area, then
    # allocation needs to be relative to land area; otherwise, it needs to be
    # relative to living area. To do this, we'll set up mask dictionaries
    # similar to the land use mask
    logger.log_msg("setting up allocation logic...")
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
    logger.log_msg("...allocating jobs and population")
    # 1. for each job variable, calculate the proportion, then allocate     
    for var in lu_mask.keys():
        # First for lu mask
        intersect_df.loc[lu[var] & lu_mask[var], f'{var}_Par_Prop'] = (
                intersect_df[parcel_liv_area][lu[var] & lu_mask[var]] / intersect_df[f'{var}_Area'][lu[var] & lu_mask[var]]
        )
        # Then for non res
        intersect_df.loc[nr[var] & all_non_res["NR"], f'{var}_Par_Prop'] = (
                intersect_df[parcel_liv_area][nr[var] & all_non_res["NR"]] / intersect_df[f'{var}_Area'][
            nr[var] & all_non_res["NR"]]
        )
        # Then for all dev
        intersect_df.loc[ad[var] & all_developed["AD"], f'{var}_Par_Prop'] = (
                intersect_df[parcel_liv_area][ad[var] & all_developed["AD"]] / intersect_df[f'{var}_Area'][ad[var] & all_developed["AD"]]
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
    # x = intersect_df.groupby(["GEOID10"])[v].apply(lambda x: x.sum())
    # x[v].apply(lambda x: [min(x), max(x)])

    # Now we can sum up totals
    logger.log_msg("...totaling allocated jobs and population")
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
    logger.log_msg("...allocating commutes")
    # Commutes will be allocated relative to total population, so total by
    # the block group and calculate the parcel share
    tp_props = intersect_df.groupby("GEOID10")["Total_Population"].sum().reset_index()
    tp_props.columns = ["GEOID10", "TP_Agg"]
    geoid_edit = tp_props[tp_props.TP_Agg == 0].GEOID10
    intersect_df = pd.merge(intersect_df, tp_props,
                            how='left',
                            on='GEOID10')
    intersect_df["TP_Par_Prop"] = intersect_df['Total_Population'] / intersect_df['TP_Agg']
    # If there are any 0s (block groups with 0 population) replace with
    # the population area population, in case commutes are predicted where
    # population isn't
    intersect_df.loc[intersect_df.GEOID10.isin(geoid_edit), "TP_Par_Prop"] = intersect_df["Population_Par_Prop"][
        intersect_df.GEOID10.isin(geoid_edit)]
    # Now we can allocate commutes
    transit_vars = ['Drove', 'Carpool', 'Transit',
                    'NonMotor', 'Work_From_Home', 'AllOther']
    for var in transit_vars:
        intersect_df[f'{var}_PAR'] = intersect_df["TP_Par_Prop"] * intersect_df[var]

    # And, now we can sum up totals
    logger.log_msg("...totaling allocated commutes")
    intersect_df['Total_Commutes'] = (
            intersect_df['Drove_PAR'] + intersect_df['Carpool_PAR'] + intersect_df['Transit_PAR'] +
            intersect_df['NonMotor_PAR'] + intersect_df['Work_From_Home_PAR'] + intersect_df['AllOther_PAR']
    )

    # Now we're ready to write

    # We don't need all the columns we have, so first we define the columns
    # we want and select them from our data. Note that we don't need to
    # maintain the parcels_id_field here, because our save file has been
    # initialized with this already!
    logger.log_msg("...selecting columns of interest")
    to_keep = [parcels_id,
               parcel_liv_area,
               parcel_lu,
               'GEOID10',
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
    logger.log_msg("...writing table of allocation results")
    sed_path = makePath(out_gdb,
                        "socioeconomic_and_demographic")
    dfToTable(intersect_df, sed_path)

    # Then we done!
    return sed_path