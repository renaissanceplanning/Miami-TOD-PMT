# global configurations
from PMT_tools.config.prepare_config import (CRASH_CODE_TO_STRING, CRASH_CITY_CODES,
                                             CRASH_SEVERITY_CODES, CRASH_HARMFUL_CODES)
from PMT_tools.config.prepare_config import (PERMITS_CAT_CODE_PEDOR, PERMITS_STATUS_DICT, PERMITS_FIELDS_DICT,
                                             PERMITS_USE, PERMITS_DROPS, )
from PMT_tools.prepare.preparer import RIF_CAT_CODE_TBL, DOR_LU_CODE_TBL
from PMT_tools.config.prepare_config import PARCEL_COMMON_KEY

from PMT_tools.PMT import gdfToFeatureClass, dfToPoints, extendTableDf, makePath

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