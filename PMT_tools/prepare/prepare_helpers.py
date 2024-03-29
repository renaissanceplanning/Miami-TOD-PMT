"""
The `prepare_helpers` module defines a host of functions that support `preparer` procedures.
Much of the heavy lifting for TOC analysis occurs here by methods that are more abstract
and parameterized than the purpose-built methods in `preparer`.
"""
import csv
import fnmatch
import json
import re
import tempfile
import zipfile
from collections.abc import Iterable
from datetime import time
from functools import reduce
from json.decoder import JSONDecodeError
import os

import dask.dataframe as dd
import networkx as nx
import scipy
import xlrd
from six import string_types
from sklearn import linear_model
from arcgis import GeoAccessor, GeoSeriesAccessor

from PMT_tools import PMT as PMT
from PMT_tools.PMT import arcpy, pd, np

# temporary
from PMT_tools.build import build_helper as b_help
from PMT_tools.config import prepare_config as p_conf


__all__ = [
    "is_gz_file",
    "validate_json",
    "field_mapper",
    "geojson_to_feature_class_arc",
    "csv_to_df",
    "split_date",
    "add_xy_from_poly",
    "clean_and_drop",
    "combine_csv_dask",
    "update_transit_times",
    "make_basic_features",
    "make_summary_features",
    "patch_basic_features",
    "prep_park_polys",
    "prep_feature_class",
    "clean_permit_data",
    "udb_line_to_polygon",
    "read_transit_xls",
    "read_transit_file_tag",
    "update_dict",
    "prep_transit_ridership",
    "clean_parcel_geometry",
    "prep_parcel_land_use_tbl",
    "enrich_bg_with_parcels",
    "enrich_bg_with_parcels",
    "get_field_dtype",
    "enrich_bg_with_econ_demog",
    "prep_parcels",
    "get_raster_file",
    "prep_imperviousness",
    "analyze_imperviousness",
    "agg_to_zone",
    "model_blockgroup_data",
    "apply_blockgroup_model",
    "allocate_bg_to_parcels",
    "estimate_maz_from_parcels",
    "consolidate_cols",
    "patch_local_regional_maz",
    "copy_net_result",
    "lines_to_centrality",
    "network_centrality",
    "parcel_walk_time_bin",
    "parcel_walk_times",
    "parcel_ideal_walk_time",
    "summarize_access",
    "generate_od_table",
    "taz_travel_stats",
    "generate_chunking_fishnet",
    "symmetric_difference",
    "merge_and_subset",
    "get_filename",
    "validate_weights",
    "calculate_contiguity_index",
    "calculate_contiguity_summary",
    "simpson_diversity",
    "shannon_diversity",
    "berger_parker_diversity",
    "enp_diversity",
    "assign_features_to_agg_area",
    "lu_diversity",
    "match_units_fields",
    "create_permits_units_reference",
    "build_short_term_parcels",
    "clean_skim_csv",
    "skim_to_graph",
    "transit_skim_joins",
]


# TODO: verify functions generally return python objects (dataframes, e.g.) and leave file writes to `preparer.py`
# general use functions
def is_gz_file(filepath):
    """
    Test if a file is zipped

    Args:
        filepath (str): path to file of interest

    Returns:
        bool
    """
    with open(filepath, "rb") as test_f:
        return test_f.read(2) == b"\x1f\x8b"


def validate_json(json_file, encoding="utf8"):
    """
    Check for valid json file

    Args:
        json_file (str): path to file
        encoding (str): name of the encoding used to decode or encode the file

    Returns:
        json deserialized to python object
    """
    with open(json_file, encoding=encoding) as file:
        try:
            return json.load(file)
        except JSONDecodeError:
            print("Invalid JSON file passed")


def field_mapper(in_fcs, use_cols, rename_dicts):
    """
    Create a field mapping for one or more feature classes

    Args:
        in_fcs (list,str): list of feature classes
        use_cols (list, str): list or tuple of lists of column names to keep
        rename_dicts (dict): dict or tuple of dicts to map field names
    
    Returns:
        arcpy.FieldMappings
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
        fields = [
            f.name
            for f in arcpy.ListFields(in_fc)
            if f.name in use and f.type not in _unmapped_types_
        ]
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


def geojson_to_feature_class_arc(
        geojson_path, geom_type, encoding="utf8", unique_id=None
):
    """
    Converts geojson to feature class in memory and adds unique_id attribute if provided

    Args:
        geojson_path (str): path to geojson file
        geom_type (str): The geometry type to convert from GeoJSON to features.
            OPTIONS: POINT, MULTIPOINT, POLYLINE, POLYGON
        encoding (str): name of the encoding used to decode or encode the file
        unique_id (str): name of unique id column, Default is None
    
    Returns:
        temp_feature (str): path to temporary feature class
    """
    if validate_json(json_file=geojson_path, encoding=encoding):
        try:
            # convert json to temp feature class
            temp_feature = PMT.make_inmem_path()
            arcpy.JSONToFeatures_conversion(
                in_json_file=geojson_path,
                out_features=temp_feature,
                geometry_type=geom_type,
            )
            if unique_id:
                PMT.add_unique_id(feature_class=temp_feature, new_id_field=unique_id)
            return temp_feature
        except:
            print("something went wrong converting geojson to feature class")


def csv_to_df(csv_file, use_cols, rename_dict):
    """
    Helper function to convert CSV file to pandas dataframe, and drop unnecessary columns
    assumes any strings with comma (,) should have those removed and dtypes infered
    
    Args:
        csv_file (str): path to csv file
        use_cols (list): list of columns to keep from input csv
        rename_dict (dict): dictionary mapping existing column name to standardized column names
    
    Returns:
        Pandas.DataFrame
    """
    if isinstance(use_cols, str):
        use_cols = [use_cols]
    df = pd.read_csv(filepath_or_buffer=csv_file, usecols=use_cols, thousands=",")
    df = df.convert_dtypes()
    df.rename(columns=rename_dict, inplace=True)
    return df


def split_date(df, date_field, unix_time=False):
    """
    Ingest date attribute and splits it out to DAY, MONTH, YEAR

    Args:
        df (pd.DataFrame): DataFrame with a date field
        date_field (str): column name
        unix_time (str): unix time stamp
    
    Returns:
        df (pd.DataFrame): DataFrame reformatted to include split day, month and year
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


def add_xy_from_poly(poly_fc, poly_key, table_df, table_key):
    """
    Calculates x,y coordinates for a given polygon feature class and returns as new
    columns of a data

    Args:
        poly_fc (str): path to polygon feature class
        poly_key (str): primary key from polygon feature class
        table_df (pd.DataFrame): pandas dataframe
        table_key (str): primary key of table df

    Returns:
        pandas.DataFrame: updated `table_df` with XY centroid coordinates appended
    """
    pts = PMT.polygons_to_points(
        in_fc=poly_fc, out_fc=PMT.make_inmem_path(), fields=poly_key, null_value=0.0
    )
    pts_sdf = pd.DataFrame.spatial.from_featureclass(pts)

    esri_ids = ["OBJECTID", "FID"]
    if any(item in pts_sdf.columns.to_list() for item in esri_ids):
        pts_sdf.drop(labels=esri_ids, axis=1, inplace=True, errors="ignore")
    # join permits to parcel points MANY-TO-ONE
    print("--- merging geo data to tabular")
    # pts = table_df.merge(right=pts_sdf, how="inner", on=table_key)
    return pd.merge(
        left=table_df, right=pts_sdf, how="inner", left_on=table_key, right_on=poly_key
    )


def clean_and_drop(feature_class, use_cols=None, rename_dict=None):
    """
    Remove and rename fields provided for a feature class to format as desired

    Args:
        feature_class (str): path to feature class
        use_cols (list): list of columns to keep
        rename_dict (dict): key, value pairs of columns to keep and new column names
    
    Returns:
        None
    """
    # reformat attributes and keep only useful
    if rename_dict is None:
        rename_dict = {}
    if use_cols is None:
        use_cols = []
    if use_cols:
        fields = [f.name for f in arcpy.ListFields(feature_class) if not f.required]
        drop_fields = [f for f in fields if f not in list(use_cols) + ["Shape"]]
        for drop in drop_fields:
            arcpy.DeleteField_management(in_table=feature_class, drop_field=drop)
    # rename attributes
    if rename_dict:
        for name, rename in rename_dict.items():
            arcpy.AlterField_management(
                in_table=feature_class,
                field=name,
                new_field_name=rename,
                new_field_alias=rename,
            )


def _merge_df_(x_specs, y_specs, on=None, how="inner", **kwargs):
    """
    Internal helper function when using dask frames
    
    See Also: combine_csv_dask
    """
    df_x, suffix_x = x_specs
    df_y, suffix_y = y_specs
    merge_df = df_x.merge(df_y, on=on, how=how, suffixes=(suffix_x, suffix_y), **kwargs)
    # Must return a tuple to support reliably calling from `reduce`
    return merge_df, suffix_y


def combine_csv_dask(
        merge_fields,
        out_table,
        *tables,
        suffixes=None,
        col_renames={},
        how="inner",
        **kwargs,
):
    """
    Merges two or more csv tables into a single table based on key common columns. All
    other columns from the original tables are included in the output csv.

    Args:
        merge_fields (list): One or more column names common to all `tables` on which they will be merged
        out_table (str): Path to the csv file to be created to store combined outputs
        tables (list): Paths to the tables to be combined.
        suffixes (list, default=None): If any tables have common column names (other than `merge_fields`) that
            would create naming collisions, the user can specify the suffixes to
            apply to each input table. If None, name collisions may generate
            unexpected colums in the output, so it is recommended to provide specific
            suffixes, especially if collisions are expected. Length of the list must
            match the number of `tables`.
        col_renames (dict): A dictionary for renaming columns in any of the provided `tables`. Keys are
            old column names, values are new column names.
        how (str, "inner" or "outer", default="inner"):  If "inner" combined csv tables will only include rows
            with common values in `merge_fields` across all `tables`. If "outer", all rows are retained
            with `nan` values for unmatched pairs in any table.
        kwargs:
            Any keyword arguments are passed to the dask dataframes `read_csv` method.
    """
    # Read csvs
    ddfs = [dd.read_csv(t, **kwargs) for t in tables]
    # Rename
    if col_renames:
        ddfs = [ddf.rename(columns=col_renames) for ddf in ddfs]
    # # Index on merge cols
    # if isinstance(merge_fields, string_types) or not isinstance(merge_fields, Iterable):
    #     ddfs = [ddf.set_index(merge_fields) for ddf in ddfs]
    # else:
    #     # make tuple column
    #     for ddf in ddfs:
    #         ddf["__index__"] = ddf[merge_fields].apply(tuple, axis=1)
    #     ddfs = [ddf.set_index("__index__", drop=True) for ddf in ddfs]
    if suffixes is None:
        df = reduce(lambda this_df, next_df: this_df.merge(next_df, on=merge_fields, how=how), ddfs)
    else:
        # for odd lengths, the last suffix will be lost because there will
        # be no collisions (applies to well-formed data only - inconsistent
        # field naming could result in field name collisions prompting a
        # suffix
        if len(ddfs) % 2 != 0:
            # Force rename suffix
            cols = [
                (c, f"{c}{suffixes[-1]}")
                for c in ddfs[-1].columns
                if c not in merge_fields
            ]
            renames = dict(cols)
            ddfs[-1] = ddfs[-1].rename(columns=renames)

        # zip ddfs and suffixes
        specs = zip(ddfs, suffixes)
        df, _ = reduce(
            lambda this, next: _merge_df_(this, next, on=merge_fields, how=how), specs
        )
    df.to_csv(
        out_table, single_file=True, index=False, header_first_partition_only=True
    )


def update_transit_times(
        od_table,
        out_table,
        competing_cols=[],
        out_col=None,
        replace_vals={},
        chunksize=100000,
        **kwargs,
):
    """
    Opens and updates a csv table containing transit travel time estimates
    between origin-destination pairs as indicated by the provided parameters.

    Args:
        od_table (str): path to a csv of OD data that includes transit travel time estimates
        out_table (str): path to the new output csv table containing updated values
        competing_cols (list, default=[]): The minimum value among competing columns will be
            written to `out_col`
        out_col (str, default=None): A new column to be populated with updated transit travel time
            estimates based on `competing_cols` comparisons and value replacement indicated by `replace_vals`
        replace_vals (dict, default={}): A dict whose keys indicate old values (those to be replaced) and whose
            values indicate new values.
        chunksize (int, default=100000): The number of rows to process at a time (to accomodate large files)
        kwargs: Keyword arguments to pass to the pandas `read_csv` method

    Returns:
        out_table (str): A new csv table with updated transit times is stored at the path specified
    """
    # Validate
    if not isinstance(replace_vals, dict):
        raise TypeError(f"Expected dict for replace_vals, got {type(replace_vals)}")
    if not isinstance(competing_cols, Iterable) or isinstance(
            competing_cols, string_types
    ):
        raise TypeError(
            f"Expected iterable of column names for competing_cols, got {type(competing_cols)}"
        )
    # Iterate over chunks
    mode = "w"
    header = True
    for chunk in pd.read_csv(od_table, chunksize=chunksize, **kwargs):
        if replace_vals:
            chunk = chunk.replace(replace_vals)
        if competing_cols:
            chunk[out_col] = chunk[competing_cols].min(axis=1)
        chunk.to_csv(out_table, mode=mode, header=header)
        mode = "a"
        header = False


# basic features functions
def _listifyInput(input):
    """
    Helper function to convert string input to list
    
    Returns:
        list
    """
    if isinstance(input, string_types):
        return input.split(";")
    else:
        return list(input)


def _stringifyList(input):
    """
    Helper function to convert input to string
    
    Returns:
        str
    """
    return ";".join(input)


def _validate_field(items, item):
    return bool(item in items)


def make_basic_features(
        bf_gdb,
        stations_fc,
        stn_id_field,
        stn_diss_fields,
        stn_corridor_fields,
        alignments_fc,
        align_diss_fields,
        align_corridor_name,
        stn_buff_dist="2640 Feet",
        align_buff_dist="2640 Feet",
        stn_areas_fc="Station_Areas",
        corridors_fc="Corridors",
        long_stn_fc="Stations_Long",
        preset_station_areas=None,
        preset_station_id=None,
        preset_corridors=None,
        preset_corridor_name=None,
        rename_dict={},
        overwrite=False,
):
    """
    In a geodatabase with basic features (station points and corridor alignments),
    create polygon feature classes used for standard mapping and summarization.
    The output feature classes include:
        - buffered corridors,
        - buffered station areas,
        - a long file of station points, where each station/corridor combo is represented as a separate feature.

    Args:
        bf_gdb (str): Path to a geodatabase with key basic features, including stations and
            alignments
        stations_fc (str): A point feature class in`bf_gdb` with station locations and columns
            indicating belonging in individual corridors (i.e., the column names reflect corridor
            names and flag whether the station is served by that corridor).
        stn_id_field (str): A field in `stations_fc` that identifies stations (common to a single
            station area)
        stn_diss_fields (list): Field(s) on which to dissolve stations when buffering
            station areas. Stations that reflect the same location by different facilities may
            be dissolved by name or ID, e.g. This may occur at intermodal locations.
            For example, where metro rail meets commuter rail - the station points may represent
            the same basic station but have slightly different geolocations.
        stn_corridor_fields (list): The columns in `stations_fc` that flag each
            stations belonging in various corridors.
        alignments_fc (str): Path to a line feature class in `bf_gdb` reflecting corridor alignments
        align_corridor_name (str): A field in `alignments_fc` that identifies the corridor it belongs to.
        align_diss_fields (list): Field(s) on which to dissolve alignments when buffering
            corridor areas.
        stn_buff_dist (str, default="2640 Feet"): A linear unit by which to buffer
            station points to create station area polygons.
        align_buff_dist (str, default="2640 Feet"): A linear unit by which to buffer
            alignments to create corridor polygons
        stn_areas_fc (str, default="Station_Areas"): The name of the output feature class to
            hold station area polygons
        corridors_fc (str, default="Corridors"): The name of the output feature class to hold corridor polygons
        long_stn_fc (str, default="Stations_Long"): The name of the output feature class to hold station features,
            elongated based on corridor belonging (to support dashboard menus)
        preset_station_areas (path or feature layer, default=None): Features that pre-define station areas will
            supplant simple station buffers where `preset_station_id` matches `stn_id_field`
        preset_station_id (str, default=None): Key field for `preset_station_areas` to lookup and replace geometries
            in the `stn_areas_fc` output
        preset_corridors (path or feature layer, default=None): Features that pre-define corridor areas will
            supplant simple alignment buffers where `preset_corridor_name` matches `stn_id_field`
        preset_corridor_name: (str, default=None): Key field for `preset_corridors` to lookup and replace geometries
            in the `corridors_fc` output
        rename_dict (dict, default={}): If given, `stn_corridor_fields` can be relabeled before pivoting
            to create `long_stn_fc`, so that the values reported in the output "Corridor" column are not
            the column names, but values mapped on to the column names
            (changing "EastWest" column to "East-West", e.g.)
        overwrite (bool): default=False
    """
    stn_diss_fields = _listifyInput(stn_diss_fields)
    stn_corridor_fields = _listifyInput(stn_corridor_fields)
    align_diss_fields = _listifyInput(align_diss_fields)

    if not _validate_field(items=stn_diss_fields, item=stn_id_field):
        raise Exception("'stn_id_field' not included in 'stn_diss_fields'")
    if not _validate_field(items=align_diss_fields, item=align_corridor_name):
        raise Exception("'stn_id_field' not included in 'stn_diss_fields'")

    old_ws = arcpy.env.workspace
    arcpy.env.workspace = bf_gdb

    # Buffer features
    #  - stations (station areas, unique)
    print("--- buffering station areas")
    PMT.check_overwrite_output(stn_areas_fc, overwrite)
    _diss_flds_ = _stringifyList(stn_diss_fields)
    arcpy.Buffer_analysis(
        in_features=stations_fc,
        out_feature_class=stn_areas_fc,
        buffer_distance_or_field=stn_buff_dist,
        dissolve_option="LIST",
        dissolve_field=_diss_flds_,
    )
    #  - alignments (corridors, unique)
    print("--- buffering corridor areas")
    PMT.check_overwrite_output(corridors_fc, overwrite)
    _diss_flds_ = _stringifyList(align_diss_fields)
    arcpy.Buffer_analysis(
        in_features=alignments_fc,
        out_feature_class=corridors_fc,
        buffer_distance_or_field=align_buff_dist,
        dissolve_option="LIST",
        dissolve_field=_diss_flds_,
    )

    # Patch buffers and
    print("--- patching preset features")
    patch_basic_features(
        station_areas_fc=stn_areas_fc,
        corridors_fc=corridors_fc,
        preset_station_areas=preset_station_areas,
        preset_station_id_field=preset_station_id,
        existing_station_id_field=stn_id_field,
        preset_corridors=preset_corridors,
        preset_corridor_name_field=preset_corridor_name,
        existing_corridor_name_field=align_corridor_name,
    )

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
    fc_path = PMT.make_path(bf_gdb, stations_fc)
    stn_df = PMT.featureclass_to_df(in_fc=fc_path, keep_fields=fields)

    # Rename columns if needed
    if rename_dict:
        stn_df.rename(columns=rename_dict, inplace=True)
        _cor_cols_ = [rename_dict.get(c, c) for c in stn_corridor_fields]
    else:
        _cor_cols_ = stn_corridor_fields

    # Sum stn_df to eliminate redundant stations (get average shapexy and max corridor vals)
    agg_dict = dict([(cc, np.max) for cc in _cor_cols_])
    agg_dict["SHAPE@X"] = np.mean
    agg_dict["SHAPE@Y"] = np.mean
    stn_df = stn_df.groupby(stn_diss_fields).agg(agg_dict).reset_index()

    # Melt to gather cols
    id_vars = stn_diss_fields + ["SHAPE@X", "SHAPE@Y"]
    long_df = stn_df.melt(
        id_vars=id_vars, value_vars=_cor_cols_, var_name="Corridor", value_name="InCor"
    )
    sel_df = long_df[long_df.InCor != 0].copy()
    long_out_fc = PMT.make_path(bf_gdb, long_stn_fc)
    PMT.check_overwrite_output(long_out_fc, overwrite)
    PMT.df_to_points(
        df=sel_df,
        out_fc=long_out_fc,
        shape_fields=["SHAPE@X", "SHAPE@Y"],
        from_sr=sr,
        to_sr=sr,
        overwrite=True,
    )

    arcpy.env.workspace = old_ws


def make_summary_features(
        bf_gdb,
        long_stn_fc,
        stn_areas_fc,
        stn_id_field,
        corridors_fc,
        cor_name_field,
        out_fc,
        stn_buffer_meters=804.672,
        stn_name_field="Name",
        stn_status_field="Status",
        stn_cor_field="Corridor",
        overwrite=False,
):
    """
    Creates a single feature class for data summarization based on station area and corridor geographies.
    The output feature class includes each station area, all combined station areas, the entire corridor area,
    and the portion of the corridor that is outside station areas.

    Args:
        bf_gdb (str): Path to basic features gdb
        long_stn_fc (str): path to long station points feature class  (deprecated in lieu of patched features)
        stn_areas_fc (str): path to station area polygons feature class
        stn_id_field (str): id field linking `stn_areas_fc` and `long_stn_fc`
        corridors_fc (str): path to corridors feature class
        cor_name_field (str): name field for corridor feature class
        out_fc (str): path to output feature class
        stn_buffer_meters (num, default=804.672 [1/2 mile])
        stn_name_field (str, default="Name"): station name field 
        stn_status_field (str, default="Status"): status of station
        stn_cor_field (str, default="Corridor): corridor field
        overwrite (bool, default=False): flag indicating whether to overwrite existing copy
    """

    old_ws = arcpy.env.workspace
    arcpy.env.workspace = bf_gdb

    sr = arcpy.Describe(stn_areas_fc).spatialReference
    mpu = float(sr.metersPerUnit)
    buff_dist = stn_buffer_meters / mpu

    # Make output container - polygon with fields for Name, Corridor
    print(f"--- creating output feature class {out_fc}")
    PMT.check_overwrite_output(out_fc, overwrite)
    out_path, out_name = os.path.split(out_fc)
    arcpy.CreateFeatureclass_management(
        out_path, out_name, "POLYGON", spatial_reference=sr
    )
    # - Add fields
    arcpy.AddField_management(out_fc, "STN_ID", "LONG")
    arcpy.AddField_management(out_fc, p_conf.STN_NAME_FIELD, "TEXT", field_length=80)
    arcpy.AddField_management(out_fc, p_conf.STN_STATUS_FIELD, "TEXT", field_length=80)
    arcpy.AddField_management(out_fc, p_conf.CORRIDOR_NAME_FIELD, "TEXT", field_length=80)
    arcpy.AddField_management(out_fc, p_conf.SUMMARY_AREAS_COMMON_KEY, "LONG")

    # POPULATE SUMMAREAS with CORRIDORS including ENTIRE CORRIDOR
    # Add all corridors with name="(Entire corridor)", corridor=cor_name_field
    print("--- adding corridor polygons")
    out_fields = [
        "SHAPE@",
        "STN_ID",
        stn_name_field,
        stn_status_field,
        stn_cor_field,
        p_conf.SUMMARY_AREAS_COMMON_KEY,
    ]
    cor_fields = ["SHAPE@", cor_name_field]
    cor_polys = {}
    i = 0
    with arcpy.da.InsertCursor(out_fc, out_fields) as ic:
        with arcpy.da.SearchCursor(corridors_fc, cor_fields) as sc:
            for sr in sc:
                i += 1
                # Add row for the whole corridor
                poly, corridor = sr
                out_row = [poly, -1, "(Entire corridor)", "NA", corridor, i]
                ic.insertRow(out_row)
                # Keep the polygons in a dictionary for use later
                cor_polys[corridor] = poly

    # Add all station areas with name= stn_name_field, corridor=stn_cor_field
    print("--- adding station polygons by corridor")
    stn_fields = [
        "SHAPE@",
        stn_id_field,
        stn_name_field,
        stn_status_field,
        stn_cor_field,
    ]
    # missing = PMT.which_missing(table=long_stn_fc, field_list=stn_fields)
    cor_stn_polys = {}
    with arcpy.da.InsertCursor(out_fc, out_fields) as ic:
        with arcpy.da.SearchCursor(long_stn_fc, stn_fields) as sc:
            for sr in sc:
                i += 1
                # Add row for each station/corridor combo
                point, stn_id, stn_name, stn_status, corridor = sr

                # Get poly from stn_areas_fc
                where_clause = (
                        arcpy.AddFieldDelimiters(stn_areas_fc, stn_id_field) + f"={stn_id}"
                )
                with arcpy.da.SearchCursor(
                        stn_areas_fc, "SHAPE@", where_clause=where_clause
                ) as ref_c:
                    poly = reduce(
                        lambda x, y: x.union(y), [ref_r[0] for ref_r in ref_c]
                    )
                out_row = [poly, stn_id, stn_name, stn_status, corridor, i]
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
            out_row = [all_stn_poly, -2, "(All stations)", "NA", corridor, i]
            ic.insertRow(out_row)
            # Non-station areas
            i += 1
            cor_poly = cor_polys[corridor]
            non_stn_poly = cor_poly.difference(all_stn_poly)
            out_row = [non_stn_poly, -3, "(Outside station areas)", "NA", corridor, i]
            ic.insertRow(out_row)
    arcpy.RepairGeometry_management(in_features=out_fc, delete_null="DELETE_NULL")
    arcpy.env.workspace = old_ws


def patch_basic_features(
        station_areas_fc,
        corridors_fc,
        preset_station_areas=None,
        preset_station_id_field=None,
        existing_station_id_field=None,
        preset_corridors=None,
        preset_corridor_name_field=None,
        existing_corridor_name_field=None,
):
    """
    Modifies the basic features database to updata station area and/or corridor geometries
    based on provided preset features (i.e., custom areas that deviate from the simple buffers
    created by `make_basice_features`)

    Args:
        station_areas_fc (str or feature layer)
        corridors_fc (str or feature layer)
        preset_station_areas (str or feature layer, default=None): If provided, these geometries will
            be used to update station area features (`StationAreas` and `SummaryAreas`)
        preset_station_id_field (str, default=None): The field in `preset_station_areas` that corresponds to
            "basic_features/StationAreas.Id." It is used to map new geometries into the `StationAreas` and
            `SummaryAreas` feature classes.
        existing_station_id_field (str, default=None): The field in station_areas_fc represnting the primary key
        preset_corridors (str or feature layer, default=None): If provided, these geometries will be
            used to update corridor features (`Corridors` and `SummaryAreas`)
        preset_corridor_name_field (str, default=None): The field in `preset_corridors` that corresponds to
            "basic_features/Corridors.Corridor." It is used to map new geometries into the `Corridors` and
            `SummaryAreas` feature classes.
        existing_corridor_name_field (str, default=None): The field in corridors_fc representing the primary key

    See Also:
        make_basic_features
        make_summary_features
    """

    def update_geography(preset_geog, preset_id, default_geog, default_id):
        default_id_type = [
            f.type for f in arcpy.ListFields(default_geog) if f.name == default_id
        ][0]
        with arcpy.da.SearchCursor(preset_geog, ["SHAPE@", preset_id]) as c:
            for r in c:
                preset_shape, station_id = r
                # lookup which station -> which station_area
                quot = ""
                if default_id_type == "String":
                    quot = "'"
                where_clause = (
                        arcpy.AddFieldDelimiters(default_geog, default_id)
                        + f"= {quot}{station_id}{quot}"
                )
                with arcpy.da.UpdateCursor(
                        default_geog, ["SHAPE@"], where_clause=where_clause
                ) as uc:
                    for ur in uc:
                        ur[0] = preset_shape
                        uc.updateRow(ur)

    if preset_station_areas is not None:
        update_geography(
            preset_geog=preset_station_areas,
            preset_id=preset_station_id_field,
            default_geog=station_areas_fc,
            default_id=existing_station_id_field,
        )

    if preset_corridors is not None:
        update_geography(
            preset_geog=preset_corridors,
            preset_id=preset_corridor_name_field,
            default_geog=corridors_fc,
            default_id=existing_corridor_name_field,
        )


# parks functions
def prep_park_polys(
        in_fcs, out_fc, geom="POLYGON", use_cols=None, rename_dicts=None, unique_id=None
):
    """
    Merges park polygon data into single feature class, creating a unique ID for each polygon and
    reformatting the table

    Args:
        in_fcs (list): list of paths to park feature classes
        out_fc (str): path to output dataset
        geom (str): geometry type of input features
        use_cols (list): list of column names to keep
        rename_dicts (list): list of dictionaries mapping attributes by feature class
        unique_id (str): name of new field to add as unique identifier (defined in prepare_config)

    Returns:
        None
    """
    # Align inputs
    if rename_dicts is None:
        rename_dicts = [{} for _ in in_fcs]
    if use_cols is None:
        use_cols = [[] for _ in in_fcs]
    if isinstance(in_fcs, str):
        in_fcs = [in_fcs]

    # handle the chance of input raw data being geojson
    if any(fc.endswith("json") for fc in in_fcs):
        in_fcs = [
            geojson_to_feature_class_arc(fc, geom_type=geom)
            if fc.endswith("json")
            else fc
            for fc in in_fcs
        ]

    # merge into one feature class temporarily
    fm = field_mapper(in_fcs=in_fcs, use_cols=use_cols, rename_dicts=rename_dicts)
    arcpy.Merge_management(inputs=in_fcs, output=out_fc, field_mappings=fm)
    PMT.add_unique_id(feature_class=out_fc, new_id_field=unique_id)
    arcpy.RepairGeometry_management(in_features=out_fc)


def prep_feature_class(
        in_fc, geom, out_fc, use_cols=None, rename_dict=None, unique_id=None
):
    """
    Tidies up a provided feature class, removing unnecessary fields and mapping existing
    fields to prefered values

    Args:
        in_fc (str): path to input feature class
        geom (str): geometry type of input features
        out_fc (str): path to output feature class
        use_cols (list): list of column names to keep
        rename_dict (list): list of dictionaries mapping attributes by feature class
        unique_id (str): name of new field to add as unique identifier

    Returns:
        None
    """
    if rename_dict is None:
        rename_dict = {}
    if use_cols is None:
        use_cols = []
    if in_fc.endswith("json"):
        in_fc = geojson_to_feature_class_arc(in_fc, geom_type=geom, unique_id=unique_id)
    out_dir, out_name = os.path.split(out_fc)
    fms = field_mapper(in_fcs=in_fc, use_cols=use_cols, rename_dicts=rename_dict)
    arcpy.FeatureClassToFeatureClass_conversion(
        in_features=in_fc, out_path=out_dir, out_name=out_name, field_mapping=fms
    )


# permit functions
def clean_permit_data(
        permit_csv,
        parcel_fc,
        permit_key,
        poly_key,
        rif_lu_tbl,
        dor_lu_tbl,
        out_file,
        out_crs,
):
    """
    Reformat and clean RER road impact permit data, specific to the TOC tool

    Args:
        permit_csv (str): path to permit csv
        parcel_fc (str): path to parcel feature class; should be most recent parcel year
        permit_key (str): foreign key of permit data that ties to parcels ("FOLIO")
        poly_key (str): primary key of parcel data that ties to permits ("FOLIO")
        rif_lu_tbl (str): path to road_impact_fee_cat_codes table (maps RIF codes to more standard LU codes)
        dor_lu_tbl (str): path to dor use code table (maps DOR LU codes to more standard and generalized categories)
        out_file (str): path to output permit point feature class
        out_crs (int): EPSG code
    
    Returns:
        None
    """
    # TODO: add validation
    # read permit data to dataframe
    permit_df = csv_to_df(
        csv_file=permit_csv,
        use_cols=p_conf.PERMITS_USE,
        rename_dict=p_conf.PERMITS_FIELDS_DICT,
    )

    # clean up and concatenate data where appropriate
    #   fix parcelno to string of 13 len
    permit_df[permit_key] = permit_df[permit_key].astype(np.str)
    permit_df[poly_key] = permit_df[permit_key].apply(lambda x: x.zfill(13))
    permit_df["CONST_COST"] = permit_df["CONST_COST"].apply(
        lambda x: x.replace('$', '')).apply(
        lambda x: x.replace(' ', '')).apply(
        lambda x: x.replace(',', '')).astype(float)
    permit_df["ADMIN_COST"] = permit_df["ADMIN_COST"].apply(
        lambda x: x.replace('$', '')).apply(
        lambda x: x.replace(' ', '')).apply(
        lambda x: x.replace(',', '')).astype(float)
    permit_df["COST"] = permit_df["CONST_COST"] + permit_df["ADMIN_COST"]
    #   id project as pedestrain oriented
    permit_df["PED_ORIENTED"] = np.where(
        permit_df.CAT_CODE.str.contains(p_conf.PERMITS_CAT_CODE_PEDOR), 1, 0
    )
    # drop fake data - Keith Richardson of RER informed us that any PROC_NUM/ADDRESS that contains with 'SMPL' or
    #   'SAMPLE' should be ignored as as SAMPLE entry
    ignore_text = [
        "SMPL",
        "SAMPLE",
    ]
    for ignore in ignore_text:
        for col in ["PROC_NUM", "ADDRESS"]:
            permit_df = permit_df[~permit_df[col].str.contains(ignore)]
    #   set landuse codes appropriately accounting for pedoriented dev
    permit_df["CAT_CODE"] = np.where(
        permit_df.CAT_CODE.str.contains(p_conf.PERMITS_CAT_CODE_PEDOR),
        permit_df.CAT_CODE.str[:-2],
        permit_df.CAT_CODE,
    )
    #   set project status
    permit_df["STATUS"] = permit_df["STATUS"].map(
        p_conf.PERMITS_STATUS_DICT, na_action="NONE"
    )
    #   add landuse codes
    lu_df = pd.read_csv(rif_lu_tbl)
    dor_df = pd.read_csv(dor_lu_tbl)
    lu_df = lu_df.merge(right=dor_df, how="inner", on="DOR_UC")
    permit_df = permit_df.merge(right=lu_df, how="inner", on="CAT_CODE")
    #   drop unnecessary columns
    permit_df.drop(columns=p_conf.PERMITS_DROPS, inplace=True)

    # convert to points
    sdf = add_xy_from_poly(
        poly_fc=parcel_fc, poly_key=poly_key, table_df=permit_df, table_key=permit_key
    )
    sdf.fillna(0.0, inplace=True)

    # get xy coords and drop SHAPE
    sdf["X"] = sdf[sdf.spatial.name].apply(lambda c: c.x)
    sdf["Y"] = sdf[sdf.spatial.name].apply(lambda c: c.y)
    sdf.drop(columns="SHAPE", inplace=True)
    # TODO: assess why sanitize columns as false fails (low priority)
    # sdf.spatial.to_featureclass(location=out_file, sanitize_columns=False)
    PMT.df_to_points(
        df=sdf, out_fc=out_file, shape_fields=["X", "Y"], from_sr=out_crs, to_sr=out_crs
    )
    return out_file


# Urban Growth Boundary
def udb_line_to_polygon(udb_fc, county_fc, out_fc):
    """
    Uses the urban development boundary line to bisect the county boundary
    and generate two polygon output features.

    During processing the UDB line features are dissolved into a single
    feature - this assumes all polylines in the shape file touch one another
    such that a single cohesive polyline feature results.

    This function also assumes that the UDB will only define a simple
    bi-section of the county boundary. If the UDB geometry becomes more
    complex over time, modifications to this function may be needed.

    Args:
        udb_fc (str): Path to the udb line features.
        county_fc (str): Path to the county boundary polygon. This is expected to only include a
            single polygon encompassing the entire county.
        out_fc (str): Path to the output feature class.
    
    Returns:
        out_fc (str): Path to the output feature class
    """
    sr = arcpy.Describe(udb_fc).spatialReference
    # Prepare ouptut feature class
    out_path, out_name = os.path.split(out_fc)
    arcpy.CreateFeatureclass_management(
        out_path=out_path,
        out_name=out_name,
        geometry_type="POLYGON",
        spatial_reference=sr,
    )
    arcpy.AddField_management(
        in_table=out_fc, field_name=p_conf.UDB_FLAG, field_type="LONG"
    )

    # Get geometry objects
    temp_udb = PMT.make_path("in_memory", "UDB_dissolve")
    diss_line = arcpy.Dissolve_management(
        in_features=udb_fc, out_feature_class=temp_udb
    )
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
    with arcpy.da.InsertCursor(out_fc, ["SHAPE@", p_conf.UDB_FLAG]) as c:
        for cut, b in zip(cuts, in_udb):
            c.insertRow([cut, b])

    return out_fc


# Transit Ridership
def read_transit_xls(xls_path, sheet=None, head_row=None, rename_dict=None):
    """
    Reads in the provided xls file and concatenates the sheets into a single data frame
    to resolve formatting issues and make the file's contents code-readable.

    XLS File Desc: Sheet 1 contains header and max rows for a sheet (65536),
    data continue on subsequent sheets without header.
    
    Args:
        xls_path (str): Path to xls file
        sheet (str, int, or list, default=None): sheet(s) in `xls_path` to concatentate. If `None`,
            read and concatenate all shees in the workbook.
        head_row (int or list, default=None): Row(s) in `xls_path` that serve as table headers. Must
            be the same for all `sheet`(s). If None, all rows are read without specifying a header.
        rename_dict (dict, default=None): Dictionary to map existing column names to more readable names
    
    Returns:
        pd.Dataframe: consolidated dataframe of data from xls
    """
    # TODO: add logic to handle files that dont match existing format
    # TODO:     - example check may be to read all pages and see if row 0
    #               is the same or pages are empty
    # read the xls into dict
    wb = xlrd.open_workbook(filename=xls_path, logfile=open(os.devnull, "w"))
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
    Extracts a tag from the file path that is used in the naming of "ON" and "OFF"
    counts per stop

    Args:
        file_path (str): Path to transit ridership file

    Returns:
        tag: String
    """
    # returns "time_year_month" tag for ridership files
    # ex: AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1411_2015_APR_standard_format.XLS
    #       ==> "1411_2015_APR"
    _, file_name = os.path.split(file_path)
    return "_".join(file_name.split("_")[7:10])


def update_dict(d, values, tag):
    """
    Adds new key:value pairs to dictionary given a set of values and
        tags in the keys. Only valuable in support of the transit ridership analaysis

    Args:
        d (dict): existing dictionary
        values (list): output value names
        tag (str): unique tag found in a column
    
    Returns:
        d (dict): updated with new key value pairs
    """
    for value in values:
        d[f"{value}_{tag}"] = value
    return d


def prep_transit_ridership(
        in_table, rename_dict, unique_id, shape_fields, from_sr, to_sr, out_fc
):
    """
    Converts transit ridership data to a feature class and reformats attributes.

    Args:
        in_table (str): xls file path
        rename_dict (dict): dictionary of {existing: new attribute} names
        unique_id (str): name of id field added
        shape_fields (list): columns to be used as shape field (x,y coords)
        from_sr (SpatialReference): the spatial reference definition for coordinates listed in 'shape_fields'
        to_sr (SpatialReference): the spatial reference definition for output features
        out_fc (str): path to the output feature class
    
    Returns:
        out_fc: Path
    """
    # on/off data are tagged by the date the file was created
    file_tag = read_transit_file_tag(file_path=in_table)
    rename_dict = update_dict(
        d=rename_dict, values=["ON", "OFF", "TOTAL"], tag=file_tag
    )
    # read the transit file to dataframe
    transit_df = read_transit_xls(xls_path=in_table, rename_dict=rename_dict)
    # reduce precision to collapse points
    for shp_fld in shape_fields:
        transit_df[shp_fld] = transit_df[shp_fld].round(decimals=4)
    transit_df = (
        transit_df.groupby(shape_fields + ["TIME_PERIOD"])
            .agg({"ON": "sum", "OFF": "sum", "TOTAL": "sum"})
            .reset_index()
    )
    transit_df["TIME_PERIOD"] = np.where(
        transit_df["TIME_PERIOD"] == "2:00 AM - 2:45 AM",
        "EARLY AM 02:45AM-05:59AM",
        transit_df["TIME_PERIOD"],
    )
    transit_df.reset_index(inplace=True)
    transit_df.rename(columns={"index": unique_id}, inplace=True)
    # convert table to points
    return PMT.df_to_points(
        df=transit_df,
        out_fc=out_fc,
        shape_fields=shape_fields,
        from_sr=from_sr,
        to_sr=to_sr,
        overwrite=True,
    )


# Parcel data
def clean_parcel_geometry(in_features, fc_key_field, new_fc_key, out_features=None):
    """
    Cleans parcel geometry (repair faulty geometries, drop records with empty geometries,
    dissolve identical polygons, etc.) and sets common key to user supplied new_fc_key.
    Output features may have non-unique key field values if the original geometry is
    multi-part.

    Args:
        in_features (str): path to raw parcel shapefile
        fc_key_field (str): primary key of raw shapefile data
        new_fc_key (str): new primary key name used throughout processing
        out_features (str): path to output parcel feature class

    Returns:
        None
    """
    try:
        if out_features is None:
            raise ValueError
        else:
            temp_fc = PMT.make_inmem_path()
            arcpy.CopyFeatures_management(
                in_features=in_features, out_feature_class=temp_fc
            )
            # Repair geom and remove null geoms
            print("--- repair geometry")
            arcpy.RepairGeometry_management(
                in_features=temp_fc, delete_null="DELETE_NULL"
            )
            # Alter field
            arcpy.AlterField_management(
                in_table=temp_fc,
                field=fc_key_field,
                new_field_name=new_fc_key,
                new_field_alias=new_fc_key,
            )
            # Dissolve polygons
            print(
                f"--- dissolving parcel polygons on {new_fc_key}, "
                f"and adding polygon count per {new_fc_key}"
            )
            arcpy.Dissolve_management(
                in_features=temp_fc,
                out_feature_class=out_features,
                dissolve_field=new_fc_key,
                statistics_fields="{} COUNT".format(new_fc_key),
                multi_part="MULTI_PART",
            )
            return out_features
    except ValueError:
        print("output feature class path must be provided")


def prep_parcel_land_use_tbl(
        parcels_fc,
        parcel_lu_field,
        parcel_fields,
        lu_tbl,
        tbl_lu_field,
        null_value=None,
        dtype_map=None,
        **kwargs,
):
    """
    Generate a table that combines parcels having detailed DOR use codes with generalized
        land use classifications.

    Args:
        parcels_fc (str): path to parcel feature class
        parcel_lu_field (str): String; The column in `parcels_fc` with each parcel's DOR use code.
        parcel_fields: [String, ...]; Other columns in `parcels_fc` (such as an ID field, e.g.) to retain
            alongside land use codes.
        lu_tbl: Path; A csv table of land use groupings to associated with each parcel,
            keyed by DOR use code.
        tbl_lu_field: String; The column in `lu_tbl` with DOR use codes.
        null_value: var or dict; default values for nulls in `parcels_fc`. If a dict is given, the keys are
            column names and values are default values to assign.
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
    par_df = PMT.featureclass_to_df(
        in_fc=parcels_fc, keep_fields=par_fields, null_val=null_value
    )

    # Read in the land use reference table
    ref_table = pd.read_csv(lu_tbl, dtype=dtype_map, **kwargs)
    # Join
    return par_df.merge(
        right=ref_table, how="left", left_on=parcel_lu_field, right_on=tbl_lu_field
    )


# TODO: remove default values and be sure to import globals in preparer.py and insert there
def enrich_bg_with_parcels(
        bg_fc,
        parcels_fc,
        sum_crit=None,
        bg_id_field=None,
        par_id_field=None,
        par_lu_field=None,
        par_bld_area=None,
        par_sum_fields=None,
):
    """
    Relates parcels to block groups based on parcel centroid location and summarizes
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
    if bg_id_field is None:
        bg_id_field = "GEOID10"
    if par_id_field is None:
        par_id_field = "PARCELNO"
    if par_lu_field is None:
        par_lu_field = "DOR_UC"
    if par_bld_area is None:
        par_bld_area = "TOT_LVG_AREA"
    if par_sum_fields is None:
        par_sum_fields = [
            "LND_VAL",
            "LND_SQFOOT",
            "JV",
            "NO_BULDNG",
            "NO_RES_UNTS",
            "TOT_LVG_AREA",
        ]
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
        with arcpy.da.SearchCursor(bg_fc, bg_fields, spatial_reference=sr) as bgc:
            for bgr in bgc:
                bg_poly, bg_id = bgr
                # Select parcels in this BG
                arcpy.SelectLayerByLocation_management(
                    parcel_fl, "HAVE_THEIR_CENTER_IN", bg_poly
                )
                # Dump selected to data frame
                par_df = PMT.featureclass_to_df(
                    in_fc=parcel_fl, keep_fields=par_fields, null_val=0
                )
                if len(par_df) == 0:
                    print(
                        f"---  --- no parcels found for BG {bg_id}"
                    )  # TODO: convert to warning?
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
    """
    Helper function to map data types from arcgis type to pandas type

    Args:
        in_table (str): path to table
        field (str): field of interest

    Returns:
        dtype of input field in format pandas can use
    """
    arc_to_pd = {"Double": np.float, "Integer": np.int, "String": np.str}
    f_dtype = [f.type for f in arcpy.ListFields(in_table) if f.name == field]
    return arc_to_pd.get(f_dtype[0])


def enrich_bg_with_econ_demog(
        tbl_path, tbl_id_field, join_tbl, join_id_field, join_fields
):
    """
    Adds data from another raw table as new columns based on the fields provided.
    Handles compressed inputs and data typing on-the-fly.

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
            tbl_df = pd.read_csv(join_tbl, usecols=join_fields, compression="gzip")
        else:
            tbl_df = pd.read_csv(join_tbl, usecols=join_fields)
        tbl_id_type = get_field_dtype(in_table=tbl_path, field=tbl_id_field)
        tbl_df[join_id_field] = tbl_df[join_id_field].astype(tbl_id_type)
        if all(item in tbl_df.columns.values.tolist() for item in join_fields):
            PMT.extend_table_df(
                in_table=tbl_path,
                table_match_field=tbl_id_field,
                df=tbl_df,
                df_match_field=join_id_field,
            )
        else:
            raise ValueError
    except ValueError:
        print("--- --- fields provided are not in join table (join_tbl)")


def prep_parcels(
        in_fc,
        in_tbl,
        out_fc,
        fc_key_field=None,
        new_fc_key_field=None,
        tbl_key_field=None,
        tbl_renames=None,
        **kwargs,
):
    """
    Starting with raw parcel features and raw parcel attributes in a table,
        clean features by repairing invalid geometries, deleting null geometries,
        and dissolving common parcel ID's. Then join attribute data based on
        the parcel ID field, managing column names in the process.
    Args:
        in_fc (str): Path or feature layer; A collection of raw parcel features (shapes)
        in_tbl (str): Path to a table of raw parcel attributes.
        out_fc (str): The path to the output feature class that will contain clean parcel
            geometries with attribute columns joined.
        fc_key_field (str, default="PARCELNO"): The field in `in_fc` that identifies each parcel feature.
        new_fc_key_field (str, default=None): parcel common key used throughout downstream processing
        tbl_key_field (str, default=None): The field in `in_csv` that identifies each parcel feature.
        tbl_renames (dict, default=None): Dictionary for renaming columns from `in_csv`. Keys are current column
            names; values are new column names.
        kwargs:
            Keyword arguments for reading csv data into pandas (dtypes, e.g.)
    Returns:
        None, updates parcel data
    """
    if fc_key_field is None:
        fc_key_field = "PARCELNO"
    if new_fc_key_field is None:
        new_fc_key_field = "FOLIO"
    if tbl_key_field is None:
        tbl_key_field = "PARCEL_ID"
    # prepare geometry data
    print("--- cleaning geometry")
    if tbl_renames is None:
        tbl_renames = {}
    out_fc = clean_parcel_geometry(
        in_features=in_fc,
        fc_key_field=fc_key_field,
        new_fc_key=new_fc_key_field,
        out_features=out_fc,
    )
    # Read tabular files
    _, ext = os.path.splitext(in_tbl)
    if ext == ".csv":
        print("--- read csv tables")
        par_df = pd.read_csv(filepath_or_buffer=in_tbl, **kwargs)
    elif ext == ".dbf":
        print("--- read dbf tables")
        par_df = PMT.dbf_to_df(dbf_file=in_tbl, upper=False)
    else:
        print("input parcel tabular data must be 'dbf' or 'csv'")
    # ensure key is 12 characters with leading 0's for join to geo data
    par_df[tbl_key_field] = par_df[tbl_key_field].map(lambda x: f"{x:0>12}")
    # Rename columns if needed
    print("--- renaming columns")
    tbl_renames[tbl_key_field] = new_fc_key_field
    par_df.rename(mapper=tbl_renames, axis=1, inplace=True)

    # Add columns to dissolved features
    print("--- joining attributes to features")
    PMT.extend_table_df(
        in_table=out_fc,
        table_match_field=new_fc_key_field,
        df=par_df,
        df_match_field=new_fc_key_field,
    )


# impervious surface
def get_raster_file(folder):
    """
    Get the name of the raster from within the zip (the .img file), there should be only one

    Args:
        folder (str): path to folder containing raster data

    Returns:
        path to raster file
    """

    rast_files = []
    raster_formats = [".img", ".tif"]
    print(f"--- finding all raster files of type {raster_formats}")
    try:
        for file in os.listdir(folder):
            for extension in raster_formats:
                if fnmatch.fnmatch(file, f"*{extension}"):
                    rast_files.append(PMT.make_path(folder, file))

        if len(rast_files) == 1:
            return rast_files[0]
        else:
            raise ValueError
    except ValueError:
        print("More than one Raster/IMG file is present in the zipped folder")


def prep_imperviousness(zip_path, clip_path, out_dir, transform_crs=None):
    """
    Clean a USGS impervious surface raster by clipping to the bounding box of a study area
        and transforming the clipped raster to a desired CRS

    Args:
        zip_path (str): Path to a .zip folder of downloaded imperviousness raster (see the
            `dl_imperviousness` function)
        clip_path (str): Path of study area polygon(s) whose bounding box will be used to clip
            the raster
        out_dir (str): Path to a save location for the clipped and transformed raster
        transform_crs (any type accepted by arcpy.SpatialReference(), default=None)
            Identifier of spatial reference to which to transform the clipped
            raster.

    Returns:
        out_raster (str): File will be clipped, transformed, and saved to the save directory; the
            save path will be returned upon completion
    """
    with tempfile.TemporaryDirectory() as temp_unzip_folder:
        print("--- unzipping imperviousness raster in temp directory")
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(temp_unzip_folder)
        raster_file = get_raster_file(folder=temp_unzip_folder)

        # define the output file from input file
        rast_name, ext = os.path.splitext(os.path.split(raster_file)[1])
        clipped_raster = PMT.make_path(temp_unzip_folder, f"clipped{ext}")

        print("--- checking if a transformation of the clip geometry is necessary")
        # Transform the clip geometry if necessary
        raster_sr = arcpy.Describe(raster_file).spatialReference
        clip_sr = arcpy.Describe(clip_path).spatialReference
        if raster_sr != clip_sr:
            print("--- reprojecting clipping geometry to match raster")
            project_file = PMT.make_path(temp_unzip_folder, "Project.shp")
            arcpy.Project_management(
                in_dataset=clip_path,
                out_dataset=project_file,
                out_coor_system=raster_sr,
            )
            clip_path = project_file

        # Grab the bounding box of the clipping file
        print("--- clipping raster data to project extent")
        bbox = arcpy.Describe(clip_path).Extent
        arcpy.Clip_management(
            in_raster=raster_file,
            rectangle="",
            out_raster=clipped_raster,
            in_template_dataset=bbox.polygon,
            clipping_geometry="ClippingGeometry",
        )

        # Transform the clipped raster
        print("--- copying/reprojecting raster out to project CRS")
        transform_crs = arcpy.SpatialReference(transform_crs)
        out_raster = PMT.make_path(out_dir, f"{rast_name}_clipped{ext}")
        if transform_crs != raster_sr:
            arcpy.ProjectRaster_management(
                in_raster=clipped_raster,
                out_raster=out_raster,
                out_coor_system=transform_crs,
                resampling_type="NEAREST",
            )
        else:
            arcpy.CopyRaster_management(
                in_raster=clipped_raster, out_rasterdataset=out_raster
            )
    return out_raster


def analyze_imperviousness(raster_points, rast_cell_area, zone_fc, zone_id_field):
    """
    Summarize percent impervious surface cover in each of a collection of zones

    Args:
        raster_points (str): Path to clipped/transformed imperviousness raster as points (see the
            `prep_imperviousness` function)
        rast_cell_area (float): numeric value for pixel area on the ground
        zone_fc (str): Path to polygon geometries to which imperviousness will be summarized
        zone_id_field (str): id field in the zone geometries
    
    Returns:
        df (pandas dataframe): table of impervious percent within the zone geometries
    """
    print("--- matching imperviousness to zone geometries")
    temp_feat = PMT.make_inmem_path()
    arcpy.Intersect_analysis(
        in_features=[raster_points, zone_fc], out_feature_class=temp_feat
    )

    # Load the intersection data
    print("--- loading raster/zone data and replacing null impervious values with 0")
    load_fields = [zone_id_field, "grid_code"]
    df = PMT.featureclass_to_df(in_fc=temp_feat, keep_fields=load_fields, null_val=0)
    # Values with 127 are nulls -- replace with 0
    df["grid_code"] = df["grid_code"].replace(127, 0)
    arcpy.Delete_management(temp_feat)

    print("--- summarizing zonal imperviousness statistics")
    # Groupby-summarise the variables of interest
    print("--- --- calculating zonal summaries")
    zonal = df.groupby(zone_id_field)["grid_code"].agg(
        [
            ("IMP_PCT", np.mean),
            ("TotalArea", lambda x: x.count() * rast_cell_area),
            ("NonDevArea", lambda x: x[x == 0].count() * rast_cell_area),
            ("DevOSArea", lambda x: x[x.between(1, 19)].count() * rast_cell_area),
            ("DevLowArea", lambda x: x[x.between(20, 49)].count() * rast_cell_area),
            ("DevMedArea", lambda x: x[x.between(50, 79)].count() * rast_cell_area),
            ("DevHighArea", lambda x: x[x >= 80].count() * rast_cell_area),
        ]
    )
    return zonal.reset_index()


def agg_to_zone(parcel_fc, agg_field, zone_fc, zone_id):
    """
    Aggregate parcel data up to a zone feature class, limited to one field for aggregation

    Args:
        parcel_fc (str): parcel feature class path
        agg_field (str): field to be aggregated
        zone_fc (str): feature class data will be summarized to
        zone_id (str): primary key of zone fc

    Returns:
        pandas.DataFrame: dataframe of zoneID and summarized attribute
    """
    parcel_pts = PMT.polygons_to_points(
        in_fc=parcel_fc,
        out_fc=PMT.make_inmem_path(),
        fields=[agg_field],
        null_value=0.0,
    )
    block_par = arcpy.SpatialJoin_analysis(
        target_features=zone_fc,
        join_features=parcel_pts,
        join_operation="JOIN_ONE_TO_MANY",
        out_feature_class=PMT.make_inmem_path(),
    )
    block_par = arcpy.Dissolve_management(
        in_features=block_par,
        out_feature_class=PMT.make_inmem_path(),
        dissolve_field=zone_id,
        statistics_fields=f"{agg_field} SUM",
    )
    arcpy.AlterField_management(
        in_table=block_par,
        field=f"SUM_{agg_field}",
        new_field_name=agg_field,
        new_field_alias=agg_field,
    )
    df = PMT.featureclass_to_df(
        in_fc=block_par, keep_fields=[zone_id, agg_field], null_val=0.0
    )
    return df


def model_blockgroup_data(
        data_path, bg_enrich_tbl_name, bg_key, fields="*", acs_years=None, lodes_years=None,
):
    """
    Fit linear models to block group-level total employment, population, and
        commutes at the block group level, and save the model coefficients for
        future prediction

    Args:
        data_path (str): path to enriched block group data, with a fixed-string wild card for
            year (see Notes)
        bg_enrich_tbl_name (str): name of enriched block group table data
            see Notes
        bg_key (str): name of primary key to block group data
        fields (list): list of fields to use for processing
        acs_years (list): list of int
            years for which ACS variables (population, commutes) are present in the data
        lodes_years (list): list of int
            years for which LODES variables (employment) are present in the data
    
    Notes:
        in `bg_enrich_path`, replace the presence of a year with the string
        "{year}". For example, if your enriched block group data for 2010-2015 is
        stored at "Data_2010.gdb/enriched", "PMT_2019.gdb/Enrichment_census_blockgroups", ..., then
        `bg_enrich_tbl_name = "Enrichment_census_blockgroups"`.
    
    Returns:
       coeffs (pandas.DataFrame): Data frame of model coefficients
    """

    print("--- reading input data (block group)")
    df = []
    year_gdb = PMT.make_path(data_path, "PMT_YEAR.gdb")
    years = np.unique(np.concatenate([acs_years, lodes_years]))
    for year in years:
        print(" ".join(["----> Loading", str(year)]))
        load_path = PMT.make_path(year_gdb.replace("YEAR", str(year)), bg_enrich_tbl_name)
        tab = PMT.featureclass_to_df(in_fc=load_path, keep_fields=fields, null_val=0.0)

        # Edit
        tab["Year"] = year
        tab["Since_2013"] = year - 2013
        tab["Total_Emp_Area"] = (
                tab["CNS_01_par"]
                + tab["CNS_02_par"]
                + tab["CNS_03_par"]
                + tab["CNS_04_par"]
                + tab["CNS_05_par"]
                + tab["CNS_06_par"]
                + tab["CNS_07_par"]
                + tab["CNS_08_par"]
                + tab["CNS_09_par"]
                + tab["CNS_10_par"]
                + tab["CNS_11_par"]
                + tab["CNS_12_par"]
                + tab["CNS_13_par"]
                + tab["CNS_14_par"]
                + tab["CNS_15_par"]
                + tab["CNS_16_par"]
                + tab["CNS_17_par"]
                + tab["CNS_18_par"]
                + tab["CNS_19_par"]
                + tab["CNS_20_par"]
        )
        if year in lodes_years:
            tab["Total_Employment"] = (
                    tab["CNS01"]
                    + tab["CNS02"]
                    + tab["CNS03"]
                    + tab["CNS04"]
                    + tab["CNS05"]
                    + tab["CNS06"]
                    + tab["CNS07"]
                    + tab["CNS08"]
                    + tab["CNS09"]
                    + tab["CNS10"]
                    + tab["CNS11"]
                    + tab["CNS12"]
                    + tab["CNS13"]
                    + tab["CNS14"]
                    + tab["CNS15"]
                    + tab["CNS16"]
                    + tab["CNS17"]
                    + tab["CNS18"]
                    + tab["CNS19"]
                    + tab["CNS20"]
            )
        if year in acs_years:
            tab["Total_Population"] = tab["Total_Non_Hisp"] + tab["Total_Hispanic"]
        df.append(tab)
    df = pd.concat(df, ignore_index=True)

    # 2. Model
    # Variable setup: defines our variables of interest for modeling
    independent_variables = [
        "LND_VAL",
        "LND_SQFOOT",
        "JV",
        "TOT_LVG_AREA",
        "NO_BULDNG",
        "NO_RES_UNTS",
        "RES_par",
        "CNS_01_par",
        "CNS_02_par",
        "CNS_03_par",
        "CNS_04_par",
        "CNS_05_par",
        "CNS_06_par",
        "CNS_07_par",
        "CNS_08_par",
        "CNS_09_par",
        "CNS_10_par",
        "CNS_11_par",
        "CNS_12_par",
        "CNS_13_par",
        "CNS_14_par",
        "CNS_15_par",
        "CNS_16_par",
        "CNS_17_par",
        "CNS_18_par",
        "CNS_19_par",
        "CNS_20_par",
        "Total_Emp_Area",
        "Since_2013",
    ]
    response = {
        "Total_Employment": lodes_years,
        "Total_Population": acs_years,
        "Total_Commutes": acs_years,
    }

    # Step 1: Overwrite NA values with 0 (where we should have data but don't)
    # -- parcel-based variables should be there every time: fill all with 0
    # -- job variables should be there for `lodes_years`: fill these with 0
    # -- dem variables should be there for `acs_years`: fill these with 0
    print("--- replacing missing values")
    parcel_na_dict = {iv: 0 for iv in independent_variables}
    df.fillna(parcel_na_dict, inplace=True)
    df.loc[
        (df.Total_Employment.isna()) & df.Year.isin(lodes_years), "Total_Employment"
    ] = 0
    df.loc[
        (df.Total_Population.isna()) & df.Year.isin(acs_years), "Total_Population"
    ] = 0
    df.loc[(df.Total_Commutes.isna()) & df.Year.isin(acs_years), "Total_Commutes"] = 0
    keep_cols = [bg_key, "Year"] + independent_variables + list(response.keys())
    df = df[keep_cols]

    # Step 2: conduct modeling by extracting a correlation matrix between candidate
    # explanatories and our responses, identifying explanatories with significant
    # correlations to our response, and fitting a MLR using these explanatories
    print("--- fitting and applying models")
    fits = []
    for key, value in response.items():
        print(" ".join(["---->", key]))
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
        p_values = pd.Series(scipy.stats.t.sf(t_stat, n - 2) * 2, index=t_stat.index)
        # Variables for the model
        mod_vars = []
        cutoff = 0.05
        while len(mod_vars) == 0:
            mod_vars = p_values[p_values.le(cutoff)].index.tolist()
            cutoff += 0.05
        # Fit a multiple linear regression
        regr = linear_model.LinearRegression()
        regr.fit(X=mdf[mod_vars], y=mdf[[key]])
        # Save the model coefficients
        fits.append(pd.Series(regr.coef_[0], index=mod_vars, name=key))

    # Step 3: combine results into a single df
    print("--- formatting model coefficients into a single table")
    coefs = pd.concat(fits, axis=1).reset_index()
    coefs.rename(columns={"index": "Variable"}, inplace=True)
    coefs.fillna(0, inplace=True)

    return coefs


def apply_blockgroup_model(
        year,
        bg_enrich_path,
        bg_geometry_path,
        bg_id_field,
        model_coefficients,
        shares_from=None,
):
    """
    Predict block group-level total employment, population, and commutes using
        pre-fit linear models, and apply a shares-based approach to subdivide
        totals into relevant subgroups

    Args:
        year (int): year of the `bg_enrich` data
        bg_enrich_path (str): path to enriched block group data; this is the data to which the
            models will be applied
        bg_geometry_path (str): path to geometry of block groups underlying the data
        bg_id_field (str): block group unique id column
        model_coefficients (pd.DataFrame): pandas.DataFrame of model coefficients
        shares_from (dict): optional
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
        alloc (pd.DataFrame): pd.DataFrame of model application results
    """

    print("--- reading input data (block group)")
    fields = [f.name for f in arcpy.ListFields(bg_enrich_path)]
    df = PMT.featureclass_to_df(in_fc=bg_enrich_path, keep_fields=fields, null_val=0.0)

    df["Since_2013"] = year - 2013
    df["Total_Emp_Area"] = (
            df["CNS_01_par"]
            + df["CNS_02_par"]
            + df["CNS_03_par"]
            + df["CNS_04_par"]
            + df["CNS_05_par"]
            + df["CNS_06_par"]
            + df["CNS_07_par"]
            + df["CNS_08_par"]
            + df["CNS_09_par"]
            + df["CNS_10_par"]
            + df["CNS_11_par"]
            + df["CNS_12_par"]
            + df["CNS_13_par"]
            + df["CNS_14_par"]
            + df["CNS_15_par"]
            + df["CNS_16_par"]
            + df["CNS_17_par"]
            + df["CNS_18_par"]
            + df["CNS_19_par"]
            + df["CNS_20_par"]
    )
    # Fill na
    independent_variables = [
        "LND_VAL",
        "LND_SQFOOT",
        "JV",
        "TOT_LVG_AREA",
        "NO_BULDNG",
        "NO_RES_UNTS",
        "RES_par",
        "CNS_01_par",
        "CNS_02_par",
        "CNS_03_par",
        "CNS_04_par",
        "CNS_05_par",
        "CNS_06_par",
        "CNS_07_par",
        "CNS_08_par",
        "CNS_09_par",
        "CNS_10_par",
        "CNS_11_par",
        "CNS_12_par",
        "CNS_13_par",
        "CNS_14_par",
        "CNS_15_par",
        "CNS_16_par",
        "CNS_17_par",
        "CNS_18_par",
        "CNS_19_par",
        "CNS_20_par",
        "Total_Emp_Area",
        "Since_2013",
    ]
    parcel_na_dict = {iv: 0 for iv in independent_variables}
    df.fillna(parcel_na_dict, inplace=True)

    # 2. Apply models
    # ---------------
    print("--- applying models to predict totals")
    # Load the coefficients
    coefs = model_coefficients
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
    # Variable setup: defines our variables of interest for modeling
    dependent_variables_emp = [
        "CNS01",
        "CNS02",
        "CNS03",
        "CNS04",
        "CNS05",
        "CNS06",
        "CNS07",
        "CNS08",
        "CNS09",
        "CNS10",
        "CNS11",
        "CNS12",
        "CNS13",
        "CNS14",
        "CNS15",
        "CNS16",
        "CNS17",
        "CNS18",
        "CNS19",
        "CNS20",
    ]
    dependent_variables_pop_tot = ["Total_Hispanic", "Total_Non_Hisp"]
    dependent_variables_pop_sub = [
        "White_Hispanic",
        "Black_Hispanic",
        "Asian_Hispanic",
        "Multi_Hispanic",
        "Other_Hispanic",
        "White_Non_Hisp",
        "Black_Non_Hisp",
        "Asian_Non_Hisp",
        "Multi_Non_Hisp",
        "Other_Non_Hisp",
    ]
    dependent_variables_trn = [
        "Drove",
        "Carpool",
        "Transit",
        "NonMotor",
        "Work_From_Home",
        "AllOther",
    ]
    acs_vars = (
            dependent_variables_pop_tot
            + dependent_variables_pop_sub
            + dependent_variables_trn
    )

    # Pull shares variables from appropriate sources, recognizing that they
    # may not all be the same!
    print("--- formatting shares data")
    # Format
    if shares_from is not None:
        if "LODES" in shares_from.keys():
            lodes = PMT.featureclass_to_df(
                in_fc=shares_from["LODES"],
                keep_fields=[bg_id_field] + dependent_variables_emp,
                null_val=0.0,
            )
        else:
            lodes = df[[bg_id_field] + dependent_variables_emp]
        if "ACS" in shares_from.keys():
            acs = PMT.featureclass_to_df(
                in_fc=shares_from["ACS"],
                keep_fields=[bg_id_field] + acs_vars,
                null_val=0.0,
            )
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
    print("--- calculating shares")
    shares_dict = {}
    for name, variables in zip(
            ["Emp", "Pop_Tot", "Pop_Sub", "Comm"],
            [
                dependent_variables_emp,
                dependent_variables_pop_tot,
                dependent_variables_pop_sub,
                dependent_variables_trn,
            ],
    ):
        sdf = shares_df[variables]
        sdf["TOTAL"] = sdf.sum(axis=1)
        for d in variables:
            sdf[d] = sdf[d] / sdf["TOTAL"]
        sdf[bg_id_field] = shares_df[bg_id_field]
        sdf.drop(columns="TOTAL", inplace=True)
        shares_dict[name] = sdf

    # Step 3: some rows have NA shares because the total for that class of variables was 0. For these block groups,
    # take the average share of all block groups that touch that one
    print("--- estimating missing shares")
    # What touches what? # TODO: add make_inmem
    temp = PMT.make_inmem_path()
    arcpy.PolygonNeighbors_analysis(
        in_features=bg_geometry_path, out_table=temp, in_fields=bg_id_field
    )
    touch = PMT.featureclass_to_df(
        in_fc=temp, keep_fields=[f"src_{bg_id_field}", f"nbr_{bg_id_field}"]
    )
    touch.rename(
        columns={f"src_{bg_id_field}": bg_id_field, f"nbr_{bg_id_field}": "Neighbor"},
        inplace=True,
    )
    # Loop filling of NA by mean of adjacent non-NAs
    ctf = 1
    i = 1
    while ctf > 0:
        # First, identify cases where we need to fill NA
        to_fill = []
        for key, value in shares_dict.items():
            f = value[value.isna().any(axis=1)]
            f = f[[bg_id_field]]
            f["Fill"] = key
            to_fill.append(f)
        to_fill = pd.concat(to_fill, ignore_index=True)
        # Create a neighbors table
        neightbor_table = pd.merge(to_fill, touch, how="left", on=bg_id_field)
        neightbor_table.rename(
            columns={bg_id_field: "Source", "Neighbor": bg_id_field}, inplace=True
        )
        # Now, merge in the shares data for appropriate rows
        fill_by_touching = {}
        nrem = []
        for key, value in shares_dict.items():
            fill_df = pd.merge(
                neightbor_table[neightbor_table.Fill == key],
                value,
                how="left",
                on=bg_id_field,
            )
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
        # Now, it's possible that some block group/year combos to be filled had 0 block groups in that year
        # touching them that had data. If this happened, we're going to repeat the process. Check by summing nrem
        # and initialize by resetting the shares dict
        ctf = sum(nrem)
        i += 1
        shares_dict = fill_by_touching

    # Step 4: merge and format the shares
    print("--- merging and formatting shares")
    filled_shares = [df.set_index(bg_id_field) for df in shares_dict.values()]
    cs_shares = pd.concat(filled_shares, axis=1).reset_index()
    cs_shares.rename(columns={"index": bg_id_field}, inplace=True)

    # 4. Block group estimation
    # -------------------------

    print("--- estimating variable levels using model estimates and shares")
    # Now, our allocations are simple multiplication problems! Hooray!
    # So, all we have to do is multiply the shares by the appropriate column
    # First, we'll merge our estimates and shares
    alloc = pd.merge(pwrite, cs_shares, on=bg_id_field)
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

    return alloc


def allocate_bg_to_parcels(
        bg_modeled_df,
        bg_geom,
        bg_id_field,
        parcel_fc,
        parcels_id="FOLIO",
        parcel_wc="",
        parcel_lu="DOR_UC",
        parcel_liv_area="TOT_LVG_AREA",
):
    """
    Allocate block group data to parcels using relative abundances (proportions of total among all parcels)
    of parcel building square footage

    Args:
        bg_modeled_df (pd.DataFrame): pandas DataFrame of modeled block group job, population, and commute
            data for allocation
        bg_geom (str): Path; path to feature class of block group polygons
        bg_id_field (str): block group key
        parcel_fc (str): Path to shape of parcel polygons, containing at a minimum a unique ID
            field, land use field, and total living area field (Florida DOR)
        parcels_id (str, default="FOLIO"): unique ID field in the parcels shape
        parcel_wc (str, default=""): where clause to select out parcels and limit allocation to only the
            selected parcels (as when allocating NearTerm permitted parcels)
        parcel_lu (str, default="DOR_UC"): land use code field in the parcels shape
        parcel_liv_area (str, default="TOT_LVG_AREA"): building square footage field in the parcels shape

    Returns:
        intersect_df (pd.DataFrame): dataframe of the resultant allocation based on model

    """
    if parcels_id is None:
        parcels_id = "FOLIO"
    if parcel_lu is None:
        parcel_lu = "DOR_UC"
    if parcel_liv_area is None:
        parcel_liv_area = "TOT_LVG_AREA"

    # Organize constants for allocation
    lodes_attrs = [
        "CNS01",
        "CNS02",
        "CNS03",
        "CNS04",
        "CNS05",
        "CNS06",
        "CNS07",
        "CNS08",
        "CNS09",
        "CNS10",
        "CNS11",
        "CNS12",
        "CNS13",
        "CNS14",
        "CNS15",
        "CNS16",
        "CNS17",
        "CNS18",
        "CNS19",
        "CNS20",
    ]
    demog_attrs = [
        "Total_Hispanic",
        "White_Hispanic",
        "Black_Hispanic",
        "Asian_Hispanic",
        "Multi_Hispanic",
        "Other_Hispanic",
        "Total_Non_Hisp",
        "White_Non_Hisp",
        "Black_Non_Hisp",
        "Asian_Non_Hisp",
        "Multi_Non_Hisp",
        "Other_Non_Hisp",
    ]
    commute_attrs = [
        "Drove",
        "Carpool",
        "Transit",
        "NonMotor",
        "Work_From_Home",
        "AllOther",
    ]
    block_group_attrs = [bg_id_field] + lodes_attrs + demog_attrs + commute_attrs

    # Initialize spatial processing by intersecting
    print("--- intersecting blocks and parcels")
    temp_spatial = PMT.make_inmem_path()
    PMT.copy_features(in_fc=bg_geom, out_fc=temp_spatial)
    # drop any duplicated fields from bg_modeled_df
    dups = [
        f.name
        for f in arcpy.ListFields(temp_spatial)
        if f.name in bg_modeled_df.columns.to_list() and f.name != bg_id_field
    ]
    bg_modeled_df.drop(columns=dups, inplace=True)
    PMT.extend_table_df(
        in_table=temp_spatial,
        table_match_field=bg_id_field,
        df=bg_modeled_df,
        df_match_field=bg_id_field,
    )

    # feature layer created in the event a where clause is provided
    parcel_fl = arcpy.MakeFeatureLayer_management(
        in_features=parcel_fc, out_layer="parcel_fl", where_clause=parcel_wc
    )
    parcel_fields = [parcels_id, parcel_lu, parcel_liv_area, "Shape_Area"]
    intersect_fc = PMT.intersect_features(
        summary_fc=temp_spatial, disag_fc=parcel_fl, disag_fields=parcel_fields
    )
    intersect_fields = parcel_fields + block_group_attrs
    intersect_df = PMT.featureclass_to_df(
        in_fc=intersect_fc, keep_fields=intersect_fields
    )

    # Format data for allocation
    print("--- formatting block group for allocation data")
    # set any value below 0 to 0 and set any land use from -1 to NA
    to_clip = (
            lodes_attrs + demog_attrs + commute_attrs + [parcel_liv_area, "Shape_Area"]
    )
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
        "CNS01": ((intersect_df[pluf] >= 50) & (intersect_df[pluf] <= 69)),
        "CNS02": (intersect_df[pluf] == 92),
        "CNS03": (intersect_df[pluf] == 91),
        "CNS04": ((intersect_df[pluf] == 17) | (intersect_df[pluf] == 19)),
        "CNS05": ((intersect_df[pluf] == 41) | (intersect_df[pluf] == 42)),
        "CNS06": (intersect_df[pluf] == 29),
        "CNS07": ((intersect_df[pluf] >= 11) & (intersect_df[pluf] <= 16)),
        "CNS08": (
                (intersect_df[pluf] == 48)
                | (intersect_df[pluf] == 49)
                | (intersect_df[pluf] == 20)
        ),
        "CNS09": (
                (intersect_df[pluf] == 17)
                | (intersect_df[pluf] == 18)
                | (intersect_df[pluf] == 19)
        ),
        "CNS10": ((intersect_df[pluf] == 23) | (intersect_df[pluf] == 24)),
        "CNS11": (
                (intersect_df[pluf] == 17)
                | (intersect_df[pluf] == 18)
                | (intersect_df[pluf] == 19)
        ),
        "CNS12": (
                (intersect_df[pluf] == 17)
                | (intersect_df[pluf] == 18)
                | (intersect_df[pluf] == 19)
        ),
        "CNS13": (
                (intersect_df[pluf] == 17)
                | (intersect_df[pluf] == 18)
                | (intersect_df[pluf] == 19)
        ),
        "CNS14": (intersect_df[pluf] == 89),
        "CNS15": (
                (intersect_df[pluf] == 72)
                | (intersect_df[pluf] == 83)
                | (intersect_df[pluf] == 84)
        ),
        "CNS16": ((intersect_df[pluf] == 73) | (intersect_df[pluf] == 85)),
        "CNS17": (
                ((intersect_df[pluf] >= 30) & (intersect_df[pluf] <= 38))
                | (intersect_df[pluf] == 82)
        ),
        "CNS18": (
                (intersect_df[pluf] == 21)
                | (intersect_df[pluf] == 22)
                | (intersect_df[pluf] == 33)
                | (intersect_df[pluf] == 39)
        ),
        "CNS19": ((intersect_df[pluf] == 27) | (intersect_df[pluf] == 28)),
        "CNS20": ((intersect_df[pluf] >= 86) & (intersect_df[pluf] <= 89)),
        "Population": (
                ((intersect_df[pluf] >= 1) & (intersect_df[pluf] <= 9))
                | ((intersect_df[pluf] >= 100) & (intersect_df[pluf] <= 102))
        ),
    }

    # Note that our activity-land use matches aren't guaranteed because they are subjectively defined.
    # To that end, we need backups in case a block group is entirely missing all possible land uses
    # for an activity.
    #   - we set up masks for 'all non-res' (all land uses relevant to any non-NAICS-1-or-2 job type)
    #   - and 'all developed' ('all non-res' + any residential land uses).
    #  ['all non-res' will be used if a land use isn't present for a given activity;
    #  [ the 'all developed' will be used if 'all non-res' fails]
    non_res_lu_codes = [
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
        24,
        27,
        28,
        29,
        30,
        31,
        32,
        33,
        34,
        35,
        36,
        37,
        38,
        39,
        41,
        42,
        48,
        49,
        72,
        73,
        82,
        84,
        85,
        86,
        87,
        88,
        89,
    ]
    all_dev_lu_codes = non_res_lu_codes + [1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 101, 102]
    all_non_res = {"NR": (intersect_df[pluf].isin(non_res_lu_codes))}
    all_developed = {"AD": (intersect_df[pluf].isin(all_dev_lu_codes))}

    # If all else fails, A fourth level we'll use (if we need to) is simply all total living area in the block group,
    #   but we don't need a mask for that. If this fails (which it rarely should), we revert to land area,
    #   which we know will work (all parcels have area right?)

    # Next, we'll total parcels by block group (this is just a simple operation
    # to give our living area totals something to join to)
    print("--- initializing living area sums")
    count_parcels_bg = intersect_df.groupby([bg_id_field])[bg_id_field].agg(["count"])
    count_parcels_bg.rename(columns={"count": "NumParBG"}, inplace=True)
    count_parcels_bg = count_parcels_bg.reset_index()

    # Now we can begin totaling living area. We'll start with jobs
    print("--- totaling living area by job type")
    # 1. get count of total living area (w.r.t. land use mask) for each
    # job type
    pldaf = "Shape_Area"
    for var in lodes_attrs:
        # mask by LU, group on bg_id_field
        area = (
            intersect_df[lu_mask[var]]
                .groupby([bg_id_field])[parcel_liv_area]
                .agg(["sum"])
        )
        area.rename(columns={"sum": f"{var}_Area"}, inplace=True)
        area = area[area[f"{var}_Area"] > 0]
        area = area.reset_index()
        area[f"{var}_How"] = "lu_mask"
        missing = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
        if len(missing) > 0:
            lev1 = intersect_df[all_non_res["NR"]]
            lev1 = lev1[lev1[bg_id_field].isin(missing)]
            area1 = lev1.groupby([bg_id_field])[parcel_liv_area].agg(["sum"])
            area1.rename(columns={"sum": f"{var}_Area"}, inplace=True)
            area1 = area1[area1[f"{var}_Area"] > 0]
            area1 = area1.reset_index()
            area1[f"{var}_How"] = "non_res"
            area = pd.concat([area, area1])
            missing1 = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
            if len(missing1) > 0:
                lev2 = intersect_df[all_developed["AD"]]
                lev2 = lev2[lev2[bg_id_field].isin(missing1)]
                area2 = lev2.groupby([bg_id_field])[parcel_liv_area].agg(["sum"])
                area2.rename(columns={"sum": f"{var}_Area"}, inplace=True)
                area2 = area2[area2[f"{var}_Area"] > 0]
                area2 = area2.reset_index()
                area2[f"{var}_How"] = "all_dev"
                area = pd.concat([area, area2])
                missing2 = list(
                    set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field])
                )
                if len(missing2) > 0:
                    lev3 = intersect_df[intersect_df[bg_id_field].isin(missing2)]
                    area3 = lev3.groupby([bg_id_field])[parcel_liv_area].agg(["sum"])
                    area3.rename(columns={"sum": f"{var}_Area"}, inplace=True)
                    area3 = area3[area3[f"{var}_Area"] > 0]
                    area3 = area3.reset_index()
                    area3[f"{var}_How"] = "living_area"
                    area = pd.concat([area, area3])
                    missing3 = list(
                        set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field])
                    )
                    if len(missing3) > 0:
                        lev4 = intersect_df[intersect_df[bg_id_field].isin(missing3)]
                        area4 = lev4.groupby([bg_id_field])[pldaf].agg(["sum"])
                        area4.rename(columns={"sum": f"{var}_Area"}, inplace=True)
                        area4 = area4.reset_index()
                        area4[f"{var}_How"] = "land_area"
                        area = pd.concat([area, area4])
        area = area.reset_index(drop=True)
        count_parcels_bg = pd.merge(count_parcels_bg, area, how="left", on=bg_id_field)

    # Repeat the above with population
    print("--- totaling living area for population")
    area = (
        intersect_df[lu_mask["Population"]]
            .groupby([bg_id_field])[parcel_liv_area]
            .agg(["sum"])
    )
    area.rename(columns={"sum": "Population_Area"}, inplace=True)
    area = area[area["Population_Area"] > 0]
    area = area.reset_index()
    area["Population_How"] = "lu_mask"
    missing1 = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
    if len(missing1) > 0:
        lev2 = intersect_df[all_developed["AD"]]
        lev2 = lev2[lev2[bg_id_field].isin(missing1)]
        area2 = lev2.groupby([bg_id_field])[parcel_liv_area].agg(["sum"])
        area2.rename(columns={"sum": "Population_Area"}, inplace=True)
        area2 = area2[area2["Population_Area"] > 0]
        area2 = area2.reset_index()
        area2["Population_How"] = "all_dev"
        area = pd.concat([area, area2])
        missing2 = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
        if len(missing2) > 0:
            lev3 = intersect_df[intersect_df[bg_id_field].isin(missing2)]
            area3 = lev3.groupby([bg_id_field])[parcel_liv_area].agg(["sum"])
            area3.rename(columns={"sum": "Population_Area"}, inplace=True)
            area3 = area3[area3["Population_Area"] > 0]
            area3 = area3.reset_index()
            area3["Population_How"] = "living_area"
            area = pd.concat([area, area3])
            missing3 = list(set(count_parcels_bg[bg_id_field]) - set(area[bg_id_field]))
            if len(missing3) > 0:
                lev4 = intersect_df[intersect_df[bg_id_field].isin(missing3)]
                area4 = lev4.groupby([bg_id_field])[pldaf].agg(["sum"])
                area4.rename(columns={"sum": "Population_Area"}, inplace=True)
                area4 = area4.reset_index()
                area4["Population_How"] = "land_area"
                area = pd.concat([area, area4])
    area = area.reset_index(drop=True)
    count_parcels_bg = pd.merge(count_parcels_bg, area, how="left", on=bg_id_field)

    # Now, we format and re-merge with our original parcel data
    print("--- merging living area totals with parcel-level data")
    # 1. fill table with NAs -- no longer needed because NAs are eliminated
    # by nesting structure
    # tot_bg = tot_bg.fillna(0)
    # 2. merge back to original data
    intersect_df = pd.merge(intersect_df, count_parcels_bg, how="left", on=bg_id_field)

    # Step 2 in allocation is taking parcel-level proportions of living area
    # relative to the block group total, and calculating parcel-level
    # estimates of activities by multiplying the block group activity total
    # by the parcel-level proportions

    # For allocation, we need a two step process, depending on how the area
    # was calculated for the activity. If "{var}_How" is land_area, then
    # allocation needs to be relative to land area; otherwise, it needs to be
    # relative to living area. To do this, we'll set up mask dictionaries
    # similar to the land use mask
    print("setting up allocation logic --- ")
    lu = {}
    nr = {}
    ad = {}
    lvg_area = {}
    lnd_area = {}
    for v in lu_mask.keys():
        lu[v] = intersect_df[f"{v}_How"] == "lu_mask"
        nr[v] = intersect_df[f"{v}_How"] == "non_res"
        ad[v] = intersect_df[f"{v}_How"] == "all_dev"
        lvg_area[v] = intersect_df[f"{v}_How"] == "living_area"
        lnd_area[v] = intersect_df[f"{v}_How"] == "land_area"

    # First up, we'll allocate jobs
    print("--- allocating jobs and population")
    # 1. for each job variable, calculate the proportion, then allocate
    for var in lu_mask.keys():
        # First for lu mask
        intersect_df.loc[lu[var] & lu_mask[var], f"{var}_Par_Prop"] = (
                intersect_df[parcel_liv_area][lu[var] & lu_mask[var]]
                / intersect_df[f"{var}_Area"][lu[var] & lu_mask[var]]
        )
        # Then for non res
        intersect_df.loc[nr[var] & all_non_res["NR"], f"{var}_Par_Prop"] = (
                intersect_df[parcel_liv_area][nr[var] & all_non_res["NR"]]
                / intersect_df[f"{var}_Area"][nr[var] & all_non_res["NR"]]
        )
        # Then for all dev
        intersect_df.loc[ad[var] & all_developed["AD"], f"{var}_Par_Prop"] = (
                intersect_df[parcel_liv_area][ad[var] & all_developed["AD"]]
                / intersect_df[f"{var}_Area"][ad[var] & all_developed["AD"]]
        )
        # Then for living area
        intersect_df.loc[lvg_area[var], f"{var}_Par_Prop"] = (
                intersect_df[parcel_liv_area][lvg_area[var]]
                / intersect_df[f"{var}_Area"][lvg_area[var]]
        )
        # Then for land area
        intersect_df.loc[lnd_area[var], f"{var}_Par_Prop"] = (
                intersect_df[pldaf][lnd_area[var]]
                / intersect_df[f"{var}_Area"][lnd_area[var]]
        )
        # Now fill NAs with 0 for proportions
        intersect_df[f"{var}_Par_Prop"] = intersect_df[f"{var}_Par_Prop"].fillna(0)

        # Now allocate (note that for pop, we're using the population ratios
        # for all racial subsets)
        if var != "Population":
            intersect_df[f"{var}_PAR"] = (
                    intersect_df[f"{var}_Par_Prop"] * intersect_df[var]
            )
        else:
            race_vars = [
                "Total_Hispanic",
                "White_Hispanic",
                "Black_Hispanic",
                "Asian_Hispanic",
                "Multi_Hispanic",
                "Other_Hispanic",
                "Total_Non_Hisp",
                "White_Non_Hisp",
                "Black_Non_Hisp",
                "Asian_Non_Hisp",
                "Multi_Non_Hisp",
                "Other_Non_Hisp",
            ]
            for rv in race_vars:
                intersect_df[f"{rv}_PAR"] = (
                        intersect_df["Population_Par_Prop"] * intersect_df[rv]
                )

    # If what we did worked, all the proportions should sum to 1. This will
    # help us identify if there are any errors
    # v = [f'{var}_Par_Prop' for var in lodes_attrs + ["Population"]]
    # x = intersect_df.groupby([bg_id_field])[v].apply(lambda x: x.sum())
    # x[v].apply(lambda x: [min(x), max(x)])

    # Now we can sum up totals
    print("--- totaling allocated jobs and population")
    intersect_df["Total_Employment"] = (
            intersect_df["CNS01_PAR"]
            + intersect_df["CNS02_PAR"]
            + intersect_df["CNS03_PAR"]
            + intersect_df["CNS04_PAR"]
            + intersect_df["CNS05_PAR"]
            + intersect_df["CNS06_PAR"]
            + intersect_df["CNS07_PAR"]
            + intersect_df["CNS08_PAR"]
            + intersect_df["CNS09_PAR"]
            + intersect_df["CNS10_PAR"]
            + intersect_df["CNS11_PAR"]
            + intersect_df["CNS12_PAR"]
            + intersect_df["CNS13_PAR"]
            + intersect_df["CNS14_PAR"]
            + intersect_df["CNS15_PAR"]
            + intersect_df["CNS16_PAR"]
            + intersect_df["CNS17_PAR"]
            + intersect_df["CNS18_PAR"]
            + intersect_df["CNS19_PAR"]
            + intersect_df["CNS20_PAR"]
    )
    intersect_df["Total_Population"] = (
            intersect_df["Total_Non_Hisp_PAR"] + intersect_df["Total_Hispanic_PAR"]
    )

    # Finally, we'll allocate transportation usage
    print("--- allocating commutes")
    # Commutes will be allocated relative to total population, so total by
    # the block group and calculate the parcel share
    tp_props = intersect_df.groupby(bg_id_field)["Total_Population"].sum().reset_index()
    tp_props.columns = [bg_id_field, "TP_Agg"]
    geoid_edit = tp_props[tp_props.TP_Agg == 0][bg_id_field]
    intersect_df = pd.merge(intersect_df, tp_props, how="left", on=bg_id_field)
    intersect_df["TP_Par_Prop"] = (
            intersect_df["Total_Population"] / intersect_df["TP_Agg"]
    )
    # If there are any 0s (block groups with 0 population) replace with
    # the population area population, in case commutes are predicted where
    # population isn't
    intersect_df.loc[
        intersect_df[bg_id_field].isin(geoid_edit), "TP_Par_Prop"
    ] = intersect_df["Population_Par_Prop"][intersect_df[bg_id_field].isin(geoid_edit)]
    # Now we can allocate commutes
    transit_vars = [
        "Drove",
        "Carpool",
        "Transit",
        "NonMotor",
        "Work_From_Home",
        "AllOther",
    ]
    for var in transit_vars:
        intersect_df[f"{var}_PAR"] = intersect_df["TP_Par_Prop"] * intersect_df[var]

    # And, now we can sum up totals
    print("--- totaling allocated commutes")
    intersect_df["Total_Commutes"] = (
            intersect_df["Drove_PAR"]
            + intersect_df["Carpool_PAR"]
            + intersect_df["Transit_PAR"]
            + intersect_df["NonMotor_PAR"]
            + intersect_df["Work_From_Home_PAR"]
            + intersect_df["AllOther_PAR"]
    )

    # Now we're ready to write

    # We don't need all the columns we have, so first we define the columns
    # we want and select them from our data. Note that we don't need to
    # maintain the parcels_id_field here, because our save file has been
    # initialized with this already!
    print("--- selecting columns of interest")
    to_keep = [
        parcels_id,
        parcel_liv_area,
        parcel_lu,
        bg_id_field,
        "Total_Employment",
        "CNS01_PAR",
        "CNS02_PAR",
        "CNS03_PAR",
        "CNS04_PAR",
        "CNS05_PAR",
        "CNS06_PAR",
        "CNS07_PAR",
        "CNS08_PAR",
        "CNS09_PAR",
        "CNS10_PAR",
        "CNS11_PAR",
        "CNS12_PAR",
        "CNS13_PAR",
        "CNS14_PAR",
        "CNS15_PAR",
        "CNS16_PAR",
        "CNS17_PAR",
        "CNS18_PAR",
        "CNS19_PAR",
        "CNS20_PAR",
        "Total_Population",
        "Total_Hispanic_PAR",
        "White_Hispanic_PAR",
        "Black_Hispanic_PAR",
        "Asian_Hispanic_PAR",
        "Multi_Hispanic_PAR",
        "Other_Hispanic_PAR",
        "Total_Non_Hisp_PAR",
        "White_Non_Hisp_PAR",
        "Black_Non_Hisp_PAR",
        "Asian_Non_Hisp_PAR",
        "Multi_Non_Hisp_PAR",
        "Other_Non_Hisp_PAR",
        "Total_Commutes",
        "Drove_PAR",
        "Carpool_PAR",
        "Transit_PAR",
        "NonMotor_PAR",
        "Work_From_Home_PAR",
        "AllOther_PAR",
    ]
    intersect_df = intersect_df[to_keep]

    return intersect_df


# MAZ/TAZ data prep helpers
def estimate_maz_from_parcels(
        par_fc,
        par_id_field,
        maz_fc,
        maz_id_field,
        taz_id_field,
        se_data,
        se_id_field,
        agg_cols,
        consolidations,
):
    """
    Estimate jobs, housing, etc. at the MAZ level based on underlying parcel data.

    Args:
        par_fc (str): Path to Parcel features
        par_id_field (str): Field identifying each parcel feature
        maz_fc (str): Path to MAZ features
        maz_id_field (str): Field identifying each MAZ feature
        taz_id_field (str): Field in `maz_fc` that defines which TAZ the MAZ feature is in.
        se_data (str): Path to a gdb table containing parcel-level socio-economic/demographic estimates.
        se_id_field (str): Field identifying each parcel in `se_data`
        agg_cols (object): [PMT.AggColumn, ...] Columns to summarize to MAZ level
        consolidations (object): [PMT.Consolidation, ...] Columns to consolidated into a single statistic
            and then summarize to TAZ level.
    
    Returns:
        DataFrame
    
    See Also:
        PMT.AggColumn
        PMT.Consolidation
    """
    # intersect
    int_fc = PMT.intersect_features(summary_fc=maz_fc, disag_fc=par_fc)
    # Join
    b_help.join_attributes(
        to_table=int_fc,
        to_id_field=par_id_field,
        from_table=se_data,
        from_id_field=se_id_field,
        join_fields="*",
        drop_dup_cols=True,
    )
    # PMT.joinAttributes(to_table=int_fc, to_id_field=par_id_field, from_table=se_data, from_id_field=se_id_field,
    #                    join_fields="*")
    # Summarize
    gb_cols = [PMT.Column(maz_id_field), PMT.Column(taz_id_field)]
    df = b_help.summarize_attributes(
        in_fc=int_fc,
        group_fields=gb_cols,
        agg_cols=agg_cols,
        consolidations=consolidations,
    )
    # df = PMT.summarizeAttributes(in_fc=int_fc, group_fields=gb_cols, agg_cols=agg_cols, consolidations=consolidations)
    return df


# Consolidate MAZ data (for use in areas outside the study area)
# TODO: This could become a more generalized method
def consolidate_cols(df, base_fields, consolidations):
    """
    Use the `PMT.Consolidation` class to combine columns and
        return a clean data frame.

    Args:
        df (pd.DataFrame): pandas Dataframe
        base_fields (list): [String, ...]; Field(s) in `df` that are not subject to consolidation but which
            are to be retained in the returned data frame.
        consolidations (iterable): [Consolidation, ...]; Specifications for output columns that consolidate columns
            found in `df`.
    
    Returns:
        clean_df (pd.DataFrame): A new data frame with columns reflecting `base_field` and `consolidations`.
    
    See Also:
        PMT.Consolidation
    """
    if isinstance(base_fields, str):
        base_fields = [base_fields]

    clean_cols = base_fields + [c.name for c in consolidations]
    renames = {}
    for c in consolidations:
        if hasattr(c, "input_cols"):
            df[c.name] = df[c.input_cols].agg(c.cons_method, axis=1)
        else:
            renames[c.name] = c.rename

    clean_df = df[clean_cols].rename(columns=renames)
    return clean_df


def patch_local_regional_maz(maz_par_df, maz_par_key, maz_df, maz_key):
    """
    Create a region wide MAZ socioeconomic/demographic data frame based
        on parcel-level and MAZ-level data. Where MAZ features do not overlap
        with parcels, use MAZ-level data.
    
    Args:
        maz_par_df (pd.DatarFrame): df of parcel data aggregated up to MAZ level
        maz_par_key (str): MAZ common id key
        maz_df (pd.DataFrame): df of MAZ data
        maz_key (str): MAZ common id key
    
    Returns:
        pd.DataFrame: MAZ data patched in with parcel level data rolled up to MAZ
    """
    # Create a filter to isolate MAZ features having parcel overlap
    patch_fltr = np.in1d(maz_df[maz_key], maz_par_df[maz_par_key])
    matching_rows = maz_df[patch_fltr].copy()
    # Join non-parcel-level data (school enrollments, e.g.) to rows with
    #  other columns defined by parcel level data, creating a raft of
    #  MAZ features with parcel overlap that have all desired columns
    all_par = maz_par_df.merge(
        matching_rows, how="inner", on=maz_par_key, suffixes=("", "_M")
    )
    # Drop extraneous columns generated by the join
    drop_cols = [c for c in all_par.columns if c[-2:] == "_M"]
    if drop_cols:
        all_par.drop(columns=drop_cols, inplace=True)
    # MAZ features with no parcel overlap already have all attributes
    #  and can be combined with the `all_par` frame created above
    return pd.concat([all_par, maz_df[~patch_fltr]])


def copy_net_result(source_fds, target_fds, fc_names):
    # TODO: Generalize function name and docstring, as this now just copies one or more fcs across fds's
    """
    Since some PMT years use the same OSM network, a solved network analysis
        can be copied from one year to another to avoid redundant processing.
        This function is a helper function called by PMT wrapper functions. It
        is not intended to be run independently.

    Args:
        source_fds (str): Path to source FeatureDataset
        target_fds (str): Path to destination FeatureDataset
        fc_names (str, list): [String, ...] The feature class(es) to be copied from an already-solved
            analysis. Provide the names only (not paths).
    
    Returns:
        None: Copies network service area feature classes to the target year
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
        src_fc = PMT.make_path(source_fds, fc_name)
        tgt_fc = PMT.make_path(target_fds, fc_name)
        if arcpy.Exists(tgt_fc):
            arcpy.Delete_management(tgt_fc)
        arcpy.FeatureClassToFeatureClass_conversion(src_fc, target_fds, fc_name)

    # TODO: these may not exist when just copying centrality results
    # for mode in ["walk", "bike"]:
    #     for dest_grp in ["stn", "parks"]:
    #         for run in ["MERGE", "NO_MERGE", "OVERLAP", "NON_OVERLAP"]:
    #             fc_name = f"{mode}_to_{dest_grp}_{run}"


def lines_to_centrality(line_features, impedance_attribute):
    """
    Using the "lines" layer output from an OD matrix problem, calculate
        node centrality statistics and store results in a csv table.
    
    Args:
        line_features (str): ODMatrix/Lines feature layer
        impedance_attribute (str): field
    
    Returns:
        sum_df (pd.DataFrame): summarized df with centrality measure appended

    """
    imp_field = f"Total_{impedance_attribute}"
    # Dump to df
    df = pd.DataFrame(arcpy.da.TableToNumPyArray(line_features, ["Name", imp_field]))
    names = ["N", "Node"]
    df[names] = df["Name"].str.split(" - ", n=1, expand=True)
    # Summarize
    sum_df = df.groupby("Node").agg({"N": "size", imp_field: sum}).reset_index()
    # Calculate centrality
    sum_df["centrality"] = (sum_df.N - 1) / sum_df[imp_field]
    # Add average length
    sum_df["AvgLength"] = 1 / sum_df.centrality
    # Add centrality index
    sum_df["CentIdx"] = sum_df.N / sum_df.AvgLength
    return sum_df


def network_centrality(
        in_nd,
        in_features,
        net_loader,
        name_field="OBJECTID",
        impedance_attribute="Length",
        cutoff="1609",
        restrictions="",
        chunk_size=1000,
):
    """
    Uses Network Analyst to create and iteratively solve an OD matrix problem
        to assess connectivity among point features.

    The evaluation analyses how many features can reach a given feature and
        what the total and average travel impedances are. Results are reported
        for traveling TO each feature (i.e. features as destinations), which may
        be significant if oneway or similar restrictions are honored.
    
    Args:
        in_nd (str): Path to  NetworkDataset
        in_features (str): Path to Feature Class or Feature Layer
            A point feature class or feature layer that will serve as origins and
            destinations in the OD matrix
        net_loader (object): NetLoader; Provide network location loading preferences using a NetLoader
            instance.
        name_field (str, default="OBJECTID"): A field in `in_features` that identifies each feature.
            Generally this should be a unique value.
        impedance_attribute (str, default="Length"): The attribute in `in_nd` to use when solving
            shortest paths among features in `in_features`.
        cutoff (str, default="1609"):  A number (as a string) that establishes the search radius for
            evaluating node centrality. Counts and impedances from nodes within this threshold are summarized.
            Units are implied by the `impedance_attribute`.
        restrictions (str, default=""): If `in_nd` includes restriction attributes, provide a
            semi-colon-separated string listing which restrictions to honor in solving the OD matrix.
        chunk_size (int, default=1000): Destination points from `in_features` are loaded iteratively in chunks
            to manage memory. The `chunksize` determines how many features are
            analyzed simultaneously (more is faster but consumes more memory).
    
    Returns:
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
        time_of_day="",
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
        search_query=net_loader.search_query,
    )
    # Step 3 - iterate through destinations
    print("Iterate destinations and solve")
    # Use origin field maps to expedite loading
    fm = "Name Name #;CurbApproach CurbApproach 0;SourceID SourceID #;SourceOID SourceOID #;PosAlong PosAlong #;SideOfEdge SideOfEdge #"
    dest_src = arcpy.MakeFeatureLayer_management(
        "OD Cost Matrix\Origins", "DEST_SOURCE"
    )
    for chunk in PMT.iter_rows_as_chunks(dest_src, chunksize=chunk_size):
        # printed dots track progress over chunks
        print(".", end="")
        arcpy.AddLocations_na(
            in_network_analysis_layer="OD Cost Matrix",
            sub_layer="Destinations",
            in_table=chunk,
            field_mappings=fm,
            append="CLEAR",
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
    
    Args:
        in_table (str): path to walk time table
        bin_field (str): Name of bin field
        time_field (str): Name of time field
        code_block (str): Code block encapsulated as a string
            Defines a python function `assignBin()` with if/else statements
            to group times in `time_field` into bins to be stored as string
            values in `bin_field`.
    """
    arcpy.AddField_management(
        in_table=in_table, field_name=bin_field, field_type="TEXT", field_length=20
    )
    arcpy.CalculateField_management(
        in_table=in_table,
        field=bin_field,
        expression=f"assignBin(!{time_field}!)",
        code_block=code_block,
    )


def parcel_walk_times(
        parcel_fc,
        parcel_id_field,
        ref_fc,
        ref_name_field,
        ref_time_field,
        preselect_fc,
        target_name,
):
    """
    For features in a parcel feature class, summarize walk times reported
        in a reference features class of service area lines. Generates fields
        recording the nearest reference feature, walk time to the nearest
        reference feature, number of reference features within the service
        area walk time cutoff, and a minimum walk time "category" field.
    
    Args:
        parcel_fc (str): Path to the parcel features to which walk time estimates will be appended.
        parcel_id_field (str): The field in `parcel_fc` that uniquely identifies each feature.
        ref_fc (str): Path to a feature class of line features with travel time estimates from/to
            key features (stations, parks, etc.)
        ref_name_field (str): A field in `ref_fc` that identifies key features (which station, e.g.)
        ref_time_field (str): A field in `ref_fc` that reports the time to walk from each line
            feature from/to key features.
        preselect_fc (str): A feature class used to subset parcel_fc prior to spatial join
        target_name (str): A string suffix included in output field names.
    
    Returns:
        walk_time_df: DataFrame
            A data frame with columns storing walk time data:
            `nearest_{target_name}`, `min_time_{target_name}`,
            `n_{target_name}`
    
    See Also:
        parcel_ideal_walk_time
    """
    sr = arcpy.Describe(ref_fc).spatialReference
    # Name time fields
    min_time_field = f"min_time_{target_name}"
    nearest_field = f"nearest_{target_name}"
    number_field = f"n_{target_name}"
    # Intersect layers
    print("--- intersecting parcels and network outputs")
    parcel_lyr = arcpy.MakeFeatureLayer_management(
        in_features=parcel_fc, out_layer="parcel_lyr"
    )
    arcpy.SelectLayerByLocation_management(
        in_layer=parcel_lyr,
        overlap_type="HAVE_THEIR_CENTER_IN",
        select_features=preselect_fc,
    )
    int_fc = PMT.make_inmem_path()
    int_fc = arcpy.SpatialJoin_analysis(
        target_features=parcel_lyr,
        join_features=ref_fc,
        out_feature_class=int_fc,
        join_operation="JOIN_ONE_TO_MANY",
        join_type="KEEP_ALL",
        match_option="WITHIN_A_DISTANCE",
        search_radius="80 Feet",
    )
    # Summarize
    print(f"--- summarizing by {parcel_id_field}, {ref_name_field}")
    sum_tbl = PMT.make_inmem_path()
    statistics_fields = [[ref_time_field, "MIN"], [ref_time_field, "MEAN"]]
    case_fields = [parcel_id_field, ref_name_field]
    sum_tbl = arcpy.Statistics_analysis(
        in_table=int_fc,
        out_table=sum_tbl,
        statistics_fields=statistics_fields,
        case_field=case_fields,
    )
    # Delete intersect features
    arcpy.Delete_management(int_fc)

    # Dump sum table to data frame
    print("--- converting to data frame")
    sum_fields = [f"MEAN_{ref_time_field}"]
    dump_fields = [parcel_id_field, ref_name_field] + sum_fields
    int_df = PMT.table_to_df(in_tbl=sum_tbl, keep_fields=dump_fields)
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
    walk_time_df = pd.concat([which_name, min_time, number], axis=1).reset_index()
    renames = [parcel_id_field, nearest_field, min_time_field, number_field]
    walk_time_df.columns = renames
    return walk_time_df


def parcel_ideal_walk_time(
        parcels_fc,
        parcel_id_field,
        target_fc,
        target_name_field,
        radius,
        target_name,
        overlap_type="HAVE_THEIR_CENTER_IN",
        sr=None,
        assumed_mph=3,
):
    """
    Estimate walk time between parcels and target features (stations, parks,
        e.g.) based on a straight-line distance estimate and an assumed walking
        speed.
    
    Args:
        parcels_fc (str): Path to parcel feature class
        parcel_id_field (str): A field that uniquely identifies features in `parcels_fc`
        target_fc (str): Path to a feature class used to estimate straight line distance from parcels
        target_name_field (str): A field that uniquely identifies features in `target_fc`
        radius (str): A "linear unit" string for spatial selection ('5280 Feet', e.g.)
        target_name (str): A string suffix included in output field names.
        overlap_type (str, default="HAVE_THEIR_CENTER_IN"): A string specifying selection type (see ArcGIS )
        sr (arcpy.SpatialReference): A spatial reference code, string, or object to ensure parcel and
            target features are projected consistently. If `None`, the spatial
            reference from `parcels_fc` is used.
        assumed_mph (int/float): default=3; The assumed average walk speed expressed in miles per hour.
    
    Returns:
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
        with arcpy.da.SearchCursor(tgt_lyr, tgt_fields, spatial_reference=sr) as tgt_c:
            for tgt_r in tgt_c:
                tgt_name, tgt_feature = tgt_r
                tgt_x = tgt_feature.centroid.X
                tgt_y = tgt_feature.centroid.Y
                # select parcels in target buffer
                arcpy.SelectLayerByLocation_management(
                    in_layer=par_lyr,
                    overlap_type=overlap_type,
                    select_features=tgt_feature,
                    search_distance=radius,
                )
                # dump to df
                par_df = pd.DataFrame(
                    arcpy.da.FeatureClassToNumPyArray(
                        par_lyr, par_fields, spatial_reference=sr
                    )
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


def summarize_access(
        skim_table,
        o_field,
        d_field,
        imped_field,
        se_data,
        id_field,
        act_fields,
        imped_breaks,
        units="minutes",
        join_by="D",
        chunk_size=100000,
        **kwargs,
):
    """
    Reads an origin-destination skim table, joins activity data,
        and summarizes activities by impedance bins.

    Args:
        skim_table (str): Path to SKIM table
        o_field (str): Origin field
        d_field (str): Destination field
        imped_field (str): Impedance field
        se_data (str): Path to socioeconomic data table
        id_field (str): `se_data`'s id field
        act_fields (list): activity type fields (job types, e.g.)
        imped_breaks (int/float): list of break points by time
        units (str, default="minutes"): cost units of `imped_field`
        join_by (str, default="D"): join `se_data` based on o_field ("O") or d ("D")
        chunk_size (int, default=100000): number of rows to process simultaneously
            (larger values will finish faster by require more memory)
        kwargs: Keyword arguments for reading the skim table
    
    Returns:
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
        raise ValueError(f"Expected 'D' or 'O' as `join_by` value - got {join_by}")
    bin_field = f"BIN_{units}"
    # Read the activity data
    _a_fields_ = [id_field] + act_fields
    act_df = pd.DataFrame(arcpy.da.TableToNumPyArray(se_data, _a_fields_))

    # Read the skim table
    out_dfs = []
    use_cols = [o_field, d_field, imped_field]
    print("--- --- --- binning skims")
    for chunk in pd.read_csv(
            skim_table, usecols=use_cols, chunksize=chunk_size, **kwargs
    ):
        # Define impedance bins
        low = -np.inf
        criteria = []
        labels = []
        for i_break in imped_breaks:
            crit = np.logical_and(
                chunk[imped_field] >= low, chunk[imped_field] < i_break
            )
            criteria.append(crit)
            labels.append(f"{i_break}{units}")
            low = i_break
        # Apply categories
        chunk[bin_field] = np.select(criteria, labels, f"{i_break}{units}p")
        labels.append(f"{i_break}{units}p")
        # Join the activity data
        join_df = chunk.merge(act_df, how="inner", left_on=left_on, right_on=id_field)
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
        out_df[pivot_fields], index=gb_field, columns=bin_field, aggfunc=np.sum
    )
    pivot.columns = PMT.col_multi_index_to_names(pivot.columns, separator="")
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
        sum_df[avg_fields], how="outer", left_index=True, right_index=True
    )

    return final_df.reset_index()


def generate_od_table(
        origin_pts,
        origin_name_field,
        dest_pts,
        dest_name_field,
        in_nd,
        imped_attr,
        cutoff,
        net_loader,
        out_table,
        restrictions=None,
        use_hierarchy=False,
        uturns="ALLOW_UTURNS",
        o_location_fields=None,
        d_location_fields=None,
        o_chunk_size=None,
):
    """
    Creates and solves an OD Matrix problem for a collection of origin and
        destination points using a specified network dataset. Results are
        exported as a csv file.
    
    Args:
        origin_pts (str): Path to point feature class representing origin locations
        origin_name_field (str): Unique ID field of point data
        dest_pts (str): Path to point feature class representing destination locations
        dest_name_field (str): unique ID of destination points
        in_nd (str): Path to network dataset
        imped_attr (str): String; impedance attribute
        cutoff (int): numeric
        net_loader (class): NetLoader object defininig how our network is loaded/configured
        out_table (str): Path to output table
        restrictions (list): [String, ...], default=None
            List of restriction attributes to apply during the analysis.
        use_hierarchy (bool): Boolean, default=False
        uturns (str): String, default="ALLOW_UTURNS"
        o_location_fields (list, default=None): [String, ...], if `origin_pts` have pre-calculated
            network location fields, list the fields in order ("SourceOID", "SourceID", "PosAlong",
            "SideOfEdge", "SnapX", "SnapY", "Distance",). This speeds up processing times since
            spatial analysis to load locations on the network is not needed.
        d_location_fields (list, default=None): [String, ...], same as `o_location_fields` but for
            destination points.
        o_chunk_size (int, default=None): if given, origin locations will be analyzed in chunks
            of the specified length to avoid memory errors.
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
        time_of_day=None,
    )
    net_layer_ = net_layer.getOutput(0)

    try:
        PMT._loadLocations(
            net_layer=net_layer_,
            sublayer="Destinations",
            points=dest_pts,
            name_field=dest_name_field,
            net_loader=net_loader,
            net_location_fields=d_location_fields,
        )
        # Iterate solves as needed
        if o_chunk_size is None:
            o_chunk_size = int(arcpy.GetCount_management(origin_pts)[0])
        write_mode = "w"
        header = True
        for o_pts in PMT.iter_rows_as_chunks(origin_pts, chunksize=o_chunk_size):
            # TODO: update printing: too many messages when iterating
            PMT._loadLocations(
                net_layer_,
                "Origins",
                o_pts,
                origin_name_field,
                net_loader,
                o_location_fields,
            )
            PMT._solve(net_layer_)
            print("--- --- solved, dumping to data frame")
            # Get output as a data frame
            sublayer_names = arcpy.na.GetNAClassNames(net_layer_)
            extend_lyr_name = sublayer_names["ODLines"]
            try:
                extend_sublayer = net_layer_.listLayers(extend_lyr_name)[0]
            except:
                extend_sublayer = arcpy.mapping.ListLayers(net_layer, extend_lyr_name)[
                    0
                ]
            out_fields = ["Name", f"Total_{imped_attr}"]
            columns = ["Name", imped_attr]
            # out_fields += [f"Total_{attr}" for attr in accum]
            # columns += [c for c in accum]
            df = PMT.table_to_df(in_tbl=extend_sublayer, keep_fields=out_fields)
            df.columns = columns
            # Split outputs
            if len(df) > 0:
                names = ["OName", "DName"]
                df[names] = df["Name"].str.split(" - ", n=1, expand=True)

                # Save
                df.to_csv(out_table, index=False, mode=write_mode, header=header)

                # Update writing params
                write_mode = "a"
                header = False
    except:
        raise
    finally:
        print("--- ---deleting network problem")
        arcpy.Delete_management(net_layer)


def taz_travel_stats(
        od_table,
        o_field,
        d_field,
        veh_trips_field,
        auto_time_field,
        dist_field,
        taz_df,
        taz_id_field,
        hh_field,
        jobs_field,
        chunksize=100000,
        **kwargs,
):
    """
    Calculate rates of vehicle trip generation, vehicle miles of travel, and
        average trip length.

    Args:
        od_table (str): Path to a csv table containing origin-destination information, including
            number of vehicle trips, travel time by car, travel distance by car,
            and travel time by transit
        o_field (str): Field identifying origins in `od_tables`
        d_field (str): Field identifying destinations in `od_tables`
        veh_trips_field (str): Field recording number of vehicle trips in `od_tables`
        auto_time_field (str): Field recording highway travel time in `od_tables`
        dist_field (str): Field recording highway travel distance (miles) in `od_tables`
        taz_df (pandas.DataFrame): DataFrame of TAZ economic and demographic data
        taz_id_field (str): Field identifying TAZs in `taz_df` (corresponds to `o_field` and `d_field`)
        hh_field (str): Field recording number of households in `taz_df`
        jobs_field (str): Field recording number of jobs in `taz_df`
        chunksize (int, default=100000): Number of OD rows to process at one time. More is faster
            but users more memory.
        kwargs: Keyword arguments to pass to pandas.read_csv method when loading od data

    Returns:
        taz_stats_df (pd.DataFrame): Table of vehicle trip generation rates, trip legnths, and VMT
            estimates by TAZ.
    """
    # Read skims, trip tables
    o_sums = []
    d_sums = []
    key_fields = [o_field, d_field]
    sum_fields = [veh_trips_field, auto_time_field, dist_field, "VMT", "VHT"]
    renames = {hh_field: "HH", jobs_field: "JOB"}
    weight_fields = ["HH", "JOB"]
    suffixes = ("_FROM", "_TO")
    for chunk in pd.read_csv(od_table, chunksize=chunksize, **kwargs):
        chunk.replace(np.inf, 0, inplace=True)
        chunk["VMT"] = chunk[veh_trips_field] * chunk[dist_field]
        chunk["VHT"] = chunk[veh_trips_field] * chunk[auto_time_field]
        for df_list, key_field in zip([o_sums, d_sums], key_fields):
            df_list.append(chunk.groupby(key_field).sum()[sum_fields].reset_index())
    # Assemble summaries and re-summarize
    o_df = pd.concat(o_sums)
    d_df = pd.concat(d_sums)
    o_df = o_df.groupby(o_field).sum()
    d_df = d_df.groupby(d_field).sum()

    # Calculate rates
    dfs = [o_df, d_df]
    weight_field = "__activity__"
    taz_df[weight_field] = taz_df[hh_field] + taz_df[jobs_field]
    # for df, key_field, weight_field in zip(dfs, key_fields, weight_fields):
    for df, key_field in zip(dfs, key_fields):
        # Basic
        df["AVG_DIST"] = df.VMT / df[veh_trips_field]
        df["AVG_TIME"] = df.VHT / df[veh_trips_field]
        # Join taz_data
        ref_index = pd.Index(df.index)
        taz_indexed = taz_df.rename(columns=renames).set_index(taz_id_field)
        taz_indexed = taz_indexed.reindex(ref_index, fill_value=0)
        df[weight_field] = taz_indexed[weight_field]
        fltr = df[weight_field] == 0
        df[f"VMT_PER_ACT"] = np.select([fltr], [0], df.VMT / df[weight_field])
        df[f"TRIPS_PER_ACT"] = np.select(
            [fltr], [0], df[veh_trips_field] / df[weight_field]
        )

    # Combine O and D frames
    taz_stats_df = dfs[0].merge(
        dfs[1], how="outer", left_index=True, right_index=True, suffixes=suffixes
    )
    keep_fields = [
        "AVG_DIST_FROM",
        "AVG_TIME_FROM",
        "VMT_PER_ACT_FROM",
        "TRIPS_PER_ACT_FROM",
        "AVG_DIST_TO",
        "AVG_TIME_TO",
        "VMT_PER_ACT_TO",
        "TRIPS_PER_ACT_TO",
    ]
    taz_stats_df.index.name = taz_id_field
    return taz_stats_df[keep_fields].reset_index()


def generate_chunking_fishnet(template_fc, out_fishnet_name, chunks=20):
    """
    Generates a fishnet feature class that minimizes the rows and columns based on
        number of chunks and template_fc proportions.

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
    candidate_orientations = [
        [i, chunks // i] for i in range(1, chunks + 1) if chunks % i == 0
    ]

    orientation_matching = np.argmin(
        [
            abs(orientation[0] / orientation[1] - hw_ratio)
            for orientation in candidate_orientations
        ]
    )
    orientation = candidate_orientations[orientation_matching]
    quadrat_nrows = orientation[0]
    quadrat_ncols = orientation[1]

    # With the extent information and rows/columns, we can create our quadrats
    # by creating a fishnet over the parcels
    quadrat_origin = " ".join([str(xmin), str(ymin)])
    quadrat_ycoord = " ".join([str(xmin), str(ymin + 10)])
    quadrat_corner = " ".join([str(xmax), str(ymax)])
    quadrats_fc = PMT.make_inmem_path(file_name=out_fishnet_name)
    arcpy.CreateFishnet_management(
        out_feature_class=quadrats_fc,
        origin_coord=quadrat_origin,
        y_axis_coord=quadrat_ycoord,
        number_rows=quadrat_nrows,
        number_columns=quadrat_ncols,
        corner_coord=quadrat_corner,
        template=ext,
        geometry_type="POLYGON",
    )
    return quadrats_fc


def symmetric_difference(target_fc, update_fc, out_fc_name):
    # TODO: add capability to define output location instead of in_memory space
    """
    If Advanced arcpy license not available this will calculate the
        symmetrical difference of two sets of polygons.

    Args:
        target_fc (str): path to input feature class
        update_fc (str): path to update feature class (same geometry type required)
        out_fc_name (str): name of output feature class
    
    Returns:
        out_fc (str): path to output file in in_memory space
    """
    # check geometry of target and update match
    target_shape = arcpy.Describe(target_fc).shapeType
    update_shape = arcpy.Describe(target_shape).shapeType
    if target_shape != update_shape:
        raise TypeError("The target and update features are not the same geometry type")

    _, diff_name = os.path.split(os.path.splitext(update_fc)[0])
    # union datasets
    out_fc = PMT.make_inmem_path(file_name=out_fc_name)
    arcpy.Union_analysis(in_features=[target_fc, update_fc], out_feature_class=out_fc)
    # select features from the diff fc uisng the -1 FID code to ID
    where = arcpy.AddFieldDelimiters(update_fc, f"FID_{diff_name}") + " <> -1"
    temp = arcpy.MakeFeatureLayer_management(
        in_features=out_fc, out_layer="_temp_", where_clause=where
    )
    if int(arcpy.GetCount_management(temp)[0]) > 0:
        arcpy.DeleteRows_management(temp)
    return out_fc


def merge_and_subset(feature_classes, subset_fc):
    """
    Helper function to merge a list of feature classes and subset them based on a
    provided area of interest subset feature class

    Args:
        feature_classes (list): list of (paths to) feature classes
        subset_fc (str): Path to subset feature class polygon

    Returns:
        merge_fc (str): path to an in-memory feature class of all combined fcs
    """
    # validate inputs
    if isinstance(feature_classes, str):
        feature_classes = [feature_classes]
    if arcpy.Describe(subset_fc).shapeType != "Polygon":
        raise ValueError("subset fc must be a polygon feature class")
    if not all(
            [
                arcpy.Describe(fc).shapeType
                for fc in feature_classes
                if arcpy.Descibe(fc).shapeType == "Polygon"
            ]
    ):
        raise ValueError("all feature classes should be 'Polygon' shapeType")

    project_crs = arcpy.Describe(subset_fc).spatialReference
    temp_dir = tempfile.TemporaryDirectory()
    prj_fcs = []
    for fc in feature_classes:
        print(f"--- projecting and clipping {fc}")
        arcpy.env.outputCoordinateSystem = project_crs
        basename = get_filename(fc)
        prj_fc = PMT.make_path(temp_dir.name, f"{basename}.shp")
        arcpy.Clip_analysis(
            in_features=fc, clip_features=subset_fc, out_feature_class=prj_fc
        )
        prj_fcs.append(prj_fc)
    merge_fc = PMT.make_inmem_path()
    arcpy.Merge_management(inputs=prj_fcs, output=merge_fc)
    return merge_fc


def get_filename(file_path):
    """Helper function to extract a file name from a file path"""
    basename, ext = os.path.splitext(os.path.split(file_path)[1])
    return basename


def validate_weights(weights):
    """Helper function to validate weights provided to contiguity index calcs are properly formatted"""
    if type(weights) == str:
        weights = weights.lower()
        if weights == "rook":
            return dict(
                {
                    "top_left": 0,
                    "top_center": 1,
                    "top_right": 0,
                    "middle_left": 1,
                    "self": 1,
                    "middle_right": 1,
                    "bottom_left": 0,
                    "bottom_center": 1,
                    "bottom_right": 0,
                }
            )
        elif weights == "queen":
            return dict(
                {
                    "top_left": 1,
                    "top_center": 2,
                    "top_right": 1,
                    "middle_left": 2,
                    "self": 1,
                    "middle_right": 2,
                    "bottom_left": 1,
                    "bottom_center": 2,
                    "bottom_right": 1,
                }
            )
        elif weights == "nn":
            return dict(
                {
                    "top_left": 1,
                    "top_center": 1,
                    "top_right": 1,
                    "middle_left": 1,
                    "self": 1,
                    "middle_right": 1,
                    "bottom_left": 1,
                    "bottom_center": 1,
                    "bottom_right": 1,
                }
            )
        else:
            raise ValueError(
                "Invalid string specification for 'weights'; "
                "'weights' can only take 'rook', 'queen', or 'nn' as a string\n"
            )
    elif type(weights) == dict:
        k = weights.keys()
        missing = list(
            {
                "top_left",
                "top_center",
                "top_right",
                "middle_left",
                "self",
                "middle_right",
                "bottom_left",
                "bottom_center",
                "bottom_right",
            }
            - set(k)
        )
        if len(missing) != 0:
            raise ValueError(
                f'Necessary keys missing from "weights"; '
                f"missing keys include: "
                f'{", ".join([str(m) for m in missing])}'
            )
    else:
        raise ValueError(
            "".join(
                [
                    "'weights' must be a string or dictionary; ",
                    "if string, it must be 'rook', 'queen', or 'nn', and "
                    "if dictionary, it must have keys 'top_left','top_center','top_right',"
                    "'middle_left','self','middle_right','bottom_left','bottom_center',"
                    "'bottom_right'\n",
                ]
            )
        )


def calculate_contiguity_index(
        quadrats_fc, parcels_fc, mask_fc, parcels_id_field, cell_size=40, weights="nn"
):
    """Calculate contiguity of developable area

    Args:
        quadrats_fc (str): path to fishnet of chunks for processing
        parcels_fc (str): path to parcel polygons; contiguity will be summarized and reported
            at this scale
        mask_fc (str): path to mask polygons used to eliminate areas that are not developable
        parcels_id_field (str): name of parcel primary key field
        cell_size (int, default=40): cell size for raster over which contiguity will be calculated.
            (in the units of the input data crs)
        weights (str or dict, default="nn"): weights for neighbors in contiguity calculation
            (see notes for how to specify weights)
   
    Returns:
        pd.DataFrame: table of polygon-level (sub-parcel) contiguity indices

    Notes:
        Weights can be provided in one of two ways:
            1. one of three defaults: "rook", "queen", or "nn".
                - "nn" (nearest neighbor) weights give all neighbors a weight of 1,
                    regardless of orientation.
                - "rook" weights give all horizontal/vertical neighbors a weight of 1,
                    and all diagonal neighbors a weight of 0
                - "queen" weights give all horizontal/vertical neighbors a weight of 2,
                    and all diagonal neighbors a weight of 1

            2. a dictionary of weights for each of 9 possible neighbors. This dictionary must have the keys
                ["top_left", "top_center", "top_right", "middle_left", "self", "middle_right",
                "bottom_left", "bottom_center", "bottom_right"].  If providing weights as a dictionary, a 
                good strategy is to set "self"=1, and then set other weights according to a perceived relative
                importance to the cell itself. Cell-specific rates rarely need to be specified.
    
    Raises:
        ValueError:
            If weights are an invalid string or a dictionary with invalid keys (see Notes)
    """

    # Weights setup
    print("--- checking weights")
    weights = validate_weights(weights)

    print("--- --- Creating temporary processing workspace")
    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(
        out_folder_path=temp_dir, out_name="Intermediates.gdb"
    )
    intmd_gdb = PMT.make_path(temp_dir, "Intermediates.gdb")

    print("--- --- copying the parcels feature class to avoid overwriting")
    fmap = arcpy.FieldMappings()
    fmap.addTable(parcels_fc)
    fields = {f.name: f for f in arcpy.ListFields(parcels_fc)}
    for fname, fld in fields.items():
        if fld.type not in ("OID", "Geometry") and "shape" not in fname.lower():
            if fname != parcels_id_field:
                fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
    parcels_copy = PMT.make_path(intmd_gdb, "parcels")
    p_path, p_name = os.path.split(parcels_copy)
    arcpy.FeatureClassToFeatureClass_conversion(
        in_features=parcels_fc, out_path=p_path, out_name=p_name, field_mapping=fmap
    )

    print("--- --- tagging parcels with a chunk ID")
    arcpy.AddField_management(
        in_table=parcels_copy, field_name="ChunkID", field_type="LONG"
    )
    p_layer = arcpy.MakeFeatureLayer_management(
        in_features=parcels_copy, out_layer="_p_layer"
    )
    chunks = []
    with arcpy.da.SearchCursor(quadrats_fc, ["OID@", "SHAPE@"]) as search:
        for srow in search:
            chunk_id, geom = srow
            arcpy.SelectLayerByLocation_management(
                in_layer=p_layer,
                overlap_type="HAVE_THEIR_CENTER_IN",
                select_features=geom,
            )
            arcpy.CalculateField_management(
                in_table=p_layer, field="ChunkID", expression=chunk_id
            )
            chunks.append(chunk_id)
    # difference parcels and buildings/water bodies/protected areas

    print("--- --- differencing parcels and buildings")
    difference_fc = symmetric_difference(
        target_fc=parcels_copy, update_fc=mask_fc, out_fc_name="difference"
    )

    print("--- --- converting difference to singlepart polygons")
    diff_fc = PMT.make_inmem_path(file_name="diff")
    arcpy.MultipartToSinglepart_management(
        in_features=difference_fc, out_feature_class=diff_fc
    )

    print("--- --- adding a unique ID field for individual polygons")
    PMT.add_unique_id(feature_class=diff_fc, new_id_field="PolyID")

    print("--- --- extracting a polygon-parcel ID reference table")
    ref_df = PMT.featureclass_to_df(
        in_fc=diff_fc, keep_fields=[parcels_id_field, "PolyID"], null_val=-1.0
    )
    arcpy.Delete_management(difference_fc)

    # loop through the chunks to calculate contiguity:
    print("--- chunk processing contiguity and developable area")
    contiguity_stack = []
    diff_lyr = arcpy.MakeFeatureLayer_management(
        in_features=diff_fc, out_layer="_diff_lyr_"
    )
    for i in chunks:
        print(f"--- --- chunk {str(i)} of {str(len(chunks))}")

        print("--- --- --- selecting chunk")
        selection = f'"ChunkID" = {str(i)}'
        parcel_chunk = arcpy.SelectLayerByAttribute_management(
            in_layer_or_view=diff_lyr,
            selection_type="NEW_SELECTION",
            where_clause=selection,
        )

        print("--- --- --- rasterizing chunk")
        rp = PMT.make_inmem_path()
        arcpy.FeatureToRaster_conversion(
            in_features=parcel_chunk, field="PolyID", out_raster=rp, cell_size=cell_size
        )

        print("--- --- --- loading chunk raster")
        ras_array = arcpy.RasterToNumPyArray(in_raster=rp, nodata_to_value=-1)
        arcpy.Delete_management(rp)

        # calculate total developable area
        print("--- --- --- calculating developable area by polygon")
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
            print("*** no polygons in this quadrat, proceeding to next chunk ***")
        else:
            print("--- --- --- initializing cell neighbor identification")
            ras_dim = ras_array.shape
            nrow = ras_dim[0]
            ncol = ras_dim[1]

            id_tab_self = pd.DataFrame(
                {
                    "Row": np.repeat(np.arange(nrow), ncol),
                    "Col": np.tile(np.arange(ncol), nrow),
                    "ID": ras_array.flatten(),
                }
            )
            id_tab_neighbor = pd.DataFrame(
                {
                    "NRow": np.repeat(np.arange(nrow), ncol),
                    "NCol": np.tile(np.arange(ncol), nrow),
                    "NID": ras_array.flatten(),
                }
            )

            print("--- --- --- identifying non-empty cells")
            row_oi = id_tab_self[id_tab_self.ID != -1].Row.to_list()
            col_oi = id_tab_self[id_tab_self.ID != -1].Col.to_list()

            print("--- --- --- identifying neighbors of non-empty cells")
            row_basic = [np.arange(x - 1, x + 2) for x in row_oi]
            col_basic = [np.arange(x - 1, x + 2) for x in col_oi]

            meshed = [
                np.array(np.meshgrid(x, y)).reshape(2, 9).T
                for x, y in zip(row_basic, col_basic)
            ]
            meshed = np.concatenate(meshed, axis=0)
            meshed = pd.DataFrame(meshed, columns=["NRow", "NCol"])

            meshed.insert(1, "Col", np.repeat(col_oi, 9))
            meshed.insert(0, "Row", np.repeat(row_oi, 9))

            print("--- --- --- filtering to valid neighbors by index")
            meshed = meshed[
                (meshed.NRow >= 0)
                & (meshed.NRow < nrow)
                & (meshed.NCol >= 0)
                & (meshed.NCol < ncol)
                ]

            print("--- --- --- tagging cells and their neighbors with polygon IDs")
            meshed = pd.merge(
                meshed,
                id_tab_self,
                left_on=["Row", "Col"],
                right_on=["Row", "Col"],
                how="left",
            )
            meshed = pd.merge(
                meshed,
                id_tab_neighbor,
                left_on=["NRow", "NCol"],
                right_on=["NRow", "NCol"],
                how="left",
            )

            print("--- --- --- fitering to valid neighbors by ID")
            meshed = meshed[meshed.ID == meshed.NID]
            meshed = meshed.drop(columns="NID")

            # With neighbors identified, we now need to define cell weights for contiguity calculations.
            # These are based off the specifications in the 'weights' inputs to the function. So, we tag each
            # cell-neighbor pair in 'valid_neighbors' with a weight.
            print("--- --- --- tagging cells and neighbors with weights")
            conditions = [
                (
                    np.logical_and(
                        meshed["NRow"] == meshed["Row"] - 1,
                        meshed["NCol"] == meshed["Col"] - 1,
                    )
                ),
                (
                    np.logical_and(
                        meshed["NRow"] == meshed["Row"] - 1,
                        meshed["NCol"] == meshed["Col"],
                    )
                ),
                (
                    np.logical_and(
                        meshed["NRow"] == meshed["Row"] - 1,
                        meshed["NCol"] == meshed["Col"] + 1,
                    )
                ),
                (
                    np.logical_and(
                        meshed["NRow"] == meshed["Row"],
                        meshed["NCol"] == meshed["Col"] - 1,
                    )
                ),
                (
                    np.logical_and(
                        meshed["NRow"] == meshed["Row"], meshed["NCol"] == meshed["Col"]
                    )
                ),
                (
                    np.logical_and(
                        meshed["NRow"] == meshed["Row"],
                        meshed["NCol"] == meshed["Col"] + 1,
                    )
                ),
                (
                    np.logical_and(
                        meshed["NRow"] == meshed["Row"] + 1,
                        meshed["NCol"] == meshed["Col"] - 1,
                    )
                ),
                (
                    np.logical_and(
                        meshed["NRow"] == meshed["Row"] + 1,
                        meshed["NCol"] == meshed["Col"],
                    )
                ),
                (
                    np.logical_and(
                        meshed["NRow"] == meshed["Row"] + 1,
                        meshed["NCol"] == meshed["Col"] + 1,
                    )
                ),
            ]
            choices = [
                "top_left",
                "top_center",
                "top_right",
                "middle_left",
                "self",
                "middle_right",
                "bottom_left",
                "bottom_center",
                "bottom_right",
            ]
            meshed["Type"] = np.select(conditions, choices)
            meshed["Weight"] = [weights[key] for key in meshed["Type"]]

            # To initialize the contiguity calculation, we sum weights by cell.
            # We lose the ID in the groupby though, which we need to get to contiguity,
            # so we need to merge back to our cell-ID table
            print("--- --- --- summing weight by cell")
            weight_tbl = (
                meshed.groupby(["Row", "Col"])[["Weight"]].agg("sum").reset_index()
            )
            weight_tbl = pd.merge(
                weight_tbl,
                id_tab_self,
                left_on=["Row", "Col"],
                right_on=["Row", "Col"],
                how="left",
            )

            # We are now finally at the point of calculating contiguity! It's a pretty simple function,
            # which we apply over our IDs. This is the final result of our chunk process, so we'll also rename our "ID"
            # field to "PolyID", because this is the proper name for the ID over which we've calculated contiguity.
            # This will make our life easier when chunk processing is complete, and we move into data formatting
            # and writing
            print("--- --- --- calculating contiguity by polygon")
            weight_max = sum(weights.values())
            contiguity = (
                weight_tbl.groupby("ID")
                    .apply(lambda x: (sum(x.Weight) / len(x.Weight) - 1) / (weight_max - 1))
                    .reset_index(name="Contiguity")
            )
            contiguity.columns = ["PolyID", "Contiguity"]

            # For reporting results, we'll merge the contiguity and developable
            # area tables
            print("--- --- --- merging contiguity and developable area information")
            contiguity = pd.merge(
                contiguity, area, left_on="PolyID", right_on="PolyID", how="left"
            )

            # We're done chunk processing -- we'll put the resulting data frame
            # in our chunk results list as a final step
            print("--- --- --- appending chunk results to master list")
            contiguity_stack.append(contiguity)

    # Contiguity results formatting
    print("--- formatting polygon-level results")
    print("--- --- combining chunked results into table format")
    contiguity_df = pd.concat(contiguity_stack, axis=0)

    print("--- --- filling table with missing polygons")
    contiguity_df = pd.merge(
        ref_df, contiguity_df, left_on="PolyID", right_on="PolyID", how="left"
    )

    print("--- overwriting missing values with 0")
    contiguity_df = contiguity_df.fillna(value={"Contiguity": 0, "Developable_Area": 0})

    # clean up in_memory space and temporary data
    arcpy.Delete_management(parcels_copy)
    arcpy.Delete_management(intmd_gdb)
    arcpy.Delete_management(diff_fc)
    return contiguity_df


def calculate_contiguity_summary(
        full_results_df,
        parcels_id_field,
        summary_funcs=["min", "max", "median", "mean"],
        area_scaling=True,
):
    """Summarize contiguity/developable area results from
        `analyze_contiguity_index` from sub-parcel to parcel

    Args:
        full_results_df (pandas.DataFrame): dataframe output of `analyze_contiguity_index`
        parcels_id_field (str): name of a field used to identify the parcels in the summarized
            parcel results
        summary_funcs (list): [str, ...]
            functions to be used to summarize contiguity to the parcel; available
            options include min, max, mean, and median
            Default is all options
        area_scaling (bool): should a scaled version of developable area be calculated? If `True`,
            a "scaled_area" statistic will be calculated as developable area times
            contiguity index (at the parcel level). The default is True
    
    Returns:
        pandas dataframe: a table of summarized results attributed with:
            1. A parcel identifier (as specified in `analyze_contiguity_index`
                when the featur class was initialized)
            2. Parcel developable area (summed to the parcel)
            3. {fun}-summarized contiguity, for each function in `summary_funs`
            4. {fun}-scaled area, for each of {fun}-summarized contiguity, if
                `area_scaling = True`
    """

    # Summarizing up to the parcel
    # ----------------------------
    print("--- summarizing contiguity and developable area to the parcels")

    # We want to summarize contiguity to the parcel. We'll do that using
    # every function in 'summary_funs'.
    print("--- --- summarizing contiguity to the parcels")
    ctgy_summary = []
    ctgy_variables = []
    for func in summary_funcs:
        print(f"--- --- --- {func}")
        var_name = f"{func.title()}_Contiguity"
        ci = (
            full_results_df.groupby(parcels_id_field)
                .agg({"Contiguity": getattr(np, func)})
                .reset_index()
        )
        ci.columns = [parcels_id_field, var_name]
        ctgy_summary.append(ci)
        ctgy_variables.append(var_name)

    # The results for each function are stored in a separate table, so we now
    # merge them into a single table
    print("--- ---formatting contiguity summary results")
    ctgy_summary = [df.set_index(parcels_id_field) for df in ctgy_summary]
    ctgy_summary = pd.concat(ctgy_summary, axis=1)
    ctgy_summary = ctgy_summary.reset_index()

    # The only way to summarize developable area is by sum, so we'll take
    # care of that now.
    print("--- ---summarizing developable area to the parcels")
    area_summary = (
        full_results_df.groupby(parcels_id_field)[["Developable_Area"]]
            .agg("sum")
            .reset_index()
    )

    # The final summary step is then merging the contiguity and developable
    # area summary results
    print("--- ---merging contiguity and developable area summaries")
    df = pd.merge(
        area_summary,
        ctgy_summary,
        left_on=parcels_id_field,
        right_on=parcels_id_field,
        how="left",
    )

    # If an area scaling is requested (area_scaling = True), that means
    # we need to create a combined statistic for contiguity and area. To do
    # this, we simply multiply contiguity by developable area (essentially,
    # we're weighting developable area by how contiguous it is). We do this
    # for all contiguity summaries we calculated
    if area_scaling:
        print("--- --- calculating combined contiguity-developable area statistics")
        for i in ctgy_variables:
            var_name = i.replace("Contiguity", "Scaled_Area")
            df[var_name] = df["Developable_Area"] * df[i]

    return df


def simpson_diversity(
        in_df,
        group_col,
        weight_col=None,
        total_col=None,
        pct_col=None,
        count_lu=None,
        **kwargs,
):
    """
    Simpson index: mathematically, the probability that a random draw of
        one unit of land use A would be followed by a random draw of one unit
        of land use B.
        
        Ranges from 0 (only one land use present)
        to 1 (all land uses present in equal abundance)

        This function is not intended to be run directly. Use `lu_diversity`.
    """

    temp_df = in_df.assign(SIN=in_df[weight_col] * (in_df[weight_col] - 1))
    temp_df["SID"] = temp_df[total_col] * (temp_df[total_col] - 1)
    diversity_col = temp_df.groupby(group_col).apply(
        lambda x: sum(x.SIN) / np.unique(x.SID)[0]
    )
    diversity_col.name = "Simpson"
    # Adjust to 0-1 scale
    diversity_col = 1 - diversity_col
    return diversity_col


def shannon_diversity(
        in_df,
        group_col,
        weight_col=None,
        total_col=None,
        pct_col=None,
        count_lu=None,
        **kwargs,
):
    """
    Shannon index: borrowing from information theory, Shannon quantifies
       the uncertainty in predicting the land use of a random one unit draw.
       
       The higher the uncertainty, the higher the diversity. Ranges from 0
       (only one land use present) to -log(1/|land uses|) (all land uses
       present in equal abundance).

       This function is not intended to be run directly. Use `lu_diversity`.
    """
    temp_df = in_df.assign(PLP=in_df[pct_col] * np.log(in_df[pct_col]))
    diversity_col = temp_df.groupby(group_col).apply(lambda x: sum(x.PLP) * -1)
    diversity_col.name = "Shannon"
    # Adjust to 0-1 scale
    diversity_col /= -np.log(1 / count_lu)
    return diversity_col


def berger_parker_diversity(
        in_df,
        group_col,
        weight_col=None,
        total_col=None,
        pct_col=None,
        count_lu=None,
        **kwargs,
):
    """
    Berger-Parker index: the maximum proportional abundance, giving a
       measure of dominance.
       
       Ranges from 1 (only one land use present) to
       1/|land uses| (all land uses present in equal abundance). Lower values
       indicate a more even spread, while high values indicate the dominance
       of one land use.

       This function is not intended to be run directly. Use `lu_diversity`.
    """
    diversity_col = in_df.groupby(group_col)[pct_col].max()
    diversity_col.name = "BergerParker"
    # Adjust to 0-1 scale
    best_possible = 1 / count_lu
    worst_possible = 1
    diversity_col = 1 - (
            (diversity_col - best_possible) / (worst_possible - best_possible)
    )
    return diversity_col


def enp_diversity(
        in_df,
        group_col,
        weight_col=None,
        total_col=None,
        pct_col=None,
        count_lu=None,
        **kwargs,
):
    """
    Effective number of parties (ENP): a count of land uses, as weighted
       by their proportional abundance. 
       
       A land use contributes less to ENP if it is relatively rare, and 
       more if it is relatively common. Ranges from
       1 (only one land use present) to |land uses| (all land uses present in
       equal abunance).

       This function is not intended to be run directly. Use `lu_diversity`.
    """
    temp_df = in_df.assign(P2=in_df[pct_col] ** 2)
    diversity_col = temp_df.groupby(group_col).apply(lambda x: 1 / sum(x.P2))
    diversity_col.name = "ENP"
    # Adjust to 0-1 scale
    best_possible = count_lu
    worst_possible = 1
    diversity_col = (diversity_col - worst_possible) / (best_possible - worst_possible)
    return diversity_col


def assign_features_to_agg_area(
        in_features, agg_features=None, buffer=None, in_fields="*", as_df=False
):
    """
    Generates a feature class or table that relates disaggregate features to
    those in an aggregate area. Optionally, the aggregate areas can be generated
    from the disaggregate features using a buffer, creating a "floating zone"
    around each.

    Disaggregate features are always assigned to aggregate areas based on the
    intersection of their centroids with the aggreagte area features.

    Args:
        in_features (str): path to a feature class of disaggregate features
            to be related to features in `agg_features`
        agg_features (str): path to a feature class of aggreagate areas. If none,
            `buffer` must be provided.
        buffer (str): If `agg_features` is not provided, a buffer may be provided to create
            floating zones around each feature in `in_features`. May be a linear
            distance that includes distance and units ("2640 Feet", e.g.) or a
            field in `in_features` that specifies a linear distance for each feature.
        in_fields (list, default="*"): Field(s) in `in_features` to retain in the result. By default, all
            fields are retained, but a list can be specified to return only named
            fields to make the output more concise.
        as_df (bool): If True, returns a data frame (table) of the intersect between `in_features`
            centroids and `agg_features` or floating zones. Otherwise returns the path
            to a temporary feature class with intersection geometries and attributes.

    Returns:
        fc path or DataFrame (see `as_df` arg)

    See Also:
        PMT.intersectFeatures
    """
    if agg_features is None:
        if buffer is None:
            raise RuntimeError("one of `agg_features` or `buffer` must be provided")
        # buffer technique
        # TODO: add flag to dump to centroids and buffer just the centroid features
        agg_path = PMT.make_inmem_path()
        agg_features = arcpy.Buffer_analysis(
            in_features=in_features,
            out_features=agg_path,
            buffer_distance_or_field=buffer,
        )
    return PMT.intersect_features(agg_features, in_features, in_fields, as_df=as_df)


def lu_diversity(
        in_df,
        groupby_field,
        lu_field,
        div_funcs,
        weight_field=None,
        count_lu=None,
        regional_comp=False,
):
    """
    Wrapper function to run the four land use diversity methods (Simpson, Shannon, Berger-Parker, or ENP)

    Args:
        in_df (pandas.DataFrame): Dataframe containing the data to be analyzed
        groupby_field (str): The field in in_df used to group the data
        lu_field (str): The field in in_df containing the land use categorical values
        div_funcs (list): List of the functions to generate diversity indices
        weight_field (str): The field used to generate weights
        count_lu (int): Count of relevant land use classes
        regional_comp (bool, default=False): Boolean indicating whether to perform a region-wide comparison

    Returns:
        panadas.DataFrame

    Notes:
        If `weight_field` is none, a field called "COUNT" containing row counts by
            `groupby_field` values is created and used for weighting.
    """
    # Prep
    if isinstance(groupby_field, string_types):
        gblu_fields = [groupby_field, lu_field]
    else:
        gblu_fields = groupby_field + [lu_field]
    if weight_field is None:
        weight_field = "COUNT"
        PMT.count_rows(
            in_df, groupby_field, out_field=weight_field, skip_nulls=True, inplace=True
        )
    tot_weight_field = f"Tot_{weight_field}"
    pct_weight_field = f"Pct_{weight_field}"

    # Drop rows with 0 weight
    positive = in_df[weight_field] > 0
    _in_df_ = in_df[positive].copy()

    # Get summary stats in ref df's
    agg_dict = {weight_field: sum}
    wt_by_lu_grp = _in_df_.groupby(gblu_fields).agg(agg_dict).reset_index()
    wt_by_grp = _in_df_.groupby(groupby_field).agg(agg_dict).reset_index()
    wt_by_grp.rename(columns={weight_field: tot_weight_field}, inplace=True)
    wt_by_lu_grp = wt_by_lu_grp.merge(wt_by_grp, on=groupby_field)
    wt_by_lu_grp[pct_weight_field] = (
            wt_by_lu_grp[weight_field] / wt_by_lu_grp[tot_weight_field]
    )
    if count_lu is None:
        count_lu = len(np.unique(_in_df_[lu_field]))

    # Set diversity function args
    div_kwargs = {
        "in_df": wt_by_lu_grp,
        "group_col": groupby_field,
        "weight_col": weight_field,
        "total_col": tot_weight_field,
        "pct_col": pct_weight_field,
        "count_lu": count_lu,
    }
    # Calc simpson, shannon, etc.
    div_df = pd.concat([div_func(**div_kwargs) for div_func in div_funcs], axis=1)

    # Regional comp
    if regional_comp:
        reg_by_lu = _in_df_.groupby(lu_field).agg(agg_dict).reset_index()
        reg_by_lu[tot_weight_field] = _in_df_[weight_field].sum()
        reg_by_lu[pct_weight_field] = (
                reg_by_lu[weight_field] / reg_by_lu[tot_weight_field]
        )
        reg_by_lu["RegID"] = "Region"
        div_kwargs["in_df"] = reg_by_lu
        div_kwargs["group_col"] = "RegID"
        reg_df = pd.concat([div_func(**div_kwargs) for div_func in div_funcs], axis=1)

        # Comp to region results
        for col in div_df.columns:
            comp_col = f"{col}_Adj"
            div_df[comp_col] = div_df[col] / reg_df[col][0]

    return div_df.reset_index()


def match_units_fields(d):
    """
    Helper function to match units to a field

    Args:
        d (dict): a dictionary of the format `{unit_name: parcel_field, ...}`, where the
            `unit_name` is one of the unit names present in the permits_df data's `permits_units_field`, and
            the `parcel_field` is the field name in the parcels_df corresponding to that unit. It should
            be used to identify non-building area fields in the parcels_df for which we can calculate
            a building area/unit for the multiplier. `parcel_field` can also take the form of a basic
            function (+,-,/,*) of a column, see Notes for specifications

    Returns:
        match_fields, match_functions (lists): listed outputs of field and functions
    """
    match_fields = []
    match_functions = []
    for key in d.keys():
        # Field
        field = re.findall("^(.*[A-Za-z])", d[key])[0]
        if field not in match_fields:
            match_fields.append(field)
        # Function
        fun = d[key].replace(field, "")
        if fun != "":
            fun = "".join(
                ['parcels_df["', field, '"] = parcels_df["', field, '"]', fun]
            )
            if field not in match_functions:
                match_functions.append(fun)
                # Overwrite value
        d[key] = field
    return match_fields, match_functions


def create_permits_units_reference(
        parcels,
        permits,
        lu_key,
        parcels_living_area_key,
        permit_value_key,
        permits_units_name="sq. ft.",
        units_match_dict=None,
):
    """
    Creates a reference table by which to convert various units provided in
        the Florida permits_df data to building square footage

    Args:
        parcels (str): path to MOST RECENT parcels_df data (see notes)
        permits (str): path to the building permits_df data
        lu_key (str): field name for a land use field present in BOTH the parcels_df and permits_df data
        parcels_living_area_key (str): field name in the parcels_df for the total living area (building area)
        permit_value_key (str): field name in the permits_df for the unit of measurement for permit types
        permits_units_name (str): unit name for building area in the `permits_units_field`
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
    if units_match_dict is None:
        units_match_dict = {}
    print("--- identifying relevant parcel fields")
    # Loading data
    print("--- reading/formatting parcels_df")
    # To set up for loading the parcels_df, we'll need to identify desired unit fields that are not building
    # square footage. These are provided in the values of the units_match_dict; however, some of them might be given
    # as functions (i.e. to match to a unit, a function of a parcels_df field is required). So, we need
    # to isolate the field names and the functional components
    match_fields, match_functions = match_units_fields(d=units_match_dict)
    parcels_fields = [lu_key, parcels_living_area_key] + match_fields
    parcels_df = PMT.featureclass_to_df(
        in_fc=parcels, keep_fields=parcels_fields, null_val=0.0
    )
    for fun in match_functions:
        exec(fun)

    # Permits: like with the parcels_df, we only need to keep a few fields: the lu_match_field, and the units_field.
    # Also, we only need to keep unique rows of this frame (otherwise we'd just be repeating calculations!)
    print("--- reading/formatting permits_df")
    permits_fields = [lu_key, permit_value_key]
    permits_df = PMT.featureclass_to_df(
        in_fc=permits, keep_fields=permits_fields, null_val=0.0
    )
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
    print("--- calculating multipliers and overwrites")
    rows = np.arange(len(permits_df.index))
    units_multipliers = []
    units_overwrites = []

    for row in rows:
        # Unit and land use for the row
        unit = permits_df[permit_value_key][row]
        lu = permits_df[lu_key][row]
        # Parcels of the row's land use
        plu = parcels_df[parcels_df[lu_key] == lu]

        # Case (1)
        if unit == permits_units_name:
            units_multipliers.append(1.0)
            units_overwrites.append(-1.0)
        # Cases (2) and (3)
        else:
            # Case (2)
            if unit in units_match_dict.keys():
                sqft_per_unit = (
                        plu[parcels_living_area_key] / plu[units_match_dict[unit]]
                )
                median_value = np.nanmedian(sqft_per_unit)
                units_multipliers.append(median_value)
                units_overwrites.append(-1.0)
            # Case (3)
            else:
                sq_ft = plu[parcels_living_area_key]
                if len(sq_ft) > 0:
                    median_value = np.nanmedian(sq_ft)
                else:
                    median_value = -1.0
                units_multipliers.append(-1.0)
                units_overwrites.append(median_value)

    # Since our results are lists, we can just attach them back to the permits_df as new columns
    print("--- binding results to the permits_df data")
    permits_df["Multiplier"] = units_multipliers
    permits_df["Overwrite"] = units_overwrites

    # Done
    return permits_df


def build_short_term_parcels(
        parcel_fc,
        parcels_id_field,
        parcels_lu_field,
        parcels_living_area_field,
        parcels_land_value_field,
        parcels_total_value_field,
        parcels_buildings_field,
        permit_fc,
        permits_ref_df,
        permits_id_field,
        permits_lu_field,
        permits_units_field,
        permits_values_field,
        permits_cost_field,
        units_field_match_dict={},
):
    """
    Using current parcel data and current permits, generate a near-term estimate of
    parcel changes as a temp feature class.

    Args:
        parcel_fc (str): Path to current parcel feature class
        parcels_id_field (str): Primary key for parcel data
        parcels_lu_field (str): Land use code attribute
        parcels_living_area_field (str): Building floor area field
        parcels_land_value_field (str): Parcel land value field
        parcels_total_value_field (str): Combined building and land value field
        parcels_buildings_field (str): Count of buildings per parcel field
        permit_fc (str): Path to permitted development that has been spatialized
        permits_ref_df (pandas.DataFrame): Table of reference units to map values from permits to parcel
        permits_id_field (str): Foreign key in permits layer tying parcels and permits
        permits_lu_field (str): Permits land use field
        permits_units_field (str): Permits unit type field (ex: sq.ft)
        permits_values_field (str): Permit unit value field (ex: 2526 --> reference to permits_units_field)
        permits_cost_field (str): Combined administrative and construction cost field in permit data
        units_field_match_dict (dict): k,v pair of parcel field and the unit list in permit_units_field that match

    Returns:
        temp_parcels (str): path to in_memory feature class with updated parcel data
    """
    # First, we need to initialize a save feature class. The feature class
    # will be a copy of the parcels with a unique ID (added by the function)
    print("--- initializing a save feature class")

    """ read in parcels and make blank copy """
    # setup temp_fc to hold parcel data
    temp_parcels = PMT.make_inmem_path()
    temp_dir, temp_name = os.path.split(temp_parcels)
    # add unique_id to parcels (roll back if failure
    print("--- --- adding a unique ID field for individual parcels")
    # process_id_field = PMT.add_unique_id(
    #     feature_class=parcel_fc, #new_id_field="ProcessID"
    # )

    print("--- --- reading/formatting parcels")
    # read in all of our data
    #   - read the parcels (after which we'll remove the added unique ID from the original data).
    parcels_fields = [
        f.name
        for f in arcpy.ListFields(parcel_fc)
        if f.name not in ["OBJECTID", "Shape", "Shape_Length", "Shape_Area"]
    ]
    parcels_df = PMT.featureclass_to_df(
        in_fc=parcel_fc, keep_fields=parcels_fields, null_val=0.0
    )
    parcels_df["PERMIT"] = 0

    # copy parcels to temp location keeping only process_id temp file
    print("--- --- creating empty copy of feature class")
    fmap = arcpy.FieldMappings()
    fm = arcpy.FieldMap()
    fm.addInputField(parcel_fc, parcels_id_field)
    fmap.addFieldMap(fm)
    arcpy.FeatureClassToFeatureClass_conversion(
        in_features=parcel_fc, out_path=temp_dir, out_name=temp_name, field_mapping=fmap
    )

    """ read the permits in and format """
    print("--- --- reading/formatting permits_df")
    permits_fields = [
        permits_id_field,
        permits_lu_field,
        permits_units_field,
        permits_values_field,
        permits_cost_field,
    ]
    permits_df = PMT.featureclass_to_df(
        in_fc=permit_fc, keep_fields=permits_fields, null_val=0.0
    )
    permits_df = permits_df[permits_df[permits_values_field] >= 0]

    #   - join the permits and ref together on the permits_lu_field and permits_units_field
    print("--- --- merging permits_df and permits_df reference")
    permits_df = pd.merge(
        left=permits_df,
        right=permits_ref_df,
        left_on=[permits_lu_field, permits_units_field],
        right_on=[permits_lu_field, permits_units_field],
        how="left",
    )

    # Now we add to permits_df the field matches to the parcels (which will be
    # helpful come the time of updating parcels from the permits_df)
    parcel_key_fields = [
        parcels_land_value_field,
        parcels_total_value_field,
        parcels_living_area_field,
    ]
    increment_fields = ["UPDT_LVG_AREA"]
    update_cols = []
    if units_field_match_dict is not None:
        print("--- --- joining units-field matches")
        for new_col, units_tag in units_field_match_dict.items():
            update_col = f"UPDT_{new_col}"
            fltr = np.logical_or.reduce(
                [permits_df[permits_units_field] == tag for tag in units_tag]
            )
            permits_df[update_col] = np.select(
                [fltr], [permits_df[permits_values_field]], 0
            )
            permits_df[update_col] = pd.to_numeric(
                permits_df[update_col], errors="coerce"
            )
            parcel_key_fields.append(new_col)
            update_cols.append(update_col)
            increment_fields.append(update_col)

    # calculate the new building square footage for parcel in the permit features
    # using the reference table multipliers and overwrites
    print("--- --- applying unit multipliers and overwrites")
    new_living_area = []
    for value, multiplier, overwrite in zip(
            permits_df[permits_values_field],
            permits_df["Multiplier"],
            permits_df["Overwrite"],
    ):
        if (overwrite == -1.0) and (multiplier != -1.0):
            new_living_area.append(value * multiplier)
        elif (multiplier == -1.0) and (overwrite != -1.0):
            new_living_area.append(overwrite)
        else:
            new_living_area.append(0)

    print("--- --- appending new living area values to permits_df data")
    permits_df["UPDT_LVG_AREA"] = new_living_area
    permits_df.drop(columns=["Multiplier", "Overwrite"], inplace=True)

    # update the parcels with the info from the permits_df
    agg_cols = {
        permits_values_field: np.sum,
        permits_lu_field: np.argmax,
        permits_cost_field: np.sum,
        "UPDT_LVG_AREA": np.sum,
    }
    agg_cols.update(dict([(col, np.sum) for col in update_cols]))
    permit_lu_gp = pd.DataFrame(
        data=permits_df.groupby([permits_id_field, permits_lu_field]).sum()
    )
    permit_lu_gp.reset_index(inplace=True)
    permit_update = permit_lu_gp.groupby(permits_id_field).agg(agg_cols)

    permit_update.fillna(0, inplace=True)
    permit_update[parcels_buildings_field] = 1
    permit_update.loc[permit_update["UPDT_LVG_AREA"] == 0, parcels_buildings_field] = 0
    permit_update["PERMIT"] = 1
    permit_update.rename(
        columns={
            permits_id_field: parcels_id_field,
            permits_lu_field: parcels_lu_field,
        },
        inplace=True,
    )

    # Finally, we want to update the value field
    print("--- --- estimating parcel value after permit development")
    pids = np.unique(permits_df[permits_id_field])
    permit_parcels = parcels_df[parcels_df[parcels_id_field].isin(pids)]
    pv = permit_parcels.groupby(parcels_id_field)[parcel_key_fields].sum().reset_index()
    permit_update = pd.merge(permit_update, pv, on=parcels_id_field, how="left")
    permit_update["NV"] = (
            permit_update[parcels_land_value_field] + permit_update[permits_cost_field]
    )
    permit_update[parcels_living_area_field] = (
            permit_update[parcels_living_area_field] + permit_update["UPDT_LVG_AREA"]
    )
    permit_update[parcels_total_value_field] = np.maximum(
        permit_update["NV"], permit_update[parcels_total_value_field]
    )
    for new_col in units_field_match_dict.keys():
        update_col = f"UPDT_{new_col}"
        permit_update[new_col] = permit_update[new_col] + permit_update[update_col]
    permit_update = permit_update.set_index(p_conf.PARCEL_COMMON_KEY)

    # make the replacements.
    print("--- --- replacing parcel data with updated information")
    parcel_update = parcels_df.set_index(p_conf.PARCEL_COMMON_KEY)
    parcel_update.update(permit_update)
    parcel_update.reset_index(inplace=True)
    permit_update.reset_index(inplace=True)
    # tac on new lvg area field, no_res_units for good measure
    parcel_update = pd.merge(
        left=parcel_update,
        right=permit_update[[parcels_id_field] + increment_fields],
        how="outer",
        on=parcels_id_field,
    )
    parcel_update.fillna(0, inplace=True)

    print("--- --- joining results to save feature class...")
    PMT.extend_table_df(
        in_table=temp_parcels,
        table_match_field=parcels_id_field,
        df=parcel_update,
        df_match_field=parcels_id_field,
    )
    return temp_parcels


def clean_skim_csv(
        in_file,
        out_file,
        imp_field,
        drop_val=0,
        renames={},
        node_offset=0,
        node_fields=["F_TAP", "T_TAP"],
        chunksize=100000,
        **kwargs,
):
    """
    Reads an OD table and drops rows where `imp_field` = `drop_val` is true. Optionallly renumbers nodes
    by applying (adding) an offset to the original values. Saves a new csv containing key columns 
    (`node_fields` and `imp_field`)

    Args:
        in_file (str): Path to a long OD table in csv format
        out_file (str): Path to a new (shortened) OD table to store function outputs
        imp_field (str): The name of the field in `in_file` containing impedance (time, distance, cost)
            values between OD pairs. If this field is renamed using `renames`, the new name should be provided here.
        drop_val (int/float, default=0): Rows where `imp_field` is equal to `drop_val` are dropped from the skim
        renames (dict): Keys are column names in `in_file`, values are new names for those columns in`out_file`
        node_offset (int, default=0): If origin and destiantion nodes need to be renumbered, this value will be
            added to the original values in `node_fields`. (This is used when the multiple skims are being used to
            create a network and node number collisions need to be handled.)
        node_fields (str): [String,...] List of fields containing node values. At a minimum, there should be two
            fields listed: the origin and destiantion fields. All fields listed will have the `node_offset` (if given)
            applied. If columns are renamed used `renames`, give the new column names, not the old ones.
        chunksize (int, default=10000): Number of rows to process at a given time. More rows are faster but require 
            more memory.
        kwargs: Keywords to use when loading `in_file` with `pd.read_csv`.

    Returns:
        `out_file: path to output cleaned skim
    """
    # TODO: support multiple imped_fields
    # TODO: support multiple drop values
    # TODO: support comparison drop values (>, <, !=, etc.)
    header = True
    mode = "w"
    for chunk in pd.read_csv(in_file, chunksize=chunksize, **kwargs):
        if renames:
            chunk.rename(columns=renames, inplace=True)
        fltr = chunk[imp_field] != drop_val
        chunk = chunk[fltr].copy()
        for nf in node_fields:
            chunk[nf] += node_offset
        chunk.to_csv(out_file, mode=mode, header=header)
        header = False
        mode = "a"

    return out_file


def _df_to_graph_(df, source, target, attrs, create_using, renames):
    """
    Helper function to convert a pandas.Dataframe to a networkx Graph object

    Args:
        df (pandas.DataFrame): pandas dataframe of formatted SKIM
        source (str): The origin field. If fields are renamed used `renames`, give the new name.
        target (str): The destination field. If fields are renamed used `renames`, give the new name.
        attrs (list): [String,...]; Column names containing values to include as edge attributes
        create_using (networx graph constructor, default=nx.DiGraph): The type of graph to build from the csv.
        renames (dict): Keys are column names in `df`, values are new names for those columns
            to appear as edge attributes.

    Returns:
        networkx.Graph
    """
    if renames:
        df.rename(columns=renames, inplace=True)
    return nx.from_pandas_edgelist(
        df, source=source, target=target, edge_attr=attrs, create_using=create_using
    )


def skim_to_graph(
        in_csv, source, target, attrs, create_using=nx.DiGraph, renames={}, **kwargs
):
    """
    Converts a long OD table from a csv into a networkx graph, such that each
    OD row becomes an edge in the graph, with its origin and destination added as nodes.

    Args:
        in_csv (str): Path to skim csv file
        source (str): The origin field. If fields are renamed used `renames`, give the new name.
        target (str): The destination field. If fields are renamed used `renames`, give the new name.
        attrs (list): [String,...]; Column names containing values to include as edge attributes
        create_using (networx graph constructor, default=nx.DiGraph): The type of graph to build from the csv.
        renames (dict): Keys are column names in `in_file`, values are new names for those columns
            to appear as edge attributes.
        kwargs: Keywords to use when loading `in_file` with `pd.read_csv`.
    
    Returns:
        networkx.Graph
    """
    if "chunksize" in kwargs:
        graph_list = []
        for chunk in pd.read_csv(in_csv, **kwargs):
            graph_list.append(
                _df_to_graph_(chunk, source, target, attrs, create_using, renames)
            )
        return reduce(nx.compose, graph_list)
    else:
        df = pd.read_csv(in_csv, **kwargs)
        return _df_to_graph_(df, source, target, attrs, create_using, renames)


def transit_skim_joins(
        taz_to_tap,
        tap_to_tap,
        out_skim,
        o_col="OName",
        d_col="DName",
        imp_col="Minutes",
        origin_zones=None,
        destination_zones=None,
        total_cutoff=np.inf
):
    """
    Creates a full skim from TAZ to TAZ by transit based on TAP to TAP skims and
    TAZ to TAP access/egress skims. TAP = transit access point.
    
    This function assumes `taz_to_tap` and `tap_to_tap` have identical column headings
    for key fields.

    Args:
        taz_to_tap (str): Path to a csv OD table with estimated impedances between TAZs
            and accessible TAPs. This represents travel time outside the transit vehicle.
        tap_to_tap (str): Path to a csv OD table with estimated impdedances between TAPs. This
            represents travel time in the transit vehicle.
        out_skim (str): Path to an output csv OD table with estimated impedances between TAZs,
            inclusive of in-vehicle and out-of-vehicle impedance.
        o_col (str, default="OName"): the column in `taz_to_tap` and `tap_to_tap` that identifies
            the origin-end feature of each record.
        d_col (strdefault="OName"): the column in `taz_to_tap` and `tap_to_tap` that identifies
            the destination-end feature of each record.
        imp_col (str, default="Minutes"): the column in `taz_to_tap` and `tap_to_tap` that records
            the impedance for each OD pair.
        origin_zones (list, default=None): To limit the output OD table to specific origin locations,
            provide the origin TAZ id's in a list.
        destination_zones (list, default=None): same as `origin_zones` except for destination TAZs.
        total_cutoff (numeric, default=np.inf): If given, the output OD table will be truncated to
            include only OD pairs having estimated impedances less than or equal to the cutoff.

    Returns:
        None: results are stored in a new csv table at `out_skim`
    """
    # TODO: enrich to set limits on access time, egress time, IVT
    # TODO: handle column output names
    # Read tables
    z2p_dd = dd.read_csv(taz_to_tap)
    p2p_dd = dd.read_csv(tap_to_tap)

    # Prep tables to handle column collisions
    z2p_dd = z2p_dd.rename(
        columns={o_col: "OName", d_col: "Boarding_stop", imp_col: "access_time"}
    )
    p2p_dd = p2p_dd.rename(
        columns={o_col: "Boarding_stop", d_col: "Alighting_stop", imp_col: "IVT"},
    )

    # Merge to get alighting stops reachable from origin TAZ via boarding stop
    o_merge = z2p_dd.merge(p2p_dd, how="inner", on="Boarding_stop")

    # Rename access skim columns to reflect egress times
    z2p_dd = z2p_dd.rename(
        columns={
            "Boarding_stop": "Alighting_stop",
            "OName": "DName",
            "access_time": "egress_time",
        },
    )

    # Merge to get destination zones reachable via alighting stop
    d_merge = o_merge.merge(z2p_dd, how="inner", on="Alighting_stop")

    # Calculate total time
    d_merge["Minutes"] = d_merge[["access_time", "IVT", "egress_time"]].sum(axis=1)

    # Filter
    result = d_merge[d_merge["Minutes"] <= total_cutoff]
    if origin_zones:
        result = result[result["OName"].isin(origin_zones)]
    if destination_zones:
        result = result[result["DName"].isin(destination_zones)]

    # Export result
    out_cols = ["OName", "DName", "Minutes"]
    result = result[out_cols].groupby(out_cols[:2]).min()
    result.to_csv(
        out_skim,
        single_file=True,
        index=True,
        header_first_partition_only=True,
        chunksize=100000,
    )


def full_skim(
        tap_to_tap, taz_to_tap, taz_to_taz, cutoff, taz_nodes, all_tazs,
        impedance_attr="Minutes"
):
    """
    Creates a full skim from TAZ to TAZ by transit based on TAP to TAP skims and
    TAZ to TAP access/egress skims. TAP = transit access point.
    
    This is an alternative to `transit_skim_joins` that converts long OD tables to networkx
    DiGraph objects, combining TAP to TAP and TAZ to TAP skims in a single graph, solving
    TAZ to TAZ paths and recording outputs in a csv table. It is slower than `transit_skim_join`
    but can handle larger networks that may raise memory errors there.

    args:
        tap_to_tap (str): Path to the TAP to TAP OD skim input
        taz_to_tap (str): Path to the TAZ to TAP OD skim input
        taz_to_taz (str): Path to the TAZ to TAZ OD skim output to be created
        cutoff (int, float): A cutoff value (in units of `impedance_attr`) to apply
            such that only TAZ to TAZ records within the cutoff are stored in `taz_to_taz`
        taz_nodes (list): A list of TAZ's from which to analyze TAZ to TAZ impedances.
        all_tazs (list): A list of all TAZ's.
        impedance_attr (str, deafult="Minutes"): The column in `tap_to_tap` and `taz_to_tap`
            that records OD impedance estimates.
    
    Returns:
        None: results are stored in a new csv table at `out_skim`
    """
    G = skim_to_graph(
        in_csv=tap_to_tap,
        source="OName",
        target="DName",
        attrs=impedance_attr,
        create_using=nx.DiGraph,
    )
    # Make tap to taz network (as base graph, converted to digraph)
    print(" - - building TAZ to TAP graph")
    H = skim_to_graph(
        in_csv=taz_to_tap,
        source="OName",
        target="DName",
        attrs=impedance_attr,
        create_using=nx.Graph,
        renames={},
    ).to_directed()
    # Combine networks and solve taz to taz
    print(" - - combining graphs")
    FULL = nx.compose(G, H)
    print(
        f" - - solving TAZ to TAZ for {len(taz_nodes)} origins (of {len(all_tazs)} taz's)"
    )
    #taz_to_taz = PMT.make_path(clean_serpm_dir, f"TAZ_to_TAZ_{skim_version}_{model_year}.csv")

    with open(taz_to_taz, "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [p_conf.SKIM_O_FIELD, p_conf.SKIM_D_FIELD, p_conf.SKIM_IMP_FIELD]
        )
        for i in taz_nodes:
            if FULL.has_node(i):
                i_dict = nx.single_source_dijkstra_path_length(
                    G=FULL, source=i, cutoff=cutoff, weight=impedance_attr
                )
                out_rows = []
                for j, time in i_dict.items():
                    if j in all_tazs:
                        out_row = (i, j, time)
                        out_rows.append(out_row)
                writer.writerows(out_rows)