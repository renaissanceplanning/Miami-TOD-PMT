import os
from pathlib import Path
import shutil
import time

import arcpy
import numpy as np
import pandas as pd
from six import string_types

from PMT_tools import PMT

DATA_ROOT = ""


def make_path(in_folder, *subnames):
    """Dynamically set a path (e.g., for iteratively referencing
        year-specific geodatabases)
    Args:
        in_folder (str): String or Path
        subnames (list/tuple): A list of arguments to join in making the full path
            `{in_folder}/{subname_1}/.../{subname_n}
    Returns:
        Path
    """
    return os.path.join(in_folder, *subnames)


SCRIPTS = Path(__file__).parent
DATA = make_path(DATA_ROOT, "Data")
RAW = make_path(DATA, "RAW")
CLEANED = make_path(DATA, "CLEANED")
REF = make_path(SCRIPTS, "ref")
BUILD = make_path(DATA, "BUILD")
BASIC_FEATURES = make_path(DATA, "PMT_BasicFeatures.gdb", "BasicFeatures")
YEAR_GDB_FORMAT = make_path(DATA, "PMT_{year}.gdb")
RIF_CAT_CODE_TBL = make_path(REF, "road_impact_fee_cat_codes.csv")
DOR_LU_CODE_TBL = make_path(REF, "Land_Use_Recode.csv")

YEARS = [2014, 2015, 2016, 2017, 2018, 2019, "NearTerm"]
SNAPSHOT_YEAR = 2019


class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""


class Timer:
    def __init__(self):
        self._start_time = None

    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()
        print("Timer has started...")

    def stop(self):
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        elapsed_time = (time.perf_counter() - self._start_time)
        if elapsed_time > 60:
            elapsed_time = elapsed_time/60
            print(f"Elapsed time: {elapsed_time:0.4f} minutes")
        if elapsed_time > 3600:
            elapsed_time /= 3600
            print(f"Elapsed time: {elapsed_time:0.4f} hours")
        self._start_time = None


def validate_directory(directory):
    if os.path.isdir(directory):
        return directory
    else:
        try:
            os.makedirs(directory)
            return directory
        except:
            raise


def check_overwrite_path(output, overwrite=True):
    output = Path(output)
    if output.exists():
        if overwrite:
            if output.is_file():
                print(f"--- --- deleting existing file {output.name}")
                output.unlink()
            if output.is_dir():
                print(f"--- --- deleting existing folder {output.name}")
                shutil.rmtree(output)
        else:
            print(
                f"Output file/folder {output} already exists"
            )


def _list_table_paths(gdb, criteria="*"):
    """
    internal function, returns a list of all tables within a geodatabase
    Args:
        gdb (str/path): string path to a geodatabase
        criteria (list): wildcards to limit the results returned from ListTables;
            list of table names generated from trend table parameter dictionaries,
            table name serves as a wildcard for the ListTables method, however if no criteria is given
            all table names in the gdb will be returned

    Returns (list):
        list of full paths to tables in geodatabase
    """
    old_ws = arcpy.env.workspace
    arcpy.env.workspace = gdb
    if isinstance(criteria, string_types):
        criteria = [criteria]
    # Get tables
    tables = []
    for c in criteria:
        tables += arcpy.ListTables(c)
    arcpy.env.workspace = old_ws
    return [PMT.make_path(gdb, table) for table in tables]


def _list_fc_paths(gdb, fds_criteria="*", fc_criteria="*"):
    """
    internal function, returns a list of all feature classes within a geodatabase
    Args:
        gdb (str/path): string path to a geodatabase
        fds_criteria (str/list): wildcards to limit results returned. List of
            feature datasets
        fc_criteria (str/list): wildcard to limit results returned. List of
            feature class names.

    Returns (list):
        list of full paths to feature classes in geodatabase
    """
    old_ws = arcpy.env.workspace
    arcpy.env.workspace = gdb
    paths = []
    if isinstance(fds_criteria, string_types):
        fds_criteria = [fds_criteria]
    if isinstance(fc_criteria, string_types):
        fc_criteria = [fc_criteria]
    # Get feature datasets
    fds = []
    for fdc in fds_criteria:
        fds += arcpy.ListDatasets(fdc)
    # Get feature classes
    for fd in fds:
        for fc_crit in fc_criteria:
            fcs = arcpy.ListFeatureClasses(feature_dataset=fd, wild_card=fc_crit)
            paths += [PMT.make_path(gdb, fd, fc) for fc in fcs]
    arcpy.env.workspace = old_ws
    return paths


def _make_access_col_specs(activities, time_breaks, mode, include_average=True):
    """
    helper function to generate access column specs
    Args:
        activities (list): list of job sectors
        time_breaks (list): integer list of time bins
        mode (list): string list of transportation modes
        include_average (bool): flag to create a long column of average minutes to access
            a given mode

    Returns:
        cols (list), renames (dict); list of columns created, dict of old/new name pairs
    """
    cols = []
    new_names = []
    for a in activities:
        for tb in time_breaks:
            col = f"{a}{tb}Min"
            cols.append(col)
            new_names.append(f"{col}{mode[0]}")
        if include_average:
            col = f"AvgMin{a}"
            cols.append(col)
            new_names.append(f"{col}{mode[0]}")
    renames = dict(zip(cols, new_names))
    return cols, renames


def _createLongAccess(int_fc, id_field, activities, time_breaks, mode, domain=None):
    """

    Args:
        int_fc:
        id_field:
        activities:
        time_breaks:
        mode:
        domain:

    Returns:

    """
    # result is long on id_field, activity, time_break
    # TODO: update to use Column objects? (null handling, e.g.)
    # --------------
    # Dump int fc to data frame
    acc_fields, renames = _make_access_col_specs(
        activities, time_breaks, mode, include_average=False
    )
    if isinstance(id_field, string_types):
        id_field = [id_field]  # elif isinstance(Column)?

    all_fields = id_field + list(renames.values())
    df = PMT.featureclass_to_df(in_fc=int_fc, keep_fields=all_fields, null_val=0.0)
    # Set id field(s) as index
    df.set_index(id_field, inplace=True)

    # Make tidy hierarchical columns
    levels = []
    order = []
    for tb in time_breaks:
        for a in activities:
            col = f"{a}{tb}Min{mode[0]}"
            idx = df.columns.tolist().index(col)
            levels.append((a, tb))
            order.append(idx)
    header = pd.DataFrame(
        np.array(levels)[np.argsort(order)], columns=["Activity", "TimeBin"]
    )
    mi = pd.MultiIndex.from_frame(header)
    df.columns = mi
    df.reset_index(inplace=True)
    # Melt
    melt_df = df.melt(id_vars=id_field)
    melt_df["from_time"] = melt_df["TimeBin"].apply(
        lambda time_break: _get_time_previous_time_break_(time_breaks, time_break)
    )
    return melt_df


def _get_time_previous_time_break_(time_breaks, tb):
    if isinstance(tb, string_types):
        tb = int(tb)
    idx = time_breaks.index(tb)
    if idx == 0:
        return 0
    else:
        return time_breaks[idx - 1]


def table_difference(this_table, base_table, idx_cols, fields="*", **kwargs):
    """
    helper function to calculate the difference between this_table and base_table
        ie... this_table minus base_table
    Args:
        this_table (str): path to a snapshot table
        base_table (str): path to a previous years snapshot table
        idx_cols (list): column names used to generate a common index
        fields (list): if provided, a list of fields to calculate the difference on;
            Default: "*" indicates all fields
        **kwargs: keyword arguments for featureclass_to_df

    Returns:
        pandas.Dataframe; df of difference values
    """
    # Fetch data frames
    this_df = PMT.featureclass_to_df(in_fc=this_table, keep_fields=fields, **kwargs)
    base_df = PMT.featureclass_to_df(in_fc=base_table, keep_fields=fields, **kwargs)
    # Set index columns
    base_df.set_index(idx_cols, inplace=True)
    this_df.set_index(idx_cols, inplace=True)
    this_df = this_df.reindex(base_df.index, fill_value=0)  # is this necessary?
    # Drop all remaining non-numeric columns
    base_df_n = base_df.select_dtypes(["number"])
    this_df_n = this_df.select_dtypes(["number"])

    # Take difference
    diff_df = this_df_n - base_df_n
    # Restore index columns
    diff_df.reset_index(inplace=True)

    return diff_df