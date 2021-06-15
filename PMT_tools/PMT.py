"""
The `PMT` module provides a centralized repository of commonly used classes and functions
supporting the development of the TOC toolkit. Many of these focus on file management
and mundane but critical procedural support. It also sets constant variables for relative
file locations and analysis parameters such as the years of data to be analyzed and reported.
"""
import fnmatch
import importlib
import os
import re
import shutil
import tempfile

# %% imports
import time
import uuid
from collections.abc import Iterable
from pathlib import Path

EPSG_LL = 4326
EPSG_FLSPF = 2881
EPSG_WEB_MERC = 3857
# import arcpy last as arc messes with global states on import likely changing globals in a way that doesnt allow
# other libraries to locate their expected resources
# see if arcpy available to accommodate non-windows environments
if importlib.util.find_spec('arcpy') is not None:
    import arcpy
    has_arcpy = True

    SR_WGS_84 = arcpy.SpatialReference(EPSG_LL)
    SR_FL_SPF = arcpy.SpatialReference(EPSG_FLSPF)  # Florida_East_FIPS_0901_Feet
    SR_WEB_MERCATOR = arcpy.SpatialReference(EPSG_WEB_MERC)
else:
    has_arcpy = False

import numpy as np
import pandas as pd
#from simpledbf import Dbf5
from six import string_types


__classes__ = [
    "TimeError",
    "Timer",
    "Column",
    "DomainColumn",
    "AggColumn",
    "CollCollection",
    "Consolidation",
    "MeltColumn",
    "Join",
    "Comp",
    "And",
    "Or",
    "NetLoader",
    "ServiceAreaAnalysis",
]
__functions__ = [
    "make_path",
    "make_inmem_path",
    "validate_directory",
    "validate_geodatabase",
    "validate_feature_dataset",
    "checkOverwriteOutput",
    "dbf_to_df",
    "intersect_features",
    "json_to_featureclass",
    "json_to_table",
    "iter_rows_as_chunks",
    "copy_features",
    "col_multi_index_to_names",
    "extend_table_df",
    "df_to_table",
    "table_to_df",
    "featureclass_to_df",
    "which_missing",
    "multipolygon_to_polygon_arc",
    "is_multipart",
    "polygons_to_points",
    "add_unique_id",
    "count_rows",
    "gen_sa_lines",
    "gen_sa_polys",
    "table_difference"
    "",
]
__all__ = __classes__ + __functions__


def make_path(in_folder, *subnames):
    """Dynamically set a path (e.g., for iteratively referencing year-specific geodatabases).
        {in_folder}/{subname_1}/../{subname_n}
    Args:
        in_folder (str): String or Path
        subnames (list/tuple): A list of arguments to join in making the full path

    Returns (str):
        str: String path
    """
    return os.path.join(in_folder, *subnames)


# %% CONSTANTS - FOLDERS
DATA_ROOT = ""      # needs to be set prior to using any tools

# commonly used reference data
SCRIPTS = Path(__file__).parent
REF = make_path(SCRIPTS, "ref")
RIF_CAT_CODE_TBL = make_path(REF, "road_impact_fee_cat_codes.csv")
DOR_LU_CODE_TBL = make_path(REF, "Land_Use_Recode.csv")

# standardized data paths
DATA = make_path(DATA_ROOT, "Data")
RAW = make_path(DATA, "RAW")
CLEANED = make_path(DATA, "CLEANED")
BUILD = make_path(DATA, "BUILD")
BASIC_FEATURES = make_path(DATA, "PMT_BasicFeatures.gdb", "BasicFeatures")
YEAR_GDB_FORMAT = make_path(DATA, "PMT_YEAR.gdb")

# year sets utilized for processing,
#   on update these should be updated to include the newest year(s) of data
YEARS = [2014, 2015, 2016, 2017, 2018, 2019, "NearTerm"]
SNAPSHOT_YEAR = 2019




# %% UTILITY CLASSES

# timer classes
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


# column and aggregation classes
class Column:
    """
    A class that defines key attributes of an existing or prospective column to facilitate processing
    
    Attributes:
        name (str): The original name of the column
        default (var): The default value to fill missing/nan values.
            Type depends on column data type.
        rename (str, default=None): If given, this column will be renamed in a dataframe.
        domain (obj, default=None): DomainColumn; If given, values in this Column will be used to
            define a new column that assings them to an Ordinal domain. This is primarily
            useful to enforce ordering of rows in a dataframe by category.
    """
    def __init__(self, name, default=0.0, rename=None, domain=None):
        self.name = name
        self.default = default
        self.rename = rename
        self.domain = domain
        if self.rename is None:
            self.rename = self.name

    def __setattr__(self, name, value):
        if name == "domain":
            if value is not None and not isinstance(value, DomainColumn):
                raise TypeError(f"`domain` must be DomainColumn, got {type(value)}")
        super().__setattr__(name, value)

    def apply_domain(self, df, col=None):
        """
        Method to create the domain column associated with this Column in the given dataframe.

        args:
            df (pd.DataFrame): a dataframe having this Column as a member
            col (str, default=None): if None, this Column's values are mapped onto its Domain
                attribute to generate a new column `self.Domain.name`. This argument is generally
                not needed, but some child classes rely on the ability to specify another column
                as the domain reference.
        """
        if self.domain is not None:
            if col is None:
                col = self.name
            criteria = []
            results = []
            for ref_val, dom_val in self.domain.domain_map.items():
                criteria.append(df[col] == ref_val)
                results.append(dom_val)
                df[self.domain.name] = np.select(
                    condlist=criteria, choicelist=results, default=self.domain.default
                )


class DomainColumn(Column):
    """
    A Column that applies a domain mapping (original values to ordinal values) to another
    Column.

    Attributes:
        name (str): the name of this DomainColumn when added to any data frame
        default (var): the value to apply to missing values (those not found in
            `domain_map`)
        domain_map (dict): key-value pairs that relate original values in a reference 
            Column to ordinal values recorded in this DomainColumn.
    """
    def __init__(self, name, default=-1, domain_map={}):
        # domain_map = {value_in_ref_col: domain_val}
        # TODO: validate to confirm domain_val as int
        Column.__init__(self, name, default)
        self.domain_map = domain_map


class AggColumn(Column):
    """
    A Column that will be aggregated in a downstream group by/agg process.

    Attributes:
        name (str): the name of the column to aggregate
        agg_method (str or callable): the aggregation method to apply (see pandas.DataFrame.agg)
        default (numeric, default=0.0): values to replace missing/nan records with prior to aggregation
        rename (str, default=None): see `Column`
        domain (object, default=None): see `Column`, `DomainColumn`
    """
    def __init__(self, name, agg_method=sum, default=0.0, rename=None, domain=None):
        Column.__init__(self, name, default, rename, domain)
        self.agg_method = agg_method


class CollCollection(AggColumn):
    """
    A building-block class that is a child of AggColumn (i.e., it anticipates a 
    downstream aggregateion process). The `CollCollection` specifies
    multiple column parameters to facilitate various column procedures. This class
    is never initialized. See `Consolitation`, `MeltColumn`.

    Attributes:
        Name (str): see `AggColumn`
        input_cols (list): Names of columns that will be used to generate a new column
        agg_method (str or callabe): see `AggColumn`
        default (var or dict): One or more default values to apply to `input_cols` to replace
            missing/nan values. If multiple defaults are given, specify them as a dictionary
            whose keys are `input_cols` and whose values are corresponding defaults.
    """
    def __init__(self, name, input_cols, agg_method=sum, default=0.0, domain=None):
        AggColumn.__init__(self, name, agg_method, default)
        self.input_cols = input_cols

    def __setattr__(self, name, value):
        if name == "input_cols":
            valid = True
            if isinstance(value, string_types):
                valid = False
            elif not isinstance(value, Iterable):
                valid = False
            # elif len(value) <= 1:
            #     valid = False
            elif not isinstance(value[0], string_types):
                valid = False
            # Set property of raise error
            if valid:
                super().__setattr__(name, value)
            else:
                raise ValueError(f"Expected iterable of column names for `input_cols`")
        else:
            super().__setattr__(name, value)

    def defaultsDict(self):
        """
        If self.
        """
        if isinstance(self.default, Iterable) and not isinstance(
                self.default, string_types
        ):
            return dict(zip(self.input_cols, self.default))
        else:
            return dict(zip(self.input_cols, [self.default for ic in self.input_cols]))


class Consolidation(CollCollection):
    """
    A column collection that collapses multiple columns into a single column
    through a row-wise aggregation.

    Attributes:
        name (str): Name of the new column to be created when `input_cols` are 
            consolidated.
        input_cols (list): Names of columns that will be consolidated into a new column
        cons_method (str or callable): the aggregation method to apply row-wise to
            generate a new column from `input_cols` (see pandas.DataFrame.agg)
        agg_method (str or callabe): see `AggColumn`
        default (var or dic): see `CollCollection`
    """
    def __init__(self, name, input_cols, cons_method=sum, agg_method=sum, default=0.0):
        CollCollection.__init__(self, name, input_cols, agg_method, default)
        self.cons_method = cons_method


class MeltColumn(CollCollection):
    """
    A column collection that collapses multiple columns into a single column through
    table elongation (melting).

    Attributes:
        label_col (str): Name of the new column to be created to store `input_cols`
            headings when the table is melted.
        val_col (str): Name of the new column to be created to store `input_cols`
            values when the table is melted.
        input_cols (list): Names of columns that will be melted into the new columns
            `label_col` and `val_col`.
        agg_method (str or callabe): see `AggColumn`
        default (var or dic): see `CollCollection`
        domain (object, default=None): see `Column`, `DomainColumn`
    """
    def __init__(
            self, label_col, val_col, input_cols, agg_method=sum, default=0.0, domain=None
    ):
        CollCollection.__init__(self, val_col, input_cols, agg_method, default)
        self.label_col = label_col
        self.val_col = val_col
        self.domain = domain

    def apply_domain(self, df):
        """
        DomainColumn specifications are applied in the same way as other Column objects,
        but for the `MeltColumn` class, `self.label_col` is used for mapping the domain.
        """
        super().apply_domain(df, col=self.label_col)


# class Join(CollCollection):
#     """
#     A column collection that collapses multiple columns into a single column through
#     table elongation (melting).

#     Attributes:
#         label_col (str): Name of the new column to be created to store `input_cols`
#             headings when the table is melted.
#         val_col (str): Name of the new column to be created to store `input_cols`
#             values when the table is melted.
#         input_cols (list): Names of columns that will be melted into the new columns
#             `label_col` and `val_col`.
#         agg_method (str or callabe): see `AggColumn`
#         default (var or dic): see `CollCollection`
#         domain (object, default=None): see `Column`, `DomainColumn`
#     """
#     def __init__(self, on_col, input_cols, agg_method=sum, default=0.0):
#         CollCollection.__init__(self, None, input_cols, agg_method, default)
#         self.on_col = on_col

    """Comparison Classes """
class Comp:
    """
    A naive class that allows string-based specification of comparison operators for
    process configuration support purposes.

    Attributes:
        comp_method (str): Comparison methods provided as strings:
            - "==" is 'equals' or `__eq__()`
            - "!=" is 'not equal to' or `__ne__()`
            - "<" is 'less than' or `__lt__()`
            - "<=" is 'is less than or equal to' or `__le__()`
            - ">" is 'greater than' or `__gt__()`
            - ">=" is 'greater than or equal to' or `__ge__()`
        v (var): the value to compare other values against using the `comp_method`
    """

    def __init__(self, comp_method, v):
        _comp_methods = {
            "==": "__eq__",
            "!=": "__ne__",
            "<": "__lt__",
            "<=": "__le__",
            ">": "__gt__",
            ">=": "__ge__",
        }
        self.comp_method = _comp_methods[comp_method]
        self.v = v

    def eval(self, val):
        """
        Evaluate the comparison of this value against `self.v`

        args:
            val (var): the value to comparea against `self.v` using `self.comp_method`
        """
        return getattr(val, self.comp_method)(self.v)


class And:
    """
    A stack of `Comp` objects that define conditions that must all evaluate to True when
    applied against a given value.

    Attributes:
        criteria (list): a list of `Comp` objects.
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
        Evaluate the comparison of this value(s) against all values in `self.criteria` using
        the comparison methods specified for each criterion.

        args:
            *vals: one more values to evaluate against `self.criteria`. Returns a boolean 
            vector that has True values only where all criteria are met.
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
    A stack of `Comp` or 'And' objects that define conditions, any of which must evaluate to True
    when applied agaianst a vector of values.

    Attributes:
        vector (np.array-like): A vector of values to test against `self.criteria`.
        criteria (list): [Comp, And, ...] Criteria that will be applied to check if values in `vector`
            meet any.
    """

    def __init__(self, vector, criteria):
        self.vector = vector
        if isinstance(criteria, Iterable):
            self.criteria = criteria  # TODO: validate criteria
        else:
            self.criteria = [criteria]

    def eval(self):
        """
        Returns a boolean vector like `self.vector` that has True values where the values in
        `self.vector` evaluate to True for any criterion given in `self.criteria`.
        """
        return np.logical_or.reduce([c.eval(self.vector) for c in self.criteria])


class NetLoader:
    """
    A naive class for specifying network location loading preferences.
    Simplifies network functions by passing loading specifications as
    a single argument. This class does no validation of assigned preferences.

    All attributes correspond to arcpy attribute specifications defined here:
    https://pro.arcgis.com/en/pro-app/latest/tool-reference/network-analyst/add-locations.htm

    Attributes:
        search_tolerance (str)
        search_criteria (str)
        match_type (str)
        append (str)
        snap (str)
        offset (str)
        exclude_restricted (str)
        search_query (str)
    """

    def __init__(
            self,
            search_tolerance,
            search_criteria,
            match_type="MATCH_TO_CLOSEST",
            append="APPEND",
            snap="NO_SNAP",
            offset="5 Meters",
            exclude_restricted="EXCLUDE",
            search_query=None,
    ):
        self.search_tolerance = search_tolerance
        self.search_criteria = search_criteria
        self.match_type = match_type
        self.append = append
        self.snap = snap
        self.offset = offset
        self.exclude_restricted = exclude_restricted
        self.search_query = search_query


class ServiceAreaAnalysis:
    """Specifies elements of a Network Analyst Service Area Problem and
        provides a method for solving and exporting service arae lines
        and polygons.

    Args:
        name (str): Name of the service area problem to generate
        network_dataset (str): Path to the network dataset that will
            determine the service area
        facilities (str or Feature Layer): Path to point features representing
            locations for which service areas will be created.
        name_field (str): A field in `facilities` that identifies each facility
            (can tag a group of facilities)
        net_loader (object): NetLoader; specifications for how to relate `facilities`
            to features in the `network_dataset`.
    """
    def __init__(self, name, network_dataset, facilities, name_field, net_loader):
        self.name = name
        self.network_dataset = network_dataset
        self.facilities = facilities
        self.name_field = name_field
        self.net_loader = net_loader
        self.overlaps = ["OVERLAP", "NON_OVERLAP"]
        self.merges = ["NO_MERGE", "MERGE"]

    def solve(
            self,
            imped_attr,
            cutoff,
            out_ws,
            restrictions="",
            use_hierarchy=False,
            net_location_fields="",
    ):
        """Create service area lines and polygons for this object's `facilities`.

        Args:
            imped_attr (str): The impedance attribute in this object's `network_dataset` to use
                in estimating service areas.
            cutoff (numeric): The size of the service area to create (in units corresponding to
                those used by `imped_attr`).
            out_ws (str): Path to a workspace where service area feature class ouptuts will be
                stored.
            restrictions (str): A semi-colon-separated list of restriction attributes in
                `self.network_dataset` to honor when creating service areas.
            use_hierarchy (bool, default=False)
            net_location_fields (str, default=""): if `self.facilities` have pre-calculated
                network location fields, list the fields in order ("SourceOID", "SourceID", "PosAlong",
                "SideOfEdge", "SnapX", "SnapY", "Distance",). This speeds up processing times since
                spatial analysis to load locations on the network is not needed.
        """
        for overlap, merge in zip(self.overlaps, self.merges):
            print(f"...{overlap}/{merge}")
            # Lines
            lines = make_path(out_ws, f"{self.name}_{overlap}")
            lines = gen_sa_lines(
                self.facilities,
                self.name_field,
                self.network_dataset,
                imped_attr,
                cutoff,
                self.net_loader,
                lines,
                from_to="TRAVEL_TO",
                overlap=overlap,
                restrictions=restrictions,
                use_hierarchy=use_hierarchy,
                uturns="ALLOW_UTURNS",
                net_location_fields=net_location_fields,
            )
            # Polygons
            polys = make_path(out_ws, f"{self.name}_{merge}")
            polys = gen_sa_polys(
                self.facilities,
                self.name_field,
                self.network_dataset,
                imped_attr,
                cutoff,
                self.net_loader,
                polys,
                from_to="TRAVEL_TO",
                merge=merge,
                nesting="RINGS",
                restrictions=restrictions,
                use_hierarchy=use_hierarchy,
                uturns="ALLOW_UTURNS",
                net_location_fields=net_location_fields,
            )


# %% FUNCTIONS
def make_inmem_path(file_name=None):
    """Generates an in_memory path usable by arcpy that is unique to avoid any overlapping names. If a file_name is
    provided, the in_memory file will be given that name with an underscore appended to the beginning.

    Returns:
        String; in_memory path
    
    Raises:
        ValueError, if file_name has been used already
    """
    if not file_name:
        unique_name = f"_{str(uuid.uuid4().hex)}"
    else:
        unique_name = f"_{file_name}"
    try:
        in_mem_path = make_path("in_memory", unique_name)
        if arcpy.Exists(in_mem_path):
            raise ValueError
        else:
            return in_mem_path
    except ValueError:
        print("The file_name supplied already exists in the in_memory space")


def validate_directory(directory):
    """Helper function to check if a directory exists and create if not"""
    if os.path.isdir(directory):
        return directory
    else:
        try:
            os.makedirs(directory)
            return directory
        except:
            raise


def validate_geodatabase(gdb_path, overwrite=False):
    """Helper function to check if a geodatabase exists, and create if not"""
    exists = False
    desc_gdb = arcpy.Describe(gdb_path)
    if gdb_path.endswith(".gdb"):
        if arcpy.Exists(gdb_path) and desc_gdb.dataType == "Workspace":
            exists = True
            if overwrite:
                check_overwrite_output(gdb_path, overwrite=overwrite)
                exists = False
    else:
        raise Exception("path provided does not contain a geodatabase")

    if exists:
        # If we get here, the gdb exists, and it won't be overwritten
        return gdb_path
    else:
        # The gdb does not or no longer exists and must be created
        try:
            out_path, gdb_name = os.path.split(gdb_path)
            name, ext = os.path.splitext(gdb_name)
            arcpy.CreateFileGDB_management(out_folder_path=out_path, out_name=name)
            return gdb_path
        except:
            raise Exception(f"could not create geodatabase at {gdb_path}")


def validate_feature_dataset(fds_path, sr, overwrite=False):
    """Validate that a feature dataset exists and is the correct sr,
        otherwise create it and return the path

    Args:
        fds_path (str): String; path to existing or desired feature dataset
        sr (spatial reference): arcpy.SpatialReference object
    
    Returns:
        fds_path (str): String; path to existing or newly created feature dataset
    """
    try:
        # verify the path is through a geodatabase
        if fnmatch.fnmatch(name=fds_path, pat="*.gdb*"):
            if (
                    arcpy.Exists(fds_path)
                    and arcpy.Describe(fds_path).spatialReference == sr
            ):
                if overwrite:
                    check_overwrite_output(fds_path, overwrite=overwrite)
                else:
                    return fds_path
            # Snippet below only runs if not exists/overwrite and can be created.
            out_gdb, name = os.path.split(fds_path)
            out_gdb = validate_geodatabase(gdb_path=out_gdb)
            arcpy.CreateFeatureDataset_management(
                out_dataset_path=out_gdb, out_name=name, spatial_reference=sr
            )
            return fds_path
        else:
            raise ValueError

    except ValueError:
        print("...no geodatabase at that location, cannot create feature dataset")


def check_overwrite_output(output, overwrite=False):
    """A helper function that checks if an output file exists and
        deletes the file if an overwrite is expected.
    
    Args:
        output: Path
            The file to be checked/deleted
        overwrite: Boolean
            If True, `output` will be deleted if it already exists.
            If False, raises `RuntimeError`.
    
    Raises:
        RuntimeError:
            If `output` exists and `overwrite` is False.
    """
    if arcpy.Exists(output):
        if overwrite:
            print(f"--- --- deleting existing file {output}")
            arcpy.Delete_management(output)
        else:
            raise RuntimeError(f"Output file {output} already exists")


def check_overwrite_path(output, overwrite=True):
    """Non-arcpy version of check_overwrite_output"""
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


def dbf_to_df(dbf_file):
    """Reads in dbf file and returns Pandas DataFrame object
    
    Args:
        dbf_file: String; path to dbf file
    
    Returns:
        pandas DataFrame object
    """
    db = Dbf5(dbf=dbf_file)
    return db.to_dataframe()


def intersect_features(
        summary_fc,
        disag_fc,
        disag_fields="*",
        as_df=False,
        in_temp_dir=False,
        full_geometries=False,
):
    """Creates a temporary intersected feature class for disaggregation of data

    Args:
        summary_fc (str): Path to path to polygon feature class with data to be disaggregated from
        disag_fc (str): Path to polygon feature class with data to be disaggregated to
        disag_fields (list): List of fields to pass over to intersect function
        as_df (bool): If True, returns a data frame (table) of the resulting intersect. Otherwise
            returns the path to a temporary feature class
        in_temp_dir (bool): If True, intersected features are stored in a temp directory, otherwise
            they are stored in memory
        full_geometries (bool): If True, intersections are run against the complete geometries of
            features in `disag_fc`, otherwise only centroids are used.

    Returns:
        int_fc (str): Path to temp intersected feature class
    """
    desc = arcpy.Describe(value=disag_fc)
    if in_temp_dir:
        # Create a temporary gdb for storing the intersection result
        temp_dir = tempfile.mkdtemp()
        arcpy.CreateFileGDB_management(
            out_folder_path=temp_dir, out_name="Intermediates.gdb"
        )
        out_gdb = make_path(temp_dir, "Intermediates.gdb")

        # Convert disag features to centroids
        disag_full_path = arcpy.Describe(disag_fc).catalogPath
        disag_ws, disag_name = os.path.split(disag_full_path)
        out_fc = make_path(out_gdb, disag_name)
        points_fc = make_path(out_gdb, f"{disag_name}_pts")
    else:
        out_fc = make_inmem_path()
        points_fc = make_inmem_path()

    if not full_geometries:
        # dump to centroids
        if desc.shapeType not in ["Polygon", "Point", "Multipoint"]:
            raise Exception(
                "disagg features must be polygon to limit intersection to centroids"
            )
        disag_fc = polygons_to_points(
            in_fc=disag_fc,
            out_fc=points_fc,
            fields=disag_fields,
            skip_nulls=False,
            null_value=0,
        )
    # Run intersection
    arcpy.Intersect_analysis(
        in_features=[summary_fc, disag_fc], out_feature_class=out_fc
    )

    # return intersect
    if as_df:
        return featureclass_to_df(out_fc, keep_fields="*", null_val=0.0)
    else:
        return out_fc


def json_to_featureclass(
        json_obj, out_file, new_id_field="ROW_ID", exclude=None, sr=4326, overwrite=False
):
    """Creates a feature class or shape file from a json object.
    
    Args:
        json_obj (dict)
        out_fc (str)
        new_id_field (str): name of new field to be added
        exclude (List; [String,...]): list of columns to exclude
        sr (spatial reference), default=4326: A spatial reference specification.
            Authority/factory code, WKT, WKID, ESRI name, path to .prj file, etc.
        overwrite (bool): True/False whether to overwrite an existing dataset
    
    Returns:
        out_fc: Path

    See Also:
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
        check_overwrite_output(output=out_file, overwrite=overwrite)
    out_path, out_name = os.path.split(out_file)
    arcpy.CreateFeatureclass_management(
        out_path=out_path,
        out_name=out_name,
        geometry_type=geom_type,
        spatial_reference=sr,
    )
    arcpy.AddField_management(out_file, new_id_field, "LONG")

    # Add geometries
    with arcpy.da.InsertCursor(out_file, ["SHAPE@", new_id_field]) as c:
        for i, geom in enumerate(geom_stack):
            row = [geom, i]
            c.insertRow(row)

    # Create attributes dataframe
    prop_df = pd.concat(prop_stack)
    prop_df[new_id_field] = np.arange(len(prop_df))
    exclude = [excl for excl in exclude if excl in prop_df.columns.to_list()]
    prop_df.drop(labels=exclude, axis=1, inplace=True)
    if arcpy.Describe(out_file).dataType.lower() == "shapefile":
        prop_df.fillna(0.0, inplace=True)

    # Extend table
    print([f.name for f in arcpy.ListFields(out_file)])
    print(prop_df.columns)
    return extend_table_df(
        in_table=out_file,
        table_match_field=new_id_field,
        df=prop_df,
        df_match_field=new_id_field,
    )


def json_to_table(json_obj, out_file):
    """Creates an ArcGIS table from a json object. Assumes potentially a geoJSON object.

    Args:
        json_obj (dict): dict
        out_file (str): Path to output table

    Returns:
        out_file (str): Path

    SeeAlso:
        jsonToFeatureClass
    """
    # convert to dataframe
    if "features" in json_obj:
        df = pd.DataFrame.from_dict(data=json_obj["features"])
        df.drop(columns="geometry", inplace=True)
    else:
        df = pd.DataFrame.from_dict(data=json_obj)
    return df_to_table(df, out_file)


def iter_rows_as_chunks(in_table, chunksize=1000):
    """
    A generator to iterate over chunks of a table for arcpy processing.
    
    This method cannot be reliably applied to a table view or feature
        layer with a current selection as it alters selections as part
        of the chunking process.
    
    Args:
        in_table (Table View or Feature Layer): The rows/features to process
        chunksize (int, default=1000): The number of rows/features to process at a time
    
    Returns:
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
        expr = f"{expr_ref} > {i} AND {expr_ref} <= {i + chunksize}"
        arcpy.SelectLayerByAttribute_management(
            in_layer_or_view=in_table, selection_type="NEW_SELECTION", where_clause=expr
        )
        yield in_table


def copy_features(in_fc, out_fc, drop_columns=[], rename_columns={}):
    """Copy features from a raw directory to a cleaned directory.
        During copying, columns may be dropped or renamed.
    
    Args:
        in_fc (str): Path to input feature class
        out_fc (str): Path to output feature class
        drop_columns (list): [String,...]; A list of column names to drop when copying features.
        rename_columns (dict): {String: String,...} A dictionary with keys that reflect raw column names and
            values that assign new names to these columns.
    
    Returns:
        out_fc (str): Path to the file location for the copied features.
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


def col_multi_index_to_names(columns, separator="_"):
    """
    For a collection of columns in a data frame, collapse index levels to
    flat column names. Index level values are joined using the provided
    `separator`.

    Args:
        columns (pandas.Index): The columns to flatten (i.e, df.columns)
        separator (str): The string value used to flatten multi-level column names
    
    Returns:
        flat_columns: pd.Index
    """
    if isinstance(columns, pd.MultiIndex):
        columns = columns.to_series().apply(lambda col: separator.join(col))
    return columns


def extend_table_df(in_table, table_match_field, df, df_match_field, **kwargs):
    """
    Use a pandas data frame to extend (add columns to) an existing table based
    through a join on key columns. Key values in the existing table must be
    unique.

    Args:
        in_table (str, feature layer, or table view): Path to the existing table to be extended
        table_match_field (str): The field in `in_table` on which to join values from `df`
        df (pandas.DataFrame): The data frame whose columns will be added to `in_table`
        df_match_field (str): The field in `df` on which join values to `in_table`
        kwargs: Optional keyword arguments to be passed to `arcpy.da.ExtendTable`.

    Returns:
        None; `in_table` is modified in place
    """
    # TODO: set defaults by reindexing and filling NANs based on table match field and array match field
    in_array = np.array(np.rec.fromrecords(df.values, names=df.dtypes.index.tolist()))
    arcpy.da.ExtendTable(
        in_table=in_table,
        table_match_field=table_match_field,
        in_array=in_array,
        array_match_field=df_match_field,
        **kwargs,
    )


def df_to_table(df, out_table, overwrite=False):
    """Use a pandas data frame to export an arcgis table.
    
    Args:
        df (pandas.DataFrame): DataFrame
        out_table (str): Path to output table
        overwrite (bool, default=False)
    
    Returns:
        out_table (str): Path
    """
    if overwrite:
        check_overwrite_output(output=out_table, overwrite=overwrite)
    in_array = np.array(np.rec.fromrecords(df.values, names=df.dtypes.index.tolist()))
    arcpy.da.NumPyArrayToTable(in_array, out_table)
    return out_table


def df_to_points(df, out_fc, shape_fields, from_sr, to_sr, overwrite=False):
    """Use a pandas data frame to export an arcgis point feature class.
    
    Args:
        df (pandas.DataFrame): A dataframe with valid `shape_fields` that can be 
            interpreted as x,y coordinates
        out_fc (str): Path to the point feature class to be generated.
        shape_fields (list): Columns to be used as shape fields (x, y)
        from_sr (arcpy.SpatialReference): The spatial reference definition for 
            the coordinates listed in `shape_field`
        to_sr (arcpy.SpatialReference): The spatial reference definition 
            for the output features.
        overwrite (bool, default=False)
    
    Returns:
        out_fc (str): Path
    """
    # set paths
    temp_fc = make_inmem_path()

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
    in_array = np.array(np.rec.fromrecords(df.values, names=df.dtypes.index.tolist()))
    # write to temp feature class
    arcpy.da.NumPyArrayToFeatureClass(
        in_array=in_array,
        out_table=temp_fc,
        shape_fields=shape_fields,
        spatial_reference=from_sr,
    )
    # reproject if needed, otherwise dump to output location
    if from_sr != to_sr:
        arcpy.Project_management(
            in_dataset=temp_fc, out_dataset=out_fc, out_coor_system=to_sr
        )
    else:
        out_path, out_fc = os.path.split(out_fc)
        if overwrite:
            check_overwrite_output(output=out_fc, overwrite=overwrite)
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=temp_fc, out_path=out_path, out_name=out_fc
        )
    # clean up temp_fc
    arcpy.Delete_management(in_data=temp_fc)
    return out_fc


def table_to_df(in_tbl, keep_fields="*", skip_nulls=False, null_val=0):
    """Converts a table to a pandas dataframe

    Args:
        in_tbl (str): Path to input table
        keep_fields (list, default="*"): list of fields to keep ("*" = keep all fields)
        skip_nulls (bool): Control whether records using nulls are skipped.
        null_val (int): Replaces null values from the input with a new value.

    Returns:
        df (pd.DataFrame): pandas dataframe of the table
    """
    if keep_fields == "*":
        keep_fields = [f.name for f in arcpy.ListFields(in_tbl) if not f.required]
    elif isinstance(keep_fields, string_types):
        keep_fields = [keep_fields]
    return pd.DataFrame(
        arcpy.da.TableToNumPyArray(
            in_table=in_tbl,
            field_names=keep_fields,
            skip_nulls=skip_nulls,
            null_value=null_val,
        )
    )


def featureclass_to_df(in_fc, keep_fields="*", skip_nulls=False, null_val=0):
    """Converts feature class/feature layer to pandas DataFrame object, keeping
        only a subset of fields if provided and dropping all spatial data

    Args:
        in_fc (str): Path to a feature class
        keep_fields (list, default="*"): Field names to return in the dataframe
            ("*" will return all fields)
        skip_nulls (bool): Control whether records using nulls are skipped.
        null_val (int, float, or dict): value to be used for nulls found in the data. Can be given as a
            dict of default values by field
    
    Returns:
        pandas.Dataframe
    """
    # setup fields
    if keep_fields == "*":
        keep_fields = [f.name for f in arcpy.ListFields(in_fc) if not f.required]
    elif isinstance(keep_fields, string_types):
        keep_fields = [keep_fields]

    return pd.DataFrame(
        arcpy.da.FeatureClassToNumPyArray(
            in_table=in_fc,
            field_names=keep_fields,
            skip_nulls=skip_nulls,
            null_value=null_val,
        )
    )


def which_missing(table, field_list):
    """Returns a list of the fields that are missing from a given table

    Args:
        table (str): Path to a table
        field_list (list): List of fields
    
    Returns:
        list
    """
    f_names = [f.name for f in arcpy.ListFields(table)]
    return [f for f in field_list if f not in f_names]


def multipolygon_to_polygon_arc(file_path):
    """
    Convert multipolygon geometries in a single row into
    multiple rows of simple polygon geometries, returns original file if not multipart
    
    Args:
        file_path (str): Path to input Poly/MultiPoly feature class

    Returns:
        str: path to in_memory Polygon feature class
    """
    polygon_fcs = file_path
    if is_multipart(polygon_fc=file_path):
        polygon_fcs = make_inmem_path()
        arcpy.MultipartToSinglepart_management(
            in_features=file_path, out_feature_class=polygon_fcs
        )
    return polygon_fcs


def is_multipart(polygon_fc):
    """Helper function to determine if polygon FC has multipart features

    Args:
        polygon_fc (str): Path to polygon feature class

    Returns:
        bool
    """
    multipart = []
    with arcpy.da.SearchCursor(polygon_fc, ["OID@", "SHAPE@"]) as sc:
        for row in sc:
            id, geom = row
            if geom.isMultipart:
                multipart.append(id)
    if multipart:
        return True
    else:
        return False


def polygons_to_points(in_fc, out_fc, fields="*", skip_nulls=False, null_value=0):
    """Convenience function to dump polygon features to centroids and save as a new feature class.
   
   Args:
        in_fc (str): Path to input feature class
        out_fc (str): Path to output point fc
        fields (str or list, default="*"): [String,...] fields to include in conversion
        skip_nulls (bool, default=False): Control whether records using nulls are skipped.
        null_value (int, default=0): Replaces null values from the input with a new value.

    Returns:
        out_fc (str): path to output point feature class
    """
    # TODO: adapt to search-cursor-based derivation of polygon.centroid to ensure point is within polygon
    sr = arcpy.Describe(in_fc).spatialReference
    if fields == "*":
        fields = [f.name for f in arcpy.ListFields(in_fc) if not f.required]
    elif isinstance(fields, string_types):
        fields = [fields]
    fields.append("SHAPE@XY")
    a = arcpy.da.FeatureClassToNumPyArray(
        in_table=in_fc, field_names=fields, skip_nulls=skip_nulls, null_value=null_value
    )
    arcpy.da.NumPyArrayToFeatureClass(
        in_array=a, out_table=out_fc, shape_fields="SHAPE@XY", spatial_reference=sr
    )
    fields.remove("SHAPE@XY")
    return out_fc


def add_unique_id(feature_class, new_id_field="ProcessID"):
    """Adds a unique incrementing integer value to a feature class and returns that name

    Args:
        feature_class (str): Path to a feature class
        new_id_field (str, default="ProcessID"): Name of new id field.

    Returns:
        new_id_field (str): Name of new id field
    """
    # OID = arcpy.Describe(feature_class).OIDFIeldName
    if new_id_field is None:
        new_id_field = "ProcessID"
    arcpy.AddField_management(
        in_table=feature_class, field_name=new_id_field, field_type="LONG"
    )
    # arcpy.CalculateField_management(
    #     in_table=feature_class, field=new_id_field,
    #     expression=f"!{OID}!", expression_type="PYTHON3"
    # )
    with arcpy.da.UpdateCursor(feature_class, new_id_field) as ucur:
        for i, row in enumerate(ucur):
            row[0] = i
            ucur.updateRow(row)

    return new_id_field


def count_rows(
        in_table,
        groupby_field=None,
        out_field=None,
        skip_nulls=False,
        null_value=None,
        inplace=True,
):
    """Counts rows in a table.

    Args:
        in_table (str, feature layer, table view or DataFrame): (Path to) the table for which to
            return a row count
        groupby_field (list, default=None): If given, the number of rows in the table with unique
            combinations of specified fields is returned.
        out_field (str, default=None): If given, the count is added to features in the table
        skip_nulls (bool, default=False): Control whether records using nulls are skipped.
        null_value (var or dict): Replaces null values from the input with a new value. Can be provided
            as a dict to set null replacement values for specific columns.
        inplace (bool, default=True): Only applies when `out_field` is provided.
            If True, `in_table` is updated in-place with a new field.

    Returns:
        Int, DataFrame, or None
    """
    if isinstance(in_table, pd.DataFrame):
        # Df operations
        if skip_nulls:
            _in_table_ = in_table.dropna()
        else:
            # TODO: handle dict to set defaults by column
            _in_table_ = in_table.fillna(null_value)

        if groupby_field is None:
            # No grouping required, just get the length
            result = len(_in_table_)
            if out_field is not None:
                if inplace:
                    in_table[out_field] = result
                    return
                else:
                    return in_table.assign(out_field=result)
            else:
                return result
        else:
            # Grouping
            result = _in_table_.groupby(groupby_field).size()
            if out_field is not None:
                result.name = out_field
                merge = in_table.merge(
                    result, how="left", left_on=groupby_field, right_index=True
                )
                if inplace:
                    result[out_field] = merge[out_field]
                    return
                else:
                    return in_table.assign(out_field=merge[out_field])
            else:
                return result.reset_index()
    else:
        # assume feature class/table operations
        # - dump fc to data frame
        oid_field = arcpy.Describe(in_table).OIDFieldName
        fields = ["OID@"]
        if groupby_field is not None:
            if isinstance(groupby_field, string_types):
                fields.append(groupby_field)
            else:
                fields += groupby_field
        df = featureclass_to_df(
            in_table, fields, skip_nulls=skip_nulls, null_val=null_value
        )
        df.rename(columns={"OID@": oid_field}, inplace=True)
        # Run this method on the data frame and handle output
        result = count_rows(
            df,
            groupby_field,
            out_field=out_field,
            inplace=True,
            skip_nulls=skip_nulls,
            null_value=null_value,
        )
        if out_field is None:
            return result
        else:
            result.drop(columns=groupby_field, inplace=True)
            extend_table_df(in_table, oid_field, result, oid_field)


# Network analysis helpers
def _listAccumulationAttributes(network, impedance_attribute):
    accumulation = []
    desc = arcpy.Describe(network)
    attributes = desc.attributes
    for attribute in attributes:
        if attribute.name == impedance_attribute:
            continue
        elif attribute.usageType == "Cost":
            accumulation.append(attribute.name)
    return accumulation


def _loadFacilitiesAndSolve(
        net_layer, facilities, name_field, net_loader, net_location_fields
):
    # Field mappings
    fmap_fields = ["Name"]
    fmap_vals = [name_field]
    if net_location_fields is not None and net_location_fields != "":
        fmap_fields += [
            "SourceOID",
            "SourceID",
            "PosAlong",
            "SideOfEdge",
            "SnapX",
            "SnapY",
            "Distance",
        ]
        fmap_vals += net_location_fields
    fmap = ";".join([f"{ff} {fv} #" for ff, fv in zip(fmap_fields, fmap_vals)])
    # Load facilities
    print("... ...loading facilities")
    arcpy.na.AddLocations(
        in_network_analysis_layer=net_layer,
        sub_layer="Facilities",
        in_table=facilities,
        field_mappings=fmap,
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
    # TODO: list which locations are invalid

    # Solve
    print("... ...generating service areas")
    arcpy.na.Solve(
        in_network_analysis_layer=net_layer,
        ignore_invalids="SKIP",
        terminate_on_solve_error="CONTINUE",
    )


def _exportSublayer(net_layer, sublayer, out_fc):
    # Export output
    print("... ...exporting output")
    sublayer_names = arcpy.na.GetNAClassNames(net_layer)
    result_lyr_name = sublayer_names[sublayer]
    try:
        result_sublayer = net_layer.listLayers(result_lyr_name)[0]
    except:
        result_sublayer = arcpy.mapping.ListLayers(net_layer, result_lyr_name)[0]

    if arcpy.Exists(out_fc):
        arcpy.Delete_management(out_fc)
    out_ws, out_name = os.path.split(out_fc)
    arcpy.FeatureClassToFeatureClass_conversion(result_sublayer, out_ws, out_name)


def _extendFromSublayer(out_fc, key_field, net_layer, sublayer, fields):
    print(f"... ...extending output from {sublayer}")
    sublayer_names = arcpy.na.GetNAClassNames(net_layer)
    extend_lyr_name = sublayer_names[sublayer]
    try:
        extend_sublayer = net_layer.listLayers(extend_lyr_name)[0]
    except:
        extend_sublayer = arcpy.mapping.ListLayers(net_layer, extend_lyr_name)[0]
    # Dump to array and extend
    extend_fields = ["OID@"] + fields
    extend_array = arcpy.da.TableToNumPyArray(extend_sublayer, extend_fields)
    arcpy.da.ExtendTable(out_fc, key_field, extend_array, "OID@")


def _loadLocations(
        net_layer, sublayer, points, name_field, net_loader, net_location_fields
):
    # Field mappings
    fmap_fields = ["Name"]
    fmap_vals = [name_field]
    if net_location_fields is not None:
        fmap_fields += [
            "SourceOID",
            "SourceID",
            "PosAlong",
            "SideOfEdge",
            "SnapX",
            "SnapY",
            "Distance",
        ]
        fmap_vals += net_location_fields
    fmap = ";".join([f"{ff} {fv} #" for ff, fv in zip(fmap_fields, fmap_vals)])
    # Load facilities
    print(f"... ...loading {sublayer}")
    arcpy.na.AddLocations(
        in_network_analysis_layer=net_layer,
        sub_layer=sublayer,
        in_table=points,
        field_mappings=fmap,
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
    # TODO: list which locations are invalid


def _solve(net_layer):
    # Solve
    print("... ...od matrix")
    s = arcpy.na.Solve(
        in_network_analysis_layer=net_layer,
        ignore_invalids="SKIP",
        terminate_on_solve_error="CONTINUE",
    )
    return s


def _rows_to_csv(in_table, fields, out_table, chunksize):
    header = True
    mode = "w"
    rows = []
    # Iterate over rows
    with arcpy.da.SearchCursor(in_table, fields) as c:
        for r in c:
            rows.append(r)
            if len(rows) == chunksize:
                df = pd.DataFrame(rows, columns=fields)
                df.to_csv(out_table, index=False, header=header, mode=mode)
                rows = []
                header = False
                mode = "a"
    # Save any stragglers
    df = pd.DataFrame(rows, columns=fields)
    df.to_csv(out_table, index=False, header=header, mode=mode)


# Network location functions
def gen_sa_lines(
        facilities,
        name_field,
        in_nd,
        imped_attr,
        cutoff,
        net_loader,
        out_fc,
        from_to="TRAVEL_FROM",
        overlap="OVERLAP",
        restrictions="",
        use_hierarchy=False,
        uturns="ALLOW_UTURNS",
        net_location_fields=None,
):
    """
    Creates service area lines around given facilities using a network dataset.

    Args:
        facilities (str or feature layer): The facilities for which service areas will be generated.
        name_field (str): The field in `facilities` that identifies each location.
        in_nd (str): Path to the network dataset
        imped_attr (str): The name of the impedance attribute to use when solving the network
            and generating service area lines
        cutoff (numeric): The search radius (in units of `imped_attr`) that
            defines the limits of the service area. If a list is given, the highest value defines the
            cutoff and all other values are used as break points, which are used to split output lines.
        net_loader (obj): NetLoader; Location loading preferences
        out_fc (str): Path to service area polygon feature class to be created
        from_to (str, default="TRAVEL_FROM"): If "TRAVEL_FROM", service areas reflect the reach
            of the network from `facilities`; if "TRAVEL_TO", service areas reflec the reach of the
            network to the facilities. If not applying one-way restrictions, the outcomes are effectively equivalent.
        overlap (str, deault="OVERLAP"): If "OVERLAP", individual sets of line features for each facility will be
            generated; if "SPLIT", line service area features are assigned to the nearest facility.
        restrictions (str, default=None): Specify restriction attributes (oneway, e.g.) to honor when generating
            service area lines. If the restrictions are parameterized, default parameter values are used in the solve.
        uturns (str, default="ALLOW_UTURNS"): Options are "ALLOW_UTURNS", "NO_UTURNS", "ALLOW_DEAD_ENDS_ONLY",
            "ALLOW_DEAD_ENDS_AND_INTERSECTIONS_ONLY"
        use_hierarchy (bool, default=False): If a hierarchy is defined for `in_nd`, it will be
            applied when solving the network if `use_hierarchy` is True; otherwise a simple, non-hierarchical
            solve is executed.
        net_location_fields (list, default=None): If provided, list the fields in the `facilities`
            attribute table that define newtork loading locations. Fields must be provided in the
            following order: SourceID, SourceOID, PosAlong, SideOfEdge, SnapX, SnapY, Distance.

    Returns:
        out_fc (str): Path

    See Also:
        NetLoader
    """
    if arcpy.CheckExtension("network") == "Available":
        arcpy.CheckOutExtension("network")
    else:
        raise arcpy.ExecuteError("Network Analyst Extension license is not available.")
    if use_hierarchy:
        hierarchy = "USE_HIERARCHY"
    else:
        hierarchy = "NO_HIERARCHY"
    # accumulation
    accum = _listAccumulationAttributes(in_nd, imped_attr)
    print("... ...LINES: create network problem")

    net_layer = arcpy.MakeServiceAreaLayer_na(
        # Setup
        in_network_dataset=in_nd,
        out_network_analysis_layer="__svc_lines__",
        impedance_attribute=imped_attr,
        travel_from_to=from_to,
        default_break_values=cutoff,
        # Polygon generation (disabled)
        polygon_type="NO_POLYS",
        merge=None,
        nesting_type=None,
        polygon_trim=None,
        poly_trim_value=None,
        # Line generation (enabled)
        line_type="TRUE_LINES",
        overlap=overlap,
        split="SPLIT",
        lines_source_fields="LINES_SOURCE_FIELDS",
        # Solve options
        excluded_source_name="",
        accumulate_attribute_name=accum,
        UTurn_policy=uturns,
        restriction_attribute_name=restrictions,
        hierarchy=hierarchy,
        time_of_day="",
    )
    net_layer_ = net_layer.getOutput(0)
    try:
        _loadFacilitiesAndSolve(
            "__svc_lines__", facilities, name_field, net_loader, net_location_fields
        )
        _exportSublayer(net_layer_, "SALines", out_fc)
        # Extend output with facility names
        _extendFromSublayer(out_fc, "FacilityID", net_layer_, "Facilities", ["Name"])
    except:
        raise
    finally:
        print("... ...deleting network problem")
        arcpy.Delete_management(net_layer)


def gen_sa_polys(
        facilities,
        name_field,
        in_nd,
        imped_attr,
        cutoff,
        net_loader,
        out_fc,
        from_to="TRAVEL_FROM",
        merge="NO_MERGE",
        nesting="RINGS",
        restrictions=None,
        use_hierarchy=False,
        uturns="ALLOW_UTURNS",
        net_location_fields=None,
):
    """
    Creates service area polygons around given facilities using a network dataset.

    Args:
        facilities (str or feature layer): The facilities for which service areas will be generated.
        name_field (str): The field in `facilities` that identifies each location.
        in_nd (str): Path to the network dataset
        imped_attr (str): The name of the impedance attribute to use when solving the network
            and generating service area lines
        cutoff (numeric): The search radius (in units of `imped_attr`) that
            defines the limits of the service area. If a list is given, the highest value defines the
            cutoff and all other values are used as break points, which are used to split output lines.
        net_loader (obj): NetLoader; Location loading preferences
        out_fc (str): Path to service area polygon feature class to be created
        from_to (str, default="TRAVEL_FROM"): If "TRAVEL_FROM", service areas reflect the reach
            of the network from `facilities`; if "TRAVEL_TO", service areas reflec the reach of the
            network to the facilities. If not applying one-way restrictions, the outcomes are effectively equivalent.
        restrictions (str, default=None): Specify restriction attributes (oneway, e.g.) to honor when generating
            service area lines. If the restrictions are parameterized, default parameter values are used in the solve.
        uturns (str, default="ALLOW_UTURNS"): Options are "ALLOW_UTURNS", "NO_UTURNS", "ALLOW_DEAD_ENDS_ONLY",
            "ALLOW_DEAD_ENDS_AND_INTERSECTIONS_ONLY"
        use_hierarchy (bool, default=False): If a hierarchy is defined for `in_nd`, it will be
            applied when solving the network if `use_hierarchy` is True; otherwise a simple, non-hierarchical
            solve is executed.
        net_location_fields (list, default=None): If provided, list the fields in the `facilities`
            attribute table that define newtork loading locations. Fields must be provided in the
            following order: SourceID, SourceOID, PosAlong, SideOfEdge, SnapX, SnapY, Distance.

    Returns:
        out_fc (str): Path

    See Also:
        NetLoader
    """
    if arcpy.CheckExtension("network") == "Available":
        arcpy.CheckOutExtension("network")
    else:
        raise arcpy.ExecuteError("Network Analyst Extension license is not available.")
    if use_hierarchy:
        hierarchy = "USE_HIERARCHY"
    else:
        hierarchy = "NO_HIERARCHY"
    print("... ...POLYGONS: create network problem")
    net_layer = arcpy.MakeServiceAreaLayer_na(
        # Setup
        in_network_dataset=in_nd,
        out_network_analysis_layer="__svc_areas__",
        impedance_attribute=imped_attr,
        travel_from_to=from_to,
        default_break_values=cutoff,
        # Polygon generation (disabled)
        polygon_type="DETAILED_POLYS",
        merge=merge,
        nesting_type="RINGS",
        polygon_trim="TRIM_POLYS",
        poly_trim_value="100 Meters",
        # Line generation (enabled)
        line_type="NO_LINES",
        overlap="OVERLAP",
        split="SPLIT",
        lines_source_fields="LINES_SOURCE_FIELDS",
        # Solve options
        excluded_source_name=None,
        accumulate_attribute_name=None,
        UTurn_policy=uturns,
        restriction_attribute_name=restrictions,
        hierarchy=hierarchy,
        time_of_day=None,
    )
    net_layer_ = net_layer.getOutput(0)
    try:
        _loadFacilitiesAndSolve(
            "__svc_areas__", facilities, name_field, net_loader, net_location_fields
        )
        _exportSublayer(net_layer_, "SAPolygons", out_fc)
    except:
        raise
    finally:
        print("... ...deleting network problem")
        arcpy.Delete_management(net_layer)


def _sanitize_column_names(
        geo,
        remove_special_char=True,
        rename_duplicates=True,
        inplace=False,
        use_snake_case=True,
):
    """
    Implementation for pd.DataFrame.spatial.sanitize_column_names()
    """
    original_col_names = list(geo._data.columns)

    # convert to string
    new_col_names = [str(x) for x in original_col_names]

    # use snake case
    if use_snake_case:
        for ind, val in enumerate(new_col_names):
            # skip reserved cols
            if val == geo.name:
                continue
            # replace Pascal and camel case using RE
            s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", val)
            name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
            # remove leading spaces
            name = name.lstrip(" ")
            # replace spaces with _
            name = name.replace(" ", "_")
            # clean up too many _
            name = re.sub("_+", "_", name)
            new_col_names[ind] = name

    # remove special characters
    if remove_special_char:
        for ind, val in enumerate(new_col_names):
            name = "".join(i for i in val if i.isalnum() or "_" in i)

            # remove numeral prefixes
            for ind2, element in enumerate(name):
                if element.isdigit():
                    continue
                else:
                    name = name[ind2:]
                    break
            new_col_names[ind] = name

    # fill empty column names
    for ind, val in enumerate(new_col_names):
        if val == "":
            new_col_names[ind] = "column"

    # rename duplicates
    if rename_duplicates:
        for ind, val in enumerate(new_col_names):
            if val == geo.name:
                pass
            if new_col_names.count(val) > 1:
                counter = 1
                new_name = val + str(counter)  # adds a integer suffix to column name
                while new_col_names.count(new_name) > 0:
                    counter += 1
                    new_name = val + str(
                        counter
                    )  # if a column with the suffix exists, increment suffix
                new_col_names[ind] = new_name

    # if inplace
    if inplace:
        geo._data.columns = new_col_names
    else:
        # return a new dataframe
        df = geo._data.copy()
        df.columns = new_col_names
        return df
    return True


def _list_table_paths(gdb, criteria="*"):
    """
    Internal function, returns a list of all tables within a geodatabase
    
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
    return [make_path(gdb, table) for table in tables]


def _list_fc_paths(gdb, fds_criteria="*", fc_criteria="*"):
    """
    Internal function, returns a list of all feature classes within a geodatabase
    
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
            paths += [make_path(gdb, fd, fc) for fc in fcs]
    arcpy.env.workspace = old_ws
    return paths


def _make_access_col_specs(activities, time_breaks, mode, include_average=True):
    """
    Helper function to generate access column specs

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
    Generates a table with accessibility sores that is long on activities and time breaks.

    Args:
        int_fc (str): path to an intersection between summary areas and travel zone features (TAZ, MAZ).
            Field names are expected to be reliably formatted such that columns reflecting activities
            and time breaks can be found and melted.
        id_field (str): summary area ID field
        activities (list): list of activities used to create different accessibility scores
        time_breaks (list) list of time breaks used to report access by time bin
        mode (str): scores are expected in separate tables by mode
        domain (object, defeault=None): see `DomainColumn`

    Returns:
        pd.DataFrame: a table of access scores by summary area, long on activities and time breaks
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
    df = featureclass_to_df(in_fc=int_fc, keep_fields=all_fields, null_val=0.0)
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
    Helper function to calculate the difference between this_table and base_table
        ie... this_table minus base_table
    
    Args:
        this_table (str): path to a snapshot table
        base_table (str): path to a previous year's snapshot table
        idx_cols (list): column names used to generate a common index
        fields (list): if provided, a list of fields to calculate the difference on;
            Default: "*" indicates all fields
        **kwargs: keyword arguments for `featureclass_to_df`

    Returns:
        pandas.Dataframe: df like `this_table` but containing difference values
    """
    # Fetch data frames
    this_df = featureclass_to_df(in_fc=this_table, keep_fields=fields, **kwargs)
    base_df = featureclass_to_df(in_fc=base_table, keep_fields=fields, **kwargs)
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


if __name__ == "__main__":
    print("nothing set to run")
