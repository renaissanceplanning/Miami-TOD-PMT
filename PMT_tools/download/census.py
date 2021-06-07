"""
The `census` modules provides several helper methods to acquire and aggregate LODES data
for use in the TOC tool.
"""
import os
import re
from datetime import datetime

import numpy as np
import pandas as pd
from six import string_types

from helper import download_file_from_url
from PMT_tools.config.download_config import (
    LODES_URL,
    LODES_YEARS,
    LODES_FILE_TYPES,
    LODES_STATES,
    LODES_WORKFORCE_SEGMENTS,
    LODES_PART,
    LODES_JOB_TYPES,
    LODES_AGG_GEOS,
)
from PMT_tools.utils import make_path, validate_directory, check_overwrite_path

current_year = datetime.now().year

__all__ = ["LodesFileTypeError", "validate_string_inputs", "validate_aggregate_geo_inputs", "validate_year",
           "validate_year_input", "validate_lodes_download", "aggregate_lodes_data", "download_aggregate_lodes"]


class LodesFileTypeError(Exception):
    pass


# functions
def validate_string_inputs(value, valid_inputs):
    """
    Helper function to validate that a given value is a string
    and is found in the list of valid inputs
    
    Args:
        value (var): a value to validate. If not a string type, raises `ValueError`
        valid_inputs (list or str): one or more valid string values. If `value`
            is not found among the valid values, raises `ValueError`
    
    Returns:
        bool: flag indicating if `value` is valid
    """
    try:
        if isinstance(value, list):
            _val_ = []
            for val in value:
                if isinstance(val, string_types) and isinstance(valid_inputs, list):
                    if val.lower() in [x.lower() for x in valid_inputs]:
                        _val_.append(True)
                    else:
                        _val_.append(False)
                    if all(_val_):
                        return True
        if isinstance(value, string_types) and isinstance(valid_inputs, list):
            if value.lower() in [x.lower() for x in valid_inputs]:
                return True
        else:
            raise ValueError #TODO: TypeError?
    except ValueError:
        print("either the value supplied is not a string or valid_input was not a list")


def validate_aggregate_geo_inputs(values, valid):
    """
    Helper functions to validate that a given geo is a string or list and is found
    among a set of valid values.
    
    Args:
        values (str or list): one or more (string) values to validate. If not a string type 
            or members are not string types, raises `ValueError`
        valid (list): a list valid string values. If `value` or any member of `value`
            is not found among the valid values, raises `ValueError`
    
    Returns:
        bool: flag indicating if `values` is valid
    """
    try:
        if values:
            # ensure you have a string or list
            if isinstance(values, str):
                values = [values]
            elif isinstance(values, list):
                values = values
            else:
                raise ValueError #TODO: TypeError?
            # check values are part of valids
            if isinstance(valid, list):
                return all(value in valid for value in values)
            else:
                raise ValueError #TODO: TypeError?
        else:
            return False
    except ValueError:
        print("either the geo is not a string/list or not part of the valid set")


def validate_year(year):
    """Confirm year is correct datatype and part of expected years available"""
    if isinstance(year, int):
        if year in LODES_YEARS:
            year = str(year)
            return 1 <= len(year) <= 4


def validate_year_input(year, state):
    """Validate the provided year is in the possible set for a given state"""
    try:
        if validate_year(year):
            if any(
                [
                    (year in np.arange(2002, 2017) and state == "ak"),
                    (year in np.arange(2004, 2019) and state == "az"),
                    (year in np.arange(2003, 2019) and state == "ar"),
                    (year in np.arange(2010, 2019) and state == "dc"),
                    (year in np.arange(2011, 2019) and state == "ma"),
                    (year in np.arange(2004, 2019) and state == "ms"),
                    (year in np.arange(2003, 2019) and state == "nh"),
                    (
                        year in np.arange(2002, 2019)
                        and state not in ["ak", "az", "ar", "dc", "ma", "ms", "nh"]
                    ),
                ]
            ):
                return True
            else:
                raise ValueError
        else:
            raise ValueError
    except ValueError:
        print(f"invalid 'year' or 'year/state' combination")


def validate_lodes_download(file_type, state, segment, part, job_type, year, agg_geos):
    """
    Validates the download inputs to the `dl_lodes` method, combining other validation methods
    (`file_type`, `state`, `segment`, `part`, `job_type`, and `year`)

    Args:
        file_type (str): The LODES file type to download ("OD", "RAC", or "WAC")
        state (str): The two-character postal abbreviation for the state
        segment (str): Segment of the workforce, can have the values of
            [“S000”, “SA01”, “SA02”, “SA03”,  “SE01”, “SE02”, “SE03”, “SI01”, “SI02”, “SI03”, ""]
        part (str): Part of the state file, can have a value of either “main” or “aux”. Complimentary parts of
            the state file, the main part includes jobs with both workplace and residence in the state
            and the aux part includes jobs with the workplace in the state and the residence outside of the state.
        job_type (str): LODES job types (“JT00” for All Jobs, “JT01” for Primary Jobs, “JT02” for
            All Private Jobs, “JT03” for Private Primary Jobs, “JT04” for All Federal Jobs, or “JT05”
            for Federal Primary Jobs).
        year (str): year of LODES data to download
        agg_geos (str): census geographies to aggregate lodes data to

    Returns:
        bool: flag indicating if parameters are valid

    See Also:
        download_aggregate_lodes
    """
    string_list = [file_type, state, segment, part, job_type, agg_geos]
    valids_list = [
        LODES_FILE_TYPES,
        LODES_STATES,
        LODES_WORKFORCE_SEGMENTS,
        LODES_PART,
        LODES_JOB_TYPES,
        LODES_AGG_GEOS
    ]
    st = state.lower()
    try:
        # validate year
        if validate_year_input(year=year, state=st):
            # validate string inputs
            for val, valid_list in zip(string_list, valids_list):
                if validate_string_inputs(value=val, valid_inputs=valid_list):
                    continue
                else:
                    raise ValueError
            return True
    except ValueError:
        print(
            f"one of the inputs was not the correct datatype or part of the valid set"
        )
        return False


def aggregate_lodes_data(geo_crosswalk_path, lodes_path, file_type, agg_geo):
    """
    Aggregate LODES data to desired geographic levels, and save the created
    files [to .csv.gz]

    Args:
        geo_crosswalk_path (str): path to geographic crosswalk CSV
        lodes_path (str): file path to gzipped lodes data file
        file_type (str): shorthand for type of jobs summarization
        agg_geo (str): shorthand for the geographic scale to aggregate data to

    Returns:
        pd.Dataframe; data aggregated up to provided aggregate geography
    """
    # TODO: add additional function to handle OD joins, this only works for WAC, RAC
    # so there's no message for mixed dtypes
    xwalk_dtype_dict = {
        "tabblk2010": "str",
        "st": "str",
        "cty": "str",
        "trct": "str",
        "bgrp": "str",
        "cbsa": "str",
        "zcta": "str",
    }
    try:
        crosswalk = pd.read_csv(
            geo_crosswalk_path,
            compression="gzip",
            dtype=xwalk_dtype_dict,
            low_memory=False,
        )

        if file_type == "od":
            raise LodesFileTypeError
            # dtype_change = {"w_geocode": "str", "h_geocode": "str"}
        elif file_type == "rac":
            dtype_change = {"h_geocode": "str"}
        else:
            dtype_change = {"w_geocode": str}
        lodes = pd.read_csv(
            lodes_path, compression="gzip", dtype=dtype_change, low_memory=False
        )  # so there's no

        # isolote aggregate col and isolate join cols
        lodes_cols = lodes.columns.tolist()
        agg_cols = [col for col in lodes_cols if bool(re.search("[0-9]", col)) is True]
        agg_dict = {col: "sum" for col in agg_cols}
        lodes_join = [
            col for col in lodes_cols if bool(re.search("geocode", col)) is True
        ]

        # join lodes to crosswalk
        crosswalk_cols = crosswalk.columns.tolist()
        crosswalk_join = [
            c for c in crosswalk_cols if bool(re.search("tabblk2010", c)) is True
        ]
        merge_df = pd.merge(
            lodes, crosswalk, left_on=lodes_join, right_on=crosswalk_join, how="left"
        )

        # aggregate job
        keep_cols = [agg_geo] + agg_cols
        agged = merge_df[keep_cols].groupby(agg_geo).agg(agg_dict).reset_index()
        return agged
    except LodesFileTypeError:
        print("This function doesnt currently handle 'od' data")


def download_aggregate_lodes(output_dir,
                             file_type, state, segment, part, job_type, year,
                             agg_geog=None, overwrite=False, ):
    """
    Helper function to fetch lodes data and aggregate to another census geography if one is provided

    Args:
        output_dir (str): path to location downloaded files should end up
        file_type (str): one of three LODES groupings ['od', 'rac', 'wac']

            - OD: Origin-Destination data, totals are associated with both a home Census Block and a work Census Block

            - RAC: Residence Area Characteristic data, jobs are totaled by home Census Block

            - WAC: Workplace Area Characteristic data, jobs are totaled by work Census Block

        state (str): The two-character postal abbreviation for the state
        segment (str): Segment of the workforce, can have the values of
            [“S000”, “SA01”, “SA02”, “SA03”,  “SE01”, “SE02”, “SE03”, “SI01”, “SI02”, “SI03”, ""]
        part (str): Part of the state file, can have a value of either “main” or “aux”. Complimentary parts of
            the state file, the main part includes jobs with both workplace and residence in the state
            and the aux part includes jobs with the workplace in the state and the residence outside of the state.
        job_type (str): LODES job types (“JT00” for All Jobs, “JT01” for Primary Jobs, “JT02” for
            All Private Jobs, “JT03” for Private Primary Jobs, “JT04” for All Federal Jobs, or “JT05”
            for Federal Primary Jobs).
        year (int): year of LODES data to download
        agg_geog (str): census geographies to aggregate lodes data to
        overwrite (bool): if set to True, delete the existing copy of the LODES data
    
    Returns:
        None: writes csv tables of aggregated lodes data in `output_dir`
    """
    st = state.lower()
    try:
        out_dir = validate_directory(directory=output_dir)
        if validate_lodes_download(
            file_type, state, segment, part, job_type, year, agg_geog
        ):
            if file_type == "od":
                # kept for now as it will still download but not aggregate OD
                lodes_fname = f"{st}_{file_type}_{part}_{job_type}_{str(year)}.csv.gz"
            else:
                lodes_fname = (
                    f"{st}_{file_type}_{segment}_{job_type}_{str(year)}.csv.gz"
                )
            lodes_download_url = f"{LODES_URL}/{st}/{file_type}/{lodes_fname}"
            lodes_out = make_path(out_dir, lodes_fname)
            lodes_out = lodes_out.replace(".csv.gz", "_blk.csv.gz")
            print(f"...downloading {lodes_fname} to {lodes_out}")
            check_overwrite_path(output=lodes_out, overwrite=overwrite)
            download_file_from_url(url=lodes_download_url, save_path=lodes_out)
        else:
            lodes_out = ""

        if agg_geog and lodes_out != "":
            if validate_aggregate_geo_inputs(values=agg_geog, valid=LODES_AGG_GEOS):
                if isinstance(agg_geog, string_types):
                    agg_geog = [agg_geog]
                for geog in agg_geog:
                    cross_fname = f"{state}_xwalk.csv.gz"
                    cross_out = make_path(out_dir, cross_fname)
                    agged_out = lodes_out.replace("_blk.csv.gz", f"_{geog}.csv.gz")
                    crosswalk_url = f"{LODES_URL}/{state}/{state}_xwalk.csv.gz"
                    if not os.path.exists(cross_out):
                        print(f"...downloading {cross_fname} to {cross_out}")
                        download_file_from_url(
                            url=crosswalk_url, save_path=cross_out
                        )
                    print(f"...aggregating block group level data to {geog}")

                    agged = aggregate_lodes_data(
                        geo_crosswalk_path=cross_out,
                        lodes_path=lodes_out,
                        file_type=file_type,
                        agg_geo=geog,
                    )
                    check_overwrite_path(output=agged_out, overwrite=overwrite)
                    agged.to_csv(agged_out, compression="gzip", index=False)

        else:
            print("No aggregation requested or there is no LODES data for this request")
    except:
        print("something failed")

