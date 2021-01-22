from download_config import (LODES_URL, LODES_YEARS, LODES_FILE_TYPES, LODES_STATES,
                             LODES_WORKFORCE_SEGMENTS, LODES_PART,
                             LODES_JOB_TYPES, LODES_AGG_GEOS)

from download_helper import download_file_from_url, validate_directory

import numpy as np
import pandas as pd

import os
import re
from datetime import datetime

from collections.abc import Iterable
from six import string_types

from PMT_tools.PMT import makePath

current_year = datetime.now().year


class LodesFileTypeError(Exception):
    pass


# functions
def validate_string_inputs(value, valid_inputs):
    try:
        if isinstance(value, string_types) and isinstance(valid_inputs, list):
            if value.lower() in [x.lower() for x in valid_inputs]:
                return True
        else:
            raise ValueError
    except ValueError:
        print("either the value supplied is not a string or valid_input was not a list")


def validate_aggregate_geo_inputs(values, valid):
    try:
        if values:
            # ensure you have a string or list
            if isinstance(values, str):
                values = [values]
            elif isinstance(values, list):
                values = values
            else:
                raise ValueError
            # check values are part of valids
            if isinstance(valid, list):
                return all(value in valid for value in values)
            else:
                raise ValueError
        else:
            return False
    except ValueError:
        print("either the geo is not a string/list or not part of the valid set")


def validate_year(year):
    if isinstance(year, int):
        if year in LODES_YEARS:
            year = str(year)
            return 1 <= len(year) <= 4


def validate_year_input(year, state):
    try:
        if validate_year(year):
            if any(
                    [(year in np.arange(2002, 2017) and state == "ak"),
                     (year in np.arange(2004, 2019) and state == "az"),
                     (year in np.arange(2003, 2019) and state == "ar"),
                     (year in np.arange(2010, 2019) and state == "dc"),
                     (year in np.arange(2011, 2019) and state == "ma"),
                     (year in np.arange(2004, 2019) and state == "ms"),
                     (year in np.arange(2003, 2019) and state == "nh"),
                     (year in np.arange(2002, 2019) and state not in
                      ["ak", "az", "ar", "dc", "ma", "ms", "nh"])]):
                return True
            else:
                raise ValueError
    except ValueError:
        print(f"invalid 'year' or 'year/state' combination")


def validate_lodes_download(file_type, state, segment, part, job_type, year, agg_geos):
    """
    validates the download inputs to `dl_lodes`
    (`file_type`, `state`, `segment`, `part`, `job_type`, and `year`)
    """
    string_list = [file_type, state, segment, part, job_type]
    valids_list = [LODES_FILE_TYPES, LODES_STATES, LODES_WORKFORCE_SEGMENTS,
                   LODES_PART, LODES_JOB_TYPES]
    try:
        # validate year
        if validate_year_input(year=year, state=state):
            # validate string inputs
            for val, valid_list in zip(string_list, valids_list):
                if validate_string_inputs(value=val, valid_inputs=valid_list):
                    continue
                else:
                    raise ValueError
            return True
    except ValueError:
        print(f"one of the inputs was not the correct datatype or part of the valid set")
        return False


def aggregate_lodes_data(geo_crosswalk_path, lodes_path, file_type, agg_geo):
    """
    aggregate LODES data to desired geographic levels, and save the created
    files [to .csv.gz]
    Parameters
    ----------
    geo_crosswalk_path - String; path to geographic crosswalk CSV
    lodes_path - String; file path to gzipped lodes data file
    file_type - String; shorthand for type of jobs summarization
    agg_geo - String; shorthand for the geographic scale to aggregate data to

    Returns
    -------
    string path to the aggregated data
    """
    # TODO: add additional function to handle OD joins, this only works for WAC, RAC
    # so there's no message for mixed dtypes
    xwalk_dtype_dict = {"tabblk2010": "str", "st": "str", "cty": "str",
                        "trct": "str", "bgrp": "str", "cbsa": "str", "zcta": "str"}
    save_path = lodes_path.replace("_blk.csv.gz", f"_{agg_geo}.csv.gz")
    try:
        crosswalk = pd.read_csv(geo_crosswalk_path, compression="gzip",
                                dtype=xwalk_dtype_dict, low_memory=False)

        if file_type == "od":
            raise LodesFileTypeError
            # dtype_change = {"w_geocode": "str", "h_geocode": "str"}
        elif file_type == "rac":
            dtype_change = {"h_geocode": "str"}
        else:
            dtype_change = {"w_geocode": str}
        lodes = pd.read_csv(lodes_path, compression="gzip",
                            dtype=dtype_change, low_memory=False)  # so there's no

        # isolote aggregate col and isolate join cols
        lodes_cols = lodes.columns.tolist()
        agg_cols = [col for col in lodes_cols if bool(re.search("[0-9]", col)) is True]
        agg_dict = {col: "sum" for col in agg_cols}
        lodes_join = [col for col in lodes_cols if bool(re.search("geocode", col)) is True]

        # join lodes to crosswalk
        crosswalk_cols = crosswalk.columns.tolist()
        crosswalk_join = [c for c in crosswalk_cols if bool(re.search("tabblk2010", c)) is True]
        merge_df = pd.merge(lodes, crosswalk, left_on=lodes_join, right_on=crosswalk_join, how="left")

        # aggregate job
        keep_cols = [agg_geo] + agg_cols
        agged = merge_df[keep_cols].groupby(agg_geo).agg(agg_dict).reset_index()
        agged.to_csv(save_path, compression="gzip", index=False)
        return save_path
    except LodesFileTypeError:
        print("This function doesnt currently handle 'od' data")


def download_aggregate_lodes(output_directory, file_type, state,
                             segment, part, job_type, year, agg_geog=None):
    """
    - validate directory
    - if validate LODES request:
        - download LODES zip
        lodes_path
    - if aggregate:
        - if validate_agg_geo:
            - download LODES crosswalk
            - aggregate data
    Returns
    -------

    """
    try:
        out_dir = validate_directory(directory=output_directory)
        if validate_lodes_download(file_type, state, segment, part, job_type, year, agg_geog):
            if file_type == "od":
                # kept for now as it will still download but not aggregate OD
                lodes_fname = f"{state}_{file_type}_{part}_{job_type}_{str(year)}.csv.gz"
            else:
                lodes_fname = f"{state}_{file_type}_{segment}_{job_type}_{str(year)}.csv.gz"
            lodes_download_url = f"{LODES_URL}/{state}/{file_type}/{lodes_fname}"
            lodes_out_path = makePath(out_dir, lodes_fname)
            lodes_out_path = lodes_out_path.replace(".csv.gz", "_blk.csv.gz")
            download_file_from_url(url=lodes_download_url, save_path=lodes_out_path)

        if agg_geog:
            if validate_aggregate_geo_inputs(values=agg_geog, valid=LODES_AGG_GEOS):
                if isinstance(agg_geog, string_types):
                    agg_geog = [agg_geog]
                for geog in agg_geog:
                    cross_fname = f"{state}_xwalk.csv.gz"
                    cross_out_path = makePath(out_dir, cross_fname)
                    crosswalk_url = f"{LODES_URL}/{state}/{state}_xwalk.csv.gz"
                    download_file_from_url(url=crosswalk_url, save_path=cross_out_path)
                    aggregate_lodes_data(geo_crosswalk_path=cross_out_path,
                                         lodes_path=lodes_out_path,
                                         file_type=file_type,
                                         agg_geo=geog)
        else:
            print("No aggregation requested")
    except:
        print("something failed")


if __name__ == "__main__":
    # Inputs
    file_type = "wac"
    state = "fl"
    part = ""
    segment = "S000"
    job_type = "JT00"
    year = 2017
    save_directory = "C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT\Data\Temp\LODES"
    aggregate_geo = "zcta"

    download_aggregate_lodes(output_directory=save_directory, file_type=file_type,
         state=state, segment=segment, part=part, job_type=job_type,
         year=year, agg_geog=aggregate_geo)

# hold over code for OD data
# # Next, join the xwalk to the lodes data. If it's OD, we have two join
# # fields, so it becomes a bit more complicated
# if file_type == "od":
#     w_crosswalk = crosswalk.add_prefix(prefix="w_")
#     h_crosswalk = crosswalk.add_prefix(prefix="h_")
#     crosswalk = pd.concat([w_crosswalk, h_crosswalk], axis=1)
#     crosswalk_cols = crosswalk.columns.tolist()
