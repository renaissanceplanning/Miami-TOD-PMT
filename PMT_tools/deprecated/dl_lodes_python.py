# -*- coding: utf-8 -*-
"""
Created on Mon Dec 28 09:37:34 2020

@author: Aaron Weinstock
"""

# %% Imports

import numpy as np
import os
from collections.abc import Iterable
from six import string_types
from urllib import request
import requests
from requests.exceptions import RequestException
import pandas as pd
import re

LODES_URL = "https://lehd.ces.census.gov/data/lodes/LODES7"
STATES = ["al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl",
          "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la",
          "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne",
          "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
          "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt",
          "va", "wa", "wv", "wi", "wy", "dc"]

WORKFORCE_SEGMENTS = ["S000", "SA01", "SA02", "SA03", "SE01", "SE02", "SE03", "SI01", "SI02", "SI03", ""]
JOB_TYPES = ["JT00", "JT01", "JT02", "JT03", "JT04", "JT05"]


# %% Functions
# download Imperviousness data
def download_url(url, save_path):
    if os.path.isdir(save_path):
        filename = get_filename_from_header(url)
        save_path = os.path.join(save_path, filename)
    try:
        request.urlretrieve(url, save_path)
    except:
        with request.urlopen(url) as download:
            with open(save_path, 'wb') as out_file:
                out_file.write(download.read())


def get_filename_from_header(url):
    """
    grabs a filename provided in the url object header
    Parameters
    ----------
    url - string, url path to file on server

    Returns
    -------
    filename as string
    """
    try:
        with requests.get(url) as r:
            if "Content-Disposition" in r.headers.keys():
                return re.findall("filename=(.+)", r.headers["Content-Disposition"])[0]
            else:
                return url.split("/")[-1]
    except RequestException as e:
        print(e)


def validate_lodes_download(file_type, state, segment, part, job_type, year):
    """
    validates the download inputs to `dl_lodes` (`file`, `ST`, `SEG`, `PART`,
    `TYPE`, and `YEAR`)

    Parameters
    ----------
    file_type : str, LODES file type, see `dl_lodes`
    state : str, state, see `dl_lodes`
    segment : str, segment of the workforce, see `dl_lodes`
    part : str, part of state file, see `dl_lodes`
    job_type : str, job type, see `dl_lodes`
    year : int, year, see `dl_lodes`

    Returns
    -------
    dict of formatted download inputs if all inputs are valid for `dl_lodes`,
    relevant error messages if any of the inputs are invalid.
    """

    # Conversion and checking
    # file
    global file_dtype, part_dtype, seg_dtype
    if isinstance(file_type, str):
        file_dtype = True
        if not file_type.lower() in ["od", "rac", "wac"]:
            file_message = "invalid 'file'; see function docs"
        else:
            file_message = ""
    else:
        file_message = "'file' is not a string"

    # state
    if isinstance(state, str):
        if not state.lower() in STATES:
            state_message = "invalid 'state'; see function docs"
        else:
            state_message = ""
    else:
        state_message = "'state' is not a string"

    # PART
    if isinstance(part, str):
        part_dtype = True
        if not part.lower() in ["main", "aux", ""]:
            part_message = "invalid 'part'; see function docs"
        else:
            part_message = ""
    else:
        part_message = "'part' is not a string"

    # SEG
    if isinstance(segment, str):
        seg_dtype = True
        if not segment.upper() in WORKFORCE_SEGMENTS:
            segment_message = "invalid 'segment'; see function docs"
        else:
            segment_message = ""
    else:
        segment_message = "'segment' is not a string"

    # TYPE
    if isinstance(job_type, str):
        if not job_type.upper() in JOB_TYPES:
            type_message = "invalid 'type'; see function docs"
        else:
            type_message = ""
    else:
        type_message = "'type' is not a string"

    # YEAR
    if isinstance(year, int):
        if not any(
                [(year in np.arange(2002, 2017) and state == "ak"), (year in np.arange(2004, 2019) and state == "az"),
                 (year in np.arange(2003, 2019) and state == "ar"), (year in np.arange(2010, 2019) and state == "dc"),
                 (year in np.arange(2011, 2019) and state == "ma"), (year in np.arange(2004, 2019) and state == "ms"),
                 (year in np.arange(2003, 2019) and state == "nh"),
                 (year in np.arange(2002, 2019) and state not in ["ak", "az", "ar", "dc", "ma", "ms", "nh"])]):
            year_message = "invalid 'year' or 'year/state' combination; see function docs"
        else:
            year_message = ""
    else:
        year_message = "'year' is not an integer"

    # PART and SEG
    if part_dtype and seg_dtype:
        if part == "" and segment == "":
            ps_message = "'part' and 'segment' cannot both be ''; see function docs"
        else:
            ps_message = ""
    else:
        ps_message = ""

    # PART-OD
    if part_dtype and file_dtype:
        if part == "" and file_type == "od":
            pod_message = "'part' cannot be '' if 'file' is 'od'; see function docs"
        else:
            pod_message = ""
    else:
        pod_message = ""

    # SEG-RAC/WAC
    if seg_dtype and file_dtype:
        if segment == "" and file_type in ["rac", "wac"]:
            srw_message = "'segment' cannot be '' if 'file' is 'rac' or 'wac'; see function docs"
        else:
            srw_message = ""
    else:
        srw_message = ""

    # Organize the error messages
    messages = [file_message, state_message, part_message, segment_message, type_message, year_message,
                ps_message, pod_message, srw_message]
    errors = [' '.join(["-->", m]) for m in messages if m != ""]

    # Return the error messages if any
    if not errors:
        return [file_type, state, segment, part,job_type,year]
    else:
        errors = '\n'.join(errors)
        return errors


def validate_lodes_save(directory):
    """
    validates/creates [if neccessary] the save directory for `dl_lodes`

    Parameters
    ----------
    directory : str, save directory, see `dl_LODES`

    Returns
    -------
    dict of `save_directory` if the save directory exists/is creatable,
    relevant error messages if it does/is not.
    """

    # Check if it exists. If it doesn't try to create it. If it succeeds,
    # os.mkdir returns a None, so we in turn return that None; otherwise, we
    # return an error message
    if os.path.isdir(directory):
        return {"save_directory": directory}
    else:
        try:
            os.mkdir(directory)
            return directory
        except:
            error = "--> 'save_directory' does not exist and cannot be created"
            return error


def validate_lodes_aggregate(aggregate_geo, geography):
    """
    validates the aggregation input for `dl_lodes`

    Parameters
    ----------
    aggregate_geo : str, or iterable of str; geographic levels to which to aggregate, see `dl_lodes`

    Returns
    -------
    dict of formatted aggregation inputs if all inputs are valid for
    `dl_lodes`, relevant error messages if any of the inputs are invalid.
    """

    # If 'aggregate_geo' is None, then we only need to check 'geography'. If
    # it's not, we need to make sure it has the proper structure/inputs. Also,
    # in this case, we wouldn't need the geography parameter (it is downloaded
    # automatically in aggregation), so we don't have to check or return it

    if aggregate_geo is None:
        if type(geography) is bool:
            return ({"aggregate_geo": aggregate_geo,
                     "geography": geography})
        else:
            error = "--> 'geography' is not a boolean"
            return error
    else:
        # Check if iterable, if not make it iterable
        if not isinstance(aggregate_geo, Iterable) or isinstance(aggregate_geo, string_types):
            aggregate_geo = [aggregate_geo]

        # Validate the inputs
        idx = np.arange(len(aggregate_geo))
        valid_aggs = ["st", "cty", "trct", "bgrp", "cbsa", "zcta"]
        messages = []
        for i in idx:
            opt = aggregate_geo[i]
            if type(opt) is str:
                if opt in valid_aggs:
                    message = ""
                else:
                    message = ' '.join(["'aggregate_geo' item", str(i), "is invalid; see function docs"])
            else:
                message = ' '.join(["'aggregate_geo' item", str(i), "is not a string"])
            messages.append(message)

        # Organize the error messages
        errors = [' '.join(["-->", m]) for m in messages if m != ""]

        # Return the error messages if any
        if not errors:
            return aggregate_geo
        else:
            errors = '\n'.join(errors)
            return errors


def validate_dl_lodes(file_type, state, segment, part, job_type, year, out_dir,
                      aggregate_geo, geography):
    """
    validate all inputs for `dl_lodes`; this is a wrapper function for all
    sub-validations functions (of which there are 3)

    Parameters
    ----------
    file_type : str; LODES file type, see `dl_lodes`
    state : str; state, see `dl_lodes`
    segment : str; segment of the workforce, see `dl_lodes`
    part : str; part of state file, see `dl_lodes`
    job_type : str; job type, see `dl_lodes`
    year : int; year, see `dl_lodes`
    out_dir : str; save directory, see `dl_LODES`
    aggregate_geo : str, or iterable of str, or None; geographic levels to which to aggregate, see `dl_lodes`
    geography : bool; should the geography crosswalk be downloaded if no aggregation, see`dl_lodes`

    Returns
    -------
    dict of formatted inputs if all inputs are valid for `dl_lodes`, relevant
    error messages if any of the inputs are invalid
    """

    # Call all the validation functions
    download = validate_lodes_download(file_type=file_type, state=state, segment=segment, part=part,
                                       job_type=job_type, year=year)
    save = validate_lodes_save(directory=out_dir)
    aggregate = validate_lodes_aggregate(aggregate_geo=aggregate_geo, geography=geography)

    # Error organization
    messages = [download, save, aggregate]
    errors = [m for m in messages if type(m) is str]

    # Report
    if not errors:
        return {**download, **save, **aggregate}
    else:
        errors = '\n'.join(errors)
        errors = '\n'.join(["At least one input to 'dl_lodes' is in error:",
                            errors])
        return errors


def download_lodes_zip(file_type, state, segment, part, job_type, year, out_dir):
    """
    downloads a LODES file locally according to provided specs

    Parameters
    ----------
    file_type : str; LODES file type, see `dl_lodes`
    state : str; state, see `dl_lodes`
    segment : str; segment of the workforce, see `dl_lodes`
    part : str; part of state file, see `dl_lodes`
    job_type : str; job type, see `dl_lodes`
    year : int; year, see `dl_lodes`
    out_dir : str; directory in which to save the zipped raw LODES download; see `dl_lodes`

    Returns
    -------
    file path for downloaded LODES zip
    """

    # Set the URL
    if file_type == "od":
        file_name = '_'.join([state, file_type, part, job_type, str(year)])
    else:
        file_name = '_'.join([state, file_type, segment, job_type, str(year)])
    file_name = ''.join([file_name, ".csv.gz"])
    download_url = f"{LODES_URL}/{state}/{file_type}/{file_name}"
    # Download the file
    save_path = os.path.join(out_dir, file_name)
    save_path = save_path.replace(".csv.gz", "_blk.csv.gz")
    request.urlretrieve(url=download_url, filename=save_path)

    return save_path


def download_geography_crosswalk_zip(state, out_dir):
    """
    download a state geography crosswalk locally

    Parameters
    ----------
    state : str; state, see `dl_lodes`
    out_dir : str; directory in which to save the zipped raw geography crosswalk download,  see `dl_lodes`

    Returns
    -------
    file path for the downloaded geography crosswalk zip
    """

    # Set the URL
    download_url = f"{LODES_URL}/{state}/{state}_xwalk.csv.gz"
    # Download the file
    save_path = os.path.join(out_dir, f"{state}_xwalk.csv.gz")
    request.urlretrieve(url=download_url, filename=save_path)

    return save_path


def aggregate_lodes_data(geography_crosswalk_path, lodes_path, file,
                         aggregate_geo, out_dir):
    """
    aggregate LODES data to desired geographic levels, and save the created
    files [to .csv.gz]

    Parameters
    ----------
    geography_crosswalk_path : str; path to the zipped raw geography crosswalk download file; this will be the output of
        `download_geography_xwalk_zip`
    lodes_path : str; path to the downloaded LODES file; this will be the output of `download_lodes_zip`
    file : str; LODES file type of the `loaded_lodes_data`, see `dl_lodes`
    aggregate_geo : str, or iterable of str; geographic levels to which to aggregate, see `dl_lodes`
    out_dir : str; directory in which to save the aggregated data; see `dl_lodes`

    Returns
    -------
    dict of file paths for any aggregated data
    """

    # Initialize by setting up a dictionary for results
    ret_dict = {}

    # First, we want to load the geography crosswalk
    crosswalk = pd.read_csv(geography_crosswalk_path, compression="gzip",
                            dtype={"tabblk2010": "str", "st": "str", "cty": "str",
                                   "trct": "str", "bgrp": "str", "cbsa": "str", "zcta": "str"},
                            low_memory=False)  # so there's no message for mixed dtypes

    # Now we want to load the LODES data
    if file == "od":
        dtype_change = {"w_geocode": "str", "h_geocode": "str"}
    elif file == "rac":
        dtype_change = {"h_geocode": "str"}
    else:
        dtype_change = {"w_geocode": str}
    lodes = pd.read_csv(lodes_path, compression="gzip", dtype=dtype_change, low_memory=False)  # so there's no
    # message for mixed dtypes

    # Now, we want to isolate the LODES columns we want to aggregate, and
    # format them for usage in a pandas groupby-summarize. We'll also pull our 
    # columns for joining to the crosswalk data
    lodes_cols = lodes.columns.tolist()
    agg_cols = [c for c in lodes_cols if bool(re.search("[0-9]", c)) == True]
    agg_dict = {}
    for c in agg_cols:
        agg_dict[c] = "sum"
    lodes_join = [c for c in lodes_cols if bool(re.search("geocode", c)) == True]

    # Next, join the xwalk to the lodes data. If it's OD, we have two join
    # fields, so it becomes a bit more complicated
    crosswalk_cols = crosswalk.columns.tolist()
    if file == "od":
        xwalk2 = crosswalk.copy()
        crosswalk.columns = [f"w_{col}" for col in crosswalk_cols]
        xwalk2.columns = [f"h_{col}" for col in crosswalk_cols]
        crosswalk = pd.concat([crosswalk, xwalk2], axis=1)
        crosswalk_cols = crosswalk.columns.tolist()

    xwalk_join = [c for c in crosswalk_cols if bool(re.search("tabblk2010", c)) == True]
    df = pd.merge(lodes, crosswalk, left_on=lodes_join, right_on=xwalk_join, how="left")

    # Now we're ready to aggregate!
    for level in aggregate_geo:

        # Because the level could become a list if file=="od", we isolate
        # the singular level for naming later on
        lchar = level

        # 1. Grab the columns to keep
        if file == "od":
            level = ['_'.join(["w", level]),
                     '_'.join(["h", level])]
            keep_cols = level + agg_cols
        else:
            keep_cols = [level] + agg_cols
        to_agg = df[keep_cols]

        # 2. Aggregate
        agged = to_agg.groupby(level).agg(agg_dict)
        agged = agged.reset_index()

        # 3. Save
        save_path = lodes_path.replace("_blk.csv.gz",
                                       ''.join(["_", lchar, ".csv.gz"]))
        agged.to_csv(save_path,
                     compression="gzip",
                     index=False)
        ret_dict[lchar] = save_path

    # Done
    return ret_dict


def dl_lodes(file_type, state, segment, part, job_type, year, out_dir,
             aggregate_geo=None, geography=True):
    """
    Download and save block-level employment data from LODES, and aggregate
    this data to higher geographic levels if requested. All files are
    downloaded/saved as gzipped csvs (file extension .csv.gz). Will overwrite
    the file if it already exists

    Parameters
    ----------
    file_type : str; LODES file type, see notes for options and definitions. will be
        converted to lower-case if not provided as such
    state : str; 2-letter postal code for a chosen state, see notes for options and
        definitions. will be converted to lower-case if not provided as such.
        required for all files
    segment : str; segment of the workforce, see notes for options and definitions. will
        be converted to upper-case alphanumeric if not provided as such. only
        relevant for "rac" and "wac" files, is otherwise ignored (parameter may
        be set to "")
    part : str;  part of the state file, see notes for options and definitions. will be
        converted to lower-case if not provided as such. only relevant for "od"
        files, is otherwise ignored (parameter may be set to "")
    job_type : str; job type, see notes for options and definitions. will be converted to
        upper-case alphanumeric if not provided as such. required for all files
    year : numeric; 4-digit year for a chosen year, see notes for options. required for all
        files
    save_directory : str; directory in which to save downloaded/created files. Will be created
        if it does not already exists
    aggregate_geo : str, or iterable of str, optional; other census geography levels to which to aggregate the LODES data
        (which is given at the block level), see notes for options and
        definitions. More than one option may be given, and a unique file will
        be created for each aggregation. The default is `None`, no aggregation
    geography : bool, optional; should the geography crosswalk be downloaded? Only relevant if
        `aggregate_geo = None`, as the geography crosswalk is automatically
        downloaded/saved in aggregation; ignored if `aggregate_geo is not None`.
        This option allows the user to grab the crosswalk even if they do not
        want to aggregate. The default is `True`.

    Notes
    -----
    1. options for `file_type` are:
        1. "od": Origin-Destination data, jobs totals are associated with both
        a home Census Block and a work Census Block
        2. "rac": Residence Area Characteristic data, jobs are totaled by home
        Census Block
        3: "wac": Workplace Area Characteristic data, jobs are totaled by work
        Census Block

    2. options for `state` are:
        1. any of the 50 US states
        2. the District of Columbia (Washington DC)
    US territories, including Puerto Rico and the US Virgin Islands, do not
    have employment data available through this source.

    3. options for `segment` are:
        1. "S000": all workers
        2. "SA01": workers age 29 or younger
        3. "SA02": workers age 30 to 54
        4. "SA03": workers age 55 or older
        5. "SE01": workers with earnings $1250/month or less
        6. "SE02": workers with earnings $1251/month to $3333/month
        7. "SE03": workers with earnings greater than $3333/month
        8. "SI01": workers in Goods-Producing industry sectors
        9. "SI02": workers in Trade, Transportation, and Utilities industry
        sectors
        10. "SI03": workers in All Other Services industry sectors

    4. options for `part` are:
        1. "main": includes jobs with both workplace and residence in the state
        2. "aux": includes jobs with the workplace in the state and the
        residence outside of the state.
    main and aux are complimentary parts of the OD file for a state.

    5. options for `job_type` are:
        1. "JT00": All Jobs
        2. "JT01": Primary Jobs
        3. "JT02": All Private Jobs
        4. "JT03": Private Primary Jobs
        5. "JT04": All Federal Jobs
        6. "JT05": Federal Primary Jobs

    6. options for `year` are generally 2002-2018 for most files/states.
    Exceptions include:
        1. Alaska has no OD or WAC data for 2017-2018
        2. Arizona has no OD or WAC data for 2002-2003
        3. Arkansas has no OD or WAC data for 2002
        4. Washington DC has no OD or WAC data for 2002-2009
        5. Massachusetts has no OD or WAC data for 2002-2010
        6. Mississippi has no OD or WAC data for 2002-2003
        7. New Hampshire has no OD or WAC data for 2002

    If a specified combination of the above variables has no jobs, the file
    will still exist, but will be empty (i.e. it will only contain the header
    of the file, no rows of data)

    For more information on the construction of LODES data, including variable
    definitions for the OD, RAC, and WAC files, please see the LODES technical
    documentation at:
    https://lehd.ces.census.gov/data/lodes/LODES7/LODESTechDoc7.5.pdf

    options for `aggregate_geo` are the following (block-level data  may be
    aggregated up to any or all of these):
        1. "st": state (keyed by 2-digit FIPS state code)
        2. "cty": county (keyed by 5-digit FIPS county code),
        3. "trct": Census tract (keyed by 11-digit Census tract code),
        4. "bgrp": Census block group (keyed by 12-digit Census block group code)
        5. "cbsa": CBSA (keyed by 5-digit CBSA code),
        6. "zcta": ZIP code tabulation area (keyed by 5-digit ZIP code)
    for no aggregation, leave `aggregate_geo` as the default `None`. If any
    aggregation is completed, a "geography crosswalk" file definining the
    relationships between varying geographies for the state will also be
    downloaded

    Returns
    -------
    dict of file paths for any files downloaded and/or created by this
    function: downloaded LODES file (block level), state geography crosswalk,
    and/or any aggregated results
    """

    # Step 1. Validate
    print("")
    print("1. Validating function inputs")
    validate = validate_dl_lodes(file_type=file_type, state=state, segment=segment, part=part,
                                 job_type=job_type, year=year, out_dir=out_dir,
                                 aggregate_geo=aggregate_geo, geography=geography)
    if type(validate) is str:
        raise ValueError(validate)

    # Step 2. Download the lodes file
    print("2. Downloading the LODES file")
    # Set the URL
    if file_type == "od":
        file_name = f"{state}_{file_type}_{part}_{job_type}_{str(year)}.csv.gz"
    else:
        file_name = f"{state}_{file_type}_{segment}_{job_type}_{str(year)}.csv.gz"
    lodes_download_url = f"{LODES_URL}/{state}/{file_type}/{file_name}"
    lodes_file = os.path.join(out_dir)
    download_url(url=lodes_download_url, save_path=lodes_file)
    download_lodes = download_lodes_zip(file_type=validate["file"], state=validate["state"],
                                        segment=validate["segment"],
                                        part=validate["part"], job_type=validate["job_type"], year=validate["year"],
                                        out_dir=validate["out_dir"])

    # If we need to aggregate, proceed to step 3 and 4. Otherwise, pull the
    # crosswalk if requested
    if aggregate_geo is None:
        # Format dictionary for return
        blk = {"blk": download_lodes}

        # If geography is requested, download and add file path to return
        if geography:
            print("3. Downloading the geography crosswalk")
            download_crosswalk = download_geography_crosswalk_zip(state=validate["state"],
                                                                  out_dir=validate["save_directory"])
            blk["xwalk"] = download_crosswalk

        # Done
        print("Done!")
        print("")
        return blk
    else:
        # Step 3. Download the geography xwalk
        print("3. Downloading the geography crosswalk")
        download_crosswalk = download_geography_crosswalk_zip(state=validate["state"],
                                                              out_dir=validate["save_directory"])

        # Step 4. Aggregate the lodes data
        print("4. Aggregating the LODES data")
        aggregate = aggregate_lodes_data(geography_crosswalk_path=download_crosswalk,
                                         lodes_path=download_lodes,
                                         file=validate["file"],
                                         aggregate_geo=validate["aggregate_geo"],
                                         out_dir=validate["save_directory"])

        # Format and return
        aggregate["blk"] = download_lodes
        aggregate["xwalk"] = download_crosswalk
        print("Done!")
        print("")
        return aggregate


def load_dl_lodes(file_path):
    """
    load a file downloaded/created by `dl_lodes`, mandating the proper data
    types for each column. will work on a LODES download, geography crosswalk
    download, or aggregated data.

    Parameters
    ----------
    file_path : str; file path to file downloaded/created by `dl_lodes`

    Returns
    -------
    pandas DataFrame of the loaded file
    """

    # LODES is pretty advantageous in its file naming, in that field types
    # can be identified by patterns in the field name
    # 1. ints are [A-Z]{1,3}[0-9]{2,3}
    # 2. floats are "blk[a-z]{3}dd"
    # 3. strings are everything else
    # So, we can set up a dict of dtype reading based on these

    # First, grab the field names
    field_names = pd.read_csv(file_path, compression="gzip", nrows=0)
    field_names = field_names.columns.tolist()

    # Then, set up the type dictionary
    field_types = {}
    for f in field_names:
        if bool(re.search("[A-Z]{1,3}[0-9]{2,3}", f)):
            field_types[f] = "int"
        elif bool(re.search("blk[a-z]{3}dd", f)):
            field_types[f] = "float"
        else:
            field_types[f] = "str"

    # Now, load the file and return it
    df = pd.read_csv(file_path, compression="gzip", dtype=field_types)
    return df


# %% Main
if __name__ == "__main__":
    # Inputs
    file = "wac"
    state = "fl"
    part = ""
    segment = "S000"
    job_type = "JT00"
    year = 2017
    save_directory = "K:/Projects/MiamiDade/PMT/Data/RAW/LODES"
    aggregate_geo = "bgrp"
    geography = True

    # Function
    file_paths = dl_lodes(file_type=file, state=state, segment=segment, part=part, job_type=job_type,
                          year=year, out_dir=save_directory, aggregate_geo=aggregate_geo, geography=True)

    # Load
    # print("Loading the data created above")
    # lodes = {}
    # for key,value in file_paths.items():
    #     print("--> '" + key + "' file")
    #     lodes[key] = load_dl_lodes(file_path = value)
    # print("Done!")
    # print("")
    # lodes
