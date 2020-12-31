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
import requests
import pandas as pd
import re

# %% Functions

def validate_lodes_download(file,
                            ST,
                            SEG,
                            PART,
                            TYPE,
                            YEAR):
    '''
    validates the download inputs to `dl_lodes` (`file`, `ST`, `SEG`, `PART`,
    `TYPE`, and `YEAR`)
    
    Parameters
    ----------
    file : str
        LODES file type, see `dl_lodes`
    ST : str
        state, see `dl_lodes`
    SEG : str
        segment of the workforce, see `dl_lodes`
    PART : str
        part of state file, see `dl_lodes`
    TYPE : str
        job type, see `dl_lodes`
    YEAR : int
        year, see `dl_lodes`

    Returns
    -------
    dict of formatted download inputs if all inputs are valid for `dl_lodes`, 
    relevant error messages if any of the inputs are invalid.
    '''  
    
    # Type
    # ----
    
    # Validate the type of the inputs (do this first for if/else checking
    # of proper parametrization in the next step)
    file_type = type(file) is str
    ST_type = type(ST) is str
    PART_type = type(PART) is str
    SEG_type = type(SEG) is str
    TYPE_type = type(TYPE) is str
    YEAR_type = type(YEAR) is int 
    
    # Conversion and checking
    # -----------------------
    
    # file
    if file_type == True:
        file = file.lower()
        file_bool = file in ["od", "rac", "wac"]
        if file_bool == False:
            file_message = "invalid 'file'; see function docs"
        else:
            file_message = ""
    else:
        file_message = "'file' is not a string"
    
    # ST
    if ST_type == True:
        ST = ST.lower()
        ST_bool = ST in ["al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", 
                         "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", 
                         "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", 
                         "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", 
                         "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", 
                         "va", "wa", "wv", "wi", "wy", "dc"]
        if ST_bool == False:
            ST_message = "invalid 'ST'; see function docs"
        else:
            ST_message = ""
    else:
        ST_message = "'ST' is not a string"
    
    # PART
    if PART_type == True:
        PART = PART.lower()
        PART_bool = PART in ["main", "aux", ""]
        if PART_bool == False:
            PART_message = "invalid 'PART'; see function docs"
        else:
            PART_message = ""
    else:
        PART_message = "'PART' is not a string"
    
    # SEG
    if SEG_type == True:
        SEG = SEG.upper()
        SEG_bool = SEG in ["S000", "SA01", "SA02", "SA03", "SE01", "SE02", 
                           "SE03", "SI01", "SI02", "SI03", ""]
        if SEG_bool == False:
            SEG_message = "invalid 'SEG'; see function docs"
        else:
            SEG_message = ""
    else:
        SEG_message = "'SEG' is not a string"
      
    # TYPE
    if TYPE_type == True:
        TYPE = TYPE.upper()
        TYPE_bool = TYPE in ["JT00", "JT01", "JT02", "JT03", "JT04", "JT05"] 
        if TYPE_bool == False:
            TYPE_message = "invalid 'TYPE'; see function docs"
        else:
            TYPE_message = ""
    else:
        TYPE_message = "'TYPE' is not a string"
      
    # YEAR
    if YEAR_type == True:
        YEAR_bool = any([(YEAR in np.arange(2002, 2017) and ST == "ak"),
                         (YEAR in np.arange(2004, 2019) and ST == "az"),
                         (YEAR in np.arange(2003, 2019) and ST == "ar"),
                         (YEAR in np.arange(2010, 2019) and ST == "dc"),
                         (YEAR in np.arange(2011, 2019) and ST == "ma"),
                         (YEAR in np.arange(2004, 2019) and ST == "ms"),
                         (YEAR in np.arange(2003, 2019) and ST == "nh"),
                         (YEAR in np.arange(2002, 2019) and ST not in ["ak","az","ar","dc","ma","ms","nh"])])
        if YEAR_bool == False:
            YEAR_message = "invalid 'YEAR' or 'YEAR/ST' combination; see function docs"
        else:
            YEAR_message = ""
    else:
        YEAR_message = "'YEAR' is not an integer"
        
    # PART and SEG
    if PART_type == True and SEG_type == True:
        if PART == "" and SEG == "":
            ps_message = "'PART' and 'SEG' cannot both be ''; see function docs"
        else:
            ps_message = ""
    else:
        ps_message = ""
        
    # PART-OD
    if PART_type == True and file_type == True:
        if PART == "" and file == "od":
            pod_message = "'PART' cannot be '' if 'file' is 'od'; see function docs"
        else:
            pod_message = ""
    else:
        pod_message = ""
    
    # SEG-RAC/WAC
    if SEG_type == True and file_type == True:
        if SEG == "" and file in ["rac", "wac"]:
            srw_message = "'SEG' cannot be '' if 'file' is 'rac' or 'wac'; see function docs"
        else:
            srw_message = ""
    else:
        srw_message = ""
    
        
    # Putting it together
    # -------------------
    
    # Organize the error messages
    messages = [file_message, ST_message, PART_message,
                SEG_message, TYPE_message, YEAR_message,
                ps_message, pod_message, srw_message]
    errors = [' '.join(["-->", m]) for m in messages if m != ""]
    
    # Return the error messages if any
    if errors == []:
        return({"file": file,
                "ST": ST,
                "SEG": SEG,
                "PART": PART,
                "TYPE": TYPE,
                "YEAR": YEAR})
    else:
        errors = '\n'.join(errors)
        return(errors)
    
# ----------------------------------------------------------------------------

def validate_lodes_save(save_directory):
    '''
    validates/creates [if neccessary] the save directory for `dl_lodes`

    Parameters
    ----------
    save_directory : str
        save directory, see `dl_LODES`

    Returns
    -------
    dict of `save_directory` if the save directory exists/is creatable, 
    relevant error messages if it does/is not.
    '''
    
    # Check if it exists. If it doesn't try to create it. If it succeeds,
    # os.mkdir returns a None, so we in turn return that None; otherwise, we
    # return an error message
    if os.path.isdir(save_directory):
        return({"save_directory": save_directory})
    else:
        try:
            os.mkdir(save_directory)
            return({"save_directory": save_directory})
        except:
            error = "--> 'save_directory' does not exist and cannot be created"
            return(error)
            
# ----------------------------------------------------------------------------
    
def validate_lodes_aggregate(aggregate_to,
                             geography):
    '''
    validates the aggregation input for `dl_lodes`

    Parameters
    ----------
    aggregate_to : str, or iterable of str
        geographic levels to which to aggregate, see `dl_lodes`

    Returns
    -------
    dict of formatted aggregation inputs if all inputs are valid for 
    `dl_lodes`, relevant error messages if any of the inputs are invalid.
    '''
    
    # If 'aggregate_to' is None, then we only need to check 'geography'. If
    # it's not, we need to make sure it has the proper structure/inputs. Also,
    # in this case, we wouldn't need the geography parameter (it is downloaded
    # automatically in aggregation), so we don't have to check or return it
    
    if aggregate_to is None:
        if type(geography) is bool:
            return({"aggregate_to": aggregate_to,
                    "geography": geography})
        else:
            error = "--> 'geography' is not a boolean"
            return(error)
    else:
        # Check if iterable, if not make it iterable
        if not isinstance(aggregate_to, Iterable) or isinstance(aggregate_to, string_types):
            aggregate_to = [aggregate_to]
            
        # Validate the inputs
        idx = np.arange(len(aggregate_to))
        valid_aggs = ["st", "cty", "trct", "bgrp", "cbsa", "zcta"]
        messages = []
        for i in idx:
            opt = aggregate_to[i]
            if type(opt) is str:
                if opt in valid_aggs:
                    message = ""
                else:
                    message = ' '.join(["'aggregate_to' item", str(i), "is invalid; see function docs"])
            else:
                message = ' '.join(["'aggregate_to' item", str(i), "is not a string"])
            messages.append(message)
        
        # Organize the error messages
        errors = [' '.join(["-->", m]) for m in messages if m != ""]
        
        # Return the error messages if any
        if errors == []:
            return({"aggregate_to": aggregate_to})
        else:
            errors = '\n'.join(errors)
            return(errors)
        
# ----------------------------------------------------------------------------

def validate_dl_lodes(file,
                      ST,
                      SEG,
                      PART,
                      TYPE,
                      YEAR,
                      save_directory,
                      aggregate_to,
                      geography):
    '''
    validate all inputs for `dl_lodes`; this is a wrapper function for all 
    sub-validations functions (of which there are 3)
    
    Parameters
    ----------
    file : str
        LODES file type, see `dl_lodes`
    ST : str
        state, see `dl_lodes`
    SEG : str
        segment of the workforce, see `dl_lodes`
    PART : str
        part of state file, see `dl_lodes`
    TYPE : str
        job type, see `dl_lodes`
    YEAR : int
        year, see `dl_lodes`
    save_directory : str
        save directory, see `dl_LODES`
    aggregate_to : str, or iterable of str, or None
        geographic levels to which to aggregate, see `dl_lodes`
    geography : bool
        should the geography crosswalk be downloaded if no aggregation, see
        `dl_lodes`

    Returns
    -------
    dict of formatted inputs if all inputs are valid for `dl_lodes`, relevant 
    error messages if any of the inputs are invalid
    '''
    
    # Call all the validation functions
    download = validate_lodes_download(file = file,
                                       ST = ST,
                                       SEG = SEG,
                                       PART = PART,
                                       TYPE = TYPE,
                                       YEAR = YEAR)
    save = validate_lodes_save(save_directory = save_directory)
    aggregate = validate_lodes_aggregate(aggregate_to = aggregate_to,
                                         geography = geography)
    
    # Error organization
    messages = [download, save, aggregate]
    errors = [m for m in messages if type(m) is str]
    
    # Report
    if errors == []:
        return({**download, **save, **aggregate})
    else:
        errors = '\n'.join(errors)
        errors = '\n'.join(["At least one input to 'dl_lodes' is in error:", 
                            errors])
        return(errors)

# ----------------------------------------------------------------------------

def download_lodes_zip(file,
                       ST,
                       SEG,
                       PART,
                       TYPE,
                       YEAR,
                       save_directory):
    '''
    downloads a LODES file locally according to provided specs
    
    Parameters
    ----------
    file : str
        LODES file type, see `dl_lodes`
    ST : str
        state, see `dl_lodes`
    SEG : str
        segment of the workforce, see `dl_lodes`
    PART : str
        part of state file, see `dl_lodes`
    TYPE : str
        job type, see `dl_lodes`
    YEAR : int
        year, see `dl_lodes`
    save_directory : str
        directory in which to save the zipped raw LODES download; see 
        `dl_lodes`

    Returns
    -------
    file path for downloaded LODES zip
    '''
    
    # Set the URL
    base_url = "https://lehd.ces.census.gov/data/lodes/LODES7"
    if file == "od":
        file_name = '_'.join([ST, file, PART, TYPE, str(YEAR)])
    else:
        file_name = '_'.join([ST, file, SEG, TYPE, str(YEAR)])
    file_name = ''.join([file_name, ".csv.gz"])
    download_url = '/'.join([base_url,
                             ST,
                             file,
                             file_name])
    
    # Download the file
    save_path = os.path.join(save_directory,
                             file_name)
    save_path = save_path.replace(".csv.gz", "_blk.csv.gz")
    req = requests.get(download_url)
    with open(save_path, 'wb') as f:
        f.write(req.content)
        
    # Done
    return(save_path)

# ----------------------------------------------------------------------------

def download_geography_xwalk_zip(ST,
                                 save_directory):
    '''
    download a state geography crosswalk locally

    Parameters
    ----------
    ST : str
        state, see `dl_lodes`
    save_directory : str
        directory in which to save the zipped raw geography crosswalk download; 
        see `dl_lodes`

    Returns
    -------
    file path for the downloaded geography crosswalk zip
    '''
    
    # Set the URL
    base_url = "https://lehd.ces.census.gov/data/lodes/LODES7"
    file_name = '_'.join([ST, "xwalk.csv.gz"])
    download_url = '/'.join([base_url,
                             ST,
                             file_name])
    
    # Download the file
    save_path = os.path.join(save_directory,
                             file_name)
    req = requests.get(download_url)
    with open(save_path, 'wb') as f:
        f.write(req.content)
        
    # Done
    return(save_path)

# ----------------------------------------------------------------------------

def aggregate_lodes_data(geography_xwalk_path,
                         lodes_path,
                         file,
                         aggregate_to,
                         save_directory):
    '''
    aggregate LODES data to desired geographic levels, and save the created
    files [to .csv.gz]

    Parameters
    ----------
    geography_xwalk_download_path : str
        path to the zipped raw geography crosswalk download file; this will be 
        the output of `download_geography_xwalk_zip`
    lodes_download_path : str
        path to the downloaded LODES file; this will be the output of
        `download_lodes_zip`
    file : str
        LODES file type of the `loaded_lodes_data`, see `dl_lodes`
    aggregate_to : str, or iterable of str
        geographic levels to which to aggregate, see `dl_lodes`
    save_directory : str
        directory in which to save the aggregated data; see `dl_lodes`

    Returns
    -------
    dict of file paths for any aggregated data
    '''
    
    # Initialize by setting up a dictionary for results
    ret_dict = {}
    
    # First, we want to load the geography crosswalk
    xwalk = pd.read_csv(geography_xwalk_path,
                        compression = "gzip",
                        dtype = {"tabblk2010": "str",
                                 "st": "str",
                                 "cty": "str",
                                 "trct": "str",
                                 "bgrp": "str",
                                 "cbsa": "str",
                                 "zcta": "str"},
                        low_memory = False) #so there's no message for mixed dtypes
    
    # Now we want to load the LODES data
    if file == "od":
        dtype_change = {"w_geocode": "str",
                        "h_geocode": "str"}
    elif file == "rac":
        dtype_change = {"h_geocode": "str"}
    else:
        dtype_change = {"w_geocode": str}
    lodes = pd.read_csv(lodes_path,
                        compression = "gzip",
                        dtype = dtype_change,
                        low_memory = False) #so there's no message for mixed dtypes
    
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
    xwalk_cols = xwalk.columns.tolist()
    if file == "od":
        xwalk2 = xwalk.copy()
        xwalk.columns = ['_'.join(["w", col]) for col in xwalk_cols]
        xwalk2.columns = ['_'.join(["h", col]) for col in xwalk_cols]
        xwalk = pd.concat([xwalk, xwalk2], axis = 1)
        xwalk_cols = xwalk.columns.tolist()
    
    xwalk_join = [c for c in xwalk_cols if bool(re.search("tabblk2010", c)) == True] 
    df = pd.merge(lodes,
                  xwalk,
                  left_on = lodes_join,
                  right_on = xwalk_join,
                  how = "left")
       
    # Now we're ready to aggregate!
    for level in aggregate_to:
        
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
                     compression = "gzip",
                     index = False)
        ret_dict[lchar] = save_path
    
    # Done
    return(ret_dict)  
    
# ----------------------------------------------------------------------------

def dl_lodes(file,
             ST,
             SEG,
             PART,
             TYPE,
             YEAR,
             save_directory,
             aggregate_to = None,
             geography = True):
    '''
    Download and save block-level employment data from LODES, and aggregate
    this data to higher geographic levels if requested. All files are
    downloaded/saved as gzipped csvs (file extension .csv.gz). Will overwrite
    the file if it already exists

    Parameters
    ----------
    file : str
        LODES file type, see notes for options and definitions. will be
        converted to lower-case if not provided as such
    ST : str
        2-letter postal code for a chosen state, see notes for options and
        definitions. will be converted to lower-case if not provided as such. 
        required for all files
    SEG : str
        segment of the workforce, see notes for options and definitions. will
        be converted to upper-case alphanumeric if not provided as such. only
        relevant for "rac" and "wac" files, is otherwise ignored (parameter may 
        be set to "")
    PART : str
        part of the state file, see notes for options and definitions. will be
        converted to lower-case if not provided as such. only relevant for "od"
        files, is otherwise ignored (parameter may be set to "")
    TYPE : str
        job type, see notes for options and definitions. will be converted to
        upper-case alphanumeric if not provided as such. required for all files
    YEAR : numeric
        4-digit year for a chosen year, see notes for options. required for all 
        files
    save_directory : str
        directory in which to save downloaded/created files. Will be created
        if it does not already exists
    aggregate_to : str, or iterable of str, optional
        other census geography levels to which to aggregate the LODES data
        (which is given at the block level), see notes for options and 
        definitions. More than one option may be given, and a unique file will 
        be created for each aggregation. The default is `None`, no aggregation
    geography : bool, optional
        should the geography crosswalk be downloaded? Only relevant if
        `aggregate_to = None`, as the geography crosswalk is automatically
        downloaded/saved in aggregation; ignored if `aggregate_to is not None`. 
        This option allows the user to grab the crosswalk even if they do not 
        want to aggregate. The default is `True`.
        
    Notes
    -----
    1. options for `file` are:
        1. "od": Origin-Destination data, jobs totals are associated with both 
        a home Census Block and a work Census Block
        2. "rac": Residence Area Characteristic data, jobs are totaled by home 
        Census Block
        3: "wac": Workplace Area Characteristic data, jobs are totaled by work 
        Census Block
        
    2. options for `ST` are:
        1. any of the 50 US states
        2. the District of Columbia (Washington DC)
    US territories, including Puerto Rico and the US Virgin Islands, do not 
    have employment data available through this source.
    
    3. options for `SEG` are:
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
    
    4. options for `PART` are:
        1. "main": includes jobs with both workplace and residence in the state
        2. "aux": includes jobs with the workplace in the state and the 
        residence outside of the state. 
    main and aux are complimentary parts of the OD file for a state.
    
    5. options for `TYPE` are:
        1. "JT00": All Jobs
        2. "JT01": Primary Jobs
        3. "JT02": All Private Jobs 
        4. "JT03": Private Primary Jobs
        5. "JT04": All Federal Jobs
        6. "JT05": Federal Primary Jobs
    
    6. options for `YEAR` are generally 2002-2018 for most files/states.
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
    
    options for `aggregate_to` are the following (block-level data  may be 
    aggregated up to any or all of these):
        1. "st": state (keyed by 2-digit FIPS state code)
        2. "cty": county (keyed by 5-digit FIPS county code),
        3. "trct": Census tract (keyed by 11-digit Census tract code),
        4. "bgrp": Census block group (keyed by 12-digit Census block group 
        code)
        5. "cbsa": CBSA (keyed by 5-digit CBSA code),
        6. "zcta": ZIP code tabulation area (keyed by 5-digit ZIP code)
    for no aggregation, leave `aggregate_to` as the default `None`. If any
    aggregation is completed, a "geography crosswalk" file definining the
    relationships between varying geographies for the state will also be
    downloaded
    
    Returns
    -------
    dict of file paths for any files downloaded and/or created by this 
    function: downloaded LODES file (block level), state geography crosswalk, 
    and/or any aggregated results
    '''
    
    # Step 1. Validate
    print("")
    print("1. Validating function inputs")
    validate = validate_dl_lodes(file = file,
                                 ST = ST,
                                 SEG = SEG,
                                 PART = PART,
                                 TYPE = TYPE,
                                 YEAR = YEAR,
                                 save_directory = save_directory,
                                 aggregate_to = aggregate_to,
                                 geography = geography)
    if type(validate) is str:
        raise ValueError(validate)
        
    # Step 2. Download the lodes file
    print("2. Downloading the LODES file")
    download_lodes = download_lodes_zip(file = validate["file"],
                                        ST = validate["ST"],
                                        SEG = validate["SEG"],
                                        PART = validate["PART"],
                                        TYPE = validate["TYPE"],
                                        YEAR = validate["YEAR"],
                                        save_directory = validate["save_directory"])
    
    # If we need to aggregate, proceed to step 3 and 4. Otherwise, pull the
    # crosswalk if requested
    if aggregate_to is None:
        # Format dictionary for return
        blk = {"blk": download_lodes}
        
        # If geography is requested, download and add file path to return
        if geography == True:
            print("3. Downloading the geography crosswalk")
            download_xwalk = download_geography_xwalk_zip(ST = validate["ST"],
                                                          save_directory = validate["save_directory"])
            blk["xwalk"] = download_xwalk
        
        # Done
        print("Done!")
        print("")
        return(blk)
    else:        
        # Step 3. Download the geography xwalk
        print("3. Downloading the geography crosswalk")
        download_xwalk = download_geography_xwalk_zip(ST = validate["ST"],
                                                      save_directory = validate["save_directory"])
        
        # Step 4. Aggregate the lodes data
        print("4. Aggregating the LODES data")
        aggregate = aggregate_lodes_data(geography_xwalk_path = download_xwalk,
                                         lodes_path = download_lodes,
                                         file = validate["file"],
                                         aggregate_to = validate["aggregate_to"],
                                         save_directory = validate["save_directory"])
        
        # Format and return
        aggregate["blk"] = download_lodes
        aggregate["xwalk"] = download_xwalk
        print("Done!")
        print("")
        return(aggregate)
    
# ----------------------------------------------------------------------------

def load_dl_lodes(file_path):
    '''
    load a file downloaded/created by `dl_lodes`, mandating the proper data
    types for each column. will work on a LODES download, geography crosswalk
    download, or aggregated data.

    Parameters
    ----------
    file_path : str
        file path to file downloaded/created by `dl_lodes`

    Returns
    -------
    pandas DataFrame of the loaded file
    '''
    
    # LODES is pretty advantageous in its file naming, in that field types
    # can be identified by patterns in the field name
    # 1. ints are [A-Z]{1,3}[0-9]{2,3}
    # 2. floats are "blk[a-z]{3}dd"
    # 3. strings are everything else
    # So, we can set up a dict of dtype reading based on these
    
    # First, grab the field names
    field_names = pd.read_csv(file_path,
                              compression = "gzip",
                              nrows = 0)
    field_names = field_names.columns.tolist()
    
    # Then, set up the type dictionary
    field_types = {}
    for f in field_names:
        if bool(re.search("[A-Z]{1,3}[0-9]{2,3}", f)) == True:
            field_types[f] = "int"
        elif bool(re.search("blk[a-z]{3}dd", f)) == True:
            field_types[f] = "float"
        else:
            field_types[f] = "str"
    
    # Now, load the file and return it
    df = pd.read_csv(file_path,
                     compression = "gzip",
                     dtype = field_types)
    return(df)
        
# %% Main
if __name__ == "__main__":
    
    # Inputs
    file = "wac"
    ST = "fl"
    PART = ""
    SEG = "S000"
    TYPE = "JT00"
    YEAR = 2017
    save_directory = "K:/Projects/MiamiDade/PMT/Data/RAW/LODES"
    aggregate_to = "bgrp"
    geography = True
    
    # Function
    file_paths = dl_lodes(file = file,
                          ST = ST,
                          SEG = SEG,
                          PART = PART,
                          TYPE = TYPE,
                          YEAR = YEAR,
                          save_directory = save_directory,
                          aggregate_to = aggregate_to,
                          geography = True)
    
    # Load
    # print("Loading the data created above")
    # lodes = {}
    # for key,value in file_paths.items():
    #     print("--> '" + key + "' file")
    #     lodes[key] = load_dl_lodes(file_path = value)
    # print("Done!")
    # print("")
    # lodes
    