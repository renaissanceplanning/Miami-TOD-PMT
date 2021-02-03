# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 12:59:09 2021

@author: Aaron Weinstock
"""

# %% Imports

import ftplib
import re
import pandas as pd
import numpy as np


# %% Functions

# https://stackoverflow.com/questions/1854572/traversing-ftp-listing
def traverse(ftp, depth=None):
    """
    produce a recursive listing of an ftp server contents (starting from the
    current directory)

    Parameters
    ----------
    ftp : ftplib.FTP object
        FTP connection
    depth : NoneType
        controls depth to which searching is completed; ignored if provided
        by user, searching always begins at the folder connection of the FTP
        object
        
        
    Returns
    -------
    recursive dictionary, where each key contains the contents of the 
    subdirectory or None if it corresponds to a file
    """

    # Initialize
    depth = 0
    level = {}

    # Recursively search for files through the directories
    for entry in (path for path in ftp.nlst() if path not in (".", "..")):
        try:
            ftp.cwd(entry)
            level[entry] = traverse(ftp, depth + 1)
            ftp.cwd("..")
        except ftplib.error_perm:
            level[entry] = None

    # Done
    return level


# ----------------------------------------------------------------------------

# https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
def flatten(d, parent_key="", sep="/"):
    """
    flatten nested dictionaries into a single dictionary with combined keys

    Parameters
    ----------
    d : dict
        dictionary to flatten
    parent_key : str, optional
        string to paste to the front of every key. The default is '', keys
        get no prefix
    sep : str, optional
        string used to separate nested keys. The default is '/' [for usage
        with directories/urls]

    Returns
    -------
    a flattened dictionary, with nested keys joined by `sep`
    """

    # Initialize
    items = []

    # Recursively combine keys in nested dictionaries
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        try:
            items.extend(flatten(v, new_key, sep=sep).items())
        except:
            items.append((new_key, v))

    # Done
    return dict(items)


# ----------------------------------------------------------------------------

# Written by Aaron
def get_ftp_files(folder_connection, ftp_site="sdrftp03.dor.state.fl.us"):
    """
    get file paths to all files within an FTP folder 

    Parameters
    ----------
    folder_connection : str
        folder name in an FTP directory. If the folder is not at the level
        of the main FTP site, it must be specified using pathing, e.g.
        folder/subfolder/subsubfolder
    ftp_site : str, optional
        FTP main site. The default is "sdrftp03.dor.state.fl.us", which is the
        FDOR FTP main site

    Returns
    -------
    list of file paths to FTP files
    """

    # Log into the FTP and connect to the folder
    ftp = ftplib.FTP(ftp_site)
    ftp.login("", "")
    ftp.cwd(folder_connection)

    # Grab and format links
    file_dict = traverse(ftp=ftp)
    files = flatten(file_dict)
    files = list(files.keys())

    # Format
    urls = ["/".join([ftp_site, folder_connection, f]) for f in files]

    # Done
    return urls


# ----------------------------------------------------------------------------

# Written by Aaron
def format_florida_counties(county):
    """
    produce all possible representations of a county name for the state of
    Florida (representations are case insensitive)

    Parameters
    ----------
    county : str
        county name

    Returns
    -------
    list of options for representing county names. in particular, spaces
    will be condensed to "_" or "", and the word "saint" will be tried as
    "saint", "st", and "st."
    """

    # First, FDOR refers to Miami-Dade as "Dade" only -- so if the name has
    # "Miami", call it dade
    if bool(re.search("miami", county, re.IGNORECASE)):
        county = "dade"

    # If theres a "Saint, St., or St" to start it, this is a different animal
    # because we have to consider all options. Otherwise, we really only
    # need to concern ourselves with spacing
    saint_search = "|".join(["^saint", "^st(\.) "])
    if bool(re.search(saint_search, county, re.IGNORECASE)):
        if bool(re.search("^saint", county, re.IGNORECASE)):
            rep = ["saint", "st", "st."]
        else:
            rep = ["saint ", "st ", "st. "]
        opts = [re.sub(saint_search, v, county, flags=re.IGNORECASE) for v in rep]
    else:
        opts = [county]

    # Spaces removed or as underscores
    sr = [o.replace(" ", "") for o in opts]
    su = [o.replace(" ", "_") for o in opts]
    spaced = opts + sr + su

    # Final options
    fo = np.unique(spaced).tolist()
    return fo


# ----------------------------------------------------------------------------

# Written by Aaron
def florida_counties():
    """
    produce a dataframe of Florida counties and all of their possible
    representations

    Returns
    -------
    pandas DataFrame with 4 columns:
        county: county name
        co_no: Florida DOR county number
        fips: 3-digit county FIPS code
        format: a possible representation of the county name (case insensitive)
    """

    # Florida counties
    fl = [
        "Alachua",
        "Baker",
        "Bay",
        "Bradford",
        "Brevard",
        "Broward",
        "Calhoun",
        "Charlotte",
        "Citrus",
        "Clay",
        "Collier",
        "Columbia",
        "Miami-Dade",
        "DeSoto",
        "Dixie",
        "Duval",
        "Escambia",
        "Flagler",
        "Franklin",
        "Gadsden",
        "Gilchrist",
        "Glades",
        "Gulf",
        "Hamilton",
        "Hardee",
        "Hendry",
        "Hernando",
        "Highlands",
        "Hillsborough",
        "Holmes",
        "Indian River",
        "Jackson",
        "Jefferson",
        "Lafayette",
        "Lake",
        "Lee",
        "Leon",
        "Levy",
        "Liberty",
        "Madison",
        "Manatee",
        "Marion",
        "Martin",
        "Monroe",
        "Nassau",
        "Okaloosa",
        "Okeechobee",
        "Orange",
        "Osceola",
        "Palm Beach",
        "Pasco",
        "Pinellas",
        "Polk",
        "Putnam",
        "St. Johns",
        "St. Lucie",
        "Santa Rosa",
        "Sarasota",
        "Seminole",
        "Sumter",
        "Suwannee",
        "Taylor",
        "Union",
        "Volusia",
        "Wakulla",
        "Walton",
        "Washington",
    ]

    # DOR county codes
    cono = [str(x) for x in np.arange(11, 78).tolist()]

    # FIPS
    fips = (
            np.arange(1, 24, 2).tolist()
            + [86]
            + np.arange(27, 86, 2).tolist()
            + np.arange(87, 134, 2).tolist()
    )
    fips = [str(f).zfill(3) for f in fips]

    # Iterate through names and create representations
    df = []
    for i in range(len(fl)):
        df.append(
            pd.DataFrame(
                {
                    "county": fl[i],
                    "co_no": cono[i],
                    "fips": fips[i],
                    "format": format_florida_counties(fl[i]),
                }
            )
        )
    df = pd.concat(df)

    # Done
    return df


# ----------------------------------------------------------------------------

# Written by Aaron
def extract_year(url, regex="[0-9]{4}"):
    """
    extract a year from a url string

    Parameters
    ----------
    url : str
        url to a data file
    regex : str
        regular expression to find a year. The default is `[0-9]{4}`, or 4
        numbers in a row (which works for the Florida DOR FTP site)

    Returns
    -------
    The FIRST year found; if the regex might find more than 1 unique year,
    consider a different regex
    """

    # If you find a year, return it as an integer. If there isn't one, return
    # a null value of 9999
    try:
        year = re.findall(regex, url)[0]
        year = int(year)
    except:
        year = 9999
    return year


# ----------------------------------------------------------------------------

# Written by Aaron
def extract_county_name(url, regex):
    """
    extract a florida county name from a url string

    Parameters
    ----------
    url : str
        url to a data file
    regex : str
        regular expression to find a county

    Returns
    -------
    The FIRST county found; if the regex might find more than 1 unique county,
    consider a different regex
    """

    # If you find a county, return it as an string. If there isn't one, return
    # a null value of ''
    try:
        county = re.findall(regex, url, re.IGNORECASE)[0]
    except:
        county = ""
    return county


# ----------------------------------------------------------------------------


def extract_county_number(url):
    """
    extract a Florida DOR county from a url string

    Parameters
    ----------
    url : str
        url to a data file

    Returns
    -------
    The FIRST county number found; if the regex might find more than 1 unique 
    county, consider a different regex
    """

    # If you find a number, return it as an string. If there isn't one, return
    # a null value of ''
    co_nos = [str(x) for x in np.arange(11, 78, 1).tolist()]

    county = re.findall("\D[0-9]{2}\D", url)
    if county != []:
        county = [re.findall("[0-9]{2}", x)[0] for x in county]
        county = [x for x in county if x in co_nos]
        try:
            county = county[0]
        except:
            county = ""
    else:
        county = ""
    return county


# ----------------------------------------------------------------------------

# Written by Aaron
def fdor_gis_and_tax(save_path=None):
    """
    parse the download link for all GIS and Tax Roll data available on the
    FDOR FTP site

    Parameters
    ----------
    save_path : str
        path to which to save the results. The Default is `None`, no save
        completed

    Returns
    -------
    pandas DataFrame of format with 4 columns:
        file: either "GIS" for GIS data, or "TaxRoll" for tax roll data
        year: the year associated with the data (or 9999 if there is none)
        county: the county associated with the data (or '' if there is none)
        url: link to the data
    """

    # GIS
    # ---
    print("")
    print("1. Parsing available GIS data")

    gis = get_ftp_files(
        folder_connection="Map Data", ftp_site="sdrftp03.dor.state.fl.us"
    )

    # NAL
    # ---
    print("2. Parsing available Tax Roll data")

    nal = get_ftp_files(
        folder_connection="Tax Roll Data Files", ftp_site="sdrftp03.dor.state.fl.us"
    )

    # function that wraps traverse and flatten

    # Finding years
    # -------------
    print("3. Identifying year for available data")

    years = [extract_year(u) for u in gis + nal]

    # Finding counties
    # ----------------
    print("4. Identifying county for available data")

    # By name
    fl = florida_counties()
    co_regex = "|".join(fl.format)
    counties = [extract_county_name(u, regex=co_regex) for u in gis + nal]

    # By number
    co_nos = [extract_county_number(u) for u in gis + nal]

    # Format for readability
    # ----------------------
    print("5. Formatting results for readibility")

    # Format county names
    desoto = "desoto"
    indian_river = "|".join(fl.loc[fl.county == "Indian River", "format"])
    miami_dade = "dade"
    palm_beach = "|".join(fl.loc[fl.county == "Palm Beach", "format"])
    santa_rosa = "|".join(fl.loc[fl.county == "Santa Rosa", "format"])
    st_johns = "|".join(fl.loc[fl.county == "St. Johns", "format"])
    st_lucie = "|".join(fl.loc[fl.county == "St. Lucie", "format"])

    counties_reformat = []
    for c in counties:
        c = re.sub(desoto, "DeSoto", c, flags=re.IGNORECASE)
        c = re.sub(indian_river, "Indian River", c, flags=re.IGNORECASE)
        c = re.sub(miami_dade, "Miami-Dade", c, flags=re.IGNORECASE)
        c = re.sub(palm_beach, "Palm Beach", c, flags=re.IGNORECASE)
        c = re.sub(santa_rosa, "Santa Rosa", c, flags=re.IGNORECASE)
        c = re.sub(st_johns, "St. Johns", c, flags=re.IGNORECASE)
        c = re.sub(st_lucie, "St. Lucie", c, flags=re.IGNORECASE)
        c = c.title()
        counties_reformat.append(c)

    # If a county name is empty, see if we can replace it using the number
    county_final = []
    for c, n in zip(counties_reformat, co_nos):
        if c == "":
            try:
                cfl = fl[fl.co_no == n]
                assoc = np.unique(cfl.county)[0]
                county_final.append(assoc)
            except:
                county_final.append("")
        else:
            county_final.append(c)

    # Format urls
    urls = ["".join(["ftp://", u]) for u in gis + nal]

    # File types
    file = ["GIS" for i in range(len(gis))] + ["TaxRoll" for i in range(len(nal))]

    # Bind data
    df = pd.DataFrame(
        {"file": file, "year": years, "county": county_final, "url": urls}
    )

    # Save
    # ----
    if save_path is not None:
        print("6. Saving")
        df.to_csv(save_path, index=False)

    # Done
    # ----
    print("Done!")
    print("")
    return df


# ----------------------------------------------------------------------------

# Written by Aaron
def fdor_availability(df, year=None, county=None):
    """
    filter all available FDOR GIS and Tax Roll data according to desired
    specifications

    Parameters
    ----------
    df : pandas DataFrame
        output of `fdor_gis_and_tax()` (a dataframe of available data)
    year : int, or list of int, optional
        year(s) for which data is desired. The default is None, no filtering
        will be completed on year
    county : str, or list of str, optional
        county(ies) for which data is desired. The default is None, no
        filtering will be completed on county

    Notes
    -----
    For searching by county, note the following spelling specifications:
        1. DeSoto [not Desoto]
        2. Miami-Dade [not Dade or Miami Dade]
        3. St. Johns [not St Johns or Saint Johns]
        4. St. Lucie [not St Lucie or Saint Lucie]
    If the spelling of a county does not the `fdor_gis_and_tax` results, this
    function will return no data, so please be careful with spelling!

    Returns
    -------
    pandas DataFrame of available data according to the provided specs
    
    Raises
    ------
    ValueError:
        if the specs result in no available data
    """

    # Initialize
    params = {"year": year, "county": county}

    # Loop over the parameters (if they're None, pass over)
    for key, value in params.items():
        if value is not None:
            # If it's not a list, make it a list
            if type(value) != list:
                value = [value]

            # See if any provided values are not in the data. If so, warn
            missing = [x for x in value if x not in df[key].tolist()]
            if len(missing) > 0:
                message = "".join(
                    [
                        "Warning: ",
                        ", ".join(missing),
                        " are not in the '",
                        key,
                        "' attribute",
                    ]
                )
                print(message)

            # Filter the data
            df = df[df[key].isin(value)]

    # Done
    if len(df.index) == 0:
        raise ValueError("the year/county combination has no available data")
    else:
        return df


# %% Run
if __name__ == "__main__":
    # Inputs
    save_path = r"C:\github\Miami-TOD-PMT\PMT_tools\download\parcel_ftp_011121.csv"
    # save_path = None

    # Run
    df = fdor_gis_and_tax(save_path=save_path)
    # df
    # md_current = fdor_availability(df = df,
    #                                year = 2020,
    #                                county = "Baker")
    # md_current.url.tolist()
