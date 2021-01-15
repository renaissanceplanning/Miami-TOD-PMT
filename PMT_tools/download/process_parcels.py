# -*- coding: utf-8 -*-
"""
Created on Thu Jan  7 10:54:07 2021

@author: Aaron Weinstock
"""

# %% Imports 

import urllib
import re
import numpy as np
import geopandas as gpd
import pandas as pd
from scipy import stats
import os
import pyproj

# %% GLOBALS
# base urls
gis_url = "ftp://sdrftp03.dor.state.fl.us/Map%20Data/"
tax_url = "ftp://sdrftp03.dor.state.fl.us/Tax%20Roll%20Data%20Files/"



def find_all_downloads():

# %% Functions
def find_open_years():
    '''
    identify years of availability for FDOR parcel data from their FTP data
    repository; parcel data includes both GIS data and NAL attributes.

    Returns
    -------
    dict of the following format:
        years:
            gis_only: years for which GIS data is available but NAL data isn't
            nal_only: years for which NAL data is available but GIS data isn't
            both: years for which GIS and NAL data are both available
        links:
            years in `both` (i.e. this is for all years in `both`):
                gis_url: url to the GIS data archive for the year
                nal_url: url to the NAL data archive for the year
    '''

    # parsing the ftp sites
    # ---------------------
    print("----> parsing the parcel GIS and NAL FTP sites")
    # subfolders defining years of available data
    searches = []
    for url in [gis_url, tax_url]:
        with urllib.request.urlopen(url) as u:
            page = u.read()
        page = str(page)
        links = re.findall("<DIR>\s*(.*?)\\\\r\\\\n", page)
        searches.append(links)

    # extract the years
    # -----------------
    print("----> extracting years of data by file type")

    years = []
    links = []
    for folders in searches:
        f_year = []
        f_link = []
        for item in folders:
            y = re.findall("[0-9]{4}", item)
            if len(y) > 0:
                f_link.append(item)
                for i in y:
                    f_year.append(i)
        years.append(f_year)
        links.append(f_link)

    # what's available when?
    # ----------------------
    print("----> identifying available data from matching years")

    year_list = np.unique([y for e in years for y in e]).tolist()
    public_years = {"gis_only": [], "nal_only": [], "both": []}
    public_links = {}

    for year in year_list:

        if year in years[0] and year not in years[1]:
            # year is only available for GIS data
            public_years["gis_only"].append(int(year))

        elif year in years[1] and year not in years[0]:
            # year is only available for NAL data
            public_years["nal_only"].append(int(year))

        else:
            # year is available for both GIS and NAL data
            # get the link for the NAL folder
            for nl in links[1]:
                if bool(re.search(year, nl)):
                    nal_subf_try = ''.join([nl.replace(" ", "%20"), "/"])
                    subf_url = ''.join([tax_url, nal_subf_try])
                    with urllib.request.urlopen(subf_url) as u:
                        nal_page = u.read()
                    nal_page = str(nal_page)
                    if bool(re.search("final nal", nal_page, re.IGNORECASE)):
                        nal_subf = subf_url
                        break

            # if there's a NAL folder with the name we want, get the GIS folder
            # otherwise, the year is only available for GIS data
            if "nal_subf" in locals():
                for map_link in links[0]:
                    if bool(re.search(year, map_link)):
                        gis_subf = ''.join([gis_url, map_link.replace(" ", "%20"), "/"])
                        break
                base_url = {"gis_url": gis_subf,
                            "nal_url": nal_subf}
                public_years["both"].append(int(year))
                public_links[str(year)] = base_url
            else:
                public_years["gis_only"].append(int(year))

    # done
    # ----
    availability = {"years": public_years,
                    "links": public_links}
    return availability


# ----------------------------------------------------------------------------

def format_county_name(county):
    '''
    produce all possible representations of a county name for the state of
    Florida

    Parameters
    ----------
    county : str
        county name

    Returns
    -------
    list of options for representing county names. in particular, spaces
    will be condensed to "_" or "", and the word "saint" will be tried as
    "saint", "st", and "st."
    '''

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
        opts = [re.sub(saint_search,
                       v,
                       county,
                       flags=re.IGNORECASE) for v in rep]
    else:
        opts = [county]

    # Spaces removed or as underscores
    sr = [o.replace(" ", "") for o in opts]
    su = [o.replace(" ", "_") for o in opts]
    spaced = opts + sr + su

    # Final options
    fo = np.unique(spaced).tolist()
    return (fo)


# ----------------------------------------------------------------------------

def dl_parcel_gis(county,
                  gis_url,
                  transform_crs):
    '''
    downloads the FDOR parcel GIS data for a county from an FDOR FTP map data 
    subfolder

    Parameters
    ----------
    county : list of str
        possible county name representations; output of `format_county_name` 
    gis_url : TYPE
        url to FDOR FTP site subfolder for map data (branch from
        ftp://sdrftp03.dor.state.fl.us/Map%20Data/)
    transform_crs : str, geopandas GeoDataFrame, or anything accepted by
    `pyproj.CRS.from_user_input`
        an object from which the desired transform can be inferred; includes
        a path to a shapefile, a GeoDataFrame, or some user input defining
        the CRS. see Notes for details
        
    Notes
    -----
    `pyproj.CRS.from_user_input` can take:
        - PROJ JSON string
        - PROJ JSON dict
        - WKT string
        - An authority string
        - An EPSG integer code
        - A tuple of (“auth_name”: “auth_code”)
        - An object with a to_json method
    Any of these would be valid inputs to the `transform_crs` parameter

    Returns
    -------
    geopandas GeoDataFrame of FDOR parcel boundaries for the county
    '''

    # find the folder on the url
    # --------------------------
    print("----> setting the download link")

    with urllib.request.urlopen(gis_url) as u:
        page = u.read()
    page = str(page)
    links = re.findall("\s*[0-9]{3,}\s*(.*?)\\\\r\\\\n", page)
    for l in links:
        for c in county:
            if bool(re.search(c, l, re.IGNORECASE)):
                zip_file = l
                break
    dl_url = ''.join([gis_url, zip_file])

    # load from the url
    # -----------------
    print("----> downloading the shape")

    gdf = gpd.read_file(dl_url)
    gdf = gdf.astype({"PARCELNO": str})

    # dissolve on id
    # --------------
    print("----> dissolving on parcel ID")

    gdf = gdf.dissolve(by="PARCELNO").reset_index()

    # reprojecting
    # ------------
    print("----> reprojecting")

    # If the write_crs is a file, load it and take its crs
    # If it's a geopandas, take its crs
    # If it's neither, make it using pyproj and the input
    if type(transform_crs) == str and os.path.exists(transform_crs):
        tcrs = gpd.read_file(transform_crs)
        write_crs = tcrs.crs
    elif type(transform_crs) == gpd.geodataframe.GeoDataFrame:
        write_crs = transform_crs.crs
    else:
        try:
            write_crs = pyproj.CRS.from_user_input(transform_crs)
        except:
            raise ValueError("CRS cannot be inferred or created from `transform_crs`")

    # reproject
    gdf = gdf.to_crs(write_crs)

    # done
    # ----
    return (gdf)


# ----------------------------------------------------------------------------

def dl_parcel_nal(county,
                  nal_url,
                  field_specs={}):
    '''
    downloads the FDOR parcel NAL data for a county from an FDOR FTP tax roll
    data subfolder

    Parameters
    ----------
    county : list of str
        possible county name representations; output of `format_county_name`
    nal_url : str
        url to FDOR FTP site subfolder for tax roll data data (branch from
        ftp://sdrftp03.dor.state.fl.us/Tax%20Roll%20Data%20Files/)
    fields_specs : dict
        dictionary with the following format:
            {variable: spec_dict}, where 'variable' is a variable from NAL to 
            keep and 'spec_dict' is a dict of the specs used to load and
            format the variable. 'spec_dict' has the following keys:
                dtype: type for this variable (e.g., int, str)
                rename: name to rename variable
                na_value: value used to fill NA (should be same type specified
                in dtype if dtype is given)
                aggfunc: function used to aggregate this variable over the 
                same ID (e.g., sum, mean)
        Default `{}`, pandas defaults will be used for dtype, no renaming or
        NA filling will be completed, and aggfunc will be "sum" for numerics
        or "mode" for objects
    
    Notes
    -----
    for each variable, as many keys as are relevant can and should be 
    specified. If you only want to provide the variable with no other info, 
    set the value for that key to an empty dict. Users should fill out as many 
    options in this parameter as possible; leaving as the default `{}` is
    highly discouraged (due to data size and complexity)

    Returns
    -------
    pandas DataFrame of FDOR parcel NAL data for the county
    '''

    # find the folder on the url
    # --------------------------
    print("----> setting the download link")

    with urllib.request.urlopen(nal_url) as u:
        page = u.read()
    page = str(page)
    links = re.findall("\s*[0-9]{3,}\s*(.*?)\\\\r\\\\n", page)
    for l in links:
        for c in county:
            if bool(re.search(c, l, re.IGNORECASE)):
                if bool(re.search("NAL", l)):
                    zip_file = l
                    break
    dl_url = ''.join([nal_url, zip_file])

    # load from the url
    # -----------------
    print("----> downloading the table")

    if field_specs != {}:
        cols = np.unique(["PARCEL_ID"] + list(field_specs.keys())).tolist()
    else:
        cols = None
    df = pd.read_csv(dl_url,
                     usecols=cols)

    # rename
    # ------
    print("----> renaming")

    # extract what we're renaming
    frename = {}
    for key in field_specs.keys():
        if "rename" in field_specs[key].keys():
            frename[key] = field_specs[key]["rename"]

    # rename (and cover PARCELNO manually just to be safe)
    df = df.rename(columns=frename)
    df = df.rename(columns={"PARCEL_ID": "PARCELNO"})

    # filling na
    # ----------
    print("----> filling NA")

    # extract what we're filling
    fna = {}
    for key in field_specs.keys():
        if "na_value" in field_specs[key].keys():
            fna[key] = field_specs[key]["na_value"]

    # fill (and cover PARCELNO manually just to be safe)
    df = df.fillna(value=fna)
    df = df.fillna(value={"PARCELNO": "unknown"})

    # data types
    # ----------
    print("----> setting data types")

    # extract what we're resetting
    fdt = {}
    for key in field_specs.keys():
        if "dtype" in field_specs[key].keys():
            fdt[key] = field_specs[key]["dtype"]

    # reset dtype (and cover PARCELNO manually just to be safe)
    df = df.astype(fdt)
    df = df.astype({"PARCELNO": str})

    # aggregating
    # -----------
    print("----> aggregating to parcel ID")

    if len(np.unique(df.PARCELNO)) < len(df.index):
        # extract how we're aggregating
        faggr = {}
        for key in field_specs.keys():
            if "aggfunc" in field_specs[key].keys():
                faggr[key] = field_specs[key]["aggfunc"]

        # if there's any aggfuncs missing, add them in
        agg_miss = [c for c in df.columns.tolist()
                    if c not in ["PARCELNO"] + list(faggr.keys())]
        if agg_miss == []:
            dfdt = df.dtypes
            for c in agg_miss:
                if dfdt[c] == "object":
                    agg_miss[c] = stats.mode
                else:
                    agg_miss[c] = sum

                    # aggregate
        df = df.groupby("PARCELNO").agg(faggr).reset_index()

        # modes are returned weirdly, so extract value
        mode_cols = [key for key, value in faggr.items() if value == stats.mode]
        for c in mode_cols:
            df[c] = [x[0][0] for x in df[c]]

    # done
    # ----
    return (df)


# ----------------------------------------------------------------------------

def save_parcels(gis_gdf,
                 nal_df,
                 save_path):
    '''
    merge parcel NAL data to GIS data, and save to shape

    Parameters
    ----------
    gis_gdf : geopandas GeoDataFrame
        geodataframe of parcel boundaries, with id field "PARCELNO"; this is
        the result of `dl_parcel_gis`
    nal_df : pandas DataFrame
        dataframe of parcel NAL data, with id field "PARCELNO"; this is
        the result of `dl_parcel_nal`
    save_path : str
        path to save the result

    Returns
    -------
    dict of format:
        gis_total: number of records in the GIS file
        nal_total: number of records in the NAL file
        result_total: number of records in the merged GIS/NAL results
        save_path: save path to the merged result
    '''

    # merge
    # -----
    print("----> merging shape and table")

    result = pd.merge(gis_gdf,
                      nal_df,
                      left_on="PARCELNO",
                      right_on="PARCELNO",
                      how="inner")

    # write
    # -----
    print("----> writing result to shapefile")
    result.to_file(save_path)

    # done
    # ----    
    gis_total = len(gis_gdf.index)
    nal_total = len(nal_df.index)
    result_total = len(result.index)
    rd = {"gis_total": gis_total,
          "nal_total": nal_total,
          "result_total": result_total,
          "save_path": save_path}
    return (rd)


# ----------------------------------------------------------------------------

def process_parcels(year,
                    county,
                    transform_crs,
                    save_path,
                    field_specs={},
                    get_geo=False):
    '''
    downloads a publicly-available parcel boundaries from FDOR FTP data
    archives, attributes it with NAL data for that year, and save locally as
    a shapefile

    Parameters
    ----------
    year : int
        year for which to pull data
    county : str
        name of a county in Florida
    transform_crs : str, geopandas GeoDataFrame, or anything accepted by
    `pyproj.CRS.from_user_input`
        an object from which the desired transform can be inferred; includes
        a path to a shapefile, a GeoDataFrame, or some user input defining
        the CRS. see Notes for details
    save_path : str
        path to save the result
    fields_specs : dict
        dictionary with the following format:
            {variable: spec_dict}, where 'variable' is a variable from NAL to 
            keep and 'spec_dict' is a dict of the specs used to load and
            format the variable. 'spec_dict' has the following keys:
                dtype: type for this variable (e.g., int, str)
                rename: name to rename variable
                na_value: value used to fill NA (should be same type specified
                in dtype if dtype is given)
                aggfunc: function used to aggregate this variable over the 
                same ID (e.g., sum, mean)
        Default `{}`, pandas defaults will be used for dtype, no renaming or
        NA filling will be completed, and aggfunc will be "sum" for numerics
        or "mode" for objects
        
    Notes
    -----
    `pyproj.CRS.from_user_input` can take:
        - PROJ JSON string
        - PROJ JSON dict
        - WKT string
        - An authority string
        - An EPSG integer code
        - A tuple of (“auth_name”: “auth_code”)
        - An object with a to_json method
    Any of these would be valid inputs to the `transform_crs` parameter
    
    for each variable in `field_specs`, as many keys as are relevant can and 
    should be specified. If you only want to provide the variable with no 
    other info set the value for that key to an empty dict. Users should fill 
    out as many options in this parameter as possible; leaving as the default 
    `{}` is highly discouraged (due to data size and complexity)

    Returns
    -------
    dict of format:
        gis_total: number of records in the GIS file
        nal_total: number of records in the NAL file
        result_total: number of records in the merged GIS/NAL results
        save_path: save path to the merged result
    '''

    # 1. validate the year
    # --------------------
    print("")
    print("1. Validating the year")

    syear = str(year)
    year_candidates = find_open_years()
    if year not in year_candidates["years"]["both"]:
        message = f'{syear} is not available; ' \
                  f'available year are {year_candidates["years"]["both"]}'
        raise ValueError(message)

    else:
        gis_url = year_candidates["links"][syear]["gis_url"]
        nal_url = year_candidates["links"][syear]["nal_url"]

    # 2. validate county name
    # -----------------------
    print("")
    print("2. Validating the county name")

    cty_format = format_county_name(county=county)

    # 3. gis
    # ------
    print("")
    print("3. Downloading parcel GIS data")

    gis = dl_parcel_gis(county=cty_format,
                        gis_url=gis_url,
                        transform_crs=transform_crs)

    # 4. nal
    # ------
    print("")
    print("4. Downloading parcel NAL data")

    nal = dl_parcel_nal(county=cty_format,
                        nal_url=nal_url,
                        field_specs=field_specs)

    # 5. save
    # -------
    print("")
    print("5. Merging and saving")

    save_info = save_parcels(gis_gdf=gis,
                             nal_df=nal,
                             save_path=save_path)

    # done
    # ----
    print("Done!")
    print("")
    return (save_info)


# %% Run

# Inputs
year = 2020
counties = ["Broward", "Indian River", "Martin", "Palm Beach"]  # , "Saint Lucie"]
transform_crs = os.path.join("K:/Projects/D4_CSHnetwork",
                             "Features/LU/SPCC_20",
                             "data_prep/cleaned_data/study_area",
                             "study_area.shp")
save_directory = os.path.join("K:/Projects/D4_CSHnetwork",
                              "Features/LU/SPCC_20",
                              "data_prep/cleaned_data/parcels")
field_specs = {"CO_NO": {"dtype": str, "na_value": "999", "aggfunc": stats.mode},
               "DOR_UC": {"dtype": str, "na_value": "999", "aggfunc": stats.mode},
               "TOT_LVG_AREA": {"dtype": int, "na_value": 0, "aggfunc": sum},
               "LND_SQFOOT": {"dtype": int, "na_value": 0, "aggfunc": sum},
               "NO_BULDNG": {"dtype": int, "na_value": 0, "aggfunc": sum},
               "NO_RES_UNTS": {"dtype": int, "na_value": 0, "aggfunc": sum},
               "JV": {"dtype": int, "na_value": 0, "aggfunc": sum},
               "TV_NSD": {"dtype": int, "na_value": 0, "aggfunc": sum},
               "NCONST_VAL": {"dtype": int, "na_value": 0, "aggfunc": sum},
               "ACT_YR_BLT": {"dtype": int, "na_value": 9999, "aggfunc": min},
               "EFF_YR_BLT": {"dtype": int, "na_value": 9999, "aggfunc": min}}

# Run
process_log = {}
for county in counties:
    print(county)
    county_name = county.lower().replace(" ", "_")
    save_path = os.path.join(save_directory,
                             ''.join([county_name, ".shp"]))
    pp = process_parcels(year=year,
                         county=county,
                         transform_crs=transform_crs,
                         save_path=save_path,
                         field_specs=field_specs)
    process_log[county] = pp
