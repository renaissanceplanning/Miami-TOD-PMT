"""
The `helper` module provides generalized methods to acquire data given a url endpoint,
along with some purpose-built methods to obtain and/or clean ACS, OSM, and other
datasets used in TOC analysis.
"""
import os
import re
from urllib import request

import censusdata as census
import networkx as nx
import pandas as pd
import requests
from requests.exceptions import RequestException

from PMT_tools.PMT import make_path

__all__ = ["download_file_from_url", "get_filename_from_header", "census_geoindex_to_columns", "fetch_acs",
           "download_race_vars", "download_commute_vars", "trim_components"]


def download_file_from_url(url, save_path):
    """
    Downloads file resources directly from a url endpoint to a folder

    Args:
        url (str): String; path to resource
        save_path (str): String; path to output file
    
    Returns:
        None
    """
    if os.path.isdir(save_path):
        filename = get_filename_from_header(url)
        save_path = make_path(save_path, filename)

    print(f"...downloading {save_path} from {url}")
    try:
        request.urlretrieve(url, save_path)
    except:
        with request.urlopen(url) as download:
            with open(save_path, "wb") as out_file:
                out_file.write(download.read())


def get_filename_from_header(url):
    """
    Grabs a filename provided in the url object header

    Args:
        url (str): string, url path to file on server
    
    Returns:
        filename (str): filename as string
    """
    try:
        with requests.get(url) as r:
            if "Content-Disposition" in r.headers.keys():
                return re.findall("filename=(.+)", r.headers["Content-Disposition"])[0]
            else:
                return url.split("/")[-1]
    except RequestException as e:
        print(e)


# ACS tabular data
def census_geoindex_to_columns(pd_idx, gen_geoid=True, geoid="GEOID10"):
    """
    Given an index of `censusgeo` objects, return a dataframe with
    columns reflecting the geographical hierarchy and identifying
    discrete features.

    Args:
        pd_idx (idx): Index, A pandas Index of `censusgeo` objects.
        gen_geoid (bool): Boolean, default=True; If True, the geographical hierarchy will be concatenated into a
            geoid field. If False, only the geographicl hierarchy fields are returned.
        geoid (str): String, default="GEOID10"; The name to assign the geoid column if `gen_geoid` is True.
    
    Returns:
        geo_cols (pandas.DataFrame): DataFrame; A data frame with columns reflecting the geographical hierachy of
            `index`, identifying discrete geographic features. This data frame has `index` as its index.
    """
    idx_stack = []
    for i in pd_idx.to_list():
        columns = i.hierarchy().split("> ")
        params = i.params()
        _df_ = pd.DataFrame(params)
        _df_ = pd.DataFrame(_df_[1].to_frame().T)
        _df_.columns = columns
        idx_stack.append(_df_)
    geo_cols = pd.concat(idx_stack)

    if gen_geoid:
        geo_cols[geoid] = geo_cols.values.sum(axis=1)

    return geo_cols.set_index(pd_idx)


def fetch_acs(year, acs_dataset, state, county, table, columns):
    """
    Internal function to hit the CENSUS api and extract a pandas DataFrame for
    the requested Table, State, County

    Args:
        year (int): year of interest
        acs_dataset (str): Census data source: 'acs1' for ACS 1-year estimates, 'acs5' for ACS 5-year estimates,
            'acs3' for ACS 3-year estimates, 'acsse' for ACS 1-year supplemental estimates, 'sf1' for SF1 data.
        state (str): two letter state abbreviation
        county (str): three digit FIPS code as a string
        table (str): string code for the Census table of interest ex: "B03002"
        columns (dict): key, value pairs of Census table columns and rename
            (ex: {"002E": "Total_Non_Hisp", "012E": "Total_Hispanic")

    Returns:
        pandas.DataFrame: Data frame with columns corresponding to designated variables, and row
            index of censusgeo objects representing Census geographies.
    """
    variables = [f"{table}_{c}" for c in list(columns.keys())]
    # Reconstruct dictionary with explicit ordering
    values = [columns[c.split("_")[1]] for c in variables]
    rename = dict(zip(variables, values))
    # Set the geography object
    geo = census.censusgeo([("state", state), ("county", county)])
    # Fetch data
    data = census.download(src=acs_dataset, year=year, geo=geo, var=variables)
    # Rename columns
    data.rename(columns=rename, inplace=True)
    return data


def download_race_vars(
    year, acs_dataset="acs5", state="fl", county="086", table=None, columns=None
):
    """
    Downloads population race and ethnicity variables from available ACS data
    in table B03002.
    
    Args:
        year (int): year of interest
        acs_dataset (str): String, default="acs5"; Which ACS dataset to download (3-year, 5-year, e.g.)
        state (str): String, default="12"; Which state FIPS code to download data for (`12` is Florida)
        county (str): String, defult="086"; Which county FIPS code to download data for (`086` is Miami-Dade)
        table (str): string code for the Census table of interest ex: "B03002"
        columns (dict): key, value pairs of Census table columns and rename
            (ex: {"002E": "Total_Non_Hisp", "012E": "Total_Hispanic")
    
    Returns:
        race_data (pandas.DataFrame): A data frame with columns showing population by race (white, black,
            Asian, 2 or more, or other) and ethnicity (Hispanic, non-Hispanic) for block groups in the
            specified state-county.
    
    Raises:
        ValueError
            If the table is not found (i.e. the requested year's data are not available)
    """
    # Set table and columns to fetch (with renaming specs for legibility)
    # _fetch_acs(year, acs_dataset, state, county, table, columns)
    race_data = fetch_acs(
        year=year,
        acs_dataset=acs_dataset,
        state=state,
        county=county,
        table=table,
        columns=columns,
    )
    race_variables = [f"{table}_{c}" for c in list(columns.keys())]
    # Calculate "other" race totals (those not in the specified categories)
    race_data["Other_Non_Hisp"] = (
        race_data.Total_Non_Hisp
        - race_data.White_Non_Hisp
        - race_data.Black_Non_Hisp
        - race_data.Asian_Non_Hisp
        - race_data.Multi_Non_Hisp
    )
    race_data["Other_Hispanic"] = (
        race_data.Total_Hispanic
        - race_data.White_Hispanic
        - race_data.Black_Hispanic
        - race_data.Asian_Hispanic
        - race_data.Multi_Hispanic
    )
    # Use the census geo index to make geo tag cols
    geo_cols = census_geoindex_to_columns(
        race_data.index, gen_geoid=True, geoid="GEOID10"
    )
    race_data = pd.concat([geo_cols, race_data], axis=1)

    return race_data.reset_index(drop=True)


def download_commute_vars(
    year, acs_dataset="acs5", state="12", county="086", table=None, columns=None,
):
    """
    Downloads commute (journey to work) data from available ACS data
    in table B08301.
    
    Args:
        year: Int
        acs_dataset (str, default="acs5"):
            Which ACS dataset to download (3-year, 5-year, e.g.)
        state (str, default="12"):
            Which state FIPS code to download data for ("12" is Florida)
        county: (str, defult="086"):
            Which county FIPS code to download data for ("086" is Miami-Dade)
        table (str): string code for the Census table of interest ex: "B03002"
        columns (dict): key, value pairs of Census table columns and rename
            (ex: `{"001E": "Total_Commutes", "003E": "Drove_alone"}`)
    
    Returns:
        commute_data (pandas.DataFrame)
            A data frame with columns showing commute statistics by mode
    
    Raises:
        ValueError
            If the table is not found (i.e. the requested year's data are not available)
    """
    # Set table and columns to fetch (with renaming specs for legibility)
    # Fetch data
    mode_data = fetch_acs(year, acs_dataset, state, county, table, columns)
    # Create Subtotals
    mode_data["Drove"] = mode_data.Drove_alone + mode_data.Motorcycle
    mode_data["NonMotor"] = mode_data.Bicycle + mode_data.Walk
    mode_data["AllOther"] = mode_data.Taxi + mode_data.Other
    # Calc shares
    mode_data["SOV_Share"] = mode_data.Drove / mode_data.Total_Commutes
    mode_data["HOV_Share"] = mode_data.Carpool / mode_data.Total_Commutes
    mode_data["PT_Share"] = mode_data.Transit / mode_data.Total_Commutes
    mode_data["NM_Share"] = mode_data.NonMotor / mode_data.Total_Commutes
    mode_data["Oth_Share"] = mode_data.AllOther / mode_data.Total_Commutes
    mode_data["WFH_Share"] = mode_data.Work_From_Home / mode_data.Total_Commutes

    # Use the census geo index to make geo tag cols
    geo_cols = census_geoindex_to_columns(
        mode_data.index, gen_geoid=True, geoid="GEOID10"
    )
    mode_data = pd.concat([geo_cols, mode_data], axis=1)

    return mode_data.reset_index(drop=True)


def trim_components(graph, min_edges=2, message=True):
    """
    Remove connected components less than a certain size (in number of edges) from a graph.

    Args:
        graph (nx.Graph): the networkx graph from which to remove small components
        min_edges (int, optional, default=2): the minimum number of edges required for a component to remain in the
            network; any component with FEWER edges will be removed.
        message (bool, optional, default=True): if True, prints a message indicating the number of components removed 
            from `graph`

    Returns:
        G (nx.Graph): a modified copy of the original graph with connected components smaller than `min_edges`
            removed
    """

    # Build weakly connected components -- there must be a path from A to B,
    # but not necessarily from B to A (this accounts for directed graphs)
    conn_comps = list(nx.weakly_connected_components(graph))

    # To have at least "x" edges, we need at least "x+1" nodes. So, we can
    # set a node count from the min edges
    min_nodes = min_edges + 1

    # Loop through the connected components (represented as node sets) to
    # count edges -- if we have less than the required number of nodes for
    # the required number of edges, remove the nodes that create that
    # component (thus eliminating that component)
    for cc in conn_comps:
        if len(cc) < min_nodes:
            graph.remove_nodes_from(cc)
        else:
            pass

    # If a printout of number of components removed is requested, count and
    # print here.
    if message:
        count_removed = sum([len(x) < min_nodes for x in conn_comps])
        count_message = " ".join(
            [
                str(count_removed),
                "of",
                str(len(conn_comps)),
                "were removed from the input graph",
            ]
        )
        print(count_message)
    else:
        pass

    # The graph was updated in the loop, so we can just return here
    return graph

