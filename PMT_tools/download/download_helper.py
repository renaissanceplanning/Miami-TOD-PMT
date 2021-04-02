import os
import re
from urllib import request

import censusdata as census
import networkx as nx
import pandas as pd
import requests
from requests.exceptions import RequestException

from PMT_tools.utils import makePath, check_overwrite_path


def download_file_from_url(url, save_path, overwrite=False):
    """downloads file resources directly from a url endpoint to a folder
    Args:
        url (str): String; path to resource
        save_path (str): String; path to output file
        overwrite: (bool): if True, existing data will be deleted prior to download
    Returns:
        None
    """
    if os.path.isdir(save_path):
        filename = get_filename_from_header(url)
        save_path = makePath(save_path, filename)
    if overwrite:
        check_overwrite_path(output=save_path, overwrite=overwrite)
    print(f"...downloading {save_path} from {url}")
    try:
        request.urlretrieve(url, save_path)
    except:
        with request.urlopen(url) as download:
            with open(save_path, 'wb') as out_file:
                out_file.write(download.read())


def get_filename_from_header(url):
    """grabs a filename provided in the url object header
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
    """ Given an index of `censusgeo` objects, return a dataframe with
        columns reflecting the geographical hierarchy and identifying
        discrete features.
    Args:
        pd_idx (idx): Index, A pandas Index of `censusgeo` objects.
        gen_geoid (bool): Boolean, default=True; If True, the geographical hierarchy will be concatenated into a
            geoid field. If False, only the geographicl hierarchy fields are returned.
        geoid (str): String, default="GEOID10"; The name to assign the geoid column if `gen_geoid` is True.
    Returns:
        geo_cols (pd.DataFrame): DataFrame; A data frame with columns reflecting the geographical hierachy of
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


def _fetch_acs(year, acs_dataset, state, county, table, columns, census_scale):
    variables = [f"{table}_{c}" for c in list(columns.keys())]
    # Reconstruct dictionary with explicit ordering
    values = [columns[c.split("_")[1]] for c in variables]
    rename = dict(zip(variables, values))
    # Set the geography object
    geo = census.censusgeo(
        [('state', state), ('county', county), (census_scale, '*')])
    # Fetch data
    data = census.download(src=acs_dataset, year=year, geo=geo, var=variables)
    # Rename columns
    data.rename(columns=rename, inplace=True)
    return data


def download_race_vars(year, acs_dataset="acs5", state="fl", county="086",
                       table=None, columns=None):
    """Downloads population race and ethnicity variables from available ACS data
        in table B03002.
    Args:
        year (int): year of interest
        acs_dataset (str): String, default="acs5"; Which ACS dataset to download (3-year, 5-year, e.g.)
        state (str): String, default="12"; Which state FIPS code to download data for (`12` is Florida)
        county (str): String, defult="086"; Which county FIPS code to download data for (`086` is Miami-Dade)
        table:
        columns:
    Returns:
        race_data (pd.DataFrame): A data frame with columns showing population by race (white, black,
            Asian, 2 or more, or other) and ethnicity (Hispanic, non-Hispanic) for block groups in the
            specified state-county.
    Raises:
        ValueError
            If the table is not found (i.e. the requested year's data are not available)
    """
    # Set table and columns to fetch (with renaming specs for legibility)
    _fetch_acs(year, acs_dataset, state, county, table, columns)
    race_data = _fetch_acs(year=year, acs_dataset=acs_dataset,
                           state=state, county=county,
                           table=table, columns=columns)
    race_variables = [f"{table}_{c}" for c in list(columns.keys())]
    # Calculate "other" race totals (those not in the specified categories)
    race_data["Other_Non_Hisp"] = (race_data.Total_Non_Hisp - race_data.White_Non_Hisp -
                                   race_data.Black_Non_Hisp - race_data.Asian_Non_Hisp -
                                   race_data.Multi_Non_Hisp)
    race_data["Other_Hispanic"] = (race_data.Total_Hispanic - race_data.White_Hispanic -
                                   race_data.Black_Hispanic - race_data.Asian_Hispanic -
                                   race_data.Multi_Hispanic)
    # Use the census geo index to make geo tag cols
    geo_cols = census_geoindex_to_columns(race_data.index, gen_geoid=True, geoid="GEOID10")
    race_data = pd.concat([geo_cols, race_data], axis=1)

    return race_data.reset_index(drop=True)


def download_commute_vars(year, acs_dataset="acs5", state="fl", county="086", table=None, columns=None):
    """Downloads commute (journey to work) data from available ACS data
        in table B08301.
    Args:
        year: Int
        acs_dataset: String, default="acs5"
            Which ACS dataset to download (3-year, 5-year, e.g.)
        state: String, default="12"
            Which state FIPS code to download data for (`12` is Florida)
        county: String, defult="086"
            Which county FIPS code to download data for (`086` is Miami-Dade)
        table (str):
        columns (dict):
    Returns:
        commute_data: DataFrame
            A data frame with columns showing ....
    Raises:
        ValueError
            If the table is not found (i.e. the requested year's data are not available)
    """
    # Set table and columns to fetch (with renaming specs for legibility)
    # Fetch data
    mode_data = _fetch_acs(year, acs_dataset, state, county, table, columns)
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
    geo_cols = census_geoindex_to_columns(mode_data.index, gen_geoid=True, geoid="GEOID10")
    mode_data = pd.concat([geo_cols, mode_data], axis=1)

    return mode_data.reset_index(drop=True)


def trim_components(graph, min_edges=2, message=True):
    """remove connected components less than a certain size (in number of edges)
    from a graph

    Args:
        graph : networkx graph
            the network from which to remove small components
        min_edges : int, optional
            the minimum number of edges required for a component to remain in the
            network; any component with FEWER edges will be removed. The default
            is 2.
        message : bool, optional
            should a message indicating the number of components removed be
            printed? The default is True.
    Returns:
        G : networkx graph
            the original graph, with connected components smaller than `min_edges`
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
    if message == True:
        count_removed = sum([len(x) < min_nodes for x in conn_comps])
        count_message = ' '.join([str(count_removed),
                                  "of",
                                  str(len(conn_comps)),
                                  "were removed from the input graph"])
        print(count_message)
    else:
        pass

    # The graph was updated in the loop, so we can just return here
    return graph

# bike and pedestrian crashes #DEPRECATED
# def download_bike_ped_crashes(
#         all_crashes_url=None, fields='ALL', where_clause=None,
#         out_crs='4326', out_dir=None, out_name="crashes_raw.geojson"):
#     """Reads in a feature service url and filters based on the query and
#         saves geojson copy of the file to the specified output location
#     Args:
#         all_crashes_url (str): Url path to the all crashes layer
#         fields (list): a comma-separated list of fields to request from the server
#         where_clause (dict): a dictionary key of 'where' with the value being the intended filter
#         out_crs (str): EPSG code used to define output coordinates
#         out_dir (str): Path, Directory where file will be stored
#         out_name (str): The name of the output geojson file.
#     Returns:
#         None; A geojson file of bike and pedestrian crashes is saved at
#         '{out_path}/{out_name}'
#     """
#     # handle an option to limit fields returned
#     if fields != 'ALL':
#         if isinstance(fields, list):
#             requested_fields = fields
#         else:
#             requested_fields = fields.split(',')
#     else:
#         requested_fields = None
#
#     # read data from feature server
#     # TODO: add validation for url, where clause and crs
#     features_dump = EsriDumper(url=all_crashes_url, extra_query_args=where_clause,
#                                fields=requested_fields, outSR=out_crs)
#
#     # write out data from server to geojson
#     out_file = os.path.join(out_dir, out_name)
#     with open(out_file, 'w') as dst:
#         dst.write('{"type":"FeatureCollection","features":[\n')
#         feature_iter = iter(features_dump)
#         try:
#             feature = next(feature_iter)
#             while True:
#                 dst.write(json.dumps(feature))
#                 feature = next(feature_iter)
#                 dst.write(',\n')
#         except StopIteration:
#             dst.write('\n')
#         dst.write(']}')
