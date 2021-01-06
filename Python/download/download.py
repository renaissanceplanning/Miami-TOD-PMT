from download_config import (CRASHES_SERVICE, PED_BIKE_QUERY, USE_CRASH)
from download_config import (CENSUS_FTP_HOME, CENSUS_SCALE,
                             CENSUS_STATE, CENSUS_COUNTY, CENSUS_GEO_TYPE,
                             ACS_RACE_TABLE, ACS_RACE_COLUMNS,
                             ACS_MODE_TABLE, ACS_MODE_COLUMNS)
from download_census_geo import get_one_geo_type

import json
import os
from pathlib import Path

from esridump.dumper import EsriDumper
import censusdata as census
import pandas as pd

# PMT Functions
from ..PMT import (makePath)

# PMT globals
from ..PMT import (RAW, CLEANED, YEARS)

GITHUB = True


# bike and pedestrian crashes
def download_bike_ped_crashes(
        all_crashes_url=None, fields='ALL', where_clause=None,
        out_crs='4326', out_dir=None, out_name="crashes_raw.geojson"):
    """
    Reads in a feature service url and filters based on the query and
    saves geojson copy of the file to the specified output location

    Parameters
    ----------
    all_crashes_url: String
        Url path to the all crashes layer
    fields: List
        a comma-separated list of fields to request from the server
    where_clause: dict
        a dictionary key of 'where' with the value being the intended filter
    out_crs: String
        EPSG code used to define output coordinates
    out_path: Path
        Directory where file will be stored
    out_name: String
        The name of the output geojson file.

    Returns
    -------
    None
        A geojson file of bike and pedestrian crashes is saved at
        '{out_path}/{out_name}'
    """
    # handle an option to limit fields returned
    if fields != 'ALL':
        if isinstance(fields, list):
            requested_fields = fields
        else:
            requested_fields = fields.split(',')
    else:
        requested_fields = None

    # read data from feature server
    # TODO: add validation for url, where clause and crs
    features_dump = EsriDumper(url=all_crashes_url, extra_query_args=where_clause,
                               fields=requested_fields, outSR=out_crs)

    # write out data from server to geojson
    out_file = os.path.join(out_path, out_name)
    with open(out_file, 'w') as dst:
        dst.write('{"type":"FeatureCollection","features":[\n')
        feature_iter = iter(features_dump)
        try:
            feature = next(feature_iter)
            while True:
                dst.write(json.dumps(feature))
                feature = next(feature_iter)
                dst.write(',\n')
        except StopIteration:
            dst.write('\n')
        dst.write(']}')


# ACS tabular data
def census_geoindex_to_columns(pd_idx, gen_geoid=True, geoid="GEOID10"):
    """
    Given an index of `censusgeo` objects, return a dataframe with
    columns reflecting the geographical hierarchy and identifying
    discrete features.

    Parameters
    -----------
    index: Index
        A pandas Index of `censusgeo` objects.
    gen_geoid: Boolean, default=True
        If True, the geographical hierarchy will be concatenated into a
        geoid field. If False, only the geographicl hierarchy fields are
        returned.
    geoid: String, default="GEOID10"
        The name to assign the geoid column if `gen_geoid` is True.

    Returns
    --------
    geo_cols: DataFrame
        A data frame with columns reflecting the geographical hierachy of
        `index`, identifying discrete geographic features. This data
        frame has `index` as its index.
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


def _fetch_acs(year, acs_dataset, state, county, table, columns):
    variables = [f"{table}_{c}" for c in list(columns.keys())]
    # Reconstruct dictionary with explicit ordering
    values = [columns[c.split("_")[1]] for c in variables]
    rename = dict(zip(variables, values))
    # Set the geography object
    geo = census.censusgeo(
        [('state', state), ('county', county), (CENSUS_SCALE, '*')])
    # Fetch data
    data = census.download(
        acs_dataset, year, geo, var=variables)
    # Rename columns
    data.rename(columns=rename, inplace=True)
    return data


def download_race_vars(year, acs_dataset="acs5", state=CENSUS_STATE, county=CENSUS_COUNTY):
    """
    Downloads population race and ethnicity variables from available ACS data
    in table B03002.

    Parameters
    -------------
    year: Int
    acs_dataset: String, default="acs5"
        Which ACS dataset to download (3-year, 5-year, e.g.)
    state: String, default="12"
        Which state FIPS code to download data for (`12` is Florida)
    county: String, defult="086"
        Which county FIPS code to download data for (`086` is Miami-Dade)

    Returns
    --------
    race_data: DataFrame
        A data frame with columns showing population by race (white, black,
        Asian, 2 or more, or other) and ethnicity (Hispanic, non-Hispanic)
        for block groups in the specified state-county.

    Raises
    -------
    ValueError
        If the table is not found (i.e. the requested year's data are not
        available)
    """
    # Set table and columns to fetch (with renaming specs for legibility)
    race_data = _fetch_acs(year, acs_dataset, state, county, ACS_RACE_TABLE, ACS_RACE_COLUMNS)
    race_variables = [f"{ACS_RACE_TABLE}_{c}" for c in list(ACS_RACE_COLUMNS.keys())]
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


def download_commute_vars(year, acs_dataset="acs5", state=CENSUS_STATE, county=CENSUS_COUNTY):
    """
    Downloads commute (journey to work) data from available ACS data
    in table B08301.

    Parameters
    -------------
    year: Int
    acs_dataset: String, default="acs5"
        Which ACS dataset to download (3-year, 5-year, e.g.)
    state: String, default="12"
        Which state FIPS code to download data for (`12` is Florida)
    county: String, defult="086"
        Which county FIPS code to download data for (`086` is Miami-Dade)

    Returns
    --------
    commute_data: DataFrame
        A data frame with columns showing ....

    Raises
    -------
    ValueError
        If the table is not found (i.e. the requested year's data are not
        available)
    """
    # Set table and columns to fetch (with renaming specs for legibility)
    # Fetch data
    mode_data = _fetch_acs(year, acs_dataset, state, county, ACS_MODE_TABLE, ACS_MODE_COLUMNS)
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


if __name__ == "__main__":
    if GITHUB:
        ROOT = r'C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT\Data'
        RAW = str(Path(ROOT, 'Raw'))
    # ALL RAW DATA that can be grabbed as single elements
    # download bike/ped crashes
    out_path = str(Path(RAW, "Safety_Security", "Crash_Data"))
    out_name = "bike_ped.geojson"
    download_bike_ped_crashes(
        all_crashes_url=CRASHES_SERVICE,
        fields=list(USE_CRASH),
        where_clause=PED_BIKE_QUERY,
        out_crs='4326',
        out_dir=out_path,
        out_name=out_name)

    # ALL RAW DATA that must be acquired as yearly chunks
    ###
    ''' download census data '''
    # download and extract census geographies
    dl_dir = makePath(RAW, "temp_downloads")
    ext_dir = makePath(RAW, "BlockGroups")
    for path in [dl_dir, ext_dir]:
        if not os.path.isdir(path):
            os.makedirs(path)
    get_one_geo_type(geo_type=CENSUS_GEO_TYPE,
                     download_dir=dl_dir,
                     extract_dir=ext_dir,
                     state=CENSUS_STATE, year='2019')
    bg_path = makePath(RAW, "BlockGroups")
    for year in YEARS:
        # setup folders
        race_out = makePath(bg_path, f"ACS_{year}_race.csv")
        commute_out = makePath(bg_path, f"ACS_{year}_commute.csv")
        print(f"Fetching race data ({race_out})")
        try:
            race = download_race_vars(year, acs_dataset="acs5", state="12", county="086")
            race.to_csv(race_out, index=False)
        except:
            print(f"ERROR DOWNLOADING RACE DATA ({year})")

        print(f"Fetching commute data ({commute_out})")
        try:
            commute = download_commute_vars(year, acs_dataset="acs5", state="12", county="086")
            commute.to_csv(commute_out, index=False)
        except:
            print(f"ERROR DOWNLOADING COMMUTE DATA ({year})")
