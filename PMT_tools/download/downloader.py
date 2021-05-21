"""
 This module handles downloading raw data to set up raw data folder for future processing
 Data are organized based on a configuration provided in download_config.py RAW_FOLDERS

"""
# standard modules
import os
import sys
import warnings
from shutil import rmtree

warnings.filterwarnings("ignore")
from pathlib import Path
script = Path(__file__).parent.parent.absolute()
sys.path.insert(0, script)

# global values configured for downloading
from ..config import download_config as dl_conf

# PMT globals
from ..utils import RAW, YEARS

# PMT Functions
from ..utils import validate_directory, makePath, check_overwrite_path

# helper functions from other modules
import census_geo
import census
import helper
import open_street_map


def setup_download_folder(dl_folder):
    """creates a download folder if it doenst already exist and poputlates with
    necessary folder for remaining download work
    Args:
        dl_folder:
    Returns:

    """
    download_folder = validate_directory(dl_folder)
    for folder in dl_conf.RAW_FOLDERS:
        folder = makePath(download_folder, folder)
        if not os.path.exists(folder):
            os.mkdir(folder)


# ALL RAW DATA that must be acquired as yearly chunks
###
def download_census_geo(overwrite=True):
    """ download census data
        - downloads and unzips the census block and blockgroup shapefiles
        - downloads and writes out to table the ACS race and commute data
        - downloads LODES data to table
    """
    # download and extract census geographies
    dl_dir = makePath(RAW, "temp_downloads")
    ext_dir = makePath(RAW, "CENSUS")
    for path in [dl_dir, ext_dir]:
        check_overwrite_path(output=path, overwrite=overwrite)
        validate_directory(path)
    for geo_type in dl_conf.CENSUS_GEO_TYPES:
        census_geo.get_one_geo_type(
            geo_type=geo_type,
            download_dir=dl_dir,
            extract_dir=ext_dir,
            state=dl_conf.CENSUS_STATE,
            year="2019",
        )
    rmtree(dl_dir)


def download_race_data(overwrite=True):
    # download census tabular data
    census = validate_directory(RAW, "CENSUS")
    for year in YEARS:
        # setup folders
        race_out = makePath(census, f"ACS_{year}_race.csv")
        print(f"...Fetching race data ({race_out})")
        try:
            race = helper.download_race_vars(
                year,
                acs_dataset="acs5",
                state="12",
                county="086",
                table=dl_conf.ACS_RACE_TABLE,
                columns=dl_conf.ACS_RACE_COLUMNS,
            )
            check_overwrite_path(output=race_out, overwrite=overwrite)
            race.to_csv(race_out, index=False)
        except:
            print(f"..ERROR DOWNLOADING RACE DATA ({year})")


def download_commute_data(overwrite=True):
    census = validate_directory(RAW, "CENSUS")
    for year in YEARS:
        commute_out = makePath(census, f"ACS_{year}_commute.csv")
        print(f"...Fetching commute data ({commute_out})")
        try:
            commute = helper.download_commute_vars(
                year, acs_dataset="acs5", state="12", county="086",
                table=dl_conf.ACS_MODE_TABLE,
                columns=dl_conf.ACS_MODE_COLUMNS
            )
            check_overwrite_path(output=commute_out, overwrite=overwrite)
            commute.to_csv(commute_out, index=False)
        except:
            print(f"..ERROR DOWNLOADING COMMUTE DATA ({year})")


def download_lodes_data(overwrite=True):
    """ download LODES data for job counts
        - downloads lodes files by year and optionally aggregates to a coarser geographic area
    """
    lodes_path = makePath(RAW, "LODES")
    print("LODES:")
    for year in YEARS:
        census.download_aggregate_lodes(
            output_directory=lodes_path,
            file_type="wac",
            state="fl",
            segment="S000",
            part="",
            job_type="JT00",
            year=year,
            agg_geog=["bgrp"],
            overwrite=overwrite
        )


def download_urls(overwrite=True):
    """
    download impervious surface data for 2016 (most recent vintage)
        - downloads just zip file of data, prep script will unzip and subset
    download urban growth boundary and county boundary
        - downloads geojson from open data site in raw format
    download park geometry with tabular data as geojson
        - downloads geojson for Municipal, County, and State/Fed
            parks including Facility points
        - current version downloads and converts to shapefile, this step will be skipped
            in next iteration of prep script
    """
    for file, url in dl_conf.DOWNLOAD_URL_DICT.items():
        _, ext = os.path.splitext(url)
        if ext == ".zip":
            out_file = makePath(RAW, f"{file}.zip")
        elif ext == ".geojson":
            out_file = makePath(RAW, f"{file}.geojson")
        else:
            print("downloader doesnt handle that extension")
        print(f"Downloading {out_file}")
        check_overwrite_path(output=out_file, overwrite=overwrite)
        helper.download_file_from_url(url=url, save_path=out_file)


def download_osm_data(overwrite=True):
    """ download osm data - networks and buildings
        - downloads networks as nodes.shp and edges.shp
        - downloads all buildings, subset to poly/multipoly features
        - both functions will create the output folder if not there
    """
    # TODO: incorporate Aarons function to drop tiny sections of network
    # TODO: incorporate overwrite
    out_county = makePath(RAW, "Miami-Dade_County_Boundary.geojson")
    osm_data_dir = makePath(RAW, "OPEN_STREET_MAP")
    data_crs = 4326
    open_street_map.download_osm_networks(
        output_dir=osm_data_dir, polygon=out_county, data_crs=data_crs, suffix="q1_2021"
    )
    open_street_map.download_osm_buildings(
        output_dir=osm_data_dir, polygon=out_county, data_crs=data_crs, suffix="q1_2021"
    )


def parse_args(args):
    import argparse
    parser = argparse.ArgumentParser(description="Download RAW data...")
    parser.add_argument('-s', '--setup_dl_folder', default=True)
    parser.add_argument('-u', '--dl_urls', default=True)
    parser.add_argument('-o', '--dl_osm_data', default=True)
    parser.add_argument('-c', '--dl_census', default=True)
    parser.add_argument('-l', '--dl_commutes', default=True)
    args = parser.parse_args(args)
    return args


if __name__ == "__main__":
    DEBUG = True
    if DEBUG:
        ROOT = r"C:\PMT_TEST_FOLDER"
        RAW = validate_directory(makePath(ROOT, "RAW"))
        YEARS = YEARS + ["NearTerm"]
        SNAPSHOT_YEAR = 2019

    setup_download_folder(dl_folder=RAW)
    download_urls()
    download_osm_data()
    download_census_geo()
    download_commute_data()
    download_race_data()
    download_lodes_data()


# TODO: Currently the download tools all function as expected but are organized poorly. The __main__
# TODO:function should be converted to an executable and the underlying scripts reorganized once again.
# TODO:     - download_parcels.py (pull parcel geometry by year and NAL data where available), currently parcel_ftp.py

# DEPRECATED from PMT_tools.config.download_config import (RESIDENTIAL_ENERGY_CONSUMPTION_URL,
# COMMERCIAL_ENERGY_CONSUMPTION_URL)
# specialized modules
# from esridump.dumper import EsriDumper  # DEPRECATED usage given BikePed crash data unused

# def download_crashes(): # DEPRECATED
#     """ download bike/ped crashes
#         - downloads filtered copy of the FDOT crash data for MD county as geojson
#     """
#     out_path = makePath(RAW, "Safety_Security")
#     out_name = "bike_ped.geojson"
#     validate_directory(out_path)
#     download_bike_ped_crashes(all_crashes_url=dl_conf.CRASHES_SERVICE, fields="ALL",
#                               where_clause=dl_conf.PED_BIKE_QUERY, out_crs='4326',
#                               out_dir=out_path, out_name=out_name)

# def download_energy_consumption(): # DEPRECATED FROM PROJECTS
#     """ download energy consumption tables from EIA
#         - pulls the raw commercial and residential energy consumption data for the South
#         - TODO: update the URL configuration to contain each region for modularity later
#     """
#     energy_dir = validate_directory(makePath(RAW, "ENERGY_CONSUMPTION"))
#     commercial_energy_tbl = makePath(energy_dir, "commercial_energy_consumption_EIA.xlsx")
#     residential_energy_tbl = makePath(energy_dir, "residential_energy_consumption_EIA.xlsx")
#     print("ENERGY CONSUMPTION REFERENCE:")
#     for dl_url, tbl in zip([COMMERCIAL_ENERGY_CONSUMPTION_URL, RESIDENTIAL_ENERGY_CONSUMPTION_URL],
#                            [commercial_energy_tbl, residential_energy_tbl]):
#         download_file_from_url(url=dl_url, save_path=tbl)
