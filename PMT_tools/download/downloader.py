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

# helper functions from other modules
import census_geo
import census
import helper
import open_street_map

# global values configured for downloading
import PMT_tools.config.download_config as dl_conf

# PMT globals
from PMT_tools.utils import RAW, YEARS, SNAPSHOT_YEAR

# PMT Functions
from PMT_tools.utils import Timer, validate_directory, make_path, check_overwrite_path

t = Timer()


def setup_download_folder(dl_folder="RAW"):
    """creates a download folder if it doesn't already exist and populates with
    necessary folder for remaining download work
    Args:
        dl_folder (str): path download ROOT folder
    Returns:
        None
    """
    download_folder = validate_directory(dl_folder)
    for folder in dl_conf.RAW_FOLDERS:
        folder = make_path(download_folder, folder)
        if not os.path.exists(folder):
            os.mkdir(folder)


# ALL RAW DATA that must be acquired as yearly chunks
###
def download_census_geo(overwrite=True):
    """ download census data
        - downloads and unzips the census block and blockgroup shapefiles
        - downloads and writes out to table the ACS race and commute data
        - downloads LODES data to table
        Inputs:
        - RAW\\temp_downloads   (folder path)
        - RAW\\CENSUS           (extract path)
        - CENSUS_GEO_TYPES      (list of geographies)

        Outputs:
        - RAW\\CENSUS\\BG       (block groups geogrpahies)
        - RAW\\CENSUS\\TABBLOCK (block geographies)
    """
    print("\nFetching CENSUS Geographies...")
    # download and extract census geographies
    dl_dir = make_path(RAW, "temp_downloads")
    ext_dir = make_path(RAW, "CENSUS")
    for path in [dl_dir, ext_dir]:
        check_overwrite_path(output=path, overwrite=overwrite)
        validate_directory(path)
    for geo_type in dl_conf.CENSUS_GEO_TYPES:
        census_geo.get_one_geo_type(
            geo_type=geo_type,
            download_dir=dl_dir,
            extract_dir=ext_dir,
            state=dl_conf.CENSUS_STATE,
            year=str(SNAPSHOT_YEAR),
        )
    rmtree(dl_dir)


def download_race_data(overwrite=True):
    """downloads ACS race data of interest
        Inputs:
        - RAW\\CENSUS       (root census folder)
        Outputs:
        - RAW\\CENSUS\\ACS_{year}_race.csv
    """
    # download census tabular data
    census = validate_directory(make_path(RAW, "CENSUS"))
    print("RACE:")
    for year in YEARS:
        # setup folders
        race_out = make_path(census, f"ACS_{year}_race.csv")
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
    """downloads ACS commute data of interest
        Inputs:
        - RAW\\CENSUS       (root census folder)
        Outputs:
        - RAW\\CENSUS\\ACS_{year}_commute.csv
    """
    census = validate_directory(make_path(RAW, "CENSUS"))
    print("COMMUTE:")
    for year in YEARS:
        commute_out = make_path(census, f"ACS_{year}_commute.csv")
        print(f"...Fetching commute data ({commute_out})")
        try:
            commute = helper.download_commute_vars(
                year,
                acs_dataset="acs5",
                state="12",
                county="086",
                table=dl_conf.ACS_MODE_TABLE,
                columns=dl_conf.ACS_MODE_COLUMNS,
            )
            check_overwrite_path(output=commute_out, overwrite=overwrite)
            commute.to_csv(commute_out, index=False)
        except:
            print(f"..ERROR DOWNLOADING COMMUTE DATA ({year})")


def download_lodes_data(overwrite=True):
    """ download LODES data for job counts
        - downloads lodes files by year and optionally aggregates to a coarser geographic area
        Inputs:
        - RAW\\LODES       (root lodes folder)
        Outputs:
        - RAW\\LODES\\fl_wac_S000_JT00_{year}_blk.csv.gz
        - RAW\\LODES\\fl_wac_S000_JT00_{year}_bgrp.csv.gz
        - RAW\\LODES\\fl_xwalk.csv.gz
    """
    lodes_path = validate_directory(make_path(RAW, "LODES"))
    print("LODES:")
    for year in YEARS:
        census.download_aggregate_lodes(
            output_dir=lodes_path,
            file_type="wac",
            state="fl",
            segment="S000",
            part="",
            job_type="JT00",
            year=year,
            agg_geog=["bgrp"],
            overwrite=overwrite,
        )


def download_urls(overwrite=True):
    """downloads raw data that are easily accessible via web `request' at a url endpoint
        Inputs:
        - DOWNLOAD_URL_DICT         (dictionary of output_name: url found in config.download_config)
        Outputs:    (11 files)
            ['Imperviousness',
            'MD_Urban_Growth_Boundary', 'Miami-Dade_County_Boundary',
            'Municipal_Parks', 'County_Parks', 'Federal_State_Parks', 'Park_Facilities',
            'Bike_Lanes', 'Paved_Path',  'Paved_Shoulder', 'Wide_Curb_Lane']
        - RAW\\{output_name}
    """
    for file, url in dl_conf.DOWNLOAD_URL_DICT.items():
        _, ext = os.path.splitext(url)
        if ext == ".zip":
            out_file = make_path(RAW, f"{file}.zip")
        elif ext == ".geojson":
            out_file = make_path(RAW, f"{file}.geojson")
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

        Inputs:
        - RAW\\Miami-Dade_County_Boundary.geojson       (used as AOI to define area of needed data)
        - RAW\\OPEN_STREET_MAP
        Outputs:        (generally suffix will take the form q{1-4}_{year} where q indicates the quarter of the year)
        - RAW\\OPEN_STREET_MAP\\bike_{suffix)   [network]
        - RAW\\OPEN_STREET_MAP\\buildings_{suffix)  [builidng footprints]
        - RAW\\OPEN_STREET_MAP\\drive_{suffix)  [network]
        - RAW\\OPEN_STREET_MAP\\walk_{suffix)   [network]
    """
    print("Fetching OSM NETWORK data...")
    out_county = make_path(RAW, "Miami-Dade_County_Boundary.geojson")
    osm_data_dir = make_path(RAW, "OPEN_STREET_MAP")
    data_crs = 4326
    open_street_map.download_osm_networks(
        output_dir=osm_data_dir, polygon=out_county, data_crs=data_crs, suffix="q1_2021", overwrite=overwrite
    )
    print("\nFetching OSM BUILDING data...")
    open_street_map.download_osm_buildings(
        output_dir=osm_data_dir, polygon=out_county, data_crs=data_crs, suffix="q1_2021", overwrite=overwrite
    )


def run(args):
    if args.overwrite:
        overwrite = True
    if args.setup:
        setup_download_folder(dl_folder=RAW)
    if args.urls:
        download_urls(overwrite=overwrite)
    if args.osm:
        download_osm_data(overwrite=overwrite)
    if args.census_geo:
        download_census_geo(overwrite=overwrite)
    if args.commutes:
        download_commute_data(overwrite=overwrite)
    if args.race:
        download_race_data(overwrite=overwrite)
    if args.lodes:
        download_lodes_data(overwrite=overwrite)


def main():
    # todo: add more utility to this, making the download script executable
    import argparse
    parser = argparse.ArgumentParser(prog="downloader",
                                     description="Download RAW data...")
    parser.add_argument("-x", "--overwrite", dest="overwrite", action='store_false')
    parser.add_argument("-s", "--setup", action="store_false", dest="setup", )
    parser.add_argument("-u", "--urls", action="store_false", dest="urls")
    parser.add_argument("-o", "--osm", action="store_false", dest="osm")
    parser.add_argument("-g", "--census_geo", action="store_false", dest="census_geo")
    parser.add_argument("-c", "--commutes", action="store_false", dest="commutes")
    parser.add_argument("-r", "--race", action="store_false", dest="race")
    parser.add_argument("-l", "--lodes", action="store_false", dest="lodes")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    DEBUG = True
    if DEBUG:
        ROOT = r"C:\PMT_TEST_FOLDER"
        RAW = validate_directory(make_path(ROOT, "RAW"))
        YEARS = YEARS

    t.start()
    main()
    t.stop()

# TODO: Currently the download tools all function as expected but are organized poorly. The __main__
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
