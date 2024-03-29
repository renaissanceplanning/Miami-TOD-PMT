"""
The `downloader` module handles downloading source data to the "Raw" data folder for
downstream processing and analysis. Data are organized into subfolders defined by the
`RAW_FOLDERS` variable in the `download_config` module. Purpose-specific download
functions are defined here that utilize methods defined more abstractly in supporting
modules, including `census_geo`, `census`, `helper`, and `open_street_map`.
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

import PMT_tools.download.census_geo as census_geo
import PMT_tools.download.census as census
import PMT_tools.download.helper as helper
import PMT_tools.download.open_street_map as open_street_map


# global values configured for downloading
import PMT_tools.config.download_config as dl_conf

# PMT globals
from PMT_tools.PMT import RAW, YEARS, SNAPSHOT_YEAR, EPSG_FLSPF

# PMT Functions
from PMT_tools.PMT import Timer, validate_directory, make_path, check_overwrite_path

t = Timer()


def setup_download_folder(dl_folder="RAW"):
    """
    Creates a download folder if it doesn't already exist and populates with
    necessary subfolders for remaining download work

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
    """
    Download census data
        - downloads and unzips the census block and blockgroup shapefiles
        - downloads and writes out to table the ACS race and commute data
        - downloads LODES data to table
        
    Inputs:
        - RAW//temp_downloads (folder path)
        - RAW//CENSUS (extract path)
        - CENSUS_GEO_TYPES (list of geographies)

    Outputs:
        - RAW//CENSUS//BG (block groups geogrpahies)
        - RAW//CENSUS//TABBLOCK (block geographies)
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
    """
    Downloads ACS race data of interest
        
    Inputs:
        - RAW//CENSUS (root census folder)

    Outputs:
        - RAW//CENSUS//ACS_{year}_race.csv
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
    """
    Downloads ACS commute data of interest
        
    Inputs:
        - RAW//CENSUS (root census folder)

    Outputs:
        - RAW//CENSUS//ACS_{year}_commute.csv
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
    """
    Download LODES data for job counts
        - downloads lodes files by year and optionally aggregates to a coarser geographic area
        
    Inputs:
        - RAW//LODES (root lodes folder)

    Outputs:
        - RAW//LODES//fl_wac_S000_JT00_{year}_blk.csv.gz
        - RAW//LODES//fl_wac_S000_JT00_{year}_bgrp.csv.gz
        - RAW//LODES//fl_xwalk.csv.gz
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
    """
    Downloads raw data that are easily accessible via web `request' at a url endpoint
        
    Inputs:
        - DOWNLOAD_URL_DICT (dictionary of output_name: url found in config.download_config)

    Outputs: (11 files)
        - RAW//{output_name} --> ['Imperviousness', 'MD_Urban_Growth_Boundary', 'Miami-Dade_County_Boundary',
        'Municipal_Parks', 'County_Parks', 'Federal_State_Parks', 'Park_Facilities',
        'Bike_Lanes', 'Paved_Path',  'Paved_Shoulder', 'Wide_Curb_Lane']
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
    """
    Download osm data - networks and buildings
        - downloads networks as nodes.shp and edges.shp
        - downloads all buildings, subset to poly/multipoly features
        - both functions will create the output folder if not there

    Inputs:
        - RAW//Miami-Dade_County_Boundary.geojson (used as AOI to define area of needed data)
        - RAW//OPEN_STREET_MAP
        
    Outputs: (generally suffix will take the form q{1-4}_{year} where q indicates the quarter of the year)
        - RAW//OPEN_STREET_MAP//bike_{suffix) [network]
        - RAW//OPEN_STREET_MAP//buildings_{suffix)[builidng footprints]
        - RAW//OPEN_STREET_MAP//drive_{suffix) [network]
        - RAW//OPEN_STREET_MAP//walk_{suffix) [network]
    """
    print("Fetching OSM NETWORK data...")
    area_of_interest = make_path(RAW, "Miami-Dade_County_Boundary.geojson")
    osm_data_dir = make_path(RAW, "OPEN_STREET_MAP")
    data_crs = EPSG_FLSPF
    open_street_map.download_osm_networks(
        output_dir=osm_data_dir, polygon=area_of_interest, data_crs=data_crs, suffix="q1_2021", overwrite=overwrite
    )
    print("\nFetching OSM BUILDING data...")
    open_street_map.download_osm_buildings(
        output_dir=osm_data_dir, polygon=area_of_interest, data_crs=data_crs, suffix="q1_2021", overwrite=overwrite
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
    parser.add_argument("-x", "--overwrite",    dest="overwrite",   action="store_false")
    parser.add_argument("-s", "--setup",        dest="setup",       action="store_false")
    parser.add_argument("-u", "--urls",         dest="urls",        action="store_true")
    parser.add_argument("-o", "--osm",          dest="osm",         action="store_false")
    parser.add_argument("-g", "--census_geo",   dest="census_geo",  action="store_false")
    parser.add_argument("-c", "--commutes",     dest="commutes",    action="store_false")
    parser.add_argument("-r", "--race",         dest="race",        action="store_false")
    parser.add_argument("-l", "--lodes",        dest="lodes",       action="store_false")
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
