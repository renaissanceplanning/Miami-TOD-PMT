"""
Comment:
    much of this code has been borrowed from: https://github.com/censusreporter/census-shapefile-utils
        - fetch_shapefiles.py
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
This script will download TIGER data shapefiles from the Census FTP site.
It can be used to download a set of geographies defined in GEO_TYPES_LIST,
or can be used to fetch files for a single state and/or single geography type.
Pass an -s argument to limit by state, pass a -g argument to limit
to a single geography type, and/or pass a -y argument to change the year
from 2012 to something else (e.g. 2015).

    >> python fetch_shapefiles.py
    >> python fetch_shapefiles.py -s WA
    >> python fetch_shapefiles.py -g place
    >> python fetch_shapefiles.py -y 2015
    >> python fetch_shapefiles.py -s WA -g place -y 2015

If you use the -s argument to fetch files for a single state, the script
will also download the national county, state and congressional district
files that include data for your chosen state.

The script will create DOWNLOAD_DIR and EXTRACT_DIR directories
if necessary, fetch a zipfile or set of zipfiles from the Census website,
then extract the shapefiles from each zipfile retrieved.

DISABLE_AUTO_DOWNLOADS will prevent certain geography types from being
automatically downloaded if no -g argument is passed to fetch_shapefiles.py.
This may be useful because certain files, such as those for Zip Code
Tabulation Areas, are extremely large. You can still target any geography
in GEO_TYPES_LIST specifically, however. So to fetch the ZCTA data:

    >> python fetch_shapefiles.py -g zcta5
"""

import os
import sys
import zipfile
from os.path import join, normpath

try:
    from six.moves.urllib import request as urllib2
except ImportError:
    import urllib2

from PMT_tools.download.__init__ import (
    STATE_ABBREV_LIST,
    GEO_TYPES_LIST,
    DISABLE_AUTO_DOWNLOADS,
    get_fips_code_for_state,
    FTP_HOME
)

__all__ = ["get_filename_list_from_ftp", "get_content_length",
           "download_files_in_list", "extract_downloaded_file",
           "get_one_geo_type", "get_all_geo_types"]


def get_filename_list_from_ftp(target, state):
    """
    Helper function to extract a list of files available from the provided FTP folder (target) by state

    Args:
        target (str): path to FTP site
        state (str): two character state abbreviation
    
    Returns:
        filename_list (list): list of filenames matching state
    """
    target_files = urllib2.urlopen(target).read().splitlines()
    filename_list = []

    for line in target_files:
        filename = f"{target}/{line.decode().split()[-1]}"
        filename_list.append(filename)

    if state:
        state_check = "_%s_" % get_fips_code_for_state(state)
        filename_list = filter(
            lambda filename: state_check in filename
            or ("_us_" in filename and "_us_zcta5" not in filename),
            filename_list,
        )

    return filename_list


def get_content_length(url):
    """
    Helper function to determine how large the item to be downloaded is
    
    Args:
        url (str): url path

    Returns:
        int: integer value of the content size
    """
    # u is returned by urllib2.urlopen
    if sys.version_info[0] == 2:
        return int(url.info().getheader("Content-Length"))
    else:
        return int(url.headers["Content-Length"])


def download_files_in_list(filename_list, download_dir, force=False):
    """
    Helper function to download list of files

    Args:
        filename_list (list): list of files
        download_dir (str): path to download directory
        force (bool): flag to force download of files in list

    Returns:
        list: list of files downloaded
    """
    downloaded_filename_list = []
    for file_location in filename_list:
        filename = os.path.join(download_dir, file_location.split("/")[-1])
        if force or not os.path.exists(filename):
            # Only download if required.
            u = urllib2.urlopen(file_location)
            f = open(filename, "wb")
            file_size = get_content_length(u)

            print(f"Downloading: {filename} Bytes: {file_size}")
            file_size_dl = 0
            block_sz = 8192
            while True:
                buffer = u.read(block_sz)
                if not buffer:
                    break

                file_size_dl += len(buffer)
                f.write(buffer)
                status = r"%10d  [%3.2f%%]" % (
                    file_size_dl,
                    file_size_dl * 100.0 / file_size,
                )
                status = status + chr(8) * (len(status) + 1)
                sys.stdout.write(status)
                sys.stdout.flush()

            f.close()
        downloaded_filename_list.append(filename)

    return downloaded_filename_list


def extract_downloaded_file(filename, extract_dir, unzip_dir, remove_on_error=True):
    """
    Helper function to extract file from zip to a new directory

    Args:
        filename (str): path to zipped file
        extract_dir (str): path to directory where zipped file will be written out when extracted
        unzip_dir (str): name of the directory files are unzipped to
        remove_on_error (bool): flag to delete failed unzipped files

    Returns:
        None
    """
    target_dir = normpath(join(extract_dir, unzip_dir))

    print("Extracting: " + filename + " ...")
    try:
        zipped = zipfile.ZipFile(filename, "r")
    except zipfile.BadZipFile as ze:
        if remove_on_error:
            os.remove(filename)
            raise Exception("Removed corrupt zip file (%s). Retry download." % filename)
        raise ze

    zipped.extractall(target_dir)
    zipped.close()


def get_one_geo_type(geo_type, download_dir=None, extract_dir=None, state=None, year="2019"):
    """
    Helper function to fetch a single geographic dataset

    Args:
        geo_type (str): one of the valid census geographies (see GEO_TYPES_DICT keys in __init__)
        download_dir (str): path to download directory
        extract_dir (str): path to directory geographic data are extracted to
        state (str): two character state abbreviation
        year (str): base year for TIGER geography, default is 2019 (latest appropriate year for ACS and LODES)

    Returns:
        None
    """
    if download_dir is None:
        download_dir = "download"
    if extract_dir is None:
        extract_dir = "extract"
    target = f"{FTP_HOME.replace('2019', year)}{geo_type.upper()}"

    print("Finding files in: " + target + " ...")
    filename_list = get_filename_list_from_ftp(target, state)
    downloaded_filename_list = download_files_in_list(filename_list, download_dir)

    for filename in downloaded_filename_list:
        extract_downloaded_file(filename, extract_dir, unzip_dir=geo_type.upper())


def get_all_geo_types(state=None, year="2019"):
    """
    Helper function to fetch all valid census geographies

    Args:
        state (str): two character state abbreviation, if none provided data will be pulled for entire US
        year (str): year of census geography, defaults is 2019

    Returns:
        None
    """
    AUTO_DOWNLOADS = filter(
        lambda geo_type: geo_type not in DISABLE_AUTO_DOWNLOADS, GEO_TYPES_LIST
    )
    for geo_type in AUTO_DOWNLOADS:
        get_one_geo_type(geo_type, state, year)
