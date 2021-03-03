import os
from urllib import request
import re
import fnmatch
import requests
from requests.exceptions import RequestException
from PMT_tools.PMT import makePath, checkOverwriteOutput
import arcpy


def download_file_from_url(url, save_path, overwrite=False):
    """
    downloads file resources directly from a url endpoint to a folder
    Parameters
    ----------
    url - String; path to resource
    save_path - String; path to output file

    Returns
    -------
    None
    """

    if os.path.isdir(save_path):
        filename = get_filename_from_header(url)
        save_path = makePath(save_path, filename)
    if overwrite:
        checkOverwriteOutput(output=save_path, overwrite=overwrite)
    print(f"...downloading {save_path} from {url}")
    try:
        request.urlretrieve(url, save_path)
    except:
        with request.urlopen(url) as download:
            with open(save_path, 'wb') as out_file:
                out_file.write(download.read())


def get_filename_from_header(url):
    """
    grabs a filename provided in the url object header
    Parameters
    ----------
    url - string, url path to file on server

    Returns
    -------
    filename as string
    """
    try:
        with requests.get(url) as r:
            if "Content-Disposition" in r.headers.keys():
                return re.findall("filename=(.+)", r.headers["Content-Disposition"])[0]
            else:
                return url.split("/")[-1]
    except RequestException as e:
        print(e)


def validate_directory(directory):
    if os.path.isdir(directory):
        return directory
    else:
        try:
            os.makedirs(directory)
            return directory
        except:
            error = "--> 'directory' does not exist and cannot be created"
            return error


def validate_geodatabase(gdb_path, overwrite=False):
    exists = False
    if gdb_path.endswith(".gdb"): # TODO: else raise error?
        if os.path.isdir(gdb_path):# TODO: should this be arcpy.Exists and arcpy.Describe.whatever indicates gdb?
            exists = True
            if overwrite:
                checkOverwriteOutput(gdb_path, overwrite=overwrite)
                exists = False
    if exists:
        # If we get here, the gdb exists, and it won't be overwritten
        return gdb_path
    else:
        # The gdb does not or no longer exists and must be created
        try:
            out_path, name = os.path.split(gdb_path)
            arcpy.CreateFileGDB_management(
                out_folder_path=out_path, out_name=name[:-4])
            return gdb_path
        except:
            error = "--> 'gdb' does not exist and cannot be created" #TODO: Raise?
            return error



def validate_feature_dataset(fds_path, sr, overwrite=False):
    """
    validate that a feature dataset exists and is the correct sr, otherwise create it and return the path
    Parameters
    ----------
    fds_path: String; path to existing or desired feature dataset
    sr: arcpy.SpatialReference object

    Returns
    -------
    fds_path: String; path to existing or newly created feature dataset
    """
    try:
        # verify the path is through a geodatabase
        if fnmatch.fnmatch(name=fds_path, pat="*.gdb*"):
            if arcpy.Exists(fds_path) and arcpy.Describe(fds_path).spatialReference == sr:
                if overwrite:
                    checkOverwriteOutput(fds_path, overwrite=overwrite)
                else:
                    return fds_path
            # Snipped below only runs if not exists/overwrite and can be created.
            out_gdb, name = os.path.split(fds_path)
            out_gdb = validate_geodatabase(gdb_path=out_gdb)
            arcpy.CreateFeatureDataset_management(out_dataset_path=out_gdb, out_name=name, spatial_reference=sr)
            return fds_path
        else:
            raise ValueError

    except ValueError:
        print("...no geodatabase at that location, cannot create feature dataset")