import os
from urllib import request
import re
import requests
from requests.exceptions import RequestException
from PMT_tools.PMT import makePath, checkOverwriteOutput


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