# -*- coding: utf-8 -*-

""""
Created: October 2020
@author: Charles Rudder

Download Bike and Pedestrian Crash data from FDOT open data portal
------------------------------------------------------------------
Uses a pointer to an OpenDataPortal Feature layer
    (https://gis.fdot.gov/arcgis/rest/services/Crashes_All/FeatureServer/0)
    along with a basic location filter to Miami-Dade County to filter and
    download crash data involving pedestrians and cyclists.

Assumes the data table column names are static.

Download is a geojson file

Uses this library: https://github.com/openaddresses/pyesridump to handle
the work of interacting with the ESRI feature service and extracting geojson.

If run as "main", walking and biking crashes features are downloaded for
Miami-Dade County to the RAW data folder and named bike_ped.json

"""
from config.config_downloads import (CRASHES_SERVICE, PED_BIKE_QUERY, USE_CRASH)

import json
from esridump.dumper import EsriDumper
import os
from pathlib import Path

from PMT import RAW

GITHUB = True


def dl_bike_ped_crashes(
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


if __name__ == "__main__":
    if GITHUB:
        ROOT = r'C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT\Data'
        RAW = str(Path(ROOT, 'Raw'))
    out_path = str(Path(RAW, "Safety_Security", "Crash_Data"))
    out_name = "bike_ped.geojson"
    dl_bike_ped_crashes(
        all_crashes_url=CRASHES_SERVICE,
        fields=list(USE_CRASH),
        where_clause=PED_BIKE_QUERY,
        out_crs='4326',
        out_dir=out_path,
        out_name=out_name)
