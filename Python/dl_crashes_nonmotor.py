# -*- coding: utf-8 -*-

import sys
import requests
import json
# import geopandas as gpd
import os
from dl_config import ALL_CRASHES_SERVICE, PED_BIKE_QUERY

"""
@author: Charles Rudder
Function name:
fetch_bikeped_crashes

Description:
Uses a pointer to an OpenDataPortal Feature layer (https://gis.fdot.gov/arcgis/rest/services/Crashes_All/FeatureServer/0)
    to filter and download crash data involving pedestrians and cyclists.
    Assumes the data table column names are static.
    Download is a geojson file
    
Inputs:
transform_epsg: integer of valid EPSG for transformation of buildings.
save_directory: string of path to desired save directory.
                If None (default), no save will be completed.

Returns:
    A GeoDataFrame of bike and pedestrian crashes for a user specified year range
    By default, the CRS of the returned object will be EPSG:4326

"""

# Testing specs: Miami-Dade County
transform_epsg = None
save_directory = r"K:\Projects\MiamiDade\PMT\Data\Raw\Safety_Security"

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
def urljoin(*args):
    """
    There's probably a better way of handling this.
    """
    return "/".join(map(lambda x: str(x).rstrip('/'), args))

def build_query_request(url):
    return urljoin(url, "query")

def get_json(service_url, where="1 = 1", fields='*', count_only=False, crs='4326'):
    """
    Gets the JSON file from ArcGIS
    """
    params = {
        'where': where,
        'outFields': fields,
        'returnGeometry': True,
        'outSR': crs,
        'f': "pjson",
        'orderByFields': "",
        'returnCountOnly': count_only
    }
    response = requests.get(build_query_request(service_url), params=params)
    r_json = response.json()


    gdf = gpd.GeoDataFrame.from_features(
            r_json["features"], crs=crs)
    return gdf


# def fetch_bikeped_crashes(output_crs, output_dir):


if __name__ == "__main__":
    import sys

    sys.path.append(r'D:\Users\DE7\Downloads\arcgis_rest\arcgis')
    from arcgis_rest import ArcGIS_REST

    ALL_CRASHES_SERVICE = r'https://gis.fdot.gov/arcgis/rest/services/Crashes_All/FeatureServer'
    service = ArcGIS_REST(ALL_CRASHES_SERVICE)
    layer_id = 0
    shapes = service.get(layer_id, "COUNTY_TXT='MIAMI-DADE")