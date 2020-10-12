# -*- coding: utf-8 -*-
"""
Created: October 2020
@author: Aaron Weinstock

Download OpenStreetMap Buildings
---------------------------------
Provides functions to facilitate the retreival of building footprint data from
OpenStreetMap using an Overpass query.

If run as "main", building footprints are retrieved for Miami-Dade County. 
"""
#TODO: main
#TODO: move backcasting to "prep" file?
# %% IMPORTS
import sys
import os
import geopandas as gpd
import pandas as pd
import numpy as np
import requests
from shapely.geometry.polygon import Polygon
from pyproj import CRS
from datetime import datetime
import re
import PMT
from dl_osm_networks import validateBBox

# %% TEST DATA

# Testing specs: Miami-Dade County
# study_area_polygon_path = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\Parcels_Seperate\Shapes\Miami_2019.shp"
# bbox = None
# full_data = False
# tags = ["start_date"]
# transform_epsg = None
# save_directory = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\OSM_Buildings"

# Testing specs: A 4-block area in Corvallis
# study_area_polygon_path = None
# bbox = {"south": 44.572712,
#         "west": -123.282204,
#         "north": 44.575542,
#         "east": -123.277824} 
# full_data = False
# tags = "all"
# transform_epsg = None
# save_directory = None

# %% FUNCTIONS
def fetchOsmBuildings(output_dir, study_area_polygons_path=None, bbox=None
                      full_data=False, tags="all", transform_epsg=None):
    """
    Uses an Overpass query to fetch the OSM building polygons within a
    specified bounding box or the bounding box of a provided shapefile.

    Parameters
    -----------
    output_dir: Path
        Path to output directory.
    study_area_polygon_path: Path
        Path to study area polygon(s) shapefile. If provided, the polygon
        features define the area from which to fetch OSM features and `bbox`
        is ignored. See module notes for performance and suggestions on usage.
    bbox: dict, default=None
        A dictionary with keys 'south', 'west', 'north', and 'east' of
        EPSG:4326-style coordinates, defining a bounding box for the area
        from which to fetch OSM features. Only required when
        `study_area_polygon_path` is not provided. See module notes for
        performance and suggestions on usage.
    full_data: boolean, default=False
        If True, buildings will be attributed with OSM ID, type, timestamp,
        and all OSM tags that came with the data. Otherwise, buildings will
        only include OSM ID, type, and timestamp (creation date) attributes.
    tags: [String,...], default="all"
        If `full_data` is True), this list of strings indicates the tags to
        retain in the resulting feature attributes. If "all", all tags in the
        data are returned. Ignored if `full_data` is False.
    transform_epsg: Integer, default=None
        Integer of valid EPSG code for transformation of buildings. If None and
        study area used polygons were used to define the extent of the overpass
        query, building features will be returned in the CRS of the study area
        polygons. If None and bbox is used to fetch, buildings will be
        returned in EPSG:4326, which is the OSM default.
    
    Returns
    ----------
    buildings_gdf: geodataframe
        A gdf of OSM building features. By default, the CRS of the gdf will be
        EPSG:4326 unless a tranformation is specified using `transfor_epsg` or
        a shape file with a differnt CRS is provided as 
        `study_area_polygon_path`.

    Notes
    --------
    1. OSM building polygons features will automatically be saved in the
       `output_dir`s `OSM_Buildings_{YYYYMMDDHHMMSS}.shp` where
       `YYYYMMDDHHMMSS` is the date and time at which the Overpass query was
        pushed. This is done for record keeping purposes.
    """
    # Validation of inputs
    # - study_area_polygon_path / bbox
    if study_area_polygons_path is not None:
        print("Reading and formatting provided polygons for OSMnx extraction")
        sa = gpd.read_file(study_area_polygons_path)
        # Check for multipolygons and clean up if needed
        geom_types = [geom.type for geom in sa["geometry"]]
        if "MultiPolygon" in geom_types:
            sa = PMT.multipolygonToPolygon(sa)
        # Buffer polygons (necessary for proper composition of OSMnx networks)
        sa_meters = sa.to_crs(epsg=6350)
        sa_meters["geometry"] = sa_meters.geometry.buffer(distance=1609)
        sa_buffered = sa_meters.to_crs(epsg=4326)
        use_polygons = True
    elif bbox is None:
        raise ValueError(
                "One of `study_area_polygons_path` or `bbox` must be provided")
    else:
        validateBBox(bbox)
        # And we need a dummy value for n
        use_polygons = False
    # - Tags
    if isinstance(tags, string):
        if tags != "all":
            tags = [tags]
    else:
        tags = list(tags)

    # - Transform epsg
    transform_epsg = int(transform_epsg)
    
    # - Output location
    if not os.path.exists(output_dir):
        try:
            os.mkdir(output_dir)
        except:
            raise ValueError("output_dir is invalid ({})".format(output_dir))
    
    # Data read in and setup -------------------------------------------------
    print("Setting up data for OSM querying...")
    if use_polygons:
        # Read in the study area shape (will be used for bbox)
        sa = gpd.read_file(study_area_polygon_path)
        # If the study area is not in WGS84, transform it (necessary for OSM)
        wgs84 = CRS.from_user_input(value=4326)
        study_area_crs = sa.crs
        if not study_area_crs.equals(wgs84):
            sa = sa.to_crs(wgs84)
        # Take bbox (for input to OSM query)
        bbox = sa.geometry.total_bounds
        
        # Parametrize bbox for usage in the OSM query
        south = str(bbox[1])
        west = str(bbox[0])
        north = str(bbox[3])
        east = str(bbox[2])
        _q_str_ = ",".join([south, west, north, east])
    else:
        # Convert bbox numbers to strings
        _edges_ = ["south", "west", "north", "east"]
        _bbox_ = [str(bbox[e]) for e in _edges_]
        _q_str_ = ",".join(_bbox_)
    bbox_for_query = "   ({});".format(_q_str_)
    
    # Overpass query for buildings -------------------------------------------
    # (Adapted from: https://towardsdatascience.com/loading-data-from-openstreetmap-with-python-and-the-overpass-api-513882a27fd0)
    print("Completing an Overpass query for buildings in the defined area...")
    # Organize OSM query
    overpass_url = "http://overpass-api.de/api/interpreter"
    overpass_query = '\n'.join(['\n[out:json];',
                                '(node["building"="yes"]',
                                bbox_for_query,
                                ' way["building"="yes"]',
                                bbox_for_query,
                                ' rel["building"="yes"]',
                                bbox_for_query,
                                ');',
                                'out geom meta;'])
    # Execute OSM query and return as json (record time as well)
    dt = datetime.now().strftime("%Y%m%d%H%M%S")
    response = requests.get(overpass_url,
                        params={'data': overpass_query})
    buildings_json = response.json()
    
    # Format to geopandas ----------------------------------------------------
    print("Formatting Overpass output to GeoDataFrame...")
    # TODO: OSM download to geodataframe function?
    # Shapely polygon geometry from OSM json geometries (ways only)
    polygon_geometry = []
    for bld in buildings_json["elements"]:
        # if feature is a line, make it a polygon
        if bld["type"] == "way":
            geom = pd.DataFrame(bld["geometry"])
            poly = Polygon(zip(geom["lon"], geom["lat"]))
            polygon_geometry.append(poly)
    
    # Pandas dataframe attributes from OSM id [and type and tags] (ways only)
    bid = [bld["id"] for bld in buildings_json["elements"] 
            if bld["type"] == "way"]
    bts = [bld["timestamp"] for bld in buildings_json["elements"]
            if bld["type"] == "way"]
    btype = np.repeat("way", len(bid))
    if full_data:
        btags = [bld["tags"] for bld in buildings_json["elements"]
             if bld["type"] == "way"]
        attributes = pd.DataFrame(btags).reset_index(drop=True)
        if tags != "all":
            # Check which requested tags are present
            missing = list(set(tags) - set(attributes.columns.to_list()))
            present = list(set(tags) - set(missing))
            if len(missing) > 0:
                print("-- some provided tags weren't present in the data:"
                      ', '.join([m for m in missing]))
            if len(present) == 0:
                print("None of the provdided tags were present in the data;"
                      "all tags will be returned")
            else:
                wt = np.where([t in np.array(present) for t in np.array(tags)])
                cols = np.asarray(tags)[wt].tolist()
                attributes = attributes[cols]
        attributes.insert(0, "type", btype)
        attributes.insert(0, "timestamp", bts)
        attributes.insert(0, "id", bid)
    else:
        attributes = pd.DataFrame({"id":bid, "timestamp":bts, "type":btype})

    # Create geopandas from shapely polygon geometry and pandas dataframe attributes
    buildings_gdf = gpd.GeoDataFrame(attributes,
                                     geometry = polygon_geometry)
    buildings_gdf = buildings_gdf.set_crs(wgs84)
    
    # Transform the geopandas if requested
    if transform_epsg is not None:
        if transform_epsg != 4326:
            buildings_gdf = buildings_gdf.to_crs(epsg = transform_epsg)
    elif use_polygons:
        if study_area_crs != wgs84:
            buildings_gdf = buildings_gdf.to_crs(study_area_crs)
    
     # Saving -----------------------------------------------------------------
    print("Saving...")
    file_name = "OSM_Buildings_{}.shp".format(dt)
    save_path = os.path.join(output_dir, file_name)
    buildings_gdf.to_file(save_path)
    print("-- saved to: " + save_path)

    return buildings_gdf

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# def fetchOsmBuildings(output_dir, study_area_polygons_path=None, bbox=None
#                       full_data=False, tags="all", transform_epsg=None):
#     # Validation of inputs ---------------------------------------------------
    
#     # study_area_polygon_path and bbox
#     if study_area_polygon_path is not None and bbox is not None:
#         # study_area_polygon_path and bbox can't both be provided:
#         sys.exit("'Only one of 'study_area_polygon_path' and 'bbox' should be provided; the other must be None")
#     elif study_area_polygon_path is None and bbox is None:
#         # study_area_polygon_path and bbox can't both be None:
#         sys.exit("'One (and only one) of 'study_area_polygon_path' and 'bbox' must be provided")
#     elif study_area_polygon_path is not None and bbox is None:
#         # study_area_polygon_path: must be a string, valid path, and be a .shp
#         if type(study_area_polygon_path) is not str:
#             sys.exit("'study_area_polygon_path' must be a string pointing to a shapefile of parcel polygons")
#         if not os.path.exists(study_area_polygon_path):
#             sys.exit("'study_area_polygon_path' is not a valid path")
#         if study_area_polygon_path.split(".")[-1] != "shp":
#             sys.exit("'study_area_polygon_path' does not point to a shapefile")
#     elif study_area_polygon_path is None and bbox is not None:
#         # bbox: must be dict with keys south, west, north, and east, with
#         # valid values (numbers in the proper range of coordinates) for each
#         if type(bbox) is not dict:
#             sys.exit("'bbox' must be a dictionary with keys 'south', 'west', 'north', and 'east'")
#         else:
#             k = bbox.keys()
#             missing = list(set(['south','west','north','east']) - set(k))
#             if len(missing) != 0:
#                 sys.exit(''.join(["Necessary keys missing from 'bbox'; ",
#                                   "missing keys include: ",
#                                   ', '.join([str(m) for m in missing]),
#                                   "\n"]))
#             else:
#                 try:
#                     s = bbox["south"] >= -90 and bbox["south"] <= 90
#                 except:
#                     sys.exit("'south' is not a number; all 'bbox' values must be numbers")
#                 try:
#                     w = bbox["west"] >= -180 and bbox["west"] <= 180
#                 except:
#                     sys.exit("'west' is not a number; all 'bbox' values must be numbers")
#                 try:
#                     n = bbox["north"] >= -90 and bbox["north"] <= 90
#                 except:
#                     sys.exit("'north' is not a number; all 'bbox' values must be numbers")
#                 try:
#                     e = bbox["east"] >= -180 and bbox["east"] <= 180
#                 except:
#                     sys.exit("'east' is not a number; all 'bbox' values must be numbers")
#                 dd = {"south":s, "west":w, "north":n, "east":e}
#                 inv = [k for k in dd.keys() if dd[k] == False]
#                 if len(inv) != 0:
#                     sys.exit(''.join(["The following 'bbox' entries have invalid EPSG:4326 coordinates: ",
#                                       ', '.join([i for i in inv]),
#                                       "; 'south' and 'north' must be on [-90,90], and 'west' and 'east' must be on [-180,180]"]))
         
#     # full_data: must be a logical:
#     if type(full_data) is not bool:
#         sys.exit("'full_data' must be either 'True' or 'False'")
    
#     # tags
#     if type(tags) is str:
#         if tags != "all":
#             sys.exit("'tags' must be 'all' or a list of strings indicating desired OSM tags to return with the data")
#     elif type(tags) is list:
#         s = any(type(t) is not str for t in tags)
#         if s:
#             sys.exit("'tags' must be 'all' or a list of strings indicating desired OSM tags to return with the data")
#     else:
#         sys.exit("'tags' must be 'all' or a list of strings indicating desired OSM tags to return with the data")  
            
#     # transform_epsg:
#     if transform_epsg is not None:
#         if type(transform_epsg) is not int:
#             sys.exit("'transform_epsg' must be a valid EPSG [expressed as an integer]")
    
#     # save_directory
#     if save_directory is not None:
#         if type(save_directory) is not str:
#             sys.exit("'save_directory' must be a string of the desired save location")
#         if not os.path.exists(save_directory):
#             try: 
#                 os.mkdir(save_directory)
#             except:
#                 sys.exit("'save_directory' is not a valid directory (or otherwise cannot be created)")
    
#     # Data read in and setup -------------------------------------------------
    
#     print("Setting up data for OSM querying (including reading/transforming if a path was provided)...")
    
#     if study_area_polygon_path is not None:
#         # Read in the study area shape (will be used for bbox)
#         sa = gpd.read_file(study_area_polygon_path)
        
#         # If the study area is not in WGS84, transform to that (necessary for OSM)
#         wgs84 = CRS.from_user_input(value=4326)
#         study_area_crs = sa.crs
#         if not study_area_crs.equals(wgs84):
#             sa = sa.to_crs(wgs84)
        
#         # Take bbox (for input to OSM query)
#         bbox = sa.geometry.total_bounds
        
#         # Parametrize bbox for usage in the OSM query
#         south = str(bbox[1])
#         west = str(bbox[0])
#         north = str(bbox[3])
#         east = str(bbox[2])
#         bbox_for_query = ''.join(["   (",
#                                   ','.join([south, west, north, east]),
#                                   ");"])
#     else:
#         # Convert bbox numbers to strings
#         for k in bbox.keys():
#             bbox[k] = str(bbox[k])
#         bbox_for_query = ''.join(["   (",
#                                   ','.join([bbox["south"], 
#                                             bbox["west"], 
#                                             bbox["north"], 
#                                             bbox["east"]]),
#                                   ");"])
    
#     # Overpass query for buildings -------------------------------------------
#     # (Adapted from: https://towardsdatascience.com/loading-data-from-openstreetmap-with-python-and-the-overpass-api-513882a27fd0)
    
#     print("Completing an Overpass query for buildings in the defined area...")
    
#     # Organize OSM query
#     overpass_url = "http://overpass-api.de/api/interpreter"
#     overpass_query = '\n'.join(['\n[out:json];',
#                                 '(node["building"="yes"]',
#                                 bbox_for_query,
#                                 ' way["building"="yes"]',
#                                 bbox_for_query,
#                                 ' rel["building"="yes"]',
#                                 bbox_for_query,
#                                 ');',
#                                 'out geom meta;'])
    
#     # Execute OSM query and return as json (record time as well)
#     dt = datetime.now().strftime("%Y%m%d%H%M%S")
#     response = requests.get(overpass_url, 
#                         params={'data': overpass_query})
#     buildings_json = response.json()    
    
#     # Format to geopandas ----------------------------------------------------
    
#     print("Formatting Overpass output to GeoDataFrame (including transform if requested)...")
#     # OSM download to geodataframe function?
    
#     # shapely polygon geometry from OSM json geometries (ways only)
#     polygon_geometry = []
#     for bld in buildings_json["elements"]:
#         if bld["type"] == "way":
#             geom = pd.DataFrame(bld["geometry"])
#             poly = Polygon(zip(geom["lon"], geom["lat"]))
#             polygon_geometry.append(poly)
#         else:
#             continue
    
#     # pandas dataframe attributes from OSM id [and type and tags] (ways only)
#     bid = [bld["id"] for bld in buildings_json["elements"] if bld["type"] == "way"]
#     bts = [bld["timestamp"] for bld in buildings_json["elements"] if bld["type"] == "way"]
#     btype = np.repeat("way", len(bid))
#     if full_data == True:
#         btags = [bld["tags"] for bld in buildings_json["elements"] if bld["type"] == "way"]
#         attributes = pd.DataFrame(btags).reset_index(drop=True)
#         if tags != "all":
#             missing = list(set(tags) - set(attributes.columns.to_list()))
#             present = list(set(tags) - set(missing))
#             if len(present) == 0:
#                 print("None of the provdided tags were present in the data; all tags will be returned")
#             else:
#                 wt = np.where([t in np.array(present) for t in np.array(tags)])
#                 cols = np.asarray(tags)[wt].tolist()  
#                 attributes = attributes[cols]
#             if len(missing) > 0:
#                 print("-- some provided tags weren't present in the data: " + ', '.join([m for m in missing]))
#         attributes.insert(0, "type", btype)
#         attributes.insert(0, "timestamp", bts)
#         attributes.insert(0, "id", bid)
#     else:
#         attributes = pd.DataFrame({"id":bid, "timestamp":bts, "type":btype})
        
#     # Create geopandas from shapely polygon geometry and pandas dataframe attributes
#     buildings_gdf = gpd.GeoDataFrame(attributes,
#                                      geometry = polygon_geometry)
#     buildings_gdf = buildings_gdf.set_crs(wgs84)
    
#     # Transform the geopandas if requested
#     if transform_epsg is not None:
#         if transform_epsg != 4326:
#             buildings_gdf = buildings_gdf.to_crs(epsg = transform_epsg)
#     else:
#         if study_area_polygon_path is not None:
#             buildings_gdf = buildings_gdf.to_crs(study_area_crs)
    
#      # Saving -----------------------------------------------------------------
    
#     if save_directory is not None:
#         print("Saving...")
#         file_name = ''.join(["OSM_Buildings_", dt, ".shp"])
#         save_path = os.path.join(save_directory, file_name)
#         buildings_gdf.to_file(save_path)
#         print("-- saved to: " + save_path)
    
#     # Done -------------------------------------------------------------------
    
#     print("Done!\n")
#     return buildings_gdf
    
# # ----------------------------------------------------------------------------
# # ----------------------------------------------------------------------------
# # ----------------------------------------------------------------------------

"""
Function name:
backcast_osm_buildings

Description: 
Uses the OSM start_date tag or the OSM metadata feature creation timestamp to 
backcast buildings (the output of fetch_osm_buildings) to a particular date
[and time, if desired]

Inputs:
osm_buildings: either the GeoDataFrame output of fetch_osm_polygons, or string
               of file path pointing to the shapefile of this output.
how: string of either 'timestamp' or 'start_date', defining which field will
     be used to backcast
date_time: string of date/time used to backcast the data. See 'Notes' for
           formatting
before: boolean, should buildings created before or after date_time be 
        returned?
        If True (default), every building created on or before date_time is 
        returned.
        If False, every building created after date_time is returned.
save_directory: string of path to desired save directory.
                If None (default), no save will be completed.
        
Notes:
1. date_time should be specified according to the following convention:
   If how == "timestamp":
       >> YYYY-MM-DD HH:MM:SS
   If how == "start_date":
       >> YYYY-MM-DD
   If time is provided, time should be specified in UTC, on the 24-hour clock. 
   For basic conversions from UTC to US Time Zones, note that:
       - UTC is 4 hours ahead of eastern
       - UTC is 5 hours ahead of central
       - UTC is 6 hours ahead of mountain
       - UTC is 7 hours ahead of pacific
   If you need help with conversion from your local time zone, check out:
   https://www.onlineconverter.com/time-zone
2. the file will automatically be saved as: 
       >> "OSM_Buildings_{YYYYMMDDHHMMSS}_{How}_{When}_{YYYYMMDD[HHMMSS]}.shp"
   where:
       1. the first YYYYMMDDHHMMSS is the date and time of Overpass query
          request for the original buildings data
       2. How is either "TS" (timestamp) or "SD" (start date), referencing how
          the data was filtered
       3. When is either "Bef" or "Aft", referencing whether buildings with
          dates/times before or after the filter dates/times were retained
       4. the second YYYYMMDD[HHMMSS] is the date (and time, if filtering was
          done by timestamp) used to filter the data. the time would be in UTC
   this is done for record keeping purposes.

Returns:
A date/time-filtered GeoDataFrame of OSM buildings, with the same attributes
and CRS as the input data

@author: Aaron Weinstock
"""

# Testing specs: Miami-Dade County
osm_buildings = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\OSM_Buildings\OSM_Buildings_20201001111703.shp"
how = "timestamp"
date_time = "2014-01-01 04:00:00"
before = True
save_directory = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\OSM_Buildings"

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

def backcast_osm_buildings(osm_buildings,
                           how,
                           date_time,
                           before=True,
                           save_directory=None):
    
    # Validation of inputs ---------------------------------------------------
    
    # osm_buildings: must be a string to .shp or gdf styled as above
    if type(osm_buildings) is str:
        if os.path.exists(osm_buildings):
            if osm_buildings.split(".")[-1] != "shp":
                sys.exit("'osm_buildings' is a valid file path, but does not point to a .shp")
            else:
                print("Reading OSM buildings from provided path...")
                try:
                    orig_dt = re.findall("[0-9]{14}", osm_buildings)[0]
                except:
                    orig_dt = "NoDate"
                osm_buildings = gpd.read_file(osm_buildings)
        else:
            sys.exit("'osm_buildings' is a string, but is not a valid file path")
    
    if not isinstance(osm_buildings, gpd.geodataframe.GeoDataFrame):
        sys.exit("'osm_buildings' is an object, but is not a geopandas GeoDataFrame")
    else:
        need_columns = ["id","type","timestamp"] 
        present_columns = osm_buildings.columns.to_list()       
        missing = list(set(need_columns) - set(present_columns))
        if len(missing) > 0:
            sys.exit("'osm_buildings' is not the output of the 'fetch_osm_buildings' function") 

    # how: must be 'timestamp' or 'start_date'
    if how != "timestamp" and how != "start_date":
        sys.exit("'how' must be one of 'timestamp' or 'start_date'")
    if how == "start_date" and "start_date" not in present_columns:
        sys.exit("filter on 'start_date' requested, but 'start_date' is not in the data")
    
    # date_time: according to specified format
    if type(date_time) is not str:
        sys.exit("'date_time' must be a string")
    else:
        if how == "timestamp":
            fm = bool(re.search("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$", date_time))
            if fm == False:
                sys.exit("'date_time' is improperly formatted; must be of the form YYYY-MM-DD HH:MM:SS for 'how'='timestamp'")
        else:
            fm = bool(re.search("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", date_time))
            if fm == False:
                sys.exit("'date_time' is improperly formatted; must be of the form YYYY-MM-DD for 'how'='start_date'")
       
    # before: must be boolean
    if type(before) is not bool:
        sys.exit("'full_data' must be either 'True' or 'False'")
    
    # save_directory
    if save_directory is not None:
        if type(save_directory) is not str:
            sys.exit("'save_directory' must be a string of the desired save location")
        if not os.path.exists(save_directory):
            try: 
                os.mkdir(save_directory)
            except:
                sys.exit("'save_directory' is not a valid directory (or otherwise cannot be created)")
    
    # Properly formatting our date time -------------------------------------
    
    print("Formatting date/time for filtering...")
    
    # Need to format our start_dates to match input because they can vary!
    # Of course, this is only done if we're filtering on start_date
    if how == "timestamp":
        date_time = ''.join([date_time.replace(" ","T"), "Z"])
        formatted_dt = osm_buildings.timestamp.tolist()
    else:
        sd = [str(d) for d in osm_buildings.start_date.tolist()]
        formatted_dt = []
        # There are other ways date can be specified, but I'm only concerning
        # myself with "exact dates" (not "approximations")
        # see: https://wiki.openstreetmap.org/wiki/Key:start_date for all
        # specifications of "start_date"
        if before == True:
            for i in sd:
                if bool(re.search("^[0-9]{4}$", i)) == True:
                    formatted_dt.append(''.join([i, "-12-31"]))
                elif bool(re.search("^[0-9]{4}-[0-9]{2}$", i)) == True:
                    formatted_dt.append(''.join([i, "-31"]))
                elif bool(re.search("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", i)) == True:
                    formatted_dt.append(i)
                else:
                    formatted_dt.append('0000-00-00')
            unread_count = formatted_dt.count("0000-00-00")
        else:
            for i in sd:
                if bool(re.search("^[0-9]{4}$", i)) == True:
                    formatted_dt.append(''.join([i, "-01-01"]))
                elif bool(re.search("^[0-9]{4}-[0-9]{2}$", i)) == True:
                    formatted_dt.append(''.join([i, "-01"]))
                elif bool(re.search("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", i)) == True:
                    formatted_dt.append(i)
                else:
                    formatted_dt.append("9999-99-99")
            unread_count = formatted_dt.count("9999-99-99")
        total_count = len(formatted_dt)
        percent_unread = str(np.round(unread_count / total_count, 3) * 100)
        print("-- about " + percent_unread + "% (" + str(unread_count) + " of " +
              str(total_count) + " buildings) had a missing or un-readable start_date")
        print("-- buildings with un-readable start_dates will all be retained")
    
    # Turn to array for easy processing with numpy
    formatted_dt = np.asarray(formatted_dt)
               
    # Filtering the geodataframe --------------------------------------------
    
    if before == True:
        wr = np.where(date_time >= formatted_dt)
    else:
        wr = np.where(date_time < formatted_dt)
    osm_filt = osm_buildings.loc[wr]
    
    # Saving ----------------------------------------------------------------
    
    if save_directory is not None:
        print("Saving...")
        if how == "timestamp":
            on = "TS"
        else:
            on = "SD"
        if before == True:
            t = "Bef"
        else:
            t = "Aft"
        filt_dt = '_'.join([on, t, re.sub("-|:|T|Z", "", date_time)])
        file_name = ''.join(["OSM_Buildings_", 
                             orig_dt,
                             "_",
                             filt_dt,
                             ".shp"])
        save_path = os.path.join(save_directory, file_name)
        osm_filt.to_file(save_path)
        print("-- saved to: " + save_path)
    
    # Done -------------------------------------------------------------------
    
    print("Done!\n")
    return osm_filt
    
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

        
        

    
    
    
    
# %%
