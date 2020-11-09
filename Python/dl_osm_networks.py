# -*- coding: utf-8 -*-
"""
Created: October 2020
@author: Aaron Weinstock

Download OpenStreetMap Networks
---------------------------------
Provides functions to facilitate the retreival of travel network data from
OpenStreetMap using the `osmnx` module. Driving, walking, and/or biking
networks may be retrieved.

If run as "main", walking and biking network features are downloaded for
Miami-Dade County.
"""
# TODO: what if __name__ == __main__?

# %% IMPORTS
import sys
import os
import osmnx as ox
import networkx as nx
import geopandas as gpd
import pickle
from six import string_types
import PMT
import functools

# %% GLOBALS
SUFFIX = "_q3_2019"

# %% FUNCTIONS
def validateBBox(bbox):
    """
    Given a dictionary defining a bounding box, confirm its values are
    valid. North/south values must be between -90 and 90 (latitude);
    east/west values must be between -180 and 180 (longitude).

    Parameters
    ------------
    bbox: dict
        A dictionary with keys 'south', 'west', 'north', and 'east' of
        EPSG:4326-style coordinates

    Returns
    --------
    None
        This function simply raises exceptions if an invalid bounding
        box is provided

    Raises
    --------
    ValueError
        - If required keys are not found
        - If provided coordinate values are not within valid ranges
    """
    required = ["north", "south", "east", "west"]
    keys = list(bbox.keys())
    check = [r for r in required if r in keys]
    if check != required:
        raise ValueError("Required bbox keys missing (found {})".format(check))
    else:
        n = bbox["north"]
        s = bbox["south"]
        e = bbox["east"]
        w = bbox["west"]
        values = [n, s, e, w]
        ranges = [(-90, 90), (-90, 90), (-180, 180), (-180, 180)]
        problems = []
        for i, value in enumerate(values):
            range_ = ranges[i]
            if range_[0] < value < range_[1]:
                continue
            else:
                d = required[i]
                problems.append("{}: {}".format(d, value))
        if problems:
            raise ValueError("Invalid bounds provided - {}".format(problems))


def fetchOsmNetworks(output_dir, study_area_polygons_path=None, bbox=None,
                     net_types=['drive','walk','bike'], pickle_save=False,
                     suffix=""):
    """
    Download an OpenStreetMap network within the area defined by a polygon
    feature class of a bounding box.

    Parameters
    ------------
    output_dir: Path
        Path to output directory. Each modal network (specified by 
        `net_types`) is saved to this directory within an epoynmous
        folder  as a shape file. If `pickle_save` is True, pickled
        graph objects are also stored in this directory in the 
        appropriate subfolders.
    study_area_polygon_path: Path, default=None
        Path to study area polygon(s) shapefile. If provided, the polygon
        features define the area from which to fetch OSM features and `bbox`
        is ignored. See module notes for performance and suggestions on usage.
    bbox: dict, default=None
        A dictionary with keys 'south', 'west', 'north', and 'east' of
        EPSG:4326-style coordinates, defining a bounding box for the area
        from which to fetch OSM features. Only required when
        `study_area_polygon_path` is not provided. See module notes for
        performance and suggestions on usage.
    net_types: [String,...], default=["drive", "walk", "bike"]
        A list containing any or all of "drive", "walk", or "bike", specifying
        the desired OSM network features to be downlaoded.
    pickle_save: boolean, default=False
        If True, the downloaded OSM networks are saved as python `networkx`
        objects using the `pickle` module. See module notes for usage.
    suffix: String, default=""
        Downloaded datasets may optionally be stored in folders with a suffix
        appended, differentiating networks by date, for example.
    
    Returns
    ---------
    G: dict
        A dictionary of networkx graph objects. Keys are mode names based on
        `net_types`; values are graph objects.

    Notes:
    ----------
    1. Experientially, osmnx network pulls tend to run rather slowly for areas
    larger than a typical US county. If the study area is larger than a county,
    it is recommended to provide a "study_area_polygons_path", to a shapefile
    with the study area broken down into smaller patches (e.g. component
    counties or tracts). A network will then be composed from individual
    extractions of the component geometries in the shapefile, which will likely
    be more performant than a singular network query. If your study area
    is roughly the size of a county or less, it is recommended to use "bbox",
    or provide a "study_area_polygons_path" with just one polygon, as this will
    likely be more performant than the multiple extractions/composition method.

    2. If pickle_save is True, the following will be saved:
        - A networkx graph object of each modal network for the entire coverage
        area specified by function inputs.
        - If a polygon feature class with multiple polygons is used to define
        the processing extents, a pickled dictionary for each mode, containing
        polygon object ID's as keys and localized graph objects as values.

    'pickle'-saved objects can be read back into python as python objects,
    which may be convenient if you'd like to do future processing on these
    networks in python. The networks are pickled as networkx graphs.
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

    # - Network types
    if isinstance(net_types, string_types):
        net_types = [net_types]
    valid_net_types = ["drive", "walk", "bike"]
    problems = [nt for nt in net_types if nt.lower() not in valid_net_types]
    if problems:
        raise ValueError("Invalid net_type specified ({})".format(problems))
    else:
        net_types = [nt.lower() for nt in net_types]
    
    # - Output location
    if not os.path.exists(output_dir):
        try:
            os.mkdir(output_dir)
        except:
            raise ValueError("output_dir is invalid ({})".format(output_dir))

    # Fetch network features
    mode_nets = {}
    for net_type in net_types:
        net_folder = net_type + suffix
        print("OSMnx " + net_type + " network extraction")
        # 1. Completing the OSMnx query
        if use_polygons:
            print("-- extracting sub-networks by provided polygons...")
            n = len(sa_buffered)
            graphs = []
            for i in range(n):
                print("-- -- {} of {} polygons".format(i + 1, n))
                poly = sa_buffered.geometry.iloc[i]
                graphs.append(
                    ox.graph_from_polygon(
                        poly, network_type=net_type, retain_all=True
                        )
                )
            # Compose features
            print("-- -- composing network")
            if len(graphs) > 1:
                g = functools.reduce(nx.compose(*graphs))
            else:
                g = graphs[0]
            
            # Pickle if requested
            #TODO: confirm pickling sub-networks is desirable
            if pickle_save == True:
                print("-- saving the extracted networks as pickle")
                out_f = os.path.join(
                    output_dir, net_folder, "osmnx_graph_dict.p")
                with open(out_f, "wb") as pickle_file:
                    pickle.dump(graphs, pickle_file)
                print("---- saved to: " + out_f)
        else:
            print("-- extracting a composed network by bounding box...")
            g = ox.graph_from_bbox(north=bbox["north"],
                                   south=bbox["south"],
                                   east=bbox["east"],
                                   west=bbox["west"],
                                   network_type=net_type,
                                   retain_all=True)
        # Pickle if requested
        if pickle_save == True:
            print("-- saving the composed network as pickle")
            out_f = os.path.join(output_dir, net_folder, "osmnx_composed_net.p")
            with open(out_f, 'wb') as pickle_file:
                pickle.dump(g, pickle_file)
            print("---- saved to: {}".format(out_f))

        # 2. Saving as shapefile
        print("-- saving network shapefile...")
        out_f = os.path.join(output_dir, net_folder)
        ox.save_graph_shapefile(G=g, filepath=out_f)
        # need to change this directory
        print("---- saved to: " + out_f)
            
        # 3. Add the final graph to the dictionary of networks
        mode_nets[net_type] = g
    return mode_nets

# %% MAIN
if __name__ == "__main__":
    # Fetch current OSM walk and bike networks for Miami-Dade County
    output_dir = PMT.makePath(PMT.RAW, "osm_networks")
    sa_polys = PMT.makePath(PMT.RAW, "CensusGeo", "MiamiDadeBoundary.shp")
    fetchOsmNetworks(output_dir, study_area_polygons_path=sa_polys, bbox=None,
                     net_types=["walk", "bike"], pickle_save=False)


# %% PREVIOUS
# def fetchOsmNetworks(output_dir, study_area_polygons_path=None, bbox=None,
#                      net_types=['drive','walk','bike'], pickle_save=False):
    # """
    # Download an OpenStreetMap network within the area defined by a polygon
    # feature class of a bounding box.

    # Parameters
    # ------------
    # output_dir: Path



    #     Path to output directory. Each element in 'net_types' will get its own
    #     epoynmous folder in the save directory containing its outputs.
    #             Must be provided if pickle_save is True.
    #             If None (default), no save will be completed.




    # study_area_polygon_path: Path, default=None
    #     Path to study area polygon(s) shapefile. If provided, the polygon
    #     features define the area from which to fetch OSM features and `bbox`
    #     is ignored. See module notes for performance and suggestions on usage.
    # bbox: dict, default=None
    #     A dictionary with keys 'south', 'west', 'north', and 'east' of
    #     EPSG:4326-style coordinates, defining a bounding box for the area
    #     from which to fetch OSM features. Only required when
    #     `study_area_polygon_path` is not provided. See modeul notes for
    #     performance and suggestions on usage.
    # net_types: [String,...], default=["drive", "walk", "bike"]
    #     A list containing any or all of "drive", "walk", or "bike", specifying
    #     the desired OSM network features to be downlaoded.
    # pickle_save: boolean, default=False
    #     If True, the downloaded OSM networks are saved as python `networkx`
    #     objects using the `pickle` module. See module notes for usage.
    
    # """

    # # Validation of inputs 
    # # - study_area_polygon_path and bbox
    # if study_area_polygons_path is not None:
    #     use_polygons = True
    # elif bbox is None:
    #     raise ValueError("One of `study_area_polygons_path` or `bbox` must be provided")
    # else:
    #     validateBBox(bbox)
    #     use_polygons = False
    # # - Network types
    # if isinstance(net_types, string_types):
    #     net_types = [net_types]
    # valid_net_types = ["drive", "walk", "bike"]
    # problems = [nt for nt in net_types if nt.lower() not in valid_net_types]
    # if problems:
    #     raise ValueError("Invalide net_type specified ({})".format(problems))
    # else:
    #     net_types = [nt.lower() for nt in net_types]

    ##### 
    # TODO: Drop upon confirmation that the above appropriately handles inputs
    # elif study_area_polygons_path is None and bbox is None:
    #     # study_area_polygons_path and bbox can't both be None:
    #     sys.exit("'One (and only one) of 'study_area_polygons_path' and 'bbox' must be provided")
    # elif study_area_polygons_path is not None and bbox is None:
    #     # study_area_polygons_path: must be a string, valid path, and be a .shp
    #     if type(study_area_polygons_path) is not str:
    #         sys.exit("'study_area_polygons_path' must be a string pointing to a shapefile of parcel polygons")
    #     if not os.path.exists(study_area_polygons_path):
    #         sys.exit("'study_area_polygons_path' is not a valid path")
    #     if study_area_polygons_path.split(".")[-1] != "shp":
    #         sys.exit("'study_area_polygons_path' does not point to a shapefile")
    # elif study_area_polygons_path is None and bbox is not None:
    #     # bbox: must be dict with keys south, west, north, and east, with
    #     # valid values (numbers in the proper range of coordinates) for each
    #     if type(bbox) is not dict:
    #         sys.exit("'bbox' must be a dictionary with keys 'south', 'west', 'north', and 'east'")
    #     else:
    #         k = bbox.keys()
    #         missing = list(set(['south','west','north','east']) - set(k))
    #         if len(missing) != 0:
    #             sys.exit(''.join(["Necessary keys missing from 'bbox'; ",
    #                               "missing keys include: ",
    #                               ', '.join([str(m) for m in missing]),
    #                               "\n"]))
    #         else:
    #             try:
    #                 s = bbox["south"] >= -90 and bbox["south"] <= 90
    #             except:
    #                 sys.exit("'south' is not a number; all 'bbox' values must be numbers")
    #             try:
    #                 w = bbox["west"] >= -180 and bbox["west"] <= 180
    #             except:
    #                 sys.exit("'west' is not a number; all 'bbox' values must be numbers")
    #             try:
    #                 n = bbox["north"] >= -90 and bbox["north"] <= 90
    #             except:
    #                 sys.exit("'north' is not a number; all 'bbox' values must be numbers")
    #             try:
    #                 e = bbox["east"] >= -180 and bbox["east"] <= 180
    #             except:
    #                 sys.exit("'east' is not a number; all 'bbox' values must be numbers")
    #             dd = {"south":s, "west":w, "north":n, "east":e}
    #             inv = [k for k in dd.keys() if dd[k] == False]
    #             if len(inv) != 0:
    #                 sys.exit(''.join(["The following 'bbox' entries have invalid EPSG:4326 coordinates: ",
    #                                   ', '.join([i for i in inv]),
    #                                   "; 'south' and 'north' must be on [-90,90], and 'west' and 'east' must be on [-180,180]"]))
    #
    # # net_type: must be string with elements drive, walk, or bike
    # if type(net_types) is not list:
    #     sys.exit("'net_type' must be a list containing any or all of 'drive', 'walk', or 'bike'")
    # ts = [t in ['drive','walk','bike'] for t in net_types]
    # if False in ts:
    #     sys.exit("'net_types' can only contain 'drive', 'walk', or 'bike'")
    # pickle_save: must be a logical:
    # if type(pickle_save) is not bool:
    #     sys.exit("'pickle_save' must be either 'True' or 'False'")
        
    # # save_directory
    # if save_directory is not None:
    #     if type(save_directory) is not str:
    #         sys.exit("'save_directory' must be a string of the desired save location")
    #     if not os.path.exists(save_directory):
    #         try: 
    #             os.mkdir(save_directory)
    #         except:
    #             sys.exit("'save_directory' is not a valid directory (or otherwise cannot be created)") 
    # else:
    #     if pickle_save == True:
    #         sys.exit("If 'pickle_save' is True, 'save_directory' must be provided")
        
    # Set up for osmnx query -------------------------------------------------
        
    # If it's a polygon, read it in and set it up.
    # If it's a bbox, we don't need to do anything to it.
    # if study_area_polygons_path is not None:
    #     print("Reading and reformatting the provided polygons for OSMnx extraction...\n")
    #     # Convert any multipolygons to single polygons (for OSMnx extraction)
    #     sa = gpd.read_file(study_area_polygons_path)
    #     geom_types = [geom.type for geom in sa["geometry"]]
    #     if "MultiPolygon" in geom_types:
    #         # SUB IN YOUR NEW FUNCTION ITS BETTER AND FASTER
    #         # Adapted from https://gist.github.com/mhweber/cf36bb4e09df9deee5eb54dc6be74d26
    #         def multipolygon_to_polygon(gdf):
    #             poly_df = gpd.GeoDataFrame(columns=gdf.columns)
    #             for idx, row in gdf.iterrows():
    #                 if type(row.geometry) == Polygon:
    #                     poly_df = poly_df.append(row, ignore_index=True)
    #                 else:
    #                     mult_df = gpd.GeoDataFrame(columns=gdf.columns)
    #                     recs = len(row.geometry)
    #                     mult_df = mult_df.append([row]*recs, ignore_index=True)
    #                     for geom in range(recs):
    #                         mult_df.loc[geom,"geometry"] = row.geometry[geom]
    #                     poly_df = poly_df.append(mult_df, ignore_index=True)
    #             return poly_df
    #         sa = multipolygon_to_polygon(sa)
    #    
    #     # Buffer polygons (necessary for proper composition of OSMnx networks)        
    #     sa_meters = sa.to_crs(epsg = 6350)
    #     sa_meters["geometry"] = sa_meters.geometry.buffer(distance=1609)
    #     sa_buffered = sa_meters.to_crs(epsg = 4326)
    #     n = len(sa_buffered.index)
    # else:
    #     # we need a dummy value for n
    #     n = 999
    
    # # Loop through network options, extracting and saving networks -----------
    
    # # Dictionary for graphs, which we'll add to iteratively
    # ono = dict()
    
    # for net_type in net_types:
    #     print("OSMnx " + net_type + " network extraction")
    
    #     # 0. set up the save directory 
        
    #     net_save_directory = os.path.join(save_directory, net_type)
    #     if not os.path.exists(net_save_directory):
    #         os.mkdir(net_save_directory)
    
    #     # 1. Completing the OSMnx query
        
    #     if study_area_polygons_path is not None and n > 1:
    #         # Pull the networks by component polygons into a dictionary
    #         print("-- extracting sub-networks by provided polygons...")
    #         graphs = {}
    #         for i in range(n):
    #             k = str(i+1)
    #             prog = '/'.join([k,str(n)])
    #             print("---- extracting for polygon " + prog)
    #             key = str(i)
    #             poly = sa_buffered['geometry'].iloc[i]
    #             graphs[key] = ox.graph_from_polygon(poly,
    #                                                 network_type = net_type,
    #                                                 retain_all = True)
    #         # Pickle if requested
    #         if pickle_save == True:
    #             print("-- saving the extracted networks as pickle")
    #             fd_file_name = '_'.join([net_type, "osmnx_graph_dictionary.p"])
    #             fp_full_dictionary = os.path.join(net_save_directory,
    #                                               fd_file_name)
    #             with open(fp_full_dictionary, 'wb') as pickle_file:
    #                 pickle.dump(graphs, pickle_file)
    #             print("---- saved to: " + fp_full_dictionary)
    #         # Compose the network
    #         print("-- composing " + str(n) + " sub-networks...")
    #         k1 = '/'.join([str(1),str(n)])
    #         k2 = '/'.join([str(2),str(n)])
    #         print("---- composing sub-networks " + k1 + " and " + k2)
    #         g = nx.compose(graphs['0'], graphs['1'])
    #         i = 2
    #         while i < n:
    #             k = str(i+1)
    #             prog = '/'.join([k,str(n)])
    #             print("---- composing sub-network " + prog)
    #             key = str(i)
    #             g = nx.compose(g, graphs[key])
    #             i += 1
    #         # Pickle if requested
    #         if pickle_save == True:
    #             print("-- saving the composed network as pickle")
    #             fn_file_name = '_'.join([net_type, "osmnx_composed_network.p"])
    #             fp_full_networkx = os.path.join(net_save_directory,
    #                                             fn_file_name)
    #             with open(fp_full_networkx, 'wb') as pickle_file:
    #                 pickle.dump(g, pickle_file)
    #             print("---- saved to: " + fp_full_networkx)
    #     elif study_area_polygons_path is not None and n == 1:
    #         # Pull the networks by single polygon
    #         print("-- extracting a composed network by polygon...")
    #         g = ox.graph_from_polygon(sa_buffered["geometry"],
    #                                   network_type = net_type,
    #                                   retain_all = True)
    #         # Pickle if requested
    #         if pickle_save == True:
    #             print("-- saving the composed network as pickle")
    #             fn_file_name = '_'.join([net_type, "osmnx_composed_network.p"])
    #             fp_full_networkx = os.path.join(net_save_directory,
    #                                             fn_file_name)
    #             with open(fp_full_networkx, 'wb') as pickle_file:
    #                 pickle.dump(g, pickle_file)
    #             print("---- saved to: " + fp_full_networkx)
    #     else:
    #         # Pull the networks by bbox
    #         print("-- extracting a composed network by bounding box...")
    #         g = ox.graph_from_bbox(north = bbox["north"],
    #                                south = bbox["south"],
    #                                east = bbox["east"],
    #                                west = bbox["west"],
    #                                network_type = net_type,
    #                                retain_all = True)
    #         # Pickle if requested
    #         if pickle_save == True:
    #             print("-- saving the composed network as pickle")
    #             fn_file_name = '_'.join([net_type, "osmnx_composed_network.p"])
    #             fp_full_networkx = os.path.join(net_save_directory,
    #                                             fn_file_name)
    #             with open(fp_full_networkx, 'wb') as pickle_file:
    #                 pickle.dump(g, pickle_file)
    #             print("---- saved to: " + fp_full_networkx)
            
    #     # 2. Saving as shapefile
    
    #     if save_directory is not None:
    #         print("-- saving network shapefile...")
    #         save_path = os.path.join(net_save_directory, 
    #                                  net_type)
    #         ox.save_graph_shapefile(G = g, 
    #                                 filepath = save_path)
    #         # need to change this directory
    #         print("---- saved to: " + save_path)
            
    #     # 3. Add the final graph to the dictionary of networks
        
    #     ono[net_type] = g
    #     print("\n")
    
    # # Done -------------------------------------------------------------------
    
    # print("Done!\n")
    # return ono
    
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------