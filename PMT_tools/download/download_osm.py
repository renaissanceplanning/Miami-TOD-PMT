"""
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
import os

import osmnx as ox
import networkx as nx
import geopandas as gpd
import pandas as pd

from datetime import datetime
import pickle
from six import string_types
import functools
from PMT_tools.PMT import EPSG_LL, EPSG_FLSPF
from PMT_tools.PMT import multipolygonToPolygon

# globals for scripts
VALID_NETWORK_TYPES = ["drive", "walk", "bike"]


def validate_bbox(bbox):
    """
    Given a dictionary defining a bounding box, confirm its values are
    valid. North/south values must be between -90 and 90 (latitude);
    east/west values must be between -180 and 180 (longitude).

    Parameters
    ------------
    bbox: dict, A dictionary with keys 'south', 'west', 'north', and 'east' of EPSG:4326-style coordinates

    Returns
    --------
    None, This function simply raises exceptions if an invalid bounding box is provided

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
        values = [bbox["north"], bbox["south"], bbox["east"], bbox["west"]]
        ranges = [(-90, 90), (-90, 90), (-180, 180), (-180, 180)]
        problems = []
        for i, value in enumerate(values):
            range_ = ranges[i]
            if range_[0] < value < range_[1]:
                continue
            else:
                d = required[i]
                problems.append(f"{d}: {value}")
        if problems:
            raise ValueError(f"Invalid bounds provided - {problems}")


def validate_inputs(study_area_poly=None, bbox=None):
    if not any([study_area_poly, bbox]):
        raise ValueError(
            "You must provide some sort of geometry type for osmnx download"
        )
    if study_area_poly is not None:
        print("Reading and formatting provided polygons for OSMnx extraction")
        sa = gpd.read_file(study_area_poly)
        # ensure data are multipolygon
        sa = multipolygonToPolygon(sa)
        # Buffer polygons (necessary for proper composition of OSMnx networks)
        sa_proj = sa.to_crs(epsg=EPSG_FLSPF)
        sa_buff = sa_proj.buffer(distance=5280).to_crs(epsg=EPSG_LL)
        use_polygons = True
    elif bbox is None:
        raise ValueError(
            "One of `study_area_polygons_path` or `bbox` must be provided")
    else:
        validate_bbox(bbox)
        # And we need a dummy value for n
        sa_buff = None
        use_polygons = False
    return sa_buff, use_polygons


def validate_network_types(network_types):
    if isinstance(network_types, string_types):
        network_types = [network_types]
    problems = [nt for nt in network_types if nt.lower() not in VALID_NETWORK_TYPES]
    if problems:
        raise ValueError("Invalid net_type specified ({})".format(problems))
    else:
        return [nt.lower() for nt in network_types]


def download_osm_networks(output_dir,
                          polygon=None,
                          bbox=None,
                          net_types=['drive', 'walk', 'bike'],
                          pickle_save=False,
                          suffix=""):
    """
    Download an OpenStreetMap network within the area defined by a polygon
    feature class of a bounding box.

    Parameters
    ------------
    output_dir: Path, Path to output directory. Each modal network (specified by
        `net_types`) is saved to this directory within an epoynmous
        folder  as a shape file. If `pickle_save` is True, pickled
        graph objects are also stored in this directory in the
        appropriate subfolders.
    polygon: Path, default=None
        Path to study area polygon(s) shapefile. If provided, the polygon
        features define the area from which to fetch OSM features and `bbox`
        is ignored. See module notes for performance and suggestions on usage.
    bbox: dict, default=None
        A dictionary with keys 'south', 'west', 'north', and 'east' of
        EPSG:4326-style coordinates, defining a bounding box for the area
        from which to fetch OSM features. Only required when
        `study_area_polygon_path` is not provided. See module notes for
        performance and suggestions on usage.
    net_types: list, [String,...],
               default=["drive", "walk", "bike"]
        A list containing any or all of "drive", "walk", or "bike", specifying
        the desired OSM network features to be downloaded.
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
    """
    # Validation of inputs
    # - ensure study polygon is singlepart
    sa_buff, use_polygons = validate_inputs(study_area_poly=polygon, bbox=bbox)

    # - ensure Network types are valid and formatted correctly
    net_types = validate_network_types(network_types=net_types)

    # - Output location
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Fetch network features
    mode_nets = {}
    for net_type in net_types:
        net_folder = net_type + suffix
        print(f"OSMnx {net_type} network extraction")
        # 1. Completing the OSMnx query
        if use_polygons:
            print("-- extracting sub-networks by provided polygons...")
            n = len(sa_buff)
            graphs = []
            for i in range(n):
                print("-- -- {} of {} polygons".format(i + 1, n))
                poly = sa_buff.geometry.iloc[i]
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
        else:
            print("-- extracting a composed network by bounding box...")
            g = ox.graph_from_bbox(north=bbox["north"],
                                   south=bbox["south"],
                                   east=bbox["east"],
                                   west=bbox["west"],
                                   network_type=net_type,
                                   retain_all=True)
        # Pickle if requested
        if pickle_save:
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


def download_osm_buildings(output_dir,
                           polygon=None,
                           bbox=None,
                           fields=['osmid', 'building', 'name']):
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
    # - ensure study polygon is singlepart
    sa_buff, use_polygons = validate_inputs(study_area_poly=polygon, bbox=bbox)

    # - Output location
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Data read in and setup -------------------------------------------------
    print("Setting up data for OSM querying...")
    if use_polygons:
        n = len(sa_buff)
        buildings = []
        for i in range(n):
            print(f"-- -- {i + 1} of {n} polygons")
            poly = sa_buff.geometry.iloc[i]
            buildings.append(
                ox.geometries_from_polygon(polygon=poly, tags={"building": True})
            )
        # Compose features
        print("-- -- composing buildings")
        if len(buildings) > 1:
            buildings_gdf = gpd.GeoDataFrame(pd.concat(buildings, ignore_index=True))
        else:
            buildings_gdf = buildings[0]

    else:
        buildings_gdf = ox.geometries_from_bbox(north=bbox["north"],
                                                south=bbox["south"],
                                                east=bbox["east"],
                                                west=bbox["west"],
                                                tags={"building": True})
    # drop non-polygon features and subset fields
    drop_cols = [col for col in buildings_gdf.columns if col not in fields]
    buildings_gdf.drop(columns=drop_cols, inplace=True).reset_index()
    buildings_gdf = buildings_gdf[buildings_gdf.geom_type.isin(['MultiPolygon', 'Polygon'])]

    # Saving -----------------------------------------------------------------
    print("Saving...")
    dt = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = "OSM_Buildings_{}.shp".format(dt)
    save_path = os.path.join(output_dir, file_name)
    buildings_gdf.to_file(save_path)
    print("-- saved to: " + save_path)

    return buildings_gdf
