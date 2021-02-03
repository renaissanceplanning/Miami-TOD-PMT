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
import geopandas as gpd

from datetime import datetime
import pickle

from six import string_types

from PMT_tools.download.download_helper import validate_directory
from PMT_tools.PMT import EPSG_LL, EPSG_FLSPF, EPSG_WEB_MERC
from PMT_tools.PMT import multipolygonToPolygon, makePath

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


def calc_bbox(gdf):
    bounds = gdf.total_bounds
    return {'north': bounds[3],
            'south': bounds[1],
            'east': bounds[2],
            'west': bounds[0]}


def validate_inputs(study_area_poly=None, bbox=None, data_crs=EPSG_WEB_MERC):
    if not any([study_area_poly, bbox]):
        raise ValueError(
            "You must provide a polygon or bbox for osmnx download"
        )
    if study_area_poly is not None:
        print("...Reading and formatting provided polygons for OSMnx extraction")
        sa_gdf = gpd.read_file(study_area_poly)
        # buffer aoi
        sa_proj = sa_gdf.to_crs(epsg=EPSG_WEB_MERC)
        # Buffer AOI ~1 mile (necessary for proper composition of OSMnx networks)
        sa_buff = sa_proj.buffer(distance=1609.34).to_crs(epsg=EPSG_LL)
        return calc_bbox(gdf=sa_buff)
    elif bbox is None:
        raise ValueError(
            "One of `study_area_polygons_path` or `bbox` must be provided")
    else:
        validate_bbox(bbox)
        return bbox


def validate_network_types(network_types):
    if isinstance(network_types, string_types):
        network_types = [network_types]
    problems = [nt for nt in network_types if nt.lower() not in VALID_NETWORK_TYPES]
    if problems:
        raise ValueError("Invalid net_type specified ({})".format(problems))
    else:
        return [nt.lower() for nt in network_types]


def download_osm_networks(output_dir, polygon=None, bbox=None, data_crs=None,
                          net_types=['drive', 'walk', 'bike'], pickle_save=False, suffix=""):
    """
    Download an OpenStreetMap network within the area defined by a polygon
    feature class of a bounding box.

    Parameters
    ------------
    output_dir: Path, Path to output directory. Each modal network (specified by `net_types`)
            is saved to this directory within an epoynmous folder  as a shape file.
            If `pickle_save` is True, pickled graph objects are also stored in this directory in the
            appropriate subfolders.
    polygon: Path, default=None; Path to study area polygon(s) shapefile. If provided, the polygon
            features define the area from which to fetch OSM features and `bbox` is ignored.
            See module notes for performance and suggestions on usage.
    bbox: dict, default=None; A dictionary with keys 'south', 'west', 'north', and 'east' of
            EPSG:4326-style coordinates, defining a bounding box for the area from which to
            fetch OSM features. Only required when `study_area_polygon_path` is not provided.
            See module notes for performance and suggestions on usage.
    net_types: list, [String,...], default=["drive", "walk", "bike"]
            A list containing any or all of "drive", "walk", or "bike", specifying
            the desired OSM network features to be downloaded.
    pickle_save: boolean, default=False; If True, the downloaded OSM networks are saved as
            python `networkx` objects using the `pickle` module. See module notes for usage.
    suffix: String, default=""; Downloaded datasets may optionally be stored in folders with
            a suffix appended, differentiating networks by date, for example.

    Returns
    ---------
    G: dict; A dictionary of networkx graph objects. Keys are mode names based on
            `net_types`; values are graph objects.
    """
    # Validation of inputs
    # TODO: separate polygon and bbox validation
    bounding_box = validate_inputs(study_area_poly=polygon, bbox=bbox, data_crs=data_crs)

    # - ensure Network types are valid and formatted correctly
    net_types = validate_network_types(network_types=net_types)

    output_dir = validate_directory(output_dir)

    # Fetch network features
    mode_nets = {}
    for net_type in net_types:
        net_folder = f"{net_type}_{suffix}"
        print(f"OSMnx {net_type} network extraction")
        print("-- extracting a composed network by bounding box...")
        g = ox.graph_from_bbox(north=bounding_box["north"], south=bounding_box["south"],
                               east=bounding_box["east"], west=bounding_box["west"],
                               network_type=net_type, retain_all=True)

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


def download_osm_buildings(output_dir, polygon=None, bbox=None, data_crs=None,
                           fields=['osmid', 'building', 'name', 'geometry'], suffix=""):
    """
    Uses an Overpass query to fetch the OSM building polygons within a
    specified bounding box or the bounding box of a provided shapefile.

    Parameters
    -----------
    output_dir: Path
        Path to output directory.
    bbox: dict, default=None
        A dictionary with keys 'south', 'west', 'north', and 'east' of
        EPSG:4326-style coordinates, defining a bounding box for the area
        from which to fetch OSM features. Only required when
        `study_area_polygon_path` is not provided. See module notes for
        performance and suggestions on usage.
    fields:
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
    # TODO: separate polygon and bbox validation
    bounding_box = validate_inputs(study_area_poly=polygon, bbox=bbox, data_crs=data_crs)

    # - Output location
    output_dir = validate_directory(makePath(output_dir, f"buildings_{suffix}"))

    # Data read in and setup -------------------------------------------------
    print("...Pulling building data from Overpass API...")
    buildings_gdf = ox.geometries_from_bbox(north=bounding_box["north"],
                                            south=bounding_box["south"],
                                            east=bounding_box["east"],
                                            west=bounding_box["west"],
                                            tags={"building": True})
    # drop non-polygon features and subset fields
    print("...Dropping non-polygon features and unneeded fields")
    buildings_gdf = buildings_gdf[buildings_gdf.geom_type.isin(['MultiPolygon', 'Polygon'])]
    drop_cols = [col for col in buildings_gdf.columns if col not in fields]
    buildings_gdf.drop(labels=drop_cols, axis=1, inplace=True)
    buildings_gdf.reset_index()

    # Saving -----------------------------------------------------------------
    print("...Saving...")
    dt = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = "OSM_Buildings_{}.shp".format(dt)
    save_path = makePath(output_dir, file_name)
    buildings_gdf.to_file(save_path)
    print("-- saved to: " + save_path)

    return buildings_gdf
