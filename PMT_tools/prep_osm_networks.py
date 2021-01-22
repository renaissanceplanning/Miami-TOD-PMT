"""
Created: October 2020
@Author: Alex Bell

Functions to create network datasets from raw OSM line features obtained
using the `dl_osm_networks` script. Establishes a functional approach to
standardize the components of network dataset development and configuration.
Consistent schemas allow network dataset templates to be used for building
similar network datasets with different OSM download vintages.

This makes analytical replication as well as refinements and updates to
network attributes and parameters over time relatively simple.

If run as "main", osm features from the RAW data folder are pushed to a
new file geodatabase in the CLEANED data folder and templates from the REF
folder are used to construct walk and bike networks.
"""
# TODO: confirm docstrings
# TODO: print status messages

# %% IMPORTS
import arcpy
import PMT
from six import string_types
import os
import shutil

# %% GLOBALS
NET_VERSIONS = ["_q3_2019"]

# %% FUNCTIONS
def classifyBikability(bike_edges):
    """
    Adds two fields to cleaned bike edge features: "bikability" and
    "cycleway". The former assigns a "level of traffic stress" (LTS)
    score to each facility based on its facility type (least comfortable
    facilties score lowest on a range from 1 to 4). The latter field
    tags facilities with cycleway facilities on them. These details are
    used in building and solving the biking network dataset.

    Parameters
    -----------
    bike_edges: Path

    Returns
    -------
    bike_edges: Path
    """
    print("...enriching bike network features")
    arcpy.AddField_management(bike_edges, "bikability", "LONG")
    # Default: moderately comfortable facilities (LTS = 3)
    arcpy.CalculateField_management(bike_edges, "bikability", "3")
    be = arcpy.MakeFeatureLayer_management(bike_edges, "__bike_edges__")
    # Select by criteria
    # - Least comfortable facilities (LTS=1)
    wc = r"highway LIKE '%runk%' OR highway LIKE '%rimary%'"
    arcpy.SelectLayerByAttribute_management(be, "NEW_SELECTION",
                                            where_clause=wc)
    arcpy.CalculateField_management(be, "bikability", "1")
    # - Uncomfortable facilities (LTS=2)
    wc = r"highway LIKE '%econdary%'"
    arcpy.SelectLayerByAttribute_management(be, "NEW_SELECTION",
                                            where_clause=wc)
    arcpy.CalculateField_management(be, "bikability", "2")
    # - Comfortable facilities (LTS = 4)
    wc = r"highway LIKE '%ycleway%' OR highway LIKE '%iving%' OR highway LIKE '%esidential%' OR highway LIKE '%ath%'"
    arcpy.SelectLayerByAttribute_management(be, "NEW_SELECTION",
                                            where_clause=wc)
    arcpy.CalculateField_management(be, "bikability", "4")
    
    # Tag cycleways
    arcpy.AddField_management(bike_edges, "cycleway", "LONG")
    arcpy.CalculateField_management(bike_edges, "cycleway", "0")
    wc = r"highway LIKE '%ycleway%'"
    arcpy.SelectLayerByAttribute_management(be, "NEW_SELECTION",
                                            where_clause=wc)
    arcpy.CalculateField_management(be, "cycleway", "1")

    # Delete feature layer
    arcpy.Delete_management(be)
    return bike_edges


def makeCleanNetworkGDB(clean_path, gdb_name="osm_networks"):
    """
    Creates a file geodatabase in the "cleaned" data directory within which to
    build network datasets for walking and biking analyses. If the gdb already 
    exists, it is deleted and replaced.

    Parameters
    ------------
    clean_path: Path
        A path the cleaned data directory where the network data will be built.
    gdb_name: String, default="osm_networks"
        The name of the new geodatabase to be created.

    Returns
    -------
    out_gdb: geodatabase
        Creates a new geodabase at `{clean_path}/{gdb_name}`
    """
    # Handle inputs
    if gdb_name[-4:] != ".gdb":
        gdb_name = gdb_name + ".gdb"
    # Delete the gdb if it exists
    gdb_path = PMT.makePath(clean_path, gdb_name)
    if arcpy.Exists(gdb_path):
        arcpy.Delete_management(gdb_path)
    # Create the new gdb
    print(f"...creating geodatabase {clean_path}\\{gdb_name}")
    return arcpy.CreateFileGDB_management(clean_path, gdb_name)


def makeNetFeatureDataSet(gdb_path, name, sr):
    """
    Make a feature dataset in a geodatbase.

    Parameters
    ---------------
    gdb_path: Path
    name: String
    sr: Spatial Reference

    Returns
    --------
    net_fd: Feature dataset
        Creates a new feature dataset at `{gdb_path}/{name}`
    """
    sr = arcpy.SpatialReference(sr)
    print(f"... ...creating feature dataset {name}")
    return arcpy.CreateFeatureDataset_management(gdb_path, name, sr)


def importOSMShape(osm_fc, to_feature_dataset, fc_name=None,
                   overwrite=False, wc=None, field_mapping=None):
    """
    A simple function to facilitate the transfer of osm features in a
    shapefile to a feature dataset for use in a network dataset.

    osm_fc: Path
        The path to the osm features to be added to the feature dataset.
    to_feature_dataset: Path
        The path to the output feature dataset where network features are
        stored and a network dataset will eventually be constructed.
    fc_name: String, default=None
        The name of the output feature class to be created. If None, the
        name will match that if `osm_fc`.
    Overwrite: Boolean, default=False
        If True, if there is already a feature class in `to_feature_dataset`
        with the same `fc_name` (as provided or implied by `osm_fc`), it will
        be deleted and replaced.
    wc: String, default=None
        A where clause to only transfer select features from `osm_fc` to
        the output feature class.
    field_mapping: FieldMappings
        An arcpy `FieldMappings` object that handles field naming, merging,
        etc.

    Returns
    -------
    net_source_fc: 
        Saves a new feature class in `to_feature_dataset`, transfering
        features from `osm_fc`.
    """
    # Set appropriate output fc name
    if fc_name is None:
        fc_name = osm_fc.rsplit('\\',1)[1]
        fc_name = fc_name.rsplit('.', 1)[0]
    # Check if output already exists
    out_path = PMT.makePath(str(to_feature_dataset), fc_name)
    if arcpy.Exists(out_path):
        if overwrite:
            arcpy.Delete_management(out_path)
        else:
            raise RuntimeError(
                "Feature class {} already exists".format(out_path))
    # Transfer data
    print(f"...copying features {osm_fc} to {to_feature_dataset}")
    return arcpy.FeatureClassToFeatureClass_conversion(
        osm_fc, to_feature_dataset, fc_name, wc, field_mapping)


def makeNetworkDataset(template_xml, out_feature_dataset, net_name="osm_ND"):
    """
    Make a network dataset from a template xml file. The features in
    `out_feature_dataset` must be consistent with those used in
    the network dataset used to creat the xml template.

    Parameters
    ------------
    template_xml: Path
        Path to a network dataset xml template
    out_feature_dataset: Path
    net_name: String, default="osm_ND"

    Returns
    ---------
    nd: Network dataset
        Creates a network dataset in `out_feature_dataset` based on the
        specifications in `template_xml`.

    See Also
    ---------
    makeNetworkDatasetTemplate
    """
    print("...applying network template")
    arcpy.na.CreateNetworkDatasetFromTemplate(
        template_xml, out_feature_dataset)
    print("...building network dataset")
    arcpy.na.BuildNetwork(
        os.path.join(
            str(out_feature_dataset), str(net_name)
        )
    )
    # save build errors
    print("...saving build errors")
    out_gdb = os.path.split(str(out_feature_dataset))[0]
    out_dir, out_name = os.path.split(out_gdb)
    temp_dir = os.environ.get("TEMP")
    if temp_dir:
        shutil.copyfile(os.path.join(temp_dir, "BuildErrors.txt"), 
                        os.path.join(out_dir, f"BuildErrors_{out_name}.txt"))


def makeNetworkDatasetTemplate(from_nd, template_xml):
    """
    Make a network dataset template from an existing network dataset. The
    template can be used to construct new network datasets using the same
    specifications later.

    Parameters
    -----------
    from_nd: Path
        Path to the existing network dataset
    template_xml: Path
        Path to an output template (xml) file.
    
    Returns
    ---------
    nd_template: xml
        Creates a network dataset xml template at the path specified by
        `template_xml`

    See Also
    ---------
    makeNetworkDataset
    """
    print("...creating network dataset template")
    return arcpy.na.CreateTemplateFromNetworkDataset_na(
        network_dataset=from_nd, output_network_dataset_template=template_xml)


# %% MAIN
if __name__ == "__main__":
    # Setup variables
    sr = 2881

    # Make gdb
    net_path = PMT.makePath(PMT.CLEANED, "OSM_Networks")

    for net_version in NET_VERSIONS:
        print(net_version)
        # Bike
        bike_name = f"bike{net_version}"
        bike_gdb = makeCleanNetworkGDB(net_path, gdb_name=bike_name)
        bike_fd = makeNetFeatureDataSet(bike_gdb, "osm", sr)
        
        # Walk
        walk_name = f"walk{net_version}"
        walk_gdb = makeCleanNetworkGDB(net_path, gdb_name=walk_name)
        walk_fd = makeNetFeatureDataSet(walk_gdb, "osm", sr)

        # Import edges
        osm_raw = PMT.makePath(PMT.RAW, "osm_networks")
        bike_raw = PMT.makePath(osm_raw, bike_name, "edges.shp")
        walk_raw = PMT.makePath(osm_raw, walk_name, "edges.shp")
        
        # Transfer features
        bike_edges = importOSMShape(bike_raw, bike_fd, overwrite=True)
        walk_edges = importOSMShape(walk_raw, walk_fd, overwrite=True)

        # Enrich features
        classifyBikability(bike_edges)

        # Build network datasets
        bike_template=PMT.makePath(PMT.REF, "osm_bike_template.xml")
        walk_template=PMT.makePath(PMT.REF, "osm_walk_template.xml")
        bike_net = makeNetworkDataset(bike_template, bike_fd, "osm_ND")
        walk_net = makeNetworkDataset(walk_template, walk_fd, "osm_ND")
