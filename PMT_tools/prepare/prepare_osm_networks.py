"""
The `prepare_osm_networks` module is similar to `prepare_helpers` except that the classes and functions
defined here focus specificalluy on creating network datasets from raw OSM line features obtained
using the `downloader` module. This module establishes a functional approach to standardize the components
of network dataset development and configuration. Consistent schemas allow network dataset templates to be
used for building similar network datasets with different OSM download vintages. This makes analytical 
replication as well as refinements and updates to network attributes and parameters over time relatively
simple.
"""
# TODO: confirm docstrings
# TODO: print status messages

import os
import shutil

# %% IMPORTS
import arcpy

from PMT_tools import PMT as PMT

# %% GLOBALS
NET_VERSIONS = ["_q1_2021"]
BIKABILITY_CLASSES = [  # where clause, expression
    (r"highway LIKE '%runk%' OR highway LIKE '%rimary%'", "1"),
    (r"highway LIKE '%econdary%'", "2"),
    (r"highway LIKE '%ycleway%' OR highway LIKE '%iving%' OR highway LIKE '%esidential%' OR highway LIKE '%ath%'", "4"),
]


# %% FUNCTIONS
def classify_bikability(bike_edges):
    """
    Adds two fields to cleaned bike edge features: "bikability" and
    "cycleway". The former assigns a "level of traffic stress" (LTS)
    score to each facility based on its facility type (least comfortable
    facilties score lowest on a range from 1 to 4). The latter field
    tags facilities with cycleway facilities on them. These details are
    used in building and solving the biking network dataset.

    Args:
        bike_edges (str): Path to bike network edge features.
    
    Returns:
        bike_edges (str)
    """
    print("...enriching bike network features")
    # Default: moderately comfortable facilities (LTS = 3)
    arcpy.CalculateField_management(in_table=bike_edges, field="bikability", expression="3", field_type="LONG")
    arcpy.CalculateField_management(in_table=bike_edges, field="cycleway", expression="0", field_type="LONG")
    be = arcpy.MakeFeatureLayer_management(bike_edges, "__bike_edges__")
    # Select by criteria
    # - Least comfortable facilities (LTS=1) - Uncomfortable facilities (LTS=2) - Comfortable facilities (LTS = 4)
    for wc, val in BIKABILITY_CLASSES:
        arcpy.SelectLayerByAttribute_management(in_layer_or_view=be, selection_type="NEW_SELECTION", where_clause=wc)
        arcpy.CalculateField_management(in_table=be, field="bikability", expression=val)

    # Tag cycleways
    wc = r"highway LIKE '%ycleway%'"
    arcpy.SelectLayerByAttribute_management(be, "NEW_SELECTION", where_clause=wc)
    arcpy.CalculateField_management(be, "cycleway", "1")

    # Delete feature layer
    arcpy.Delete_management(be)
    return bike_edges


def import_OSM_shape(osm_fc, to_feature_dataset, fc_name=None,
                     overwrite=False, wc=None, field_mapping=None):
    """
    A simple function to facilitate the transfer of osm features in a
    shapefile to a feature dataset for use in a network dataset.

    Args:
        osm_fc (str): The path to the osm features to be added to the feature dataset.
        to_feature_dataset(str): The path to the output feature dataset where network features are
            stored and a network dataset will eventually be constructed.
        fc_name (str, default=None): The name of the output feature class to be created. If None, the
            name will match that if `osm_fc`.
        overwrite (bool, default=False): If True, if there is already a feature class in `to_feature_dataset`
            with the same `fc_name` (as provided or implied by `osm_fc`), it will be deleted and replaced.
        wc: (str, default=None): A where clause to only transfer select features from `osm_fc` to
            the output feature class.
        field_mapping (arcpy.FieldMappings, default=None): An arcpy `FieldMappings` object that handles field
             naming, merging, etc.
    
    Returns:
        net_source_fc (str): Path to a new feature class in `to_feature_dataset`, transferring
            features from `osm_fc`.
    """
    # Set appropriate output fc name
    if fc_name is None:
        fc_name, ext = os.path.splitext(os.path.split(osm_fc)[1])
    # Check if output already exists
    out_path = PMT.make_path(str(to_feature_dataset), fc_name)
    PMT.check_overwrite_output(output=out_path, overwrite=overwrite)

    # Transfer data
    print(f"...copying features {osm_fc} to {to_feature_dataset}")
    return arcpy.FeatureClassToFeatureClass_conversion(osm_fc, to_feature_dataset,
                                                       fc_name, wc, field_mapping)


def make_network_dataset(template_xml, out_feature_dataset, net_name="osm_ND"):
    """
    Make a network dataset from a template xml file. The features in
    `out_feature_dataset` must be consistent with those used in
    the network dataset used to creat the xml template.

    Args:
        template_xml (str): Path to a network dataset xml template
        out_feature_dataset (str): Path to the feature dataset where network source
            features are stored and where the network dataset will be created.
        net_name (str, default="osm_ND"): The name of the network dataset to be created.
    
    Returns:
        None: Creates a network dataset in `out_feature_dataset` based on the
            specifications in `template_xml`.
    
    See Also:
        makeNetworkDatasetTemplate
    """
    print(f"...applying network template: {template_xml}")
    arcpy.na.CreateNetworkDatasetFromTemplate(network_dataset_template=template_xml,
                                              output_feature_dataset=out_feature_dataset)
    print(f"...building network dataset: {net_name}")
    nd = PMT.make_path(out_feature_dataset, net_name)
    arcpy.na.BuildNetwork(in_network_dataset=nd)

    # save build errors
    print("...saving build errors")
    out_gdb, fds = os.path.split(out_feature_dataset)
    out_dir, out_name = os.path.split(out_gdb)
    temp_dir = os.environ.get("TEMP")
    if temp_dir:
        shutil.copyfile(os.path.join(temp_dir, "BuildErrors.txt"),
                        os.path.join(out_dir, f"BuildErrors_{out_name}.txt"))


def make_network_dataset_template(from_nd, template_xml):
    """
    Make a network dataset template from an existing network dataset. The
    template can be used to construct new network datasets using the same
    specifications later.

    Args:
        from_nd (str): Path to the existing network dataset from which the template will be created.
        template_xml (str): Path to an output template (xml) file.
    
    Returns:
        nd_template (xml): Creates a network dataset xml template at the path specified by
            `template_xml`
    
    See Also:
        makeNetworkDataset
    """
    print("...creating network dataset template")
    return arcpy.na.CreateTemplateFromNetworkDataset_na(network_dataset=from_nd,
                                                        output_network_dataset_template=template_xml)

