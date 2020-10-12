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

# %% IMPORTS
import arcpy
import PMT
from six import string_types

# %% FUNCTIONS
def makeCleanNetworkGDB(clean_path, spatial_reference,
                        modes=["walk", "bike"],
                        gdb_name="osm_networks"):
    """
    Creates a file geodatabase in the "cleaned" data directory within which to
    build network datasets for walking and biking analyses. If the gdb already 
    exists, it is  deleted and replaced.

    Parameters
    ------------
    clean_path: Path
        A path the cleaned data directory where the network data will be built.
    spatial_reference: sr, wkid, .prj file, etc.
        An object, code, file etc that can be used to construct a
        `SpatialReference` object.
    modes: [String,...], default=["walk", "bike"]
        Mode name[s] for which feature datasets will be created within the new
        geodatabase.
    gdb_name: String, default="osm_networks"
        The name of the new geodatabase to be created.

    Returns
    -------
    None
        Creates a new geodabase at `{clean_path}/{gdb_name}`
    """
    # Handle inputs
    if gdb_name[-4:] != ".gdb":
        gdb_name = gdb_name + ".gdb"
    if isinstance(modes, string_types):
        modes = [modes]
    # Delete the gdb if it exists
    gdb_path = PMT.makePath(clean_path, gdb_name)
    if arcpy.Exists(gdb_path):
        arcpy.Delete_management(gdb_path)
    # Create the new gdb
    arcpy.CreateFileGDB_management(clean_path, gdb_name)
    # Create feature datasets
    sr = arcpy.SpatialReference(spatial_reference)
    for mode in modes:
        arcpy.CreateFeatureDataset_management(gdb_path, mode, sr)


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
    None:
        Saves a new feature class in `to_feature_dataset`, transfering
        features from `osm_fc`.
    """
    # Set appropriate output fc name
    if fc_name is None:
        fc_name = osm_fc.rsplit('\\',1)[1]
        fc_name = fc_name.rsplit('.', 1)[0]
    # Check if output already exists
    out_path = PMT.makePath(to_feature_dataset, fc_name)
    if arcpy.Exists(out_path):
        if overwrite:
            arcpy.Delete_management(out_path)
        else:
            raise RuntimeError(
                "Feature class {} already exists".format(out_path))
    # Transfer data
    arcpy.FeatureClassToFeatureClass_conversion(
        osm_fc, to_feature_dataset, fc_name, wc, field_mapping)


def makeNetworkDataset(template_xml, out_feature_dataset):
    """
    Make a network dataset from a template xml file. The features in
    `out_feature_dataset` must be consistent with those used in
    the network dataset used to creat the xml template.

    Parameters
    ------------
    template_xml: Path
        Path to a network dataset xml template
    out_feature_dataset: Path

    Returns
    ---------
    None:
        Creates a network dataset in `out_feature_dataset` based on the
        specifications in `template_xml`

    See Also
    ---------
    makeNetworkDatasetTemplate
    """
    arcpy.CreateNetworkDatasetFromTemplate(
        template_xml, out_feature_dataset)


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
    None:
        Creates a network dataset xml template at the path specified by
        `template_xml`

    See Also
    ---------
    makeNetworkDataset
    """
    arcpy.CreateTemplateFromNetworkDataset_na(
        network_dataset=from_nd, output_network_dataset_template=template_xml)


# %% MAIN
if __name__ == "__main__":
    # Setup variables
    sr = 2281
    osm_raw = PMT.makePath(PMT.RAW, "osm_networks")
    bike_raw = PMT.makePath(
        osm_raw, "bike", "bike_network_shape.shp", "edges.shp")
    walk_raw = PMT.makePath(
        osm_raw, "walk", "walk_network_shape.shp", "edges.shp")

    # Make gdb
    makeCleanNetworkGDB(PMT.CLEANED, sr, modes=["walk", "bike"],
                        gdb_name="osm_networks")
    bike_fd = PMT.makePath(PMT.CLEANED, "osm_networks", "bike")
    walk_fd = PMT.makePath(PMT.CLEANED, "osm_networks", "walk")
    
    # Transfer features
    importOSMShape(bike_raw, bike_fd, overwrite=True)
    importOSMShape(walk_raw, walk_df, overwrite=True)
    
    # Build network datasets
    bike_template=PMT.makePath(PMT.REF, "osm_bike_template.xml")
    walk_template=PMT.makePath(PMT.REF, "osm_walk_template.xml")
    makeNetworkDataset(bike_template, bike_fd)
    makeNetworkDataset(walk_template, walk_fd)

