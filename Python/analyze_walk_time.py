"""
Created: October 2020
@Author: Alex Bell

Provides functions to translate a network service area feature class to
parcel features for reporting.

If run as "main", creates a parcel feature class in each PMT analysis year
geodatabase called "walk_times" and reports walk time information to
transit stations and parks.
"""

# %% IMPORTS
import PMT
import arcpy
import pandas as pd
import os
from analyze_osm_networks import NetLoader, _listAccumulationAttributes


# %% GLOBALS
KEEP_FIELDS = [
    "PARCELNO", 
    "ASMNT_YR",
    "DOR_UC",
    "JV",
    "LND_VAL",
    "LND_SQFOOT",
    "CONST_CLASS",
    "EFF_YR_BLT",
    "ACT_YR_BLT",
    "TOT_LVG_AREA",
    "NO_BULDNG",
    "NO_RES_UNTS"
    ]

NET_BY_YEAR = {
    2014: "_q3_2019",
    2015: "_q3_2019",
    2016: "_q3_2019",
    2017: "_q3_2019",
    2018: "_q3_2019",
    2019: "_q3_2019"
}

CODE_BLOCK = """
def assignBin(value):
    if value <= 5:
        return "0 to 5 minutes"
    elif value <= 10:
        return "5 to 10 minutes"
    elif value <= 15:
        return "10 to 15 minutes"
    elif value <= 20:
        return "15 to 20 minutes"
    elif value <= 25:
        return "20 to 25 minutes"
    elif value <= 30:
        return "25 to 30 minutes"
    else:
        return "over 30 minutes"
"""


# %% FUNCTIONS
def initParcelWalkTimeFC(clean_parcels, out_fc, keep_fields,
                         overwrite=False):
    """
    Copies parcel features into a new feature class, retaining specified
    fields. The new feature class is used to store walk time information.

    Parameters
    ----------
    clean_Parcels: Path
        Source parcel features
    out_fc: Patch
        Path to output feature class
    keep_fields: [String, ...]
        Names of fields in `clean_parcels` to keep when copying from
        `clean_parcels`
    overwrite: Boolean, default=False
        If True, `out_fc` will be overwritten if it already exists.
        Otherwise, the function fails if `out_fc` already exists.

    Returns
    --------
    out_fc: Path
    """
    # Check for existing
    if arcpy.Exists(out_fc):
        if overwrite:
            arcpy.Delete_management(out_fc)
        else:
            raise RuntimeError(f"Output fc {out_fc} already exists")
    # Prepare field mappings
    FM = arcpy.FieldMappings()
    #FM.addTable(clean_parcels)
    for kf in keep_fields:
        fm = arcpy.FieldMap()
        try:
            fm.addInputField(clean_parcels, kf)
            fm.outputField.name = kf
            fm.outputField.aliasName = kf
            FM.addFieldMap(fm)
        except:
            print(f"-- no input field {kf}")
    # Copy clean parcels
    print(f"Initializing parcels in {out_fc}")
    out_ws, out_name = os.path.split(out_fc)
    arcpy.FeatureClassToFeatureClass_conversion(
        clean_parcels, out_ws, out_name, field_mapping=FM)
    return out_fc


def _addField(in_fc, field_name, field_type, **kwargs):
    f = arcpy.ListFields(in_fc, field_name)
    if f:
        print(f"... ... deleting existing field {field_name}")
        arcpy.DeleteField_management(in_fc, field_name)
    print(f"... adding field {field_name}")
    arcpy.AddField_management(in_fc, field_name, field_type, **kwargs)


# def _assignBin(value):
#     if value <= 5:
#         return "0 to 5 minutes"
#     elif value <= 10:
#         return "5 to 10 minutes"
#     elif value <= 15:
#         return "10 to 15 minutes"
#     elif value <= 20:
#         return "15 to 20 minutes"
#     elif value <= 25:
#         return "20 to 25 minutes"
#     elif value <= 30:
#         return "25 to 30 minutes"
#     else:
#         return "over 30 minutes"

# def _loadLocations(net_layer, sublayer, points, name_field,
#                    net_loader, net_location_fields):
#     # Field mappings
#     fmap_fields = ["Name"]
#     fmap_vals = [name_field]
#     if net_location_fields is not None:
#         fmap_fields += ["SourceOID", "SourceID", "PosAlong", "SideOfEdge",
#                         "SnapX", "SnapY", "Distance"]
#         fmap_vals += net_location_fields
#     fmap = ";".join([f"{ff} {fv} #" for ff,fv in zip(fmap_fields, fmap_vals)])
#     # Load facilities
#     print(f"... ...loading {sublayer}")
#     arcpy.na.AddLocations(
#         in_network_analysis_layer=net_layer,
#         sub_layer=sublayer,
#         in_table=points,
#         field_mappings=fmap,
#         search_tolerance=net_loader.search_tolerance,
#         sort_field="",
#         search_criteria=net_loader.search_criteria,
#         match_type = net_loader.match_type,
#         append=net_loader.append,
#         snap_to_position_along_network=net_loader.snap,
#         snap_offset=net_loader.offset,
#         exclude_restricted_elements=net_loader.exclude_restricted,
#         search_query=net_loader.search_query
#         )
#     # TODO: list which locations are invalid

# def _solve(net_layer):
#     # Solve
#     print("... ...od matrix")
#     arcpy.na.Solve(in_network_analysis_layer=net_layer,
#                     ignore_invalids="SKIP",
#                     terminate_on_solve_error="TERMINATE"
#                     )


# def genODTable(origin_pts, origin_name_field, dest_pts, dest_name_field,
#                in_nd, imped_attr, cutoff, net_loader, out_fc,
#                restrictions=None, use_hierarchy=False, uturns="ALLOW_UTURNS",
#                o_location_fields=None, d_location_fields=None):
#     """
#     Creates and solves an OD Matrix problem for a collection of origin and 
#     destination points using a specified network dataset.

#     Parameters
#     ----------
#     origin_pts: Path
#     origin_name_field: String
#     dest_pts: Path
#     dest_name_field: String
#     in_nd: Path
#     imped_attr: String
#     cutoff: numeric
#     net_loader: NetLoader

#     """
#     if use_hierarchy:
#         hierarchy = "USE_HIERARCHY"
#     else:
#         hierarchy = "NO_HIERARCHY"
#     accum = _listAccumulationAttributes(in_nd, imped_attr)

#     print("... ...OD MATRIX: create network problem")
#     net_layer = arcpy.MakeODCostMatrixLayer_na(
#         # Setup
#         in_network_dataset=in_nd,
#         out_network_analysis_layer="__od__",
#         impedance_attribute=imped_attr,
#         default_cutoff=cutoff,
#         accumulate_attribute_name=None,
#         UTurn_policy=uturns,
#         restriction_attribute_name=restrictions,
#         output_path_shape="NO_LINES",
#         hierarchy=hierarchy,
#         time_of_day=None
#     )
#     net_layer_ = net_layer.getOutput(0)
#     try:
#         _loadLocations("__od__", "Origins", origin_pts, origin_name_field,
#                         net_loader, o_location_fields)
#         _loadLocations("__od__", "Destinations", dest_pts, dest_name_field,
#                         net_loader, d_location_fields)
#         _solve("__od__")
#         print("... ... solved, dumping to data frame")
#         # Get output as a data frame
#         sublayer_names = arcpy.na.GetNAClassNames(net_layer_)
#         extend_lyr_name = sublayer_names["ODLines"]
#         try:
#             extend_sublayer = net_layer.listLayers(extend_lyr_name)[0]
#         except:
#             extend_sublayer = arcpy.mapping.ListLayers(
#                 net_layer, extend_lyr_name)[0]
#         out_fields = ["Name", f"Total_{imped_attr}"]
#         columns = ["Name", imped_attr]
#         out_fields += [f"Total_{attr}" for attr in accum]
#         columns += [c for c in accum]
#         return pd.DataFrame(
#             arcpy.da.TableToNumPyArray(extend_sublayer, out_fields),
#             columns=columns
#         )
#     except:
#         raise
#     finally:
#         print("... ...deleting network problem")
#         arcpy.Delete_management(net_layer)


def parcelWalkTimes(parcel_fc, parcel_id_field, ref_fc, ref_name_field,
                    ref_time_field, target_name):
    """
    For features in a parcel feature class, summarize walk times reported
    in a reference features class of service area lines. Generates fields
    recording the nearest reference feature, walk time to the nearest
    reference feature, number of reference features within the service
    area walk time cutoff, and a minimum walk time "category" field.

    Parameters
    -----------
    parcel_fc: Path
        The parcel features to which walk time estimates will be appended.
    parcel_id_field: String
        The field in `parcel_fc` that uniquely identifies each feature.
    ref_fc: Path
        A feature class of line features with travel time estimates from/to
        key features (stations, parks, etc.)
    ref_name_field: String
        A field in `ref_fc` that identifies key features (which station, e.g.)
    ref_time_field: String
        A field in `ref_fc` that reports the time to walk from each line
        feature from/to key features.
    target_name: String
        A string suffix included in output field names.

    Returns
    --------
    None
        `parcel_fc` is modified in place to add new fields:
        `nearest_{target_name}`, `min_time_{target_name}`,
        `n_{target_name}`, `bin_{target_name}`

    See Also
    ---------
    initParcelWalkTimeFC
    """
    sr = arcpy.Describe(ref_fc).spatialReference
    # Name time fields
    min_time_field = f"min_time_{target_name}"
    nearest_field = f"nearest_{target_name}"
    number_field = f"n_{target_name}"
    bin_field = f"bin_{target_name}"
    # Intersect layers
    print("... intersecting parcels and network outputs")
    int_fc = "in_memory\\par_wt_sj"
    int_fc = arcpy.SpatialJoin_analysis(parcel_fc, ref_fc, int_fc,
                                        join_operation="JOIN_ONE_TO_MANY",
                                        join_type="KEEP_ALL",
                                        match_option="WITHIN_A_DISTANCE",
                                        search_radius="80 Feet")
    # Summarize
    print(f"... summarizing by {parcel_id_field}, {ref_name_field}")
    sum_tbl = "in_memory\\par_wt_sj_sum"
    statistics_fields=[[ref_time_field, "MIN"],[ref_time_field, "MEAN"]]
    case_fields = [parcel_id_field, ref_name_field]
    sum_tbl = arcpy.Statistics_analysis(
        int_fc, sum_tbl, statistics_fields, case_fields)
    # Delete intersect features
    arcpy.Delete_management(int_fc)
    # Dump sum table to data frame
    print("... converting to data frame")
    sum_fields = [f"MEAN_{ref_time_field}"]
    dump_fields = [parcel_id_field, ref_name_field] + sum_fields
    int_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(sum_tbl, dump_fields)
    )
    int_df.columns = [parcel_id_field, ref_name_field, ref_time_field]
    # Delete summary table
    arcpy.Delete_management(sum_tbl)
    # Summarize
    print("... summarizing times")
    int_df = int_df.set_index(ref_name_field)
    gb = int_df.groupby(parcel_id_field)
    which_name = gb.idxmin()
    min_time = gb.min()
    number = gb.size()
    # Extend table
    print("... extending output table")
    join_df = pd.concat([which_name, min_time, number], axis=1).reset_index()
    join_df.columns = [parcel_id_field, nearest_field, min_time_field, number_field]
    PMT.extendTableDf(parcel_fc, parcel_id_field, join_df, parcel_id_field)
    # Classify result
    print("... classifying")
    _addField(parcel_fc, bin_field, "TEXT", field_length=20)
    arcpy.CalculateField_management(
        parcel_fc, bin_field, f"assignBin(!{min_time_field}!)",
        expression_type="PYTHON3", code_block=CODE_BLOCK
    )



# %% MAIN
if __name__ == "__main__":
    target_names = ["stn_walk", "park_walk"] #, "stn_bike", "park_bike"]
    ref_fcs = [
        "walk_to_stn_NON_OVERLAP",
        "walk_to_parks_NON_OVERLAP",
        #"bike_to_stn_NON_OVERLAP",
        #"bike_to_parks_NON_OVERLAP"
        ]
    parcel_id_field = "PARCELNO"
    ref_name_field = "Name"
    ref_time_field = "ToCumul_Minutes"

    for year in PMT.YEARS:
        print(year)
        clean_parcels = PMT.makePath(PMT.CLEANED, "parcels.gdb", f"Miami_{year}")
        year_gdb = PMT.makePath(PMT.DATA, f"PMT_{year}.gdb")
        parcel_fc = PMT.makePath(year_gdb, "Parcels", "walk_time")
        # Initialize parcel walk time features
        initParcelWalkTimeFC(clean_parcels, parcel_fc, KEEP_FIELDS,
                             overwrite=True)
        # Iterate over targets and references
        net_fd = PMT.makePath(year_gdb, "networks")
        for tgt_name, ref_fc in zip(target_names, ref_fcs):
            print(f"- {tgt_name}")
            ref_fc = PMT.makePath(net_fd, ref_fc)
            parcelWalkTimes(parcel_fc, parcel_id_field, ref_fc,
                            ref_name_field, ref_time_field, tgt_name)

