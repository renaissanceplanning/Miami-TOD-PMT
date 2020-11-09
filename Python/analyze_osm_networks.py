"""
Created: October 2020
@Author: Alex Bell

...
"""

# %% IMPORTS
import PMT
import arcpy

# %% GLOBALS
NET_BY_YEAR = {
    2014: "_q3_2019",
    2015: "_q3_2019",
    2016: "_q3_2019",
    2017: "_q3_2019",
    2018: "_q3_2019",
    2019: "_q3_2019"
}

# %% CLASSES
class NetLoader():
    """
    A naive class for specifying network location loading preferences.
    Simplifies network functions by passing loading specifications as
    a single argument. This class does no validation of assigned preferences.
    """
    def __init__(self, search_tolerance, search_criteria, 
                 match_type="MATCH_TO_CLOSEST", append="APPEND", snap="#",
                 offset="#",exclude_restricted="EXCLUDE", search_query="#"):
        self.search_tolerance = search_tolerance
        self.search_criteria = search_criteria
        self.match_type = match_type
        self.append = append
        self.snap = snap
        self.offset = offset
        self.exclude_restricted = exclude_restricted
        self.search_query = search_query

# %% FUNCTIONS
def _loadFacilitiesAndSolve(net_layer, sublayer, facilities, name_field,
                            net_loader, net_location_fields):
    #-------------------
    # Break into separate function
    try:
        # Field mappings
        fmap_fields = ["Name"]
        fmap_vals = [name_field]
        if net_location_fields is not None:
            fmap_fields += ["SourceOID", "SourceID", "PosAlong", "SideOfEdge",
                            "SnapX", "SnapY", "Distance"]
            fmap_vals += net_location_fields
        fmap = ";".join([f"{ff} {fv} #" for ff,fv in zip(fmap_fields, fmap_vals)])

        # Load facilities
        print("...loading facilities")
        arcpy.na.AddLocations(
            in_network_analysis_layer=net_layer,
            sub_layer="Facilities",
            in_table=facilities,
            field_mappings=fmap,
            search_tolerance=net_loader.search_tolerance,
            sort_field=None,
            search_criteria=net_loader.search_criteria,
            match_type = net_loader.match_type,
            append=net_loader.append,
            snap_to_position_along_network=net_loader.snap,
            snap_offset=net_loader.offset,
            exclude_restricted_elements=net_loader.exclude_restricted,
            search_query=net_loader.search_query
            )
        # TODO: list which locations are invalid

        # Solve
        print("...generating service area lines")
        arcpy.na.Solve(in_network_analysis_layer=net_layer,
                       ignore_invalids="SKIP",
                       terminate_on_solve_error="TERMINATE"
                       )

        # Export output
        print("...exporting output")
        sublayer_names = arcpy.na.GetNAClassNames(net_layer)
	    result_lyr_name = sublayer_names[sublayer]
	    result_sublayer = arcpy.mapping.ListLayers(
            net_layer, result_lyr_name)[0]
        out_ws, out_name = os.path.split(out_fc)
        arcpy.TableToTable_conversion(result_sublayer, output_ws, out_name)

    except:
        raise
    finally:
        arcpy.Delete_management(net_layer)

def genSALines(facilities, name_field, in_nd, imped_attr, cutoff, net_loader,
               out_fc, from_to="TRAVEL_FROM", overlap="OVERLAP",
               restrictions=None, use_hierarchy=False, uturns="ALLOW_UTURNS",
               net_location_fields=None):
    """

    Parameters
    ------------
    facilities: Path or feature layer
        The facilities for which service areas will be generated.
    name_field: String
        The field in `facilities` that identifies each location.
    in_nd: Path
        Path to the network dataset
    imped_attr: String
        The name of the impedance attribute to use when solving the network
        and generating service area lines
    cutoff: Numeric, or [Numeric,...]
        The search radius (in units of `imped_attr`) that defines the limits
        of the service area. If a list is given, the highest value defines the
        cutoff and all other values are used as break points, which are used
        to split output lines.
    net_loader: NetLoader
        Location loading preferences
    from_to: String, default="TRAVEL_FROM"
        If "TRAVEL_FROM", service areas reflect the reach of the network from
        `facilities`; if "TRAVEL_TO", service areas reflec the reach of the
        network to the facilities. If not applying one-way restrictions, the
        outcomes are effectively equivalent.
    overlap: String, deault="OVERLAP"
        If "OVERLAP", ...
    restrictions: String or [String,...], default=None
        Specify restriction attributes (oneway, e.g.) to honor when generating
        service area lines. If the restrictions are paramterized, default
        parameter values are used in the solve.
    uturns: String, default="ALLOW_UTURNS"
        Options are "ALLOW_UTURNS", "NO_UTURNS", "ALLOW_DEAD_ENDS_ONLY",
        "ALLOW_DEAD_ENDS_AND_INTERSECTIONS_ONLY"
    use_hierarchy: Boolean, default=False
        If a hierarchy is defined for `in_nd`, it will be applied when solving
        the network if `use_hierarchy` is True; otherwise a simple,
        non-hierarchical solve is executed.
    net_location_fields: [String,...], default=None
        If provided, list the fields in the `facilities` attribute table that
        define newtork loading locations. Fields must be provided in the 
        following order: SourceID, SourceOID, PosAlong, SideOfEdge, SnapX,
        SnapY, Distance.

    See Also
    -----------
    NetLoader
    """
    if use_hierarchy:
        hierarchy = "USE_HIERARCHY"
    else:
        hierarchy = "NO_HIERARHCY"
    print("...create network problem")
    net_layer = arcpy.MakeServiceAreaLayer_na(
        # Setup
        in_network_dataset=in_nd,
        out_network_analysis_layer="__svc_lines__",
        impedance_attribute=imped_attr,
        travel_from_to=from_to,
        default_break_values=cutoff,
        # Polygon generation (disabled)
        polygon_type="NO_POLYS",
        merge=None,
        nesting_type=None,
        polygon_trim=None,
        poly_trim_value=None,
        # Line generation (enabled)
        line_type="TRUE_LINES",
        overlap=overlap,
        split="SPLIT",
        lines_source_fields="LINES_SOURCE_FIELDS",
        # Solve options
        excluded_source_name=None,
        accumulate_attribute_name=None,
        UTurn_policy=uturns,
        restriction_attribute_name=restrictions,
        hierarchy=hierarchy,
        time_of_day=None
    )

    _loadFacilitiesAndSolve(net_layer, "Lines", facilities, name_field,
                            net_loader, net_location_fields)


def genSAPolys(facilities, name_field, in_nd, imped_attr, cutoff, net_loader,
               out_fc, from_to="TRAVEL_FROM", merge="NO_MERGE", nesting="RINGS",
               restrictions=None, use_hierarchy=False, uturns="ALLOW_UTURNS",
               net_location_fields=None):
    """
    

    Parameters
    ------------
    facilities: Path or feature layer
        The facilities for which service areas will be generated.
    name_field: String
        The field in `facilities` that identifies each location.
    in_nd: Path
        Path to the network dataset
    imped_attr: String
        The name of the impedance attribute to use when solving the network
        and generating service area lines
    cutoff: Numeric, or [Numeric,...]
        The search radius (in units of `imped_attr`) that defines the limits
        of the service area. If a list is given, the highest value defines the
        cutoff and all other values are used as break points, which are used
        to split output lines.
    net_loader: NetLoader
        Location loading preferences
    from_to: String, default="TRAVEL_FROM"
        If "TRAVEL_FROM", service areas reflect the reach of the network from
        `facilities`; if "TRAVEL_TO", service areas reflec the reach of the
        network to the facilities. If not applying one-way restrictions, the
        outcomes are effectively equivalent.
    overlap: String, deault="OVERLAP"
        If "OVERLAP", ...
    restrictions: String or [String,...], default=None
        Specify restriction attributes (oneway, e.g.) to honor when generating
        service area lines. If the restrictions are paramterized, default
        parameter values are used in the solve.
    uturns: String, default="ALLOW_UTURNS"
        Options are "ALLOW_UTURNS", "NO_UTURNS", "ALLOW_DEAD_ENDS_ONLY",
        "ALLOW_DEAD_ENDS_AND_INTERSECTIONS_ONLY"
    use_hierarchy: Boolean, default=False
        If a hierarchy is defined for `in_nd`, it will be applied when solving
        the network if `use_hierarchy` is True; otherwise a simple,
        non-hierarchical solve is executed.
    net_location_fields: [String,...], default=None
        If provided, list the fields in the `facilities` attribute table that
        define newtork loading locations. Fields must be provided in the 
        following order: SourceID, SourceOID, PosAlong, SideOfEdge, SnapX,
        SnapY, Distance.

    See Also
    -----------
    NetLoader
    """
    if use_hierarchy:
        hierarchy = "USE_HIERARCHY"
    else:
        hierarchy = "NO_HIERARHCY"
    print("...create network problem")
    net_layer = arcpy.MakeServiceAreaLayer_na(
        # Setup
        in_network_dataset=in_nd,
        out_network_analysis_layer="__svc_lines__",
        impedance_attribute=imped_attr,
        travel_from_to=from_to,
        default_break_values=cutoff,
        # Polygon generation (disabled)
        polygon_type="DETAILED_POLYS",
        merge=None,
        nesting_type=None,
        polygon_trim=None,
        poly_trim_value=None,
        # Line generation (enabled)
        line_type="NO_LINES",
        overlap=overlap,
        split="SPLIT",
        lines_source_fields="LINES_SOURCE_FIELDS",
        # Solve options
        excluded_source_name=None,
        accumulate_attribute_name=None,
        UTurn_policy=uturns,
        restriction_attribute_name=restrictions,
        hierarchy=hierarchy,
        time_of_day=None
    )

    _loadFacilitiesAndSolve(net_layer, "Polygons", facilities, name_field,
                            net_loader, net_location_fields)


# %% MAIN
if __name__ == "__main__":
    # Facilities
    #  - Stations
    stations = PMT.makePath(PMT.BASIC_FEATURES, "SMART_Plan_Stations")
    station_name = "Name"
    # - Parks
    parks = PMT.makePath(PMT.CLEANED, "Parks", "Facility.shp")
    parks_name = "NAME"

    # For each analysis year, analyze networks (avoid redundant solves)
    solved = []
    for year in PMT.YEARS:
        # Networks
        # - Walk
        net_suffix = NET_BY_YEAR[year]
        if net_suffix in solved:
            continue
        else:
            
        walk_nd = PMT.makePath(PMT.CLEANED, "osm_networks.gdb", "walk", "walk_nd")
        walk_imped_attr = "minutes"
        walk_cutoff = 30 # minutes
        walk_net_loader = NetLoader("1500 Meters",
                                    search_criteria=[
                                        ["edges", "SHAPE"],
                                        ["walk_ND_junctions", "NONE"]
                                        ],
                                    match_type="MATCH_TO_CLOSEST",
                                    append="APPEND",
                                    exclude_restricted="EXCLUDE")
        walk_lines = PMT.makePath(PMT.ROOT, "")
        # - Bike
        bike_nd = PMT.makePath(PMT.CLEANED, "osm_networks.gdb", "bike", "bike_nd")
        bike_imped_attr = "minutes"
        bike_cutoff = 30 # minutes
        bike_net_loader = NetLoader("1500 Meters",
                                    search_criteria=[
                                        ["edges", "SHAPE"],
                                        ["bike_ND_junctions", "NONE"]
                                        ],
                                    match_type="MATCH_TO_CLOSEST",
                                    append="APPEND",
                                    exclude_restricted="EXCLUDE")

        # Generate lines
        # - Stations
        #   - Walk
        genSALines(stations, station_name, walk_nd, walk_imped_attr, walk_cutoff,
                walk_net_loader, walk_lines, from_to="TRAVEL_TO",
                overlap="OVERLAP", restrictions=None, use_hierarchy=False,
                uturns="ALLOW_UTURNS", net_location_fields=None)
        

