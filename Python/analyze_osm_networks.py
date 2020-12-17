"""
Created: October 2020
@Author: Alex Bell

...
"""
#TODO: Docstring cleanup


# %% IMPORTS
import PMT
import arcpy
import os

if arcpy.CheckExtension("network") == "Available":
        arcpy.CheckOutExtension("network")
else:
    raise arcpy.ExecuteError("Network Analyst Extension license is not available.")

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
                 match_type="MATCH_TO_CLOSEST", append="APPEND",
                 snap="NO_SNAP", offset="5 Meters", 
                 exclude_restricted="EXCLUDE", search_query=None):
        self.search_tolerance = search_tolerance
        self.search_criteria = search_criteria
        self.match_type = match_type
        self.append = append
        self.snap = snap
        self.offset = offset
        self.exclude_restricted = exclude_restricted
        self.search_query = search_query


class NetAnalysis():
    """
    """
    def __init__(self, name, network_dataset, facilities, name_field,
                 net_loader):
        self.name = name
        self.network_dataset = network_dataset
        self.facilities = facilities
        self.name_field = name_field
        self.net_loader = net_loader
        self.overlaps = ["OVERLAP", "NON_OVERLAP"]
        self.merges = ["NO_MERGE", "MERGE"]
    
    def solve(self, imped_attr, cutoff, out_ws, restrictions="",
              use_hierarchy=False, net_location_fields=""):
        """
        """
        for overlap, merge in zip(self.overlaps, self.merges):
            print(f"...{overlap}/{merge}")
            # Lines
            lines = PMT.makePath(out_ws, f"{self.name}_{overlap}")
            lines = genSALines(
                self.facilities,
                self.name_field,
                self.network_dataset,
                imped_attr,
                cutoff,
                self.net_loader,
                lines,
                from_to="TRAVEL_TO",
                overlap=overlap,
                restrictions=restrictions,
                use_hierarchy=use_hierarchy,
                uturns="ALLOW_UTURNS",
                net_location_fields=net_location_fields
            )
            # Polygons
            polys = PMT.makePath(out_ws, f"{self.name}_{merge}")
            polys = genSAPolys(
                self.facilities,
                self.name_field,
                self.network_dataset,
                imped_attr,
                cutoff,
                self.net_loader,
                polys,
                from_to="TRAVEL_TO",
                merge=merge,
                nesting="RINGS",
                restrictions=restrictions,
                use_hierarchy=use_hierarchy,
                uturns="ALLOW_UTURNS",
                net_location_fields=net_location_fields
            )


# %% FUNCTIONS

def _getResultToCopy(net_by_year, year, solved_years):
    # Target a year using the same network
    tgt_net = net_by_year[year]
    copy_year = None
    for sy in solved_years:
        solved_net = net_by_year[sy]
        if solved_net == tgt_net:
            copy_year = sy
    return copy_year


def _listAccumulationAttributes(network, impedance_attribute):
    accumulation = []
    desc = arcpy.Describe(network)
    attributes = desc.attributes
    for attribute in attributes:
        if attribute.name == impedance_attribute:
            continue
        elif attribute.usageType == "Cost":
            accumulation.append(attribute.name)
    return accumulation


def _loadFacilitiesAndSolve(net_layer, facilities, name_field,
                            net_loader, net_location_fields):
    # Field mappings
    fmap_fields = ["Name"]
    fmap_vals = [name_field]
    if net_location_fields is not None:
        fmap_fields += ["SourceOID", "SourceID", "PosAlong", "SideOfEdge",
                        "SnapX", "SnapY", "Distance"]
        fmap_vals += net_location_fields
    fmap = ";".join([f"{ff} {fv} #" for ff,fv in zip(fmap_fields, fmap_vals)])
    # Load facilities
    print("... ...loading facilities")
    arcpy.na.AddLocations(
        in_network_analysis_layer=net_layer,
        sub_layer="Facilities",
        in_table=facilities,
        field_mappings=fmap,
        search_tolerance=net_loader.search_tolerance,
        sort_field="",
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
    print("... ...generating service areas")
    arcpy.na.Solve(in_network_analysis_layer=net_layer,
                    ignore_invalids="SKIP",
                    terminate_on_solve_error="TERMINATE"
                    )


def _exportSublayer(net_layer, sublayer, out_fc):
    # Export output
    print("... ...exporting output")
    sublayer_names = arcpy.na.GetNAClassNames(net_layer)
    result_lyr_name = sublayer_names[sublayer]
    try:
        result_sublayer = net_layer.listLayers(result_lyr_name)[0]
    except:
        result_sublayer = arcpy.mapping.ListLayers(
            net_layer, result_lyr_name)[0]
    
    if arcpy.Exists(out_fc):
        arcpy.Delete_management(out_fc)
    out_ws, out_name = os.path.split(out_fc)
    arcpy.FeatureClassToFeatureClass_conversion(
        result_sublayer, out_ws, out_name)


def _extendFromSublayer(out_fc, key_field, net_layer, sublayer, fields):
    print(f"... ...extending output from {sublayer}")
    sublayer_names = arcpy.na.GetNAClassNames(net_layer)
    extend_lyr_name = sublayer_names[sublayer]
    try:
        extend_sublayer = net_layer.listLayers(extend_lyr_name)[0]
    except:
        extend_sublayer = arcpy.mapping.ListLayers(
            net_layer, extend_lyr_name)[0]
    # Dump to array and extend
    extend_fields = ["OID@"] + fields
    extend_array = arcpy.da.TableToNumPyArray(extend_sublayer, extend_fields)
    arcpy.da.ExtendTable(out_fc, key_field, extend_array, "OID@")


def genSALines(facilities, name_field, in_nd, imped_attr, cutoff, net_loader,
               out_fc, from_to="TRAVEL_FROM", overlap="OVERLAP",
               restrictions="", use_hierarchy=False, uturns="ALLOW_UTURNS",
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
    restrictions: String or [String,...], default=""
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
        hierarchy = "NO_HIERARCHY"
    # accumulation
    accum = _listAccumulationAttributes(in_nd, imped_attr)
    print("... ...LINES: create network problem")

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
        excluded_source_name="",
        accumulate_attribute_name=accum,
        UTurn_policy=uturns,
        restriction_attribute_name=restrictions,
        hierarchy=hierarchy,
        time_of_day=""
    )
    net_layer_ = net_layer.getOutput(0)
    try:
        _loadFacilitiesAndSolve("__svc_lines__", facilities, name_field,
                                net_loader, net_location_fields)
        _exportSublayer(net_layer_, "SALines", out_fc)
        # Extend output with facility names
        _extendFromSublayer(out_fc, "FacilityID", net_layer_, "Facilities", ["Name"])
    except:
        raise
    finally:
        print("... ...deleting network problem")
        arcpy.Delete_management(net_layer)


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
    out_fc: Path
    from_to: String, default="TRAVEL_FROM"
        If "TRAVEL_FROM", service areas reflect the reach of the network from
        `facilities`; if "TRAVEL_TO", service areas reflec the reach of the
        network to the facilities. If not applying one-way restrictions, the
        outcomes are effectively equivalent.
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

    Returns
    --------
    out_fc: Path

    See Also
    -----------
    NetLoader
    """
    if use_hierarchy:
        hierarchy = "USE_HIERARCHY"
    else:
        hierarchy = "NO_HIERARCHY"
    print("... ...POLYGONS: create network problem")
    net_layer = arcpy.MakeServiceAreaLayer_na(
        # Setup
        in_network_dataset=in_nd,
        out_network_analysis_layer="__svc_areas__",
        impedance_attribute=imped_attr,
        travel_from_to=from_to,
        default_break_values=cutoff,
        # Polygon generation (disabled)
        polygon_type="DETAILED_POLYS",
        merge=merge,
        nesting_type="RINGS",
        polygon_trim="TRIM_POLYS",
        poly_trim_value="100 Meters",
        # Line generation (enabled)
        line_type="NO_LINES",
        overlap="OVERLAP",
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
    net_layer_ = net_layer.getOutput(0)
    try:
        _loadFacilitiesAndSolve("__svc_areas__", facilities, name_field,
                                net_loader, net_location_fields)
        _exportSublayer(net_layer_, "SAPolygons", out_fc)
    except:
        raise
    finally:
        print("... ...deleting network problem")
        arcpy.Delete_management(net_layer)


# %% MAIN
if __name__ == "__main__":
    # Facilities
    #  - Stations
    stations = PMT.makePath(PMT.BASIC_FEATURES, "SMART_Plan_Stations")
    station_name = "Name"
    # - Parks
    parks = PMT.makePath(PMT.CLEANED, "Parks", "Facility.shp")
    parks_name = "NAME"

    # Universal network settings
    nets = PMT.makePath(PMT.CLEANED, "osm_networks")
    search_criteria = "edges SHAPE;osm_ND_Junctions NONE"
    search_query="edges #;osm_ND_Junctions #"
    net_loader = NetLoader("1500 meters",
                           search_criteria=search_criteria,
                           match_type="MATCH_TO_CLOSEST",
                           append="APPEND",
                           exclude_restricted="EXCLUDE",
                           search_query=search_query)
    imped_attr = "Minutes"
    cutoff = "15 30"
    
    # For each analysis year, analyze networks (avoid redundant solves)
    solved = []
    solved_years = []
    for year in PMT.YEARS:
        fd = PMT.makePath(PMT.DATA, f"PMT_{year}.gdb", "Networks")
        # Networks
        net_suffix = NET_BY_YEAR[year]
        if net_suffix in solved:
            copy_year = _getResultToCopy(NET_BY_YEAR, year, solved_years)
            copy_fd = PMT.makePath(
                PMT.ROOT, f"PMT_{copy_year}.gdb", "Networks")
            print(f"- copying results from {copy_fd} to {fd}")
            for mode_net in ["walk", "bike"]:
                for dest_grp in ["stn", "parks"]:
                    for run in ["MERGE", "NO_MERGE", "OVERLAP", "NON_OVERLAP"]:
                        fc_name = f"{mode_net}_to_{dest_grp}_{run}"
                        print(f" - - {fc_name}")
                        src_fc = PMT.makePath(copy_fd, fc_name)
                        if arcpy.Exists(PMT.makePath(fd, fc_name)):
                            arcpy.Delete_management(PMT.makePath(fd, fc_name))
                        arcpy.FeatureClassToFeatureClass_conversion(
                            src_fc, fd, fc_name)
        else:
            print(f"\n{net_suffix}")
            # - Walk
            walk_nd = PMT.makePath(
                nets, f"walk{net_suffix}.gdb", "osm", "osm_ND")
            walk_net_s = NetAnalysis("walk_to_stn", walk_nd, stations,
                                     station_name, net_loader)
            walk_net_p = NetAnalysis("walk_to_parks", walk_nd, parks,
                                     parks_name, net_loader)
            # - Bike
            bike_nd = PMT.makePath(
                nets, f"bike{net_suffix}.gdb", "osm", "osm_ND")
            bike_net_s = NetAnalysis("bike_to_stn", bike_nd, stations,
                                     station_name, net_loader)
            bike_net_p = NetAnalysis("bike_to_parks", bike_nd, parks,
                                     parks_name, net_loader)
            net_analyses = [walk_net_s, walk_net_p, bike_net_s, bike_net_p]
            for net_analysis in net_analyses:
                print(f"\n - {net_analysis.name}")
                if "bike" in net_analysis.name:
                    restrictions = "oneway"
                else:
                    restrictions = ""
                net_analysis.solve(imped_attr, cutoff, fd,
                                   restrictions=restrictions,
                                   use_hierarchy=False,
                                   net_location_fields="")
            solved.append(net_suffix)
        solved_years.append(year)
