"""
"""

# %% IMPORTS
import PMT_tools.PMT as PMT
import arcpy
import pandas as pd
import numpy as np
from PMT_tools.analyze_osm_networks import NetLoader, _listAccumulationAttributes, NET_BY_YEAR

if arcpy.CheckExtension("network") == "Available":
    arcpy.CheckOutExtension("network")
else:
    raise arcpy.ExecuteError("Network Analyst Extension license is not available.")

# %% GLOBALS
OSM_DIR = PMT.makePath(PMT.CLEANED, "OSM_Networks")
SEARCH_CRITERIA = "edges SHAPE;osm_ND_Junctions NONE"
SEARCH_QUERY = "edges #;osm_ND_Junctions #"
NET_LOADER = NetLoader("1500 meters",
                       search_criteria=SEARCH_CRITERIA,
                       match_type="MATCH_TO_CLOSEST",
                       append="CLEAR",
                       exclude_restricted="EXCLUDE",
                       search_query=SEARCH_QUERY)


# %% FUNCTIONS
def _loadLocations(net_layer, sublayer, points, name_field,
                   net_loader, net_location_fields):
    # Field mappings
    fmap_fields = ["Name"]
    fmap_vals = [name_field]
    if net_location_fields is not None:
        fmap_fields += ["SourceOID", "SourceID", "PosAlong", "SideOfEdge",
                        "SnapX", "SnapY", "Distance"]
        fmap_vals += net_location_fields
    fmap = ";".join([f"{ff} {fv} #" for ff, fv in zip(fmap_fields, fmap_vals)])
    # Load facilities
    print(f"... ...loading {sublayer}")
    arcpy.na.AddLocations(
        in_network_analysis_layer=net_layer,
        sub_layer=sublayer,
        in_table=points,
        field_mappings=fmap,
        search_tolerance=net_loader.search_tolerance,
        sort_field="",
        search_criteria=net_loader.search_criteria,
        match_type=net_loader.match_type,
        append=net_loader.append,
        snap_to_position_along_network=net_loader.snap,
        snap_offset=net_loader.offset,
        exclude_restricted_elements=net_loader.exclude_restricted,
        search_query=net_loader.search_query
    )
    # TODO: list which locations are invalid


def _solve(net_layer):
    # Solve
    print("... ...od matrix")
    s = arcpy.na.Solve(in_network_analysis_layer=net_layer,
                       ignore_invalids="SKIP",
                       terminate_on_solve_error="CONTINUE"
                       )
    return s


def _rowsToCsv(in_table, fields, out_table, chunksize):
    header = True
    mode = "w"
    rows = []
    # Iterate over rows
    with arcpy.da.SearchCursor(in_table, fields) as c:
        for r in c:
            rows.append(r)
            if len(rows) == chunksize:
                df = pd.DataFrame(rows, columns=fields)
                df.to_csv(out_table, index=False, header=header, mode=mode)
                rows = []
                header = False
                mode = "a"
    # Save any stragglers
    df = pd.DataFrame(rows, columns=fields)
    df.to_csv(out_table, index=False, header=header, mode=mode)


def genODTable(origin_pts, origin_name_field, dest_pts, dest_name_field,
               in_nd, imped_attr, cutoff, net_loader, out_table,
               restrictions=None, use_hierarchy=False, uturns="ALLOW_UTURNS",
               o_location_fields=None, d_location_fields=None,
               o_chunk_size=None):
    """
    Creates and solves an OD Matrix problem for a collection of origin and 
    destination points using a specified network dataset. Results are
    exported as a csv file.

    Parameters
    ----------
    origin_pts: Path
    origin_name_field: String
    dest_pts: Path
    dest_name_field: String
    in_nd: Path
    imped_attr: String
    cutoff: numeric
    net_loader: NetLoader
    out_table: Path
    restrictions: [String, ...], default=None
    use_hierarchy: Boolean, default=False
    uturns: String, default="ALLOW_UTURNS"
    o_location_fields: [String, ...], default=None
    d_location_fields: [String, ...], default=None
    o_chunk_size: Integer, default=None
    """
    if use_hierarchy:
        hierarchy = "USE_HIERARCHY"
    else:
        hierarchy = "NO_HIERARCHY"
    # accum = _listAccumulationAttributes(in_nd, imped_attr)

    print("... ...OD MATRIX: create network problem")
    net_layer = arcpy.MakeODCostMatrixLayer_na(
        # Setup
        in_network_dataset=in_nd,
        out_network_analysis_layer="__od__",
        impedance_attribute=imped_attr,
        default_cutoff=cutoff,
        accumulate_attribute_name=None,
        UTurn_policy=uturns,
        restriction_attribute_name=restrictions,
        output_path_shape="NO_LINES",
        hierarchy=hierarchy,
        time_of_day=None
    )
    net_layer_ = net_layer.getOutput(0)

    try:
        _loadLocations(net_layer_, "Destinations", dest_pts, dest_name_field,
                       net_loader, d_location_fields)
        # Iterate solves as needed
        if o_chunk_size is None:
            o_chunk_size = arcpy.GetCount_management(origin_pts)[0]
        write_mode = "w"
        header = True
        for o_pts in PMT.iterRowsAsChunks(origin_pts, chunksize=o_chunk_size):
            _loadLocations(net_layer_, "Origins", o_pts, origin_name_field,
                           net_loader, o_location_fields)
            s = _solve(net_layer_)
            print("... ... solved, dumping to data frame")
            # Get output as a data frame
            sublayer_names = arcpy.na.GetNAClassNames(net_layer_)
            extend_lyr_name = sublayer_names["ODLines"]
            try:
                extend_sublayer = net_layer_.listLayers(extend_lyr_name)[0]
            except:
                extend_sublayer = arcpy.mapping.ListLayers(
                    net_layer, extend_lyr_name)[0]
            out_fields = ["Name", f"Total_{imped_attr}"]
            columns = ["Name", imped_attr]
            # out_fields += [f"Total_{attr}" for attr in accum]
            # columns += [c for c in accum]
            df = pd.DataFrame(
                arcpy.da.TableToNumPyArray(extend_sublayer, out_fields)
            )
            df.columns = columns
            # Split outputs
            if len(df) > 0:
                names = ["OName", "DName"]
                df[names] = df["Name"].str.split(" - ", n=1, expand=True)

                # Save
                df.to_csv(
                    out_table, index=False, mode=write_mode, header=header)

                # Update writing params
                write_mode = "a"
                header = False
    except:
        raise
    finally:
        print("... ...deleting network problem")
        arcpy.Delete_management(net_layer)


# %% MAIN
print("WALK/BIKE SKIMS")
# MAZ points
maz_pts = PMT.makePath(PMT.CLEANED, "SERPM", "maz_centroids.shp")
maz_name_field = "MAZ"

# Solve OD
solved = []
for year in PMT.YEARS:
    net_suffix = NET_BY_YEAR[year]
    if net_suffix not in solved:
        # Walk access
        print("... Walk")
        # - Build skim
        walk_nd = PMT.makePath(
            OSM_DIR, f"walk{net_suffix}.gdb", "osm", "osm_ND")
        walk_imped = "Minutes"
        walk_cutoff = 60
        walk_skim = PMT.makePath(
            PMT.CLEANED, "OSM_Networks", f"Walk_Skim{net_suffix}.csv")
        walk_lyr = arcpy.MakeFeatureLayer_management(maz_pts, "__walk__")
        genODTable(
            origin_pts=walk_lyr,
            origin_name_field=maz_name_field,
            dest_pts=maz_pts,
            dest_name_field=maz_name_field,
            in_nd=walk_nd,
            imped_attr=walk_imped,
            cutoff=walk_cutoff,
            net_loader=NET_LOADER,
            out_table=walk_skim,
            restrictions=None,
            use_hierarchy=False,
            uturns="ALLOW_UTURNS",
            o_location_fields=None,
            d_location_fields=None,
            o_chunk_size=5000
        )

        # Bike access
        print("... Bike")
        # - Build skim
        bike_nd = PMT.makePath(
            OSM_DIR, f"bike{net_suffix}.gdb", "osm", "osm_ND")
        bike_imped = "Minutes"
        bike_cutoff = 60
        bike_skim = PMT.makePath(
            PMT.CLEANED, "OSM_Networks", f"Bike_Skim{net_suffix}.csv")
        bike_restr = ["IsCycleway", "LTS1", "LTS2", "LTS3", "LTS4", "Oneway"]
        # Break into chunks
        bike_lyr = arcpy.MakeFeatureLayer_management(maz_pts, "__bike__")
        genODTable(
            origin_pts=bike_lyr,
            origin_name_field=maz_name_field,
            dest_pts=maz_pts,
            dest_name_field=maz_name_field,
            in_nd=bike_nd,
            imped_attr=bike_imped,
            cutoff=bike_cutoff,
            net_loader=NET_LOADER,
            out_table=bike_skim,
            restrictions=bike_restr,
            use_hierarchy=False,
            uturns="ALLOW_UTURNS",
            o_location_fields=None,
            d_location_fields=None,
            o_chunk_size=500
        )

        # Mark as solved
        solved.append(net_suffix)
        arcpy.Delete_management(bike_lyr)
