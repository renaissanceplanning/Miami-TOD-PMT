"""
"""


# %% IMPORTS
import PMT
import arcpy
import pandas as pd
from analyze_osm_networks import NetLoader


# %% GLOBALS
BIKE_NET = "K:/Projects/MiamiDade/PMT/Data/Cleaned/OSM_Networks/bike_q3_2019.gdb/osm/osm_ND"
IMPED_ATTR = "Length"
CUTOFF = "1609"
RESTRICTIONS = "Oneway;IsCycleway;LTS1;LTS2;LTS3;LTS4"
NODES = "K:/Projects/MiamiDade/PMT/Data/Cleaned/OSM_Networks/bike_q3_2019.gdb/osm/osm_ND_junctions"
NODE_ID = "OBJECTID"
EDGES = "K:/Projects/MiamiDade/PMT/Data/Cleaned/OSM_Networks/bike_q3_2019.gdb/osm/edges"
NL = NetLoader(
    search_tolerance="5 meters",
    search_criteria="edges NONE;osm_ND_Junctions END",
    match_type="MATCH_TO_CLOSEST",
    append="CLEAR",
    snap="NO_SNAP",
    offset="5 meters",
    exclude_restricted="INCLUDE",
    search_query="edges #;osm_ND_Junctions #"
)


# %% FUNCTIONS
def linesToCentrality(line_feaures, impedance_attribute, out_csv,
                      header, mode):
    """
    Using the "lines" layer output from an OD matrix problem, calculate
    node centrality statistics and store results in a csv table.

    Parameters
    -----------
    line_features: ODMatrix/Lines feature layer
    impedance_attribute: String
    out_csv: Path
    header: Boolean
    mode: String
    """
    imp_field = f"Total_{impedance_attribute}"
    # Dump to df
    df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(
            line_features, ["Name", imp_field]
        )
    )
    names = ["N", "Node"]
    df[names] = df["Name"].str.split(" - ", n=1, expand=True)

    # Summarize
    sum_df = df.groupby("Node").agg(
        {"N": "size", imp_field: sum}
        ).reset_index()

    # Calculate centrality
    sum_df["centrality"] = (sum_df.N - 1)/sum_df[imp_field]

    # Add average length
    sum_df["AvgLength"] = 1/sum_df.centrality

    # Add centrality index
    sum_df["CentIdx"] = sum_df.N/sum_df.AvgLength

    # Export
    sum_df.to_csv(out_csv, mode=mode, header=header, index=False)


def network_centrality(in_nd, in_features, net_loader, out_csv,
                       name_field="OBJECTID", impedance_attribute="Length",
                       cutoff="1609", restrictions="", chunksize=1000):
    """
    Uses Network Analyst to create and iteratively solve an OD matrix problem
    to assess connectivity among point features.
    
    The evaluation analyses how many features can reach a given feature and
    what the total and average travel impedances are. Results are reported
    for traveling TO each feature (i.e. features as destinations), which may
    be significant if oneway or similar restrictions are honored.

    in_nd: Path, NetworkDataset
    in_features: Path, Feature Class or Feature Layer
        A point feature class or feature layer that will serve as origins and
        destinations in the OD matrix
    net_loader: NetLoader
        Provide network location loading preferences using a NetLoader
        instance.
    out_csv: Path
        Network centrality analysis results will be stored in a csv at the
        provided path for each feature in `in_features`. If `out_csv` exists,
        it will be overwritten.
    name_field: String, default="OBJECTID"
        A field in `in_features` that identifies each feature. Generally this
        should be a unique value.
    impedance_attribute: String, default="Length"
        The attribute in `in_nd` to use when solving shortest paths among
        features in `in_features`.
    cutoff: String, default="1609"
        A number (as a string) that establishes the search radius for
        evaluating node centrality. Counts and impedances from nodes within
        this threshold are summarized. Units are implied by the
        `impedance_attribute`.
    restrictions: String, default=""
        If `in_nd` includes restriction attributes, provide a
        semi-colon-separated string listing which restrictions to honor
        in solving the OD matrix.
    chunksize: Integer, default=1000
        Destination points from `in_features` are loaded iteratively in chunks
        to manage memory. The `chunksize` determines how many features are
        analyzed simultaneously (more is faster but consumes more memory).

    Returns
    -------
    out_csv: Path
        Path to the output table containing centrality analysis results for
        each `in_feature`.
    """
    # Step 1: OD problem
    print("Make OD Problem")
    arcpy.MakeODCostMatrixLayer_na(
        in_network_dataset=in_nd,
        out_network_analysis_layer="OD Cost Matrix",
        impedance_attribute=impedance_attribute,
        default_cutoff=cutoff,
        default_number_destinations_to_find="",
        accumulate_attribute_name="",
        UTurn_policy="ALLOW_UTURNS",
        restriction_attribute_name=restrictions,
        hierarchy="NO_HIERARCHY",
        hierarchy_settings="",
        output_path_shape="NO_LINES",
        time_of_day=""
    )

    #Step 2 - add all origins
    print("Load all origins")
    in_features = in_features
    arcpy.AddLocations_na(
        in_network_analysis_layer="OD Cost Matrix",
        sub_layer="Origins",
        in_table=in_features,
        field_mappings=f"Name {name_field} #",
        search_tolerance=net_loader.search_tolerance,
        sort_field="",
        search_criteria=net_loader.search_criteria,
        match_type=net_loader.match_type,
        append=net_loader.append,
        snap_to_position_along_network=net_loader.snap,
        snap_offset=net_loader.snap_offset,
        exclude_restricted_elements=net_loader.exclude_restricted,
        search_query=net_loader.search_query
        )
    
    # Step 3 - iterate through destinations
    print("Iterate destinations and solve")
    header = True
    mode = "w"
    # Use origin field maps to expedite loading
    fm = "Name Name #;CurbApproach CurbApproach 0;SourceID SourceID #;SourceOID SourceOID #;PosAlong PosAlong #;SideOfEdge SideOfEdge #"
    for chunk in PMT.iter_rows_as_chunks("OD Cost Matrix\Origins", chunksize=chunksize):
        print(".", end="")
        arcpy.AddLocations_na(
            in_network_analysis_layer="OD Cost Matrix",
            sub_layer="Destinations",
            in_table=chunk,
            field_mappings=fm,
            # search_tolerance="5 Meters",
            # sort_field="",
            # search_criteria="edges NONE;osm_ND_Junctions END",
            # match_type="MATCH_TO_CLOSEST",
            # append="CLEAR",
            # snap_to_position_along_network="NO_SNAP",
            # snap_offset="5 Meters",
            # exclude_restricted_elements="INCLUDE",
            # search_query="edges #;osm_ND_Junctions #"
            )

        # Solve OD Matrix
        arcpy.Solve_na("OD Cost Matrix", "SKIP", "CONTINUE")

        # Dump to csv
        line_features = "OD Cost Matrix\Lines"
        linesToCentrality(
            line_features, impedance_attribute, out_csv, header, mode)
        mode = "a"
        header = False

    return out_csv


# %% MAIN
if __name__ == "__main__":
    # TODO: handle year-osm relationship; formalize in config.prepare_config.py
    # Feature layers: edges, nodes
    edges = arcpy.MakeFeatureLayer_management(EDGES, "EDGES")
    nodes = arcpy.MakeFeatureLayer_management(NODES, "NODES")
    # Select edges by attribute - service roads
    where = arcpy.AddFieldDelimiters(edges, "highway") + "LIKE '%service%'"
    arcpy.SelectLayerByAttribute_management(edges, "NEW_SELECTION", where)
    # Select nodes by location - nodes not touching services roads
    arcpy.SelectLayerByLocation_management(
        nodes, "INTERSECT", edges, selection_type="NEW_SELECTION",
        invert_spatial_relationship="INVERT")
    
    out_csv = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\OSM_Networks\Centrality.csv"

    out_csv = network_centrality(in_nd=BIKE_NET,
                                 in_features=nodes,
                                 net_loader=NL,
                                 out_csv=out_csv,
                                 name_field=NODEID,
                                 impedance_attribute=IMPED_ATTR,
                                 cutoff=CUTOFF,
                                 restrictions=RESTRICTIONS,
                                 chunksize=1000)

    