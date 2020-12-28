"""
"""


# %% IMPORTS
import PMT
import arcpy
import pandas as pd


# %% GLOBALS



# %% FUNCTIONS
def linesToCentrality(line_feaures, out_csv, header, mode):
    # Dump to df
    df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(
            line_features, ["Name", "Total_Length"]
        )
    )
    #df.columns = ["Name", "Total_Length"]
    names = ["N", "Node"]
    df[names] = df["Name"].str.split(" - ", n=1, expand=True)

    # Summarize
    sum_df = df.groupby("Node").agg(
        {"N": "size", "Total_Length": sum}
        ).reset_index()

    # Calculate centrality
    sum_df["centrality"] = (sum_df.N - 1)/sum_df.Total_Length

    # Export
    sum_df.to_csv(out_csv, mode=mode, header=header, index=False)
    

# %% Step 1
# Make OD Cost matrix
print("Make OD problem")
arcpy.MakeODCostMatrixLayer_na(
    in_network_dataset="K:/Projects/MiamiDade/PMT/Data/Cleaned/OSM_Networks/bike_q3_2019.gdb/osm/osm_ND",
    out_network_analysis_layer="OD Cost Matrix",
    impedance_attribute="Length",
    default_cutoff="1609",
    default_number_destinations_to_find="",
    accumulate_attribute_name="",
    UTurn_policy="ALLOW_UTURNS",
    restriction_attribute_name="Oneway;IsCycleway;LTS1;LTS2;LTS3;LTS4",
    hierarchy="NO_HIERARCHY",
    hierarchy_settings="",
    output_path_shape="NO_LINES",
    time_of_day=""
    )

# %% step 2
# Add Origins locations
print("Load all origins")
in_features = "K:/Projects/MiamiDade/PMT/Data/Cleaned/OSM_Networks/bike_q3_2019.gdb/osm/osm_ND_junctions"
arcpy.AddLocations_na(
    in_network_analysis_layer="OD Cost Matrix",
    sub_layer="Origins",
    in_table=in_features,
    field_mappings="Name OBJECTID #",
    search_tolerance="5 Meters",
    sort_field="",
    search_criteria="edges NONE;osm_ND_Junctions END",
    match_type="MATCH_TO_CLOSEST",
    append="CLEAR",
    snap_to_position_along_network="NO_SNAP",
    snap_offset="5 Meters",
    exclude_restricted_elements="INCLUDE",
    search_query="edges #;osm_ND_Junctions #"
    )

# %% step 3
# Add Destination locations (using origins)
print("Iterate destinations and solve")
out_csv = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\OSM_Networks\Centrality.csv"
header = True
mode = "w"
for chunk in PMT.iterRowsAsChunks("OD Cost Matrix\Origins", chunksize=1000):
    print(".", end="")
    arcpy.AddLocations_na(
        in_network_analysis_layer="OD Cost Matrix",
        sub_layer="Destinations",
        in_table=chunk,
        field_mappings="Name Name #;CurbApproach CurbApproach 0;SourceID SourceID #;SourceOID SourceOID #;PosAlong PosAlong #;SideOfEdge SideOfEdge #",
        search_tolerance="5 Meters",
        sort_field="",
        search_criteria="edges NONE;osm_ND_Junctions END",
        match_type="MATCH_TO_CLOSEST",
        append="CLEAR",
        snap_to_position_along_network="NO_SNAP",
        snap_offset="5 Meters",
        exclude_restricted_elements="INCLUDE",
        search_query="edges #;osm_ND_Junctions #"
        )

    # Solve OD Matrix
    print("-", end="")
    arcpy.Solve_na("OD Cost Matrix", "SKIP", "CONTINUE")

    # Dump to csv
    print("_", end=" ")
    line_features = "OD Cost Matrix\Lines"
    linesToCentrality(line_features, out_csv, header, mode)
    mode = "a"
    header = False




# %% MAIN
#if __name__ == "__main__":
#    pass