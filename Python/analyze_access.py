"""
Created: December 2020
@Author: Alex Bell


"""


# %% IMPORTS
import PMT
import arcpy
import pandas as pd
import numpy as np
from six import string_types
from analyze_osm_networks import NetLoader, _listAccumulationAttributes

# %% GLOBALS
TIME_BREAKS = [10, 20, 30, 40, 50, 60]


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
    fmap = ";".join([f"{ff} {fv} #" for ff,fv in zip(fmap_fields, fmap_vals)])
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
        match_type = net_loader.match_type,
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
    arcpy.na.Solve(in_network_analysis_layer=net_layer,
                    ignore_invalids="SKIP",
                    terminate_on_solve_error="TERMINATE"
                    )

def genODTable(origin_pts, origin_name_field, dest_pts, dest_name_field,
               in_nd, imped_attr, cutoff, net_loader, out_table,
               restrictions=None, use_hierarchy=False, uturns="ALLOW_UTURNS",
               o_location_fields=None, d_location_fields=None):
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

    """
    if use_hierarchy:
        hierarchy = "USE_HIERARCHY"
    else:
        hierarchy = "NO_HIERARCHY"
    accum = _listAccumulationAttributes(in_nd, imped_attr)

    print("... ...OD MATRIX: create network problem")
    net_layer = arcpy.MakeODCostMatrixLayer_na(
        # Setup
        in_network_dataset=in_nd,
        out_network_analysis_layer="__od__",
        impedance_attribute=imped_attr,
        default_cutoff=cutoff,
        accumulate_attribute_name=accum,
        UTurn_policy=uturns,
        restriction_attribute_name=restrictions,
        output_path_shape="NO_LINES",
        hierarchy=hierarchy,
        time_of_day=None
    )
    net_layer_ = net_layer.getOutput(0)
    try:
        _loadLocations("__od__", "Origins", origin_pts, origin_name_field,
                        net_loader, o_location_fields)
        _loadLocations("__od__", "Destinations", dest_pts, dest_name_field,
                        net_loader, d_location_fields)
        _solve("__od__")
        print("... ... solved, dumping to data frame")
        # Get output as a data frame
        sublayer_names = arcpy.na.GetNAClassNames(net_layer_)
        extend_lyr_name = sublayer_names["ODLines"]
        try:
            extend_sublayer = net_layer.listLayers(extend_lyr_name)[0]
        except:
            extend_sublayer = arcpy.mapping.ListLayers(
                net_layer, extend_lyr_name)[0]
        out_fields = ["Name", f"Total_{imped_attr}"]
        columns = ["Name", imped_attr]
        out_fields += [f"Total_{attr}" for attr in accum]
        columns += [c for c in accum]
        df = pd.DataFrame(
            arcpy.da.TableToNumPyArray(extend_sublayer, out_fields),
            columns=columns
        )
        # Split outputs
        df[["O_NAME", "D_NAME"] = df["Name"].str.split(" - ", n=1, expand=True)
    except:
        raise
    finally:
        print("... ...deleting network problem")
        arcpy.Delete_management(net_layer)

def summarizeAccess(skim_table, o_field, d_field, imped_field,
                    se_data, id_field, act_fields, out_table,
                    imped_breaks, units="minutes",
                    join_by="D", chunk_size=100000, **kwargs):
    """
    Reads an origin-destination skim table, joins activity data,
    and summarizes activities by impedance bins.

    Parameters
    -----------
    skim_table: Path
    o_field: String
    d_field: String
    imped_field: String
    se_data: Path
    id_field: String
    act_fields: [String, ...]
    out_table: Path
    imped_breaks: [Numeric, ...]
    units: String, default="minutes"
    join_by: String, default="D"
    chunk_size: Int, default=100000
    kwargs:
        Keyword arguments for reading the skim table

    Returns
    --------
    out_table: Path
    """
    # Prep vars
    if isinstance(act_fields, string_types):
        act_fields = [act_fields]
    if join_by == "D":
        left_on = d_field
        gb_field = o_field
    elif join_by == "O":
        left_on = o_field
        gb_field = d_field
    else:
        raise ValueError(
            f"Expected 'D' or 'O' as `join_by` value - got {join_by}")
    # Read the activity data
    _a_fields_ = [id_field, act_fields]
    act_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(se_data, _a_fields_)
    )

    # Read the skim table
    out_dfs = []
    use_cols = [o_field, d_field, imped_field]
    for chunk in pd.read_csv(
        skim_table, usecols=use_cols, chunksize=chunk_size, **kwargs):
        # Define impedance bins
        low = -np.inf
        criteria = []
        labels = []
        for i_break in imped_breaks:
            crit = np.logical_and(
                chunk[imped_field] >= low,
                chunk[imped_field] < i_break
            )
            criteria.append(crit)
            labels.append(f"<{i_break} {units}")
            low = i_break
        # Apply categories
        chunk[f"BIN_{units}"] = np.select(
            criteria, labels, f"{i_break} {units}+"
        )
        # Join the activity data
        join_df = chunk.merge(
            act_df, how="inner", left_on=left_on, right_on=id_field)
        # Summarize
        sum_df = join_df.groupby(gb_field).sum().reset_index()
        out_dfs.append(sum_df)
    # Concatenate all
    out_df = pd.concat(out_dfs)
    PMT.dfToTable(out_df, out_table)


# %% MAIN
if __name__ == "__main__":
    for year in PMT.YEARS:
        print(year)

