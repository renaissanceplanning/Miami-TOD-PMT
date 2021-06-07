# %% IMPORTS

import arcpy
import os
import tempfile
import uuid
import numpy as np
import pandas as pd
from six import string_types
from collections.abc import Iterable
from PMT_tools.PMT import Column, AggColumn, Consolidation, MeltColumn
import PMT_tools.PMT as PMT

# %% GLOBALS
SNAPSHOT_YEAR = 2019
MODES = ["Auto", "Transit", "Walk", "Bike"]
ACTIVITIES = ["TotalJobs", "ConsJobs", "HCJobs", "EnrollAdlt", "EnrollK12", "HH"]
TIME_BREAKS = [15, 30, 45, 60]


# %% FUNCTIONS
def _validateAggSpecs(var, expected_type):
    e_type = expected_type.__name__
    # Simplest: var is the expected type
    if isinstance(var, expected_type):
        # Listify
        var = [var]
    # var could be an iterable of the expected type
    # - If not iterable, it's the wrong type, so raise error
    elif not isinstance(var, Iterable):
        bad_type = type(var)
        raise ValueError(
            f"Expected one or more {e_type} objects, got {bad_type}")
    # - If iterable, confirm items are the correct type
    else:
        for v in var:
            if not isinstance(v, expected_type):
                bad_type = type(v)
                raise ValueError(
                    f"Expected one or more {e_type} objects, got {bad_type}")
    # If no errors, return var (in original form or as list)
    return var


def joinAttributes(to_table, to_id_field, from_table, from_id_field,
                   join_fields, null_value=0.0, renames={}):
    """
    """
    if join_fields == "*":
        join_fields = [f.name for f in arcpy.ListFields(from_table)
                       if not f.required and f.name != from_id_field]
    print(f"... {join_fields} to {to_table}")
    dump_fields = [from_id_field] + join_fields
    df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(
            from_table, dump_fields, null_value=null_value
        )
    )
    if renames:
        df.rename(columns=renames, inplace=True)
    PMT.extend_table_df(to_table, to_id_field, df, from_id_field)


def summarizeAttributes(in_fc, group_fields, agg_cols,
                        consolidations=None, melt_col=None):
    """

    """
    # Validation (listify inputs, validate values)
    # - Group fields
    group_fields = _validateAggSpecs(group_fields, Column)
    gb_fields = [gf.name for gf in group_fields]
    dump_fields = [gf.name for gf in group_fields]
    keep_cols = []
    null_dict = dict([(gf.name, gf.default) for gf in group_fields])
    renames = [
        (gf.name, gf.rename) for gf in group_fields if gf.rename is not None]
    # - Agg columns
    agg_cols = _validateAggSpecs(agg_cols, AggColumn)
    agg_methods = {}
    for ac in agg_cols:
        dump_fields.append(ac.name)
        keep_cols.append(ac.name)
        null_dict[ac.name] = ac.default
        agg_methods[ac.name] = ac.agg_method
        if ac.rename is not None:
            renames.append((ac.name, ac.rename))
    # - Consolidations
    if consolidations:
        consolidations = _validateAggSpecs(consolidations, Consolidation)
        for c in consolidations:
            dump_fields += [ic for ic in c.input_cols]
            keep_cols.append(c.name)
            null_dict.update(c.defaultsDict())
            agg_methods[c.name] = c.agg_method
    else:
        consolidations = []
    # - Melt columns
    if melt_col:
        melt_col = _validateAggSpecs(melt_col, MeltColumn)[0]
        dump_fields += [ic for ic in melt_col.input_cols]
        gb_fields.append(melt_col.label_col)
        keep_cols.append(melt_col.val_col)
        null_dict.update(melt_col.defaultsDict())
        agg_methods[melt_col.val_col] = melt_col.agg_method

    # Dump the intersect table to df
    int_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(
            in_fc, dump_fields, null_value=null_dict)
    )

    # Consolidate columns
    for c in consolidations:
        int_df[c.name] = int_df[c.input_cols].agg(c.cons_method, axis=1)

    # Melt columns
    if melt_col:
        id_fields = [f for f in gb_fields if f != melt_col.label_col]
        id_fields += [f for f in keep_cols if f != melt_col.val_col]
        int_df = int_df.melt(
            id_vars=id_fields,
            value_vars=melt_col.input_cols,
            var_name=melt_col.label_col,
            value_name=melt_col.val_col
        ).reset_index()

    # Group by - summarize
    all_fields = gb_fields + keep_cols
    sum_df = int_df[all_fields].groupby(gb_fields).agg(agg_methods).reset_index()

    # Apply renames
    if renames:
        sum_df.rename(columns=dict(renames), inplace=True)

    return sum_df


def _makeAccessColSpecs(activities, time_breaks, mode, include_average=True):
    cols = []
    new_names = []
    for a in activities:
        for tb in time_breaks:
            col = f"{a}{tb}Min"
            cols.append(col)
            new_names.append(f"{col}{mode[0]}")
        if include_average:
            col = f"AvgMin{a}"
            cols.append(col)
            new_names.append(f"{col}{mode[0]}")
    renames = dict(zip(cols, new_names))
    return cols, renames


def _createLongAccess(int_fc, id_field, activities, time_breaks, mode):
    # result is long on id_field, activity, time_break
    # TODO: update to use Column objects? (null handling, e.g.)
    # --------------
    # Dump int fc to data frame
    acc_fields, renames = _makeAccessColSpecs(activities, time_breaks, mode, include_average=False)
    if isinstance(id_field, string_types):
        id_field = [id_field]  # elif isinstance(Column)?

    all_fields = id_field + list(renames.values())
    df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(int_fc, all_fields, null_value=0.0)
    )
    # Set id field(s) as index
    df.set_index(id_field, inplace=True)

    # Make tidy hierarchical columns
    levels = []
    order = []
    for tb in time_breaks:
        for a in activities:
            col = f"{a}{tb}Min{mode[0]}"
            idx = df.columns.tolist().index(col)
            levels.append((a, tb))
            order.append(idx)
    header = pd.DataFrame(np.array(levels)[np.argsort(order)],
                          columns=["Activity", "TimeBin"])
    mi = pd.MultiIndex.from_frame(header)
    df.columns = mi
    df.reset_index(inplace=True)
    # Melt
    return df.melt(id_vars=id_field)


# %% MAIN
if __name__ == "__main__":
    YEAR_COL = AggColumn("Year", agg_method="mean", default=SNAPSHOT_YEAR)

    # GDB's
    print("Creating snapshot GDB")
    in_gdb = PMT.make_path(PMT.DATA, f"IDEAL_PMT_{SNAPSHOT_YEAR}.gdb")
    out_gdb_name = f"{uuid.uuid4().hex}.gdb"
    arcpy.CreateFileGDB_management(PMT.DATA, out_gdb_name)
    # out_gdb_name = "47ba6fd992f6463393c873c0b7a33fe8.gdb"
    out_gdb = PMT.make_path(PMT.DATA, out_gdb_name)

    # Copy spatial shells
    # ---------------------------
    # Networks, Points, and Polygons Feature Datasets
    for fds in ["Networks", "Points", "Polygons"]:
        print(f"... copying FDS {fds}")
        source_fd = PMT.make_path(in_gdb, fds)
        out_fd = PMT.make_path(out_gdb, fds)
        arcpy.Copy_management(source_fd, out_fd)

    # Join attributes from tables to features
    # -------------------------------------------
    print("Joining tables")
    # Feature class specs
    # - blocks
    blocks = PMT.make_path(out_gdb, "Polygons", "Blocks")
    block_id = "GEOID10"
    # - parcels
    parcels = PMT.make_path(out_gdb, "Polygons", "Parcels")
    parcel_id = "FOLIO"
    # - MAZ
    mazs = PMT.make_path(out_gdb, "Polygons", "MAZ")
    maz_id = "MAZ"
    # - TAZ
    tazs = PMT.make_path(out_gdb, "Polygons", "TAZ")
    taz_id = "TAZ"
    # - OSM nodes
    osm_nodes = PMT.make_path(out_gdb, "Networks", "nodes_bike")
    osm_id = "NODEID"
    # - Summary areas
    sum_areas = PMT.make_path(out_gdb, "Polygons", "SummaryAreas")
    sum_areas_id = "RowID"

    # Table specs
    # - block
    imperviousness = PMT.make_path(in_gdb, "Imperviousness_blocks")
    imperviousness_id = "GEOID10"
    # - parcel
    econdem = PMT.make_path(in_gdb, "EconDemog_parcels")
    econdem_id = "FOLIO"
    energycons = PMT.make_path(in_gdb, "EnergyCons_parcels")
    energycons_id = "FOLIO"
    lucodes = PMT.make_path(in_gdb, "LandUseCodes_parcels")
    lucodes_id = "FOLIO"
    walktime = PMT.make_path(in_gdb, "WalkTime_parcels")
    walktime_id = "FOLIO"
    contig = PMT.make_path(in_gdb, "Contiguity_parcels")
    contig_id = "FOLIO"
    # - MAZ
    maz_access_w = PMT.make_path(in_gdb, "access_maz_Walk")
    maz_access_b = PMT.make_path(in_gdb, "access_maz_Bike")
    maz_access_id = "MAZ"
    maz_walk_cols, maz_walk_renames = _makeAccessColSpecs(
        ACTIVITIES, TIME_BREAKS, "Walk")
    maz_bike_cols, maz_bike_renames = _makeAccessColSpecs(
        ACTIVITIES, TIME_BREAKS, "Bike")
    # - TAZ
    taz_access_a = PMT.make_path(in_gdb, "access_taz_Auto")
    taz_access_t = PMT.make_path(in_gdb, "access_taz_Transit")
    taz_access_id = "TAZ"
    taz_auto_cols, taz_auto_renames = _makeAccessColSpecs(
        ACTIVITIES, TIME_BREAKS, "Auto")
    taz_tran_cols, taz_tran_renames = _makeAccessColSpecs(
        ACTIVITIES, TIME_BREAKS, "Transit")
    taz_trip = PMT.make_path(in_gdb, "TripStats_TAZ")
    taz_trip_id = "TAZ"
    # - OSM Nodes
    centrality = PMT.make_path(in_gdb, "NetworkCentrality_nodes_bike")
    centrality_id = "Node"
    # - Summary areas
    diversity = PMT.make_path(in_gdb, "Diversity_summaryareas")
    diversity_id = "RowID_"

    # ****************************************************
    # joins
    # - blocks
    joinAttributes(to_table=blocks, to_id_field=block_id, from_table=imperviousness,
                   from_id_field=imperviousness_id, join_fields="*")
    # - parcels
    joinAttributes(parcels, parcel_id, econdem, econdem_id, "*")
    joinAttributes(parcels, parcel_id, energycons, energycons_id, "*")
    joinAttributes(parcels, parcel_id, lucodes, lucodes_id, "*")
    joinAttributes(parcels, parcel_id, walktime, walktime_id, "*")
    joinAttributes(parcels, parcel_id, contig, contig_id, "*")
    # - MAZ
    joinAttributes(mazs, maz_id, maz_access_w, maz_access_id,
                   maz_walk_cols, renames=maz_walk_renames)
    joinAttributes(mazs, maz_id, maz_access_b, maz_access_id,
                   maz_bike_cols, renames=maz_bike_renames)
    # - TAZ 
    joinAttributes(tazs, taz_id, taz_access_a, taz_access_id,
                   taz_auto_cols, renames=taz_auto_renames)
    joinAttributes(tazs, taz_id, taz_access_t, taz_access_id,
                   taz_tran_cols, renames=taz_tran_renames)
    joinAttributes(tazs, taz_id, taz_trip, taz_trip_id, "*")
    # - OSM nodes
    joinAttributes(osm_nodes, osm_id, centrality, centrality_id, "*")
    # - Summary areas TODO: developable area/contiguity
    joinAttributes(sum_areas, sum_areas_id, diversity, diversity_id, "*")
    # ****************************************************



    # Intersect features
    # --------------------------
    # Blocks || parcels
    print("Running intersections")
    print("... blocks and parcels")
    blocks = PMT.make_path(out_gdb, "Polygons", "Blocks")
    int_bp = PMT.intersect_features(blocks, parcels)
    # Summary areas || parcels
    print("... summary areas and parcels")
    int_sp = PMT.intersect_features(sum_areas, parcels)
    # Summary areas || Blocks
    print("... summary areas and Blocks")
    int_sb = PMT.intersect_features(sum_areas, blocks)
    # Summary areas || MAZs
    print("... summary areas and MAZs")
    int_sm = PMT.intersect_features(sum_areas, mazs)
    # Summary areas || TAZs
    print("... summary areas and TAZs")
    int_st = PMT.intersect_features(sum_areas, tazs)

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Enrich features through summarization
    # -----------------------------------------
    # Blocks from parcels
    print("Enriching tables")
    print("... blocks with parcel sums")
    agg_cols = [
        AggColumn("NO_RES_UNTS"),
        AggColumn("TOT_LVG_AREA"),
        AggColumn("JV"),
        AggColumn("TV_NSD"),
        AggColumn("LND_SQFOOT"),
        AggColumn("Total_Commutes"),
        AggColumn("Drove"),
        AggColumn("Carpool"),
        AggColumn("Transit"),
        AggColumn("NonMotor"),
        AggColumn("Work_From_Home"),
        AggColumn("AllOther"),
        AggColumn("BTU_RES"),
        AggColumn("NRES_BTU"),
        AggColumn("Developable_Area"),
        AggColumn("Mean_Contiguity", agg_method="mean"),
        AggColumn("Mean_Scaled_Area", agg_method="mean"),
        AggColumn("Median_Scaled_Area", agg_method=np.median),
        AggColumn("Total_Employment"),
        AggColumn("CNS16", rename="HCJobs"),
        AggColumn("CNS15", rename="EdJobs")
    ]
    consolidate = [
        Consolidation("RsrcJobs", ["CNS01", "CNS02"]),
        Consolidation("IndJobs", ["CNS05", "CNS06", "CNS08"]),
        Consolidation("ConsJobs", ["CNS07", "CNS17", "CNS18"]),
        Consolidation("OffJobs", ["CNS09", "CNS10", "CNS11",
                                  "CNS12", "CNS13", "CNS20"]),
        Consolidation("OthJobs", ["CNS03", "CNS04", "CNS14",
                                  "CNS19"])
    ]
    df = summarizeAttributes(in_fc=int_bp, group_fields=Column(block_id),
                             consolidations=consolidate, melt_col=agg_cols,)
    PMT.extend_table_df(blocks, block_id, df, block_id)

    # Summary areas from parcels
    print("... summary areas with parcel sums")
    agg_cols = [
        AggColumn("NO_RES_UNTS"),
        AggColumn("TOT_LVG_AREA"),
        AggColumn("JV"),
        AggColumn("TV_NSD"),
        AggColumn("LND_SQFOOT"),
        AggColumn("Total_Commutes"),
        AggColumn("Drove"),
        AggColumn("Carpool"),
        AggColumn("Transit"),
        AggColumn("NonMotor"),
        AggColumn("Work_From_Home"),
        AggColumn("AllOther"),
        AggColumn("BTU_RES"),
        AggColumn("NRES_BTU"),
        AggColumn("Developable_Area"),
        AggColumn("Mean_Contiguity", agg_method="mean"),
        AggColumn("Mean_Scaled_Area", agg_method="mean"),
        AggColumn("Median_Scaled_Area", agg_method=np.median),
        AggColumn("Max_Scaled_Area", agg_method="max"),
        AggColumn("MinTimeStn_walk", agg_method="mean"),
        AggColumn("MinTimePark_walk", agg_method="mean"),
        AggColumn("MinTimeStn_ideal", agg_method="mean"),
        AggColumn("MinTimeParks_ideal", agg_method="mean"),
        AggColumn("NStn_walk", agg_method="mean"),
        AggColumn("NPark_walk", agg_method="mean"),
        AggColumn("Total_Employment"),
        AggColumn("CNS16", rename="HCJobs"),
        AggColumn("CNS15", rename="EdJobs")
    ]
    consolidate = [
        Consolidation("RsrcJobs", ["CNS01", "CNS02"]),
        Consolidation("IndJobs", ["CNS05", "CNS06", "CNS08"]),
        Consolidation("ConsJobs", ["CNS07", "CNS17", "CNS18"]),
        Consolidation("OffJobs", ["CNS09", "CNS10", "CNS11",
                                  "CNS12", "CNS13", "CNS20"]),
        Consolidation("OthJobs", ["CNS03", "CNS04", "CNS14",
                                  "CNS19"])
    ]
    df = summarizeAttributes(int_sp, Column(sum_areas_id), agg_cols,
                             consolidations=consolidate)
    PMT.extend_table_df(sum_areas, sum_areas_id, df, sum_areas_id)

    # Summary areas from MAZs
    print("... summary areas with MAZ averages")
    agg_cols = [AggColumn(maz_col, agg_method="mean") for
                maz_col in maz_walk_renames.values()]
    agg_cols += [AggColumn(maz_col, agg_method="mean") for
                 maz_col in maz_bike_renames.values()]
    df = summarizeAttributes(int_sm, Column(sum_areas_id), agg_cols)
    PMT.extend_table_df(sum_areas, sum_areas_id, df, sum_areas_id)

    # Summary areas from TAZs
    print("... summary areas with TAZ averages")
    # - Access cols
    agg_cols = [AggColumn(taz_col, agg_method="mean") for
                taz_col in taz_auto_renames.values()]
    agg_cols += [AggColumn(taz_col, agg_method="mean") for
                 taz_col in taz_tran_renames.values()]
    # - Trip stats cols
    agg_cols += [
        AggColumn("VMT"),
        AggColumn("TRAN_PMT"),
        AggColumn("AVG_TIME_AU", agg_method="mean"),
        AggColumn("AVG_DIST_AU", agg_method="mean"),
        AggColumn("AVG_TIME_TR", agg_method="mean"),
        AggColumn("AVG_DIST_TR", agg_method="mean"),
    ]
    df = summarizeAttributes(int_st, Column(sum_areas_id), agg_cols)
    PMT.extend_table_df(sum_areas, sum_areas_id, df, sum_areas_id)
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++

    # Elongate tables
    # ----------------------
    print("Building long tables")
    # Long on Land use
    print("... Summary area stats by land use")
    out_table = PMT.make_path(out_gdb, "AttrByLandUse")
    _sa_group_cols_ = [Column(sum_areas_id), Column("Name"), Column("Corridor")]
    group_cols = _sa_group_cols_ + [Column("GN_VA_LU", default="Unknown")]
    agg_cols = [
        YEAR_COL,
        AggColumn("NO_RES_UNTS"),
        AggColumn("TOT_LVG_AREA"),
        AggColumn("JV"),
        AggColumn("TV_NSD"),
        AggColumn("LND_SQFOOT"),
        AggColumn("BTU_RES"),
        AggColumn("NRES_BTU")
    ]
    df = summarizeAttributes(int_sp, group_cols, agg_cols)
    PMT.df_to_table(df, out_table)

    # Long on commutes
    print("... Summary area commutes by mode")
    out_table = PMT.make_path(out_gdb, "CommutesByMode")
    agg_cols = [
        YEAR_COL,
        AggColumn("Total_Commutes")
    ]
    commutes = MeltColumn(
        label_col="CommMode",
        val_col="Commutes",
        input_cols=["Drove", "Carpool", "Transit",
                    "NonMotor", "Work_From_Home", "AllOther"]
    )
    df = summarizeAttributes(int_sp, _sa_group_cols_, agg_cols, melt_col=commutes)
    PMT.df_to_table(df, out_table)

    # Long on job type
    print("... Summary area jobs by sector")
    out_table = PMT.make_path(out_gdb, "JobsBySector")
    # - First consolidate parcel-level employment columns
    agg_cols = [
        AggColumn("CNS16", rename="HCJobs"),
        AggColumn("CNS15", rename="EdJobs")
    ]
    consolidate = [
        Consolidation("RsrcJobs", ["CNS01", "CNS02"]),
        Consolidation("IndJobs", ["CNS05", "CNS06", "CNS08"]),
        Consolidation("ConsJobs", ["CNS07", "CNS17", "CNS18"]),
        Consolidation("OffJobs", ["CNS09", "CNS10", "CNS11",
                                  "CNS12", "CNS13", "CNS20"]),
        Consolidation("OthJobs", ["CNS03", "CNS04", "CNS14",
                                  "CNS19"])
    ]
    df = summarizeAttributes(
        int_sp, Column(parcel_id), agg_cols, consolidations=consolidate)
    PMT.extend_table_df(int_sp, parcel_id, df, parcel_id)
    # - Then summarize jobs by sector
    agg_cols = [
        YEAR_COL,
        AggColumn("Total_Employment")
    ]
    sectors = MeltColumn(
        label_col="Sector",
        val_col="Jobs",
        input_cols=["RsrcJobs", "IndJobs", "ConsJobs", "OffJobs",
                    "EdJobs", "HCJobs", "OthJobs"]
    )
    df = summarizeAttributes(int_sp, _sa_group_cols_, agg_cols, melt_col=sectors)
    PMT.df_to_table(df, out_table)

    # Long on walk time to stations
    print("... Summary area by walk time to stations")
    out_table = PMT.make_path(out_gdb, "WalkTimeToStations")
    group_cols = _sa_group_cols_ + [
        Column("GN_VA_LU", default="Unknown"), Column("BinStn_walk")]
    agg_cols = [
        YEAR_COL,
        AggColumn("TOT_LVG_AREA"),
        AggColumn("NO_RES_UNTS"),
        AggColumn("FOLIO", agg_method="size", rename="NParcels")
    ]
    df = summarizeAttributes(int_sp, group_cols, agg_cols)
    PMT.df_to_table(df, out_table)

    # Long on walk time to parks
    print("... Summary area by walk time to parks")
    out_table = PMT.make_path(out_gdb, "WalkTimeToParks")
    group_cols = _sa_group_cols_ + [Column("BinStn_walk")]
    agg_cols = [
        YEAR_COL,
        AggColumn("TOT_LVG_AREA"),
        AggColumn("NO_RES_UNTS"),
        AggColumn("FOLIO", agg_method="size", rename="NParcels")
    ]
    df = summarizeAttributes(int_sp, group_cols, agg_cols)
    PMT.df_to_table(df, out_table)

    # Long on dev status
    print("... Summary area land area by dev status")
    out_table = PMT.make_path(out_gdb, "AreaByDevStatus")
    agg_cols = [
        YEAR_COL,
        AggColumn("TotalArea")
    ]
    dev_status = MeltColumn(
        label_col="DevStatus",
        val_col="Area",
        input_cols=["NonDevArea", "DevOSArea", "DevLowArea",
                    "DevMedArea", "DevHighArea"]
    )
    df = summarizeAttributes(int_sb, _sa_group_cols_, agg_cols, melt_col=dev_status)
    PMT.df_to_table(df, out_table)

    # Long on access mode
    print("... Access scores by activity and time bin")
    id_fields = ["RowID", "Name", "Corridor", YEAR_COL.name]
    for mode in MODES:
        print(f"... ... {mode}")
        df = _createLongAccess(
            sum_areas, id_fields, ACTIVITIES, TIME_BREAKS, mode)
        out_table = PMT.make_path(out_gdb, f"ActivityByTime_{mode}")
        PMT.df_to_table(df, out_table)

    # Rename output
    # arcpy.Rename_management(out_gdb, )
##

    # Trend can start from here
    """ 
        - networks,and parcels can stay out of Trend
        - points, permits only
        - can take long tables and stack by year (
        - take the difference between SN and base year at block, summary area, etc..level     
            - "_by_year" tables
            - "_difference" FCs (Snapshot - BaseYear) 
                - rename feature classes where appropriate (Blocks, BlockGroups
        - percent difference as an addition would be valuable  
    """
    # Near Term
    """
        - Ideal_PMT_NearTerm
            - networks the same
            - points, permits only
            - parcels are different from the permits {build_trend_shortterm_parcels.py}
                - use the Parcel_with_new_permits as Snapshot
                - generate trend against base year
    """