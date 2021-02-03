# %% IMPORTS

import arcpy
import os
import tempfile
import uuid
import numpy as np
import pandas as pd
from six import string_types
from collections.abc import Iterable
import PMT

# %% GLOBALS
SNAPSHOT_YEAR = 2019

MODES = ["Auto", "Transit", "Walk", "Bike"]
ACTIVITIES = ["TotalJobs", "ConsJobs", "HCJobs", "EnrollAdlt", "EnrollK12", "HH"]
TIME_BREAKS = [15, 30, 45, 60]


# %% CLASSES
class Column():
    def __init__(self, name, default=0.0, rename=None):
        self.name = name
        self.default = default
        self.rename = rename


class AggColumn(Column):
    def __init__(self, name, agg_method=sum, default=0.0, rename=None):
        Column.__init__(self, name, default, rename)
        self.agg_method = agg_method


class CollCollection(AggColumn):
    def __init__(self, name, input_cols, agg_method=sum, default=0.0):
        AggColumn.__init__(self, name, agg_method, default)
        self.input_cols = input_cols

    def __setattr__(self, name, value):
        if name == "input_cols":
            valid = True
            if isinstance(value, string_types):
                valid = False
            elif not isinstance(value, Iterable):
                valid = False
            elif len(value) <= 1:
                valid = False
            elif not isinstance(value[0], string_types):
                valid = False
            # Set property of raise error
            if valid:
                super().__setattr__(name, value)
            else:
                raise ValueError(
                    f"Expected iterable of column names for `input_cols`")
        else:
            super().__setattr__(name, value)

    def defaultsDict(self):
        if isinstance(self.default, Iterable) and \
                not isinstance(self.default, string_types):
            return dict(zip(self.input_cols, self.default))
        else:
            return dict(
                zip(self.input_cols,
                    [self.default for ic in self.input_cols]
                    )
            )


class Consolidation(CollCollection):
    def __init__(self, name, input_cols, cons_method=sum,
                 agg_method=sum, default=0.0):
        CollCollection.__init__(self, name, input_cols, agg_method, default)
        self.cons_method = cons_method


class MeltColumn(CollCollection):
    def __init__(self, label_col, val_col, input_cols,
                 agg_method=sum, default=0.0):
        CollCollection.__init__(self, val_col, input_cols, agg_method, default)
        self.label_col = label_col
        self.val_col = val_col


class Join(CollCollection):
    def __init__(self, on_col, input_cols, agg_method=sum, default=0.0):
        CollCollection.__init__(self, None, input_cols, agg_method, default)
        self.on_col = on_col


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


def intersectFeatures(summary_fc, disag_fc):
    """

    """
    # Create a temporary gdb for storing the intersection result
    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(out_folder_path=temp_dir,
                                   out_name="Intermediates.gdb")
    int_gdb = PMT.makePath(temp_dir, "Intermediates.gdb")

    # Convert disag features to centroids
    disag_full_path = arcpy.Describe(disag_fc).catalogPath
    disag_ws, disag_name = os.path.split(disag_full_path)
    out_fc = PMT.makePath(int_gdb, disag_name)
    disag_pts = PMT.polygonsToPoints(
        disag_fc, out_fc, fields="*", skip_nulls=False, null_value=0)

    # Run intersection
    int_fc = PMT.makePath(int_gdb, f"int_{disag_name}")
    arcpy.Intersect_analysis([summary_fc, disag_pts], int_fc)

    # return intersect
    return int_fc


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
    PMT.extendTableDf(to_table, to_id_field, df, from_id_field)


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
    in_gdb = PMT.makePath(
        PMT.DATA, f"IDEAL_PMT_{SNAPSHOT_YEAR}.gdb"
    )
    out_gdb_name = f"{uuid.uuid4().hex}.gdb"
    arcpy.CreateFileGDB_management(PMT.DATA, out_gdb_name)
    # out_gdb_name = "47ba6fd992f6463393c873c0b7a33fe8.gdb"
    out_gdb = PMT.makePath(PMT.DATA, out_gdb_name)

    # Copy spatial shells
    # ---------------------------
    # Networks, Points, and Polygons Feature Datasets
    for fds in ["Networks", "Points", "Polygons"]:
        print(f"... copying FDS {fds}")
        source_fd = PMT.makePath(in_gdb, fds)
        out_fd = PMT.makePath(out_gdb, fds)
        arcpy.Copy_management(source_fd, out_fd)

    # Join attributes from tables to features
    # -------------------------------------------
    print("Joining tables")
    # Feature class specs
    # - blocks
    blocks = PMT.makePath(out_gdb, "Polygons", "Blocks")
    block_id = "GEOID10"
    # - parcels
    parcels = PMT.makePath(out_gdb, "Polygons", "Parcels")
    parcel_id = "FOLIO"
    # - MAZ
    mazs = PMT.makePath(out_gdb, "Polygons", "MAZ")
    maz_id = "MAZ"
    # - TAZ
    tazs = PMT.makePath(out_gdb, "Polygons", "TAZ")
    taz_id = "TAZ"
    # - OSM nodes
    osm_nodes = PMT.makePath(out_gdb, "Networks", "nodes_bike")
    osm_id = "NODEID"
    # - Summary areas
    sum_areas = PMT.makePath(out_gdb, "Polygons", "SummaryAreas")
    sum_areas_id = "RowID"

    # Table specs
    # - block
    imperviousness = PMT.makePath(in_gdb, "Imperviousness_blocks")
    imperviousness_id = "GEOID10"
    # - parcel
    econdem = PMT.makePath(in_gdb, "EconDemog_parcels")
    econdem_id = "FOLIO"
    energycons = PMT.makePath(in_gdb, "EnergyCons_parcels")
    energycons_id = "FOLIO"
    lucodes = PMT.makePath(in_gdb, "LandUseCodes_parcels")
    lucodes_id = "FOLIO"
    walktime = PMT.makePath(in_gdb, "WalkTime_parcels")
    walktime_id = "FOLIO"
    contig = PMT.makePath(in_gdb, "Contiguity_parcels")
    contig_id = "FOLIO"
    # - MAZ
    maz_access_w = PMT.makePath(in_gdb, "access_maz_Walk")
    maz_access_b = PMT.makePath(in_gdb, "access_maz_Bike")
    maz_access_id = "MAZ"
    maz_walk_cols, maz_walk_renames = _makeAccessColSpecs(
        ACTIVITIES, TIME_BREAKS, "Walk")
    maz_bike_cols, maz_bike_renames = _makeAccessColSpecs(
        ACTIVITIES, TIME_BREAKS, "Bike")
    # - TAZ
    taz_access_a = PMT.makePath(in_gdb, "access_taz_Auto")
    taz_access_t = PMT.makePath(in_gdb, "access_taz_Transit")
    taz_access_id = "TAZ"
    taz_auto_cols, taz_auto_renames = _makeAccessColSpecs(
        ACTIVITIES, TIME_BREAKS, "Auto")
    taz_tran_cols, taz_tran_renames = _makeAccessColSpecs(
        ACTIVITIES, TIME_BREAKS, "Transit")
    taz_trip = PMT.makePath(in_gdb, "TripStats_TAZ")
    taz_trip_id = "TAZ"
    # - OSM Nodes
    centrality = PMT.makePath(in_gdb, "NetworkCentrality_nodes_bike")
    centrality_id = "Node"
    # - Summary areas
    diversity = PMT.makePath(in_gdb, "Diversity_summaryareas")
    diversity_id = "RowID_"

    # ****************************************************
    # joins
    # - blocks
    joinAttributes(blocks, block_id, imperviousness, imperviousness_id, "*")
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

    ##
    ## Trend can start from here
    ##

    # Intersect features
    # --------------------------
    # Blocks || parcels
    print("Running intersections")
    print("... blocks and parcels")
    blocks = PMT.makePath(out_gdb, "Polygons", "Blocks")
    int_bp = intersectFeatures(blocks, parcels)
    # Summary areas || parcels
    print("... summary areas and parcels")
    int_sp = intersectFeatures(sum_areas, parcels)
    # Summary areas || Blocks
    print("... summary areas and Blocks")
    int_sb = intersectFeatures(sum_areas, blocks)
    # Summary areas || MAZs
    print("... summary areas and MAZs")
    int_sm = intersectFeatures(sum_areas, mazs)
    # Summary areas || TAZs
    print("... summary areas and TAZs")
    int_st = intersectFeatures(sum_areas, tazs)

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
    df = summarizeAttributes(int_bp, Column(block_id), agg_cols,
                             consolidations=consolidate)
    PMT.extendTableDf(blocks, block_id, df, block_id)

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
    PMT.extendTableDf(sum_areas, sum_areas_id, df, sum_areas_id)

    # Summary areas from MAZs
    print("... summary areas with MAZ averages")
    agg_cols = [AggColumn(maz_col, agg_method="mean") for
                maz_col in maz_walk_renames.values()]
    agg_cols += [AggColumn(maz_col, agg_method="mean") for
                 maz_col in maz_bike_renames.values()]
    df = summarizeAttributes(int_sm, Column(sum_areas_id), agg_cols)
    PMT.extendTableDf(sum_areas, sum_areas_id, df, sum_areas_id)

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
    PMT.extendTableDf(sum_areas, sum_areas_id, df, sum_areas_id)
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++

    # Elongate tables
    # ----------------------
    print("Building long tables")
    # Long on Land use
    print("... Summary area stats by land use")
    out_table = PMT.makePath(out_gdb, "AttrByLandUse")
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
    PMT.dfToTable(df, out_table)

    # Long on commutes
    print("... Summary area commutes by mode")
    out_table = PMT.makePath(out_gdb, "CommutesByMode")
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
    PMT.dfToTable(df, out_table)

    # Long on job type
    print("... Summary area jobs by sector")
    out_table = PMT.makePath(out_gdb, "JobsBySector")
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
    PMT.extendTableDf(int_sp, parcel_id, df, parcel_id)
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
    PMT.dfToTable(df, out_table)

    # Long on walk time to stations
    print("... Summary area by walk time to stations")
    out_table = PMT.makePath(out_gdb, "WalkTimeToStations")
    group_cols = _sa_group_cols_ + [
        Column("GN_VA_LU", default="Unknown"), Column("BinStn_walk")]
    agg_cols = [
        YEAR_COL,
        AggColumn("TOT_LVG_AREA"),
        AggColumn("NO_RES_UNTS"),
        AggColumn("FOLIO", agg_method="size", rename="NParcels")
    ]
    df = summarizeAttributes(int_sp, group_cols, agg_cols)
    PMT.dfToTable(df, out_table)

    # Long on walk time to parks
    print("... Summary area by walk time to parks")
    out_table = PMT.makePath(out_gdb, "WalkTimeToParks")
    group_cols = _sa_group_cols_ + [Column("BinStn_walk")]
    agg_cols = [
        YEAR_COL,
        AggColumn("TOT_LVG_AREA"),
        AggColumn("NO_RES_UNTS"),
        AggColumn("FOLIO", agg_method="size", rename="NParcels")
    ]
    df = summarizeAttributes(int_sp, group_cols, agg_cols)
    PMT.dfToTable(df, out_table)

    # Long on dev status
    print("... Summary area land area by dev status")
    out_table = PMT.makePath(out_gdb, "AreaByDevStatus")
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
    PMT.dfToTable(df, out_table)

    # Long on access mode
    print("... Access scores by activity and time bin")
    id_fields = ["RowID", "Name", "Corridor", YEAR_COL.name]
    for mode in MODES:
        print(f"... ... {mode}")
        df = _createLongAccess(
            sum_areas, id_fields, ACTIVITIES, TIME_BREAKS, mode)
        out_table = PMT.makePath(out_gdb, f"ActivityByTime_{mode}")
        PMT.dfToTable(df, out_table)

    # Rename output
    # arcpy.Rename_management(out_gdb, )
