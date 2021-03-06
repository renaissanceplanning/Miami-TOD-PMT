# -*- coding: utf-8 -*-
""" builder.py will serve as the final processing tool

This module will do the heavy lifting to build out the PMT time step geodatabases for ingestion to the tool

Functions:
    _validate_snapshots:
        validation function to locate any existing Snapshot.gdb or ref_YEAR_Snapshot.gdb
    year_to_snapshot:
        process/task to take year gdb, making the geometries wide where needed and
        metrics long on categorical data
    _validateAggSpecs:
        validates fields provides match the supplied Column Class
    _makeAccessColSpecs:
        creates columns specifications for access variables
    _createLongAccess:
        creates a pandas dataframe for access metrics, long on activity and time bin
    joinAttributes:
        joins tabular data to an existing FC or table
    summarizeAttributes:
        consolidates and melts data as requested, returns a dataframe of the summarized metrics

    snapshot_to_trend:
        process/task to stack snapshot metrics, making them long on year, ref_YEAR_Snapshot data
        will be used to generate difference values by metric within the summary geometries
    _build_change_table:
        reads in Snap and Base year tables, compares fields in both, calculates diff

    build_near_term_parcels:
        function to replace existing parcel data with updates from building permit data to create
        a projected parcel layer  for the near term
TODO: Review function list above and move to build helper where appropriate, populate with below functions

"""
import os, sys

sys.path.insert(0, os.getcwd())
from PMT_tools.build import build_helper as bh
from PMT_tools.config import build_config as bconfig
import PMT_tools.PMT as PMT
from PMT_tools.PMT import CLEANED, BUILD
from PMT_tools.download import download_helper as dh
from six import string_types
from collections.abc import Iterable
import itertools
import pickle
import arcpy

DEBUG = True
if DEBUG:
    '''
    if DEBUG is True, you can change the path of the root directory and test any
    changes to the code you might need to handle without munging the existing data
    '''
    from PMT_tools.download.download_helper import validate_directory

    ROOT = r'D:\Users\AK7\Documents\PMT'
    RAW = validate_directory(directory=PMT.makePath(ROOT, 'PROCESSING_TEST', "RAW"))
    CLEANED = validate_directory(directory=PMT.makePath(ROOT, 'PROCESSING_TEST', "CLEANED"))
    # BUILD = validate_directory(directory=PMT.makePath(ROOT, "PROCESSING_TEST", "BUILD"))
    BUILD = r"K:\Projects\MiamiDade\PMT\Data\PROCESSING_TEST\BUILD"
    DATA = ROOT
    BASIC_FEATURES = PMT.makePath(CLEANED, "PMT_BasicFeatures.gdb")
    REF = PMT.makePath(ROOT, "Reference")
    RIF_CAT_CODE_TBL = PMT.makePath(REF, "road_impact_fee_cat_codes.csv")
    DOR_LU_CODE_TBL = PMT.makePath(REF, "Land_Use_Recode.csv")
    YEAR_GDB_FORMAT = PMT.makePath(CLEANED, "PMT_YEAR.gdb")


# SNAPSHOT Functions
def build_access_by_mode(sum_area_fc, modes, out_gdb):
    id_fields = ["RowID", "Name", "Corridor", bconfig.YEAR_COL.name]
    for mode in modes:
        print(f"... ... {mode}")
        df = bh._createLongAccess(int_fc=sum_area_fc, id_field=id_fields,
                                  activities=bconfig.ACTIVITIES, time_breaks=bconfig.TIME_BREAKS, mode=mode)
        out_table = PMT.makePath(out_gdb, f"ActivityByTime_{mode}")
        PMT.dfToTable(df, out_table)


def process_joins(in_gdb, out_gdb, fc_specs, table_specs):
    """
    Joins feature classes to associated tabular data from year set and appends to FC in output gdb
    in_gdb: String; path to g
    Returns:
    [String,...]; list of paths to joined feature classes ordered as
        Blocks, Parcels, MAZ, TAZ, SummaryAreas, NetworkNodes
    """
    
    #   tables need to be ordered the same as FCs
    _table_specs_ = []
    for fc in fc_specs:
        t_specs = [
            spec for spec in table_specs if fc[0].lower() in spec[0].lower()]
        _table_specs_.append(t_specs)
    table_specs = _table_specs_

    # join tables to feature classes, making them WIDE
    joined_fcs = []  # --> blocks, parcels, maz, taz, sa, net_nodes
    for fc_spec, table_spec in zip(fc_specs, table_specs):
        fc_name, fc_id, fds = fc_spec
        fc = PMT.makePath(out_gdb, fds, fc_name)

        for spec in table_spec:
            tbl_name, tbl_id, tbl_fields, tbl_renames = spec
            tbl = PMT.makePath(in_gdb, tbl_name)
            print(f"Joining fields from {tbl_name} to {fc_name}")
            bh.joinAttributes(to_table=fc, to_id_field=fc_id,
                              from_table=tbl, from_id_field=tbl_id,
                              join_fields=tbl_fields, renames=tbl_renames,
                              drop_dup_cols=True)
            joined_fcs.append(fc)
    return joined_fcs


def build_intersections(gdb, enrich_specs):
    """
    performs a batch intersection of polygon feature classes
    Args:
        enrich_specs:
        gdb:
    Returns:
    """
    # Intersect features for long tables
    int_out = {}
    for intersect in enrich_specs:
        # Parse specs
        summ, disag = intersect["sources"]
        summ_name, summ_id, summ_fds = summ
        disag_name, disag_id, disag_fds = disag
        summ_in = PMT.makePath(gdb, summ_fds, summ_name)
        disag_in = PMT.makePath(gdb, disag_fds, disag_name)
        full_geometries = intersect["disag_full_geometries"]
        # Run intersect
        print(f"Intersecting {summ_name} with {disag_name}")
        int_fc = PMT.intersectFeatures(
            summary_fc=summ_in, disag_fc=disag_in, in_temp_dir=True, full_geometries=full_geometries)
        # Record with specs
        sum_dict = int_out.get(summ, {})
        sum_dict[disag] = int_fc
        int_out[summ] = sum_dict

    return int_out


def build_enriched_tables(gdb, fc_dict, specs):
    # Enrich features through summarization
    for spec in specs:
        summ, disag = spec["sources"]
        fc_name, fc_id, fc_fds = summ
        d_name, d_id, d_fds = disag
        if summ == disag:
            # Simple pivot wide to long
            fc = PMT.makePath(gdb, fc_fds, fc_name)
        else:
            # Pivot from intersection
            fc = fc_dict[summ][disag]
        
        print(f"Summarizing data from {d_name} to {fc_name}")
        # summary vars
        group = spec["grouping"]
        agg = spec["agg_cols"]
        consolidate = spec["consolidate"]
        melts = spec["melt_cols"]
        summary_df = bh.summarizeAttributes(in_fc=fc, group_fields=group,
                                            agg_cols=agg, consolidations=consolidate, melt_col=melts)
        try:
            out_name = spec["out_table"]
            print(f"... to long table {out_name}")
            out_table = PMT.makePath(gdb, out_name)
            PMT.dfToTable(df=summary_df, out_table=out_table, overwrite=True)
        except KeyError:
            # extend input table
            feature_class = PMT.makePath(gdb, fc_fds, fc_name)
            PMT.extendTableDf(in_table=feature_class, table_match_field=fc_id,
                              df=summary_df, df_match_field=fc_id, append_only=False) #TODO: handle append/overwrite more explicitly


def apply_field_calcs(gdb, new_field_specs):
    # Iterate over new fields
    for nf_spec in new_field_specs:
        # Get params
        tables = nf_spec["tables"]
        new_field = nf_spec["new_field"]
        field_type = nf_spec["field_type"]
        expr = nf_spec["expr"]
        code_block = nf_spec["code_block"]
        try:
            # Get params
            params = nf_spec["params"]
            # TODO: validate? params must be iterables
            all_combos = list(itertools.product(*params))
            for combo in all_combos:
                combo_spec = nf_spec.copy()
                del combo_spec["params"]
                combo_spec["new_field"] = combo_spec["new_field"].format(*combo)
                combo_spec["expr"] = combo_spec["expr"].format(*combo)
                combo_spec["code_block"] = combo_spec["code_block"].format(*combo)
                apply_field_calcs(gdb, [combo_spec])
        except KeyError:
            add_args = {
                "field_name": new_field,
                "field_type": field_type
            }
            calc_args = {
                "field": new_field,
                "expression": expr,
                "expression_type": "PYTHON3",
                "code_block": code_block
            }
            # iterate over tables
            if isinstance(tables, string_types):
                tables = [tables]
            print(f"Adding field {new_field} to {len(tables)} tables")
            for table in tables:
                t_name, t_id, t_fds = table
                in_table = PMT.makePath(gdb, t_fds, t_name)
                # update params
                add_args["in_table"] = in_table
                calc_args["in_table"] = in_table
                if field_type == "TEXT":
                    length = nf_spec["length"]
                    add_args["field_length"] = length
                # add and calc field
                arcpy.AddField_management(**add_args)
                arcpy.CalculateField_management(**calc_args)


def sum_parcel_cols(gdb, par_spec, columns):
    par_name, par_id, par_fds = par_spec
    par_fc = PMT.makePath(gdb, par_fds, par_name)
    df = PMT.featureclass_to_df(
        in_fc=par_fc, keep_fields=columns, skip_nulls=False, null_val=0)
    return df.sum()


# TODO: complete process_year_to_snapshot
# TODO: define process_years_to_trend
# TODO: define process_near_term
# TODO: define process_long_term
def process_year_to_snapshot(year):
    """process cleaned yearly data to a Snapshot database
    Returns:

    """
    bconfig.YEAR_COL.default = year
    # Make output gdb and copy features
    out_path = dh.validate_directory(BUILD)
    in_gdb = dh.validate_geodatabase(
        PMT.makePath(CLEANED, f"PMT_{year}.gdb"), overwrite=False)
    bh.add_year_columns(in_gdb, year)
    out_gdb = bh.make_snapshot_template(in_gdb, out_path, out_gdb_name=None, overwrite=False)
    #out_gdb = PMT.makePath(BUILD, '_6365e38ef426450486bf7162e3204dd7.gdb')

    # Join tables to the features
    joined_fcs = process_joins(
       in_gdb=in_gdb, out_gdb=out_gdb, fc_specs=bconfig.FC_SPECS, table_specs=bconfig.TABLE_SPECS)

    # Calculate values as need prior to intersections
    apply_field_calcs(gdb=out_gdb, new_field_specs=bconfig.PRECALCS)

    # Summarize reference values
    par_sums = sum_parcel_cols(out_gdb, bconfig.PAR_FC_SPECS, bconfig.PAR_SUM_FIELDS)

    # Intersect tables for enrichment
    int_fcs = build_intersections(out_gdb, bconfig.ENRICH_INTS)

    # Store / load intersection fcs (temp locations) in debugging mode
    if DEBUG:
        with open(PMT.makePath(ROOT, "PROCESSING_TEST", "int_fcs.pkl"), "wb") as __f__:
            pickle.dump(int_fcs, __f__)
        # with open(PMT.makePath(ROOT, "PROCESSING_TEST", "int_fcs.pkl"), "rb") as __f__:
        #     int_fcs = pickle.load(__f__)

    # enrich tables
    build_enriched_tables(gdb=out_gdb, fc_dict=int_fcs, specs=bconfig.ENRICH_INTS)

    # elongate tables
    build_enriched_tables(gdb=out_gdb, fc_dict=int_fcs, specs=bconfig.ELONGATE_SPECS)

    # build access by mode tables
    print("... Access scores by activity and time bin")
    sa_fc, sa_id, sa_fds = bconfig.SUM_AREA_FC_SPECS
    sum_areas_fc = PMT.makePath(out_gdb, sa_fds, sa_fc)
    id_fields = [sa_id, "Name", "Corridor"] #, bconfig.YEAR_COL.name]

    for mode in bconfig.MODES:
        print(f"... ... {mode}")
        df = bh._createLongAccess(
            sum_areas_fc, id_fields, bconfig.ACTIVITIES, bconfig.TIME_BREAKS, mode)
        out_table = PMT.makePath(out_gdb, f"ActivityByTime_{mode}")
        PMT.dfToTable(df, out_table, overwrite=True)

    # Prepare regional reference columns
    reg_ref_calcs = []
    for new_field in bconfig.REG_REF_CALCS:
        nf_spec, ref_field = new_field
        if isinstance(ref_field, string_types):
            ref_val = [[par_sums[ref_field]]]
        else:
            # assume iterable
            ref_val = [[] for _ in ref_field]
            for ref_i, rf in enumerate(ref_field):
                ref_val[ref_i].append(par_sums[rf])
        nf_spec["params"] = ref_val
        reg_ref_calcs.append(nf_spec)

    # Calculated values - simple
    apply_field_calcs(out_gdb, bconfig.CALCS + reg_ref_calcs)
    
    # Delete tempfiles
    print("Removing temp files")
    for summ_key, summ_val in int_fcs.items():
        for disag_key, disag_val in summ_val.items():
            arcpy.Delete_management(disag_val)

    # Rename this output
    print("Finalizing the snapshot")
    #year_out_gdb = PMT.makePath(BUILD, f"Snapshot_{year}.gdb")
    year_out_gdb = PMT.makePath(r"K:\Projects\MiamiDade\PMT\Data\PROCESSING_TEST\BUILD", f"Snapshot_{year}.gdb") #*************
    bh.finalize_output(out_gdb, year_out_gdb)


def process_years_to_trend(years, tables, long_features, diff_features,
                           base_year=None, snapshot_year=None):
    """

    """
    # Validation
    if base_year is None:
        base_year = years[0]
    if snapshot_year is None:
        snapshot_year = years[-1]
    if base_year not in years or snapshot_year not in years:
        raise ValueError("Base year and snapshot year must be in years list")
    
    # Set criteria
    table_criteria = [spec["table"] for spec in tables]
    diff_criteria = [spec["table"][0] for spec in diff_features]
    long_criteria = [spec["table"][0] for spec in long_features]

    # make a blank geodatabase
    out_path = dh.validate_directory(BUILD)
    out_gdb = bh.make_trend_template(out_path)
    # out_gdb = r"K:\Projects\MiamiDade\PMT\Data\PROCESSING_TEST\BUILD\_b778e67ba92e4497953587e6c94122f9.gdb"

    # Get snapshot data
    for yi, year in enumerate(years):
        in_gdb = dh.validate_geodatabase(
            PMT.makePath(BUILD, f"Snapshot_{year}.gdb"), overwrite=False)
        # bh.add_year_columns(in_gdb, year) *******************************************************
        # Make every table extra long on year
        year_tables = bh._list_table_paths(in_gdb, criteria=table_criteria)
        year_fcs = bh._list_fc_paths(
            in_gdb, fds_criteria="*", fc_criteria=long_criteria)
        elongate = year_tables + year_fcs
        for elong_table in elongate:
            elong_out_name = os.path.split(elong_table)[1] + "_byYear"
            if yi == 0:
                # Initialize the output table
                print(f"Creating long table {elong_out_name}")
                arcpy.TableToTable_conversion(
                    in_rows=elong_table, out_path=out_gdb, out_name=elong_out_name)
            else:
                # Append to the output table
                print(f"Appending to long table {elong_out_name} ({year})")
                out_table = PMT.makePath(out_gdb, elong_out_name)
                arcpy.Append_management(
                    inputs=elong_table, target=out_table, schema_type="NO_TEST")
        # Get snapshot and base year params
        if year == base_year:
            base_tables = year_tables[:]
            base_fcs = bh._list_fc_paths(
                in_gdb, fds_criteria="*", fc_criteria=diff_criteria)
        elif year == snapshot_year:
            snap_tables = year_tables[:]
            snap_fcs = bh._list_fc_paths(
                in_gdb, fds_criteria="*", fc_criteria=diff_criteria)
    # Make difference tables (snapshot - base)
    for base_table, snap_table, specs in zip(base_tables, snap_tables, tables):
        out_name = os.path.split(base_table)[1] + "_diff"
        out_table = PMT.makePath(out_gdb, out_name)
        idx_cols = specs["index_cols"]
        diff_df = bh.table_difference(
            this_table=snap_table, base_table=base_table, idx_cols=idx_cols)
        print(f"Creating table {out_name}")
        PMT.dfToTable(df=diff_df, out_table=out_table, overwrite=True)

    # Make difference fcs (snapshot - base)
    for base_fc, snap_fc, spec in zip(base_fcs, snap_fcs, diff_features):
        # TODO: will raise if not all diff features are found, but maybe that's good?
        # Get specs
        fc_name, fc_id, fc_fds = spec["table"]
        idx_cols = spec["index_cols"]
        if isinstance(idx_cols, string_types):
            idx_cols = [idx_cols]
        if fc_id not in idx_cols:
            idx_cols.append(fc_id)
        out_fds = PMT.makePath(out_gdb, fc_fds)
        out_name = fc_name + "_diff"
        out_table = PMT.makePath(out_fds, out_name)
        # Field mappings
        field_mappings = arcpy.FieldMappings()
        for idx_col in idx_cols:
            fm = arcpy.FieldMap()
            fm.addInputField(base_fc, idx_col)
            field_mappings.addFieldMap(fm)
        # Copy geoms
        print(f"Creating feature class {out_name}")
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=base_fc, out_path=out_fds, out_name=out_name, field_mapping=field_mappings)
        # Get table difference
        diff_df = bh.table_difference(
            this_table=snap_fc, base_table=base_fc, idx_cols=idx_cols)
        # Extend attribute table
        drop_cols = [c for c in diff_df.columns if c in idx_cols and c != fc_id]
        diff_df.drop(columns=drop_cols, inplace=True)
        print("... adding difference columns")
        PMT.extendTableDf(
            in_table=out_table, table_match_field=fc_id, df=diff_df, df_match_field=fc_id)

    print("Finalizing the snapshot")
    final_gdb = PMT.makePath(BUILD, f"Trend.gdb")
    bh.finalize_output(out_gdb, final_gdb)

def process_near_term():
    pass


def process_long_term():
    pass


# MAIN
if __name__ == "__main__":
    # # Snapshot
    # for year in PMT.YEARS:
    #     print(year)
    #     process_year_to_snapshot(year)
    process_years_to_trend(years=PMT.YEARS, tables=bconfig.DIFF_TABLES,
        long_features=bconfig.LONG_FEATURES, diff_features=bconfig.DIFF_FEATURES)