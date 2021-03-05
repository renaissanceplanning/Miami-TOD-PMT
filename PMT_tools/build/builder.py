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
from PMT_tools.build.build_helper import (_make_snapshot_template,
                                          joinAttributes,
                                          summarizeAttributes,
                                          _createLongAccess)
from PMT_tools.config import build_config as bconfig
import PMT_tools.PMT as PMT
from PMT_tools.PMT import CLEANED, BUILD
from PMT_tools.download import download_helper as dh
from six import string_types
import itertools
import arcpy


# SNAPSHOT Functions
def build_access_by_mode(sum_area_fc, modes, out_gdb):
    id_fields = ["RowID", "Name", "Corridor", bconfig.YEAR_COL.name]
    for mode in modes:
        print(f"... ... {mode}")
        df = _createLongAccess(int_fc=sum_area_fc, id_field=id_fields,
                               activities=bconfig.ACTIVITIES, time_breaks=bconfig.TIME_BREAKS, mode=mode)
        out_table = PMT.makePath(out_gdb, f"ActivityByTime_{mode}")
        PMT.dfToTable(df, out_table)


def process_joins(in_gdb, out_gdb):
    """
    Joins feature classes to associated tabular data from year set and appends to FC in output gdb
    in_gdb: String; path to g
    Returns:
    [String,...]; list of paths to joined feature classes ordered as
        Blocks, Parcels, MAZ, TAZ, SummaryAreas, NetworkNodes
    """
    # join specs
    #   feature classes
    fc_specs = bconfig.FC_SPECS
    #   tables need to be ordered the same as FCs
    table_specs = []
    for fc in fc_specs:
        t_specs = [
            spec for spec in bconfig.TABLE_SPECS if fc[0].lower() in spec[0].lower()]
        table_specs.append(t_specs)

    # join tables to feature classes, making them WIDE
    joined_fcs = []  # --> blocks, parcels, maz, taz, sa, net_nodes
    for fc_spec, table_spec in zip(fc_specs, table_specs):
        fc_name, fc_id, fds = fc_spec
        fc = PMT.makePath(out_gdb, fc_name, fds)
        for spec in table_spec:
            tbl_name, tbl_id, tbl_fields = spec
            tbl = PMT.makePath(in_gdb, tbl_name)
            joinAttributes(
                to_table=fc,
                to_id_field=fc_id,
                from_table=tbl,
                from_id_field=tbl_id,
                join_fields=tbl_fields,
                renames={})
            joined_fcs.append(fc)
    return joined_fcs


def build_intersections(gdb, enrich_specs):  # blocks_fc, parcels_fc, maz_fc, taz_fc, sum_area_fc, )
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
        # Run intersect
        int_fc = PMT.intersectFeatures(summary_fc=summ_in, disag_fc=disag_in)
        # Record with specs
        sum_dict = int_out.get(summ, {})
        sum_dict[disag] = int_fc
        int_out[summ] = sum_dict

    return int_out


def build_enriched_tables(gdb, fc_dict, specs):
    # Enrich features through summarization
    for spec in specs:
        summ, disag = spec["sources"]
        fc = fc_dict[summ][disag]
        fc_name, fc_id, fc_fds = summ
        # out fc vars
        feature_class = PMT.makePath(gdb, fc_fds, fc_name)
        # summary vars
        group = spec["grouping"]
        agg = spec["agg_cols"]
        consolidate = spec["consolidate"]
        melts = spec["melt_cols"]
        summary_df = summarizeAttributes(in_fc=fc, group_fields=group,
                                         agg_cols=agg, consolidations=consolidate, melt_col=melts)
        try:
            out_name = spec["out_table"]
            out_table = PMT.makePath(gdb, out_name)
            PMT.dfToTable(df=summary_df, out_table=out_table, overwrite=True)
        except KeyError:
            # extend input table
            PMT.extendTableDf(in_table=feature_class, table_match_field=fc_id,
                              df=summary_df, df_match_field=fc_id)


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
            all_combos = list(itertools.product(params))
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
    out_gdb = _make_snapshot_template(in_gdb, out_path, out_gdb_name=None, overwrite=False)

    # Join tables to the features
    joined_fcs = process_joins(in_gdb=in_gdb, out_gdb=out_gdb)

    # Calculate values as need prior to intersections
    apply_field_calcs(out_gdb, bconfig.PRECALCS)

    # Summarize reference values
    par_sums = sum_parcel_cols(out_gdb, bconfig.PARCEL_FC_SPECS, bconfig.PAR_SUM_FIELDS)

    # Intersect tables for enrichment
    int_fcs = build_intersections(out_gdb, bconfig.ENRICH_INTS)

    # enrich tables
    build_enriched_tables(gdb=out_gdb, fc_dict=int_fcs, specs=bconfig.ENRICH_INTS)

    # elongate tables
    build_enriched_tables(gdb=out_gdb, fc_dict=int_fcs, specs=bconfig.ELONGATE_SPECS)

    # build access by mode tables
    print("... Access scores by activity and time bin")
    sa_fc, sa_id, sa_fds = bconfig.SUM_AREA_FC_SPECS
    sum_areas_fc = PMT.makePath(out_gdb, sa_fds, sa_fc)
    id_fields = [sa_id, "Name", "Corridor", bconfig.YEAR_COL.name]

    for mode in bconfig.MODES:
        print(f"... ... {mode}")
        df = _createLongAccess(
            sum_areas_fc, id_fields, bconfig.ACTIVITIES, bconfig.TIME_BREAKS, mode, bconfig.ACC_DOM)
        out_table = PMT.makePath(out_gdb, f"ActivityByTime_{mode}")
        PMT.dfToTable(df, out_table)

    # Prepare regional reference columns
    reg_ref_calcs = []
    for new_field in bconfig.REG_REF_CALCS:
        nf_spec, ref_field = new_field
        if isinstance(ref_field, string_types):
            ref_val = [par_sums[ref_field]]
        else:
            # assume iterable
            ref_val = [[] for _ in ref_field]
            for ref_i, rf in enumerate(ref_field):
                ref_val[ref_i].append(par_sums[rf])
        nf_spec["params"] = ref_val
        reg_ref_calcs.append(nf_spec)

    # Calculated values - simple
    apply_field_calcs(out_gdb, bconfig.CALCS + reg_ref_calcs)


def process_years_to_trend():
    in_gdb = "this is the Trend gdb started "
    pass


def process_near_term():
    pass


def process_long_term():
    pass


# MAIN
if __name__ == "__main__":
    for year in PMT.YEARS:
        process_year_to_snapshot(year)
