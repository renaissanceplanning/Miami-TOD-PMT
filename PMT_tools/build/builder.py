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
from PMT_tools.config.build_config import *
import PMT_tools.PMT as PMT
from PMT_tools.PMT import CLEANED


# SNAPSHOT Functions
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
    fc_specs = FC_SPECS
    #   tables need to be ordered the same as FCs
    block_tbl_specs = [spec for spec in TABLE_SPECS if "blocks" in spec[0].lower()]
    parcel_tbl_specs = [spec for spec in TABLE_SPECS if "parcels" in spec[0].lower()]
    maz_tbl_specs = [spec for spec in TABLE_SPECS if "maz" in spec[0].lower()]
    taz_tbl_specs = [spec for spec in TABLE_SPECS if "taz" in spec[0].lower()]
    net_tbl_specs = [spec for spec in TABLE_SPECS if "nodes" in spec[0].lower()]
    sa_tbl_specs = [spec for spec in TABLE_SPECS if "summaryareas" in spec[0].lower()]
    table_specs = [block_tbl_specs, parcel_tbl_specs, maz_tbl_specs,
                   taz_tbl_specs, sa_tbl_specs, net_tbl_specs]

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


def build_intersections(blocks_fc, parcels_fc, maz_fc, taz_fc, sum_area_fc, ):
    """
    performs a batch intersection of polygon feature classes
    Args:
        blocks_fc: String; path to Census Blocks feature class
        parcels_fc: String; path to parcels feature class
        maz_fc: String; path to MAZ feature class
        taz_fc: String; path to TAZ feature class
        sum_area_fc: String; path to SummaryArea feature class
        nodes_fc: Strin; path to Nodes feature class

    Returns:
    [String,...]; list of paths to intersected feature classes
    int_block_par, int_sumarea_par, int_sumarea_block, int_sumarea_maz, int_sumarea_taz
    """
    # Intersect features for long tables
    intersections = [(blocks_fc, parcels_fc), (sum_area_fc, parcels_fc),
                     (sum_area_fc, blocks_fc), (sum_area_fc, maz_fc),
                     (sum_area_fc, taz_fc), (sum_area_fc, nodes_fc)]
    int_out = []
    for intersect in intersections:
        summ, disag = intersect
        int_out.append(
            PMT.intersectFeatures(summary_fc=summ, disag_fc=disag)
        )
    return int_out


def build_enriched_tables(intersection_list, fc_spec_list, enrichment_dict_list, out_gdb):  # Enrich tables
    # Enrich features through summarization
    for intersection, fc_specs, var_dict in zip(
            intersection_list, fc_spec_list, enrichment_dict_list):
        # out fc vars
        feature_class = PMT.makePath(out_gdb, fc_specs[2], fc_specs[0])
        feature_class_id = fc_specs[1]
        # summary vars
        group = var_dict["grouping"]
        agg = var_dict["agg_cols"]
        consolidate = var_dict["consolidate"]
        melts = var_dict["melt_cols"]
        summary_df = summarizeAttributes(
            in_fc=intersection,
            group_fields=group,
            agg_cols=agg,
            consolidations=consolidate,
            melt_col=melts)
        PMT.extendTableDf(
            in_table=feature_class,
            table_match_field=feature_class_id,
            df=summary_df,
            df_match_field=feature_class_id)
        # TODO: calculate new summariation information (RES_DENS, FAR, e.g.)

def build_elongated_tables():
    # TODO: confirm sums are correct. Draft dashboard data suspicious.
    pass


def build_access_by_mode(sum_area_fc, modes, out_gdb):
    id_fields = ["RowID", "Name", "Corridor", YEAR_COL.name]
    for mode in modes:
        print(f"... ... {mode}")
        df = _createLongAccess(
            int_fc=sum_area_fc, id_field=id_fields,
            activities=ACTIVITIES, time_breaks=TIME_BREAKS, mode=mode)
        out_table = PMT.makePath(out_gdb, f"ActivityByTime_{mode}")
        PMT.dfToTable(df, out_table)


# TODO: complete process_year_to_snapshot
# TODO: define process_years_to_trend
# TODO: define process_near_term
# TODO: define process_long_term
def process_year_to_snapshot():
    """
    process cleaned yearly data to a Snapshot database
    Returns:

    """
    in_gdb = ''
    out_path = CLEANED
    out_gdb = _make_snapshot_template(in_gdb, out_path, out_gdb_name=None, overwrite=False)
    (blocks,
     parcels,
     maz,
     taz,
     sum_area,
     net_nodes) = process_joins(in_gdb=in_gdb, out_gdb=out_gdb)
    (int_block_par,
     int_sum_area_par,
     int_sum_area_block,
     int_sum_area_maz,
     int_sum_area_taz) = build_intersections(
        blocks_fc=blocks,
        parcels_fc=parcels,
        maz_fc=maz,
        taz_fc=taz,
        sum_area_fc=sum_area
    )
    # enrich tables
    build_enriched_tables(
        intersection_list=[int_block_par, int_sum_area_par,
                           int_sum_area_maz, int_sum_area_taz],
        fc_spec_list=[BLOCK_FC_SPECS, SUM_AREA_FC_SPECS,
                      SUM_AREA_FC_SPECS, SUM_AREA_FC_SPECS],
        enrichment_dict_list=[BLOCK_PAR_ENRICH, SA_PAR_ENRICH,
                              SA_MAZ_ENRICH, SA_TAZ_ENRICH],
        out_gdb=out_gdb
    )
    # elongate tables
    elongate_intersections = [int_sum_area_par, int_sum_area_par, int_sum_area_par,
                           int_sum_area_par, int_sum_area_par, int_sum_area_par, int_sum_area_block]
    build_enriched_tables(
        intersection_list=elongate_intersections,
        fc_spec_list=ELONGATE_SPEC_DICTS,

    )
    # build access by mode tables


def process_years_to_trend():
    pass


def process_near_term():
    pass


def process_long_term():
    pass