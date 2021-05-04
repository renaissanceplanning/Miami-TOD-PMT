# -*- coding: utf-8 -*-
""" builder.py will serve as the final processing tool

This module will do the heavy lifting to build out the PMT time step geodatabases
for ingestion to AGOL for the tool

Functions:
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
import os
import sys

from six import string_types

sys.path.insert(0, os.getcwd())
# build helper functions
from ..build import build_helper as B_HELP

# configuration variables
from ..config import build_config as B_CONF

# global project functions/variables
from .. import PMT
from ..PMT import CLEANED, BUILD, BASIC_FEATURES

import arcpy


# SNAPSHOT Functions
def process_joins(in_gdb, out_gdb, fc_specs, table_specs):
    """Joins feature classes to associated tabular data from year set and appends to FC in output gdb
        in_gdb: String; path to g
    Returns:
        [String,...]; list of paths to joined feature classes ordered as
            Blocks, Parcels, MAZ, TAZ, SummaryAreas, NetworkNodes
    """

    #   tables need to be ordered the same as FCs
    _table_specs_ = []
    for fc in fc_specs:
        t_specs = [spec for spec in table_specs if fc[0].lower() in spec[0].lower()]
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
            print(f"--- Joining fields from {tbl_name} to {fc_name}")
            B_HELP.joinAttributes(
                to_table=fc,
                to_id_field=fc_id,
                from_table=tbl,
                from_id_field=tbl_id,
                join_fields=tbl_fields,
                renames=tbl_renames,
                drop_dup_cols=True,
            )
            joined_fcs.append(fc)
    return joined_fcs


def process_year_to_snapshot(year):
    """process cleaned yearly data to a Snapshot database
    Returns:

    """
    # define numeric for year in the case of NearTerm
    calc_year = year
    if year == "NearTerm":
        calc_year = 9998

    B_CONF.YEAR_COL.default = year
    # Make output gdb and copy features
    print("Validating all data have a year attribute...")
    out_path = PMT.validate_directory(directory=BUILD)
    in_gdb = PMT.validate_geodatabase(
        gdb_path=PMT.makePath(CLEANED, f"PMT_{year}.gdb"), overwrite=False
    )
    B_HELP.add_year_columns(in_gdb=in_gdb, year=calc_year)
    print("Making Snapshot Template...")
    out_gdb = B_HELP.make_snapshot_template(
        in_gdb=in_gdb, out_path=out_path, out_gdb_name=None, overwrite=False
    )

    # Join tables to the features
    print("Joining tables to feature classes...")
    process_joins(
        in_gdb=in_gdb,
        out_gdb=out_gdb,
        fc_specs=B_CONF.FC_SPECS,
        table_specs=B_CONF.TABLE_SPECS,
    )

    # Calculate values as need prior to intersections   # TODO: make this smarter, skip if already performed
    print("Adding and calculating new fields for dashboards...")
    B_HELP.apply_field_calcs(gdb=out_gdb, new_field_specs=B_CONF.PRECALCS)

    # Summarize reference values
    print("Calculating parcels sums to generate regional statistics...")
    par_sums = B_HELP.sum_parcel_cols(
        gdb=out_gdb, par_spec=B_CONF.PAR_FC_SPECS, columns=B_CONF.PAR_SUM_FIELDS
    )

    # Intersect tables for enrichment
    print("Intersecting feature classes to generate summaries...")
    int_fcs = B_HELP.build_intersections(gdb=out_gdb, enrich_specs=B_CONF.ENRICH_INTS)

    # # Store / load intersection fcs (temp locations) in debugging mode
    # if DEBUG:
    #     with open(PMT.makePath(ROOT, "int_fcs.pkl"), "wb") as __f__:
    #         pickle.dump(int_fcs, __f__)
    #     # with open(PMT.makePath(ROOT, "PROCESSING_TEST", "int_fcs.pkl"), "rb") as __f__:
    #     #     int_fcs = pickle.load(__f__)
    # enrich tables
    print("Enriching feature classes with tabular data...")
    B_HELP.build_enriched_tables(gdb=out_gdb, fc_dict=int_fcs, specs=B_CONF.ENRICH_INTS)

    # elongate tables
    print("Elongating tabular data...")
    B_HELP.build_enriched_tables(gdb=out_gdb, fc_dict=int_fcs, specs=B_CONF.ELONGATE_SPECS)

    # build access by mode tables
    print("Access scores by activity and time bin")
    sa_fc, sa_id, sa_fds = B_CONF.SUM_AREA_FC_SPECS
    sum_areas_fc = PMT.makePath(out_gdb, sa_fds, sa_fc)
    id_fields = [sa_id, "Name", "Corridor"]  # , build_conf.YEAR_COL.name]

    for mode in B_CONF.MODES:
        print(f"--- --- {mode}")
        df = B_HELP._createLongAccess(
            int_fc=sum_areas_fc,
            id_field=id_fields,
            activities=B_CONF.ACTIVITIES,
            time_breaks=B_CONF.TIME_BREAKS,
            mode=mode,
        )
        df["Year"] = calc_year
        out_table = PMT.makePath(out_gdb, f"ActivityByTime_{mode}")
        PMT.dfToTable(df=df, out_table=out_table, overwrite=True)

    # Prepare regional reference columns
    reg_ref_calcs = []
    for new_field in B_CONF.REG_REF_CALCS:
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
    print("Calculating remaining fields for dashboards...")
    B_HELP.apply_field_calcs(gdb=out_gdb, new_field_specs=B_CONF.CALCS + reg_ref_calcs)

    # Delete tempfiles
    print("--- --- Removing temp files")
    for summ_key, summ_val in int_fcs.items():
        for disag_key, disag_val in summ_val.items():
            arcpy.Delete_management(in_data=disag_val)

    # Rename this output
    print("--- --- Finalizing the snapshot")
    if year == PMT.SNAPSHOT_YEAR:
        year = "Current"
    year_out_gdb = PMT.makePath(BUILD, f"Snapshot_{year}.gdb")
    B_HELP.finalize_output(intermediate_gdb=out_gdb, final_gdb=year_out_gdb)


def process_years_to_trend(
    years,
    tables,
    long_features,
    diff_features,
    base_year=None,
    snapshot_year=None,
    out_gdb_name=None,
):
    """
    TODO: add a try/except to delete any intermediate data created
    """
    # Validation
    if base_year is None:
        base_year = years[0]
    if snapshot_year is None:
        snapshot_year = years[-1]
    if base_year not in years or snapshot_year not in years:
        raise ValueError("Base year and snapshot year must be in years list")
    if out_gdb_name is None:
        out_gdb_name = "Trend"

    # Set criteria
    table_criteria = [spec["table"] for spec in tables]
    diff_criteria = [spec["table"][0] for spec in diff_features]
    long_criteria = [spec["table"][0] for spec in long_features]

    # make a blank geodatabase
    out_path = PMT.validate_directory(BUILD)
    out_gdb = B_HELP.make_trend_template(out_path)

    # Get snapshot data
    for yi, year in enumerate(years):
        process_year = year
        if year == snapshot_year:
            if year == "NearTerm":
                process_year = snapshot_year = "NearTerm"
            else:
                process_year = snapshot_year = "Current"
        in_gdb = PMT.validate_geodatabase(
            gdb_path=PMT.makePath(BUILD, f"Snapshot_{process_year}.gdb"),
            overwrite=False,
        )
        # bh.add_year_columns(in_gdb, year) **************************************************************************
        # Make every table extra long on year
        year_tables = B_HELP._list_table_paths(gdb=in_gdb, criteria=table_criteria)
        year_fcs = B_HELP._list_fc_paths(
            gdb=in_gdb, fds_criteria="*", fc_criteria=long_criteria
        )
        elongate = year_tables + year_fcs
        for elong_table in elongate:
            elong_out_name = os.path.split(elong_table)[1] + "_byYear"
            if yi == 0:
                # Initialize the output table
                print(f"Creating long table {elong_out_name}")
                arcpy.TableToTable_conversion(
                    in_rows=elong_table, out_path=out_gdb, out_name=elong_out_name
                )
            else:
                # Append to the output table
                print(f"Appending to long table {elong_out_name} ({process_year})")
                out_table = PMT.makePath(out_gdb, elong_out_name)
                arcpy.Append_management(
                    inputs=elong_table, target=out_table, schema_type="NO_TEST"
                )
        # Get snapshot and base year params
        if process_year == base_year:
            base_tables = year_tables[:]
            base_fcs = B_HELP._list_fc_paths(
                gdb=in_gdb, fds_criteria="*", fc_criteria=diff_criteria
            )
        elif process_year == snapshot_year:
            snap_tables = year_tables[:]
            snap_fcs = B_HELP._list_fc_paths(
                gdb=in_gdb, fds_criteria="*", fc_criteria=diff_criteria
            )

    # Make difference tables (snapshot - base)
    for base_table, snap_table, specs in zip(base_tables, snap_tables, tables):
        out_name = os.path.split(base_table)[1] + "_diff"
        out_table = PMT.makePath(out_gdb, out_name)
        idx_cols = specs["index_cols"]
        diff_df = B_HELP.table_difference(
            this_table=snap_table, base_table=base_table, idx_cols=idx_cols
        )
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
            in_features=base_fc,
            out_path=out_fds,
            out_name=out_name,
            field_mapping=field_mappings,
        )
        # Get table difference
        diff_df = B_HELP.table_difference(
            this_table=snap_fc, base_table=base_fc, idx_cols=idx_cols
        )
        # Extend attribute table
        drop_cols = [c for c in diff_df.columns if c in idx_cols and c != fc_id]
        diff_df.drop(columns=drop_cols, inplace=True)
        print("... adding difference columns")
        PMT.extendTableDf(
            in_table=out_table,
            table_match_field=fc_id,
            df=diff_df,
            df_match_field=fc_id,
        )

    # TODO: calculate percent change in value over base for summary areas

    print("Finalizing the trend")
    final_gdb = PMT.makePath(BUILD, f"{out_gdb_name}.gdb")
    B_HELP.finalize_output(intermediate_gdb=out_gdb, final_gdb=final_gdb)


# MAIN
if __name__ == "__main__":
    DEBUG = True
    if DEBUG:
        """
        if DEBUG is True, you can change the path of the root directory and test any
        changes to the code you might need to handle without munging the existing data
        """
        ROOT = (
            r"C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT_link\Data"
        )
        CLEANED = PMT.validate_directory(directory=PMT.makePath(ROOT, "CLEANED"))
        # BUILD = PMT.validate_directory(directory=PMT.makePath(r"C:\PMT_TEST_FOLDER", "BUILD"))
        BUILD = PMT.validate_directory(directory=PMT.makePath(ROOT, "BUILD"))
        DATA = ROOT
        BASIC_FEATURES = PMT.makePath(CLEANED, "PMT_BasicFeatures.gdb")
        REF = PMT.makePath(ROOT, "Reference")
        RIF_CAT_CODE_TBL = PMT.makePath(REF, "road_impact_fee_cat_codes.csv")
        DOR_LU_CODE_TBL = PMT.makePath(REF, "Land_Use_Recode.csv")
        YEAR_GDB_FORMAT = PMT.makePath(CLEANED, "PMT_YEAR.gdb")
        YEARS = ["NearTerm"]

    # Snapshot data
    print("Building snapshot databases...")
    # for year in YEARS:
    #     print(f"- Snapshot for {year}")
    #     process_year_to_snapshot(year)
    print("Building Trend database...")
    process_years_to_trend(
        years=PMT.YEARS,
        tables=B_CONF.DIFF_TABLES,
        long_features=B_CONF.LONG_FEATURES,
        diff_features=B_CONF.DIFF_FEATURES,
        out_gdb_name="Trend",
    )
    # Process near term "trend"
    print("Building Near term 'Trend' database...")
    process_years_to_trend(
        years=["Current", "NearTerm"],
        tables=B_CONF.DIFF_TABLES,
        long_features=B_CONF.LONG_FEATURES,
        diff_features=B_CONF.DIFF_FEATURES,
        base_year="Current",
        snapshot_year="NearTerm",
        out_gdb_name="NearTerm",
    )
    B_HELP.post_process_databases(
        basic_features_gdb=BASIC_FEATURES, build_dir=PMT.makePath(BUILD, "TEMP")
    )
