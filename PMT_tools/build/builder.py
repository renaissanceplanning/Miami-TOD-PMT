# -*- coding: utf-8 -*-
"""
The 'builder' module serves as the final processing tool in developing the TOC toolkit databases.

This module performs the heavy lifting to build out the PMT geodatabases by timeframe for ingestion
by AGOL for mapping and reporting in the Experience Builder website.

For each PMT_YYYY geodatabase, a Snapshot summary is generated, creating wide tables and calculating new attributes by
aggregating data up from lower spatial scales. Tables long on categorical information are created as well. Trend and
NearTerm geodatabases from the yearly geodatabases, utilizing the similar procedures and creating tables long on year
and calculating difference values for start and end time frames (Trend: start = 2014, end=current year; NearTerm:
start = current year, end = forecasted near term from permits) These procedures generate all metrics reported in the
TOC tool website as a prior calculations and transformations, supporting more performant mapping and reporting within
the site.

This module depends on the `build_helper` and `build_config` modules primarily.

Functions:
    year_to_snapshot:
        - process/task to take year gdb, making the geometries wide where needed and metrics long on categorical data

    snapshot_to_trend: (used to process Trend and NearTerm geodatabases with alterations to inputs)
        - process/task to stack snapshot metrics, making them long on year, ref_YEAR_Snapshot
        - data also generate difference values by metric within the summary geometries, blocks, MAZ, TAZ
"""
import os
import sys

from six import string_types

sys.path.insert(0, os.getcwd())
# build helper functions
from PMT_tools.build import build_helper as b_help

# configuration variables
from PMT_tools.config import build_config as b_conf
from PMT_tools.config import prepare_config as p_conf

# global project functions/variables
from PMT_tools import PMT
from PMT_tools.PMT import CLEANED, BUILD, BASIC_FEATURES

import arcpy


def process_year_to_snapshot(year):
    """
    Process cleaned yearly data to a Snapshot database

    Procedure:
        1) copies feature datasets into a temporary geodatabase
        2) performs a series of permenant joins of tabular data onto feature classes making wide tables
        3) Calculates a series of new fields in the existing feature classes
        4) calculated a dataframe of region wide parcel level statistics
        5) Intersects a series of geometries together, allowing us to aggregate and summarize data from higher to lower
            spatial scales
        6) Enrichment of existing feature class tables with the information from higher spatial resolution, in effect
            widening the tables (ex: roll parcel level data up to blocks, or parcel level data up to Station Areas)
        7) Generate new tables that are long on categorical information derived from the intersections
            (ex: pivot TOT_LVG_AREA on Land Use, taking the sum of living area by land use)
        8) Create separate access by mode tables (bike, walk, transit, auto)
        9) Calculate new attributes based on region wide summaries
        10) Calculate additional attributes for dashboards that require all previous steps to be run
        11) If successful, replace existing copy of Snapshot with newly processed version.

    Returns:
        None
    """
    # define numeric for year in the case of NearTerm
    calc_year = year
    if year == "NearTerm":
        calc_year = 9998

    b_conf.YEAR_COL.default = year
    # Make output gdb and copy features
    print("Validating all data have a year attribute...")
    out_path = PMT.validate_directory(directory=BUILD)
    in_gdb = PMT.validate_geodatabase(
        gdb_path=PMT.make_path(CLEANED, f"PMT_{year}.gdb"), overwrite=False
    )
    b_help.add_year_columns(in_gdb=in_gdb, year=calc_year)
    print("Making Snapshot Template...")
    out_gdb = b_help.make_snapshot_template(
        in_gdb=in_gdb, out_path=out_path, out_gdb_name=None, overwrite=False
    )

    # Join tables to the features
    print("Joining tables to feature classes...")
    b_help.process_joins(
        in_gdb=in_gdb,
        out_gdb=out_gdb,
        fc_specs=b_conf.FC_SPECS,
        table_specs=b_conf.TABLE_SPECS,
    )

    # Calculate values as need prior to intersections   # TODO: make this smarter, skip if already performed
    print("Adding and calculating new fields for dashboards...")
    b_help.apply_field_calcs(gdb=out_gdb, new_field_specs=b_conf.PRECALCS)

    # Summarize reference values
    print("Calculating parcels sums to generate regional statistics...")
    par_sums = b_help.sum_parcel_cols(
        gdb=out_gdb, par_spec=b_conf.PAR_FC_SPECS, columns=b_conf.PAR_SUM_FIELDS
    )

    # Intersect tables for enrichment
    print("Intersecting feature classes to generate summaries...")
    int_fcs = b_help.build_intersections(gdb=out_gdb, enrich_specs=b_conf.ENRICH_INTS)

    # # Store / load intersection fcs (temp locations) in debugging mode
    # if DEBUG:
    #     with open(PMT.makePath(ROOT, "int_fcs.pkl"), "wb") as __f__:
    #         pickle.dump(int_fcs, __f__)
    #     # with open(PMT.makePath(ROOT, "PROCESSING_TEST", "int_fcs.pkl"), "rb") as __f__:
    #     #     int_fcs = pickle.load(__f__)
    # enrich tables
    print("Enriching feature classes with tabular data...")
    b_help.build_enriched_tables(gdb=out_gdb, fc_dict=int_fcs, specs=b_conf.ENRICH_INTS)

    # elongate tables
    print("Elongating tabular data...")
    b_help.build_enriched_tables(gdb=out_gdb, fc_dict=int_fcs, specs=b_conf.ELONGATE_SPECS)

    # build access by mode tables
    print("Access scores by activity and time bin")
    sa_fc, sa_id, sa_fds = b_conf.SUM_AREA_FC_SPECS
    sum_areas_fc = PMT.make_path(out_gdb, sa_fds, sa_fc)
    id_fields = [
        p_conf.SUMMARY_AREAS_COMMON_KEY,
        p_conf.STN_NAME_FIELD,
        p_conf.CORRIDOR_NAME_FIELD,
    ]
    b_help.build_access_by_mode(sum_area_fc=sum_areas_fc,
                                modes=b_conf.MODES,
                                id_field=id_fields,
                                out_gdb=out_gdb,
                                year_val=calc_year)

    # Prepare regional reference columns
    reg_ref_calcs = []
    for new_field in b_conf.REG_REF_CALCS:
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
    b_help.apply_field_calcs(gdb=out_gdb, new_field_specs=b_conf.CALCS + reg_ref_calcs)

    # Delete tempfiles
    print("--- --- Removing temp files")
    for summ_key, summ_val in int_fcs.items():
        for disag_key, disag_val in summ_val.items():
            arcpy.Delete_management(in_data=disag_val)

    # Rename this output
    print("--- --- Finalizing the snapshot")
    if year == PMT.SNAPSHOT_YEAR:
        year = "Current"
    year_out_gdb = PMT.make_path(BUILD, f"Snapshot_{year}.gdb")
    b_help.finalize_output(intermediate_gdb=out_gdb, final_gdb=year_out_gdb)


def process_years_to_trend(years, tables, long_features, diff_features,
                           base_year=None, snapshot_year=None, out_gdb_name=None, ):
    """
    Utilizing a base and snapshot year, trend data are generated for the associated time period.

    Procedure:
        1) creates a a blank output workspace with necessary feature dataset categories uniquely named
        2) generates tables long on year for all tabular data and summary areas
        3) generated difference tables for all tabular data summary features
            (Summary Areas, Census Blocks, MAZ, and TAZ)
        4) upon completion, replace existing copy of Trend/NearTerm gdb with newly processed version.
    """
    # TODO: add a try/except to delete any intermediate data created
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
    out_gdb = b_help.make_trend_template(out_path)

    # Get snapshot data
    for yi, year in enumerate(years):
        process_year = year
        if year == snapshot_year:
            if year == "NearTerm":
                process_year = snapshot_year = "NearTerm"
            else:
                process_year = snapshot_year = "Current"
        in_gdb = PMT.validate_geodatabase(
            gdb_path=PMT.make_path(BUILD, f"Snapshot_{process_year}.gdb"),
            overwrite=False,
        )
        # Make every table extra long on year
        year_tables = PMT._list_table_paths(gdb=in_gdb, criteria=table_criteria)
        year_fcs = PMT._list_fc_paths(
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
                out_table = PMT.make_path(out_gdb, elong_out_name)
                arcpy.Append_management(
                    inputs=elong_table, target=out_table, schema_type="NO_TEST"
                )
        # Get snapshot and base year params
        if process_year == base_year:
            base_tables = year_tables[:]
            base_fcs = PMT._list_fc_paths(
                gdb=in_gdb, fds_criteria="*", fc_criteria=diff_criteria
            )
        elif process_year == snapshot_year:
            snap_tables = year_tables[:]
            snap_fcs = PMT._list_fc_paths(
                gdb=in_gdb, fds_criteria="*", fc_criteria=diff_criteria
            )

    # Make difference tables (snapshot - base)
    for base_table, snap_table, specs in zip(base_tables, snap_tables, tables):
        out_name = os.path.split(base_table)[1] + "_diff"
        out_table = PMT.make_path(out_gdb, out_name)
        idx_cols = specs["index_cols"]
        diff_df = PMT.table_difference(
            this_table=snap_table, base_table=base_table, idx_cols=idx_cols
        )
        print(f"Creating table {out_name}")
        PMT.df_to_table(df=diff_df, out_table=out_table, overwrite=True)

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
        out_fds = PMT.make_path(out_gdb, fc_fds)
        out_name = fc_name + "_diff"
        out_table = PMT.make_path(out_fds, out_name)
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
        diff_df = PMT.table_difference(
            this_table=snap_fc, base_table=base_fc, idx_cols=idx_cols
        )
        # Extend attribute table
        drop_cols = [c for c in diff_df.columns if c in idx_cols and c != fc_id]
        diff_df.drop(columns=drop_cols, inplace=True)
        print("... adding difference columns")
        PMT.extend_table_df(
            in_table=out_table,
            table_match_field=fc_id,
            df=diff_df,
            df_match_field=fc_id,
        )

    # TODO: calculate percent change in value over base for summary areas

    print("Finalizing the trend")
    final_gdb = PMT.make_path(BUILD, f"{out_gdb_name}.gdb")
    b_help.finalize_output(intermediate_gdb=out_gdb, final_gdb=final_gdb)


def process_all_snapshots(years):
    """
    Helper function to iterate all years and generate snapshot databases for list of
        years provided
    """
    # Snapshot data
    print("Building snapshot databases...")
    for year in years:
        print(f"- Snapshot for {year}")
        process_year_to_snapshot(year)


# MAIN
if __name__ == "__main__":
    DEBUG = False
    if DEBUG:
        """
        if DEBUG is True, you can change the path of the root directory and test any
        changes to the code you might need to handle without munging the existing data
        """
        ROOT = (
            r"C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT_link\Data"
        )
        CLEANED = PMT.validate_directory(directory=PMT.make_path(ROOT, "CLEANED"))
        # BUILD = PMT.validate_directory(directory=PMT.makePath(r"C:\PMT_TEST_FOLDER", "BUILD"))
        BUILD = PMT.validate_directory(directory=PMT.make_path(ROOT, "BUILD"))
        DATA = ROOT
        BASIC_FEATURES = PMT.make_path(CLEANED, "PMT_BasicFeatures.gdb")
        REF = PMT.make_path(ROOT, "Reference")
        RIF_CAT_CODE_TBL = PMT.make_path(REF, "road_impact_fee_cat_codes.csv")
        DOR_LU_CODE_TBL = PMT.make_path(REF, "Land_Use_Recode.csv")
        YEAR_GDB_FORMAT = PMT.YEAR_GDB_FORMAT
        YEARS = ["NearTerm"]

    # Snapshot data
    print("Building snapshot databases...")
    process_all_snapshots(years=YEARS)

    # Generate Trend Database
    print("Building Trend database...")
    process_years_to_trend(
        years=PMT.YEARS,
        tables=b_conf.DIFF_TABLES,
        long_features=b_conf.LONG_FEATURES,
        diff_features=b_conf.DIFF_FEATURES,
        out_gdb_name="Trend",
    )

    # Generate near term "trend" database
    print("Building Near term 'Trend' database...")
    process_years_to_trend(
        years=["Current", "NearTerm"],
        tables=b_conf.DIFF_TABLES,
        long_features=b_conf.LONG_FEATURES,
        diff_features=b_conf.DIFF_FEATURES,
        base_year="Current",
        snapshot_year="NearTerm",
        out_gdb_name="NearTerm",
    )

    # tidy up BUILD folder
    b_help.post_process_databases(
        basic_features_gdb=BASIC_FEATURES, build_dir=PMT.make_path(BUILD, "TEMP")
    )