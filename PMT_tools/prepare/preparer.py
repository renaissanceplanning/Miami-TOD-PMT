"""
The `preparer` module standardizes and formats all raw datasets into a common storage pattern, normalizing or
separating geospatial data from tabular data where possible to decrease overall file sizes. The standardized
databases include `PMT_BasicFeatures` and `PMT_YYYY` (where `YYYY` = the relevant year of data). Standardization includes
removing unnecessary attributes, renaming attributes for readability, merging data where needed, and placing outputs
in a common geodatabase structure. In addition to standardization, much of the analytical processing is performed via
this module.

Functions defined in this module are purpose-built for TOC analysis in Miami-Dade County's TOC Toolkit. They lean
on more abstract functions and classes defined in `prepare_helpers`, `PMT` and other supporting modules.

Functions:
  - process_normalized_geometries()
  - process_basic_features()
  - process_parks()
  - process_udb()
  - process_transit()
  - process_parcels()
  - process_permits()
  - enrich_block_groups()
  - process_parcel_land_use()
  - process_imperviousness()
  - process_osm_networks()
  - process_bg_apply_activity_models()
  - process_allocate_bg_to_parcels()
  - process_model_se_data()
  - process_osm_skims()
  - process_model_skims()
  - process_osm_service_areas()
  - process_centrality()
  - process_walk_times()
  - process_ideal_walk_times()
  - process_access()
  - process_contiguity()
  - process_bike_miles()
  - process_travel_stats()
  - process_walk_to_transit_skim()
  - process_serpm_transit()

"""
import csv
import datetime
import os
import sys
import warnings
from functools import reduce

import networkx as nx

warnings.filterwarnings("ignore")

sys.path.insert(0, os.getcwd())
# config global variables
from PMT_tools.config import prepare_config as prep_conf

# prep/clean helper functions
from PMT_tools.prepare import prepare_helpers as p_help
from PMT_tools.prepare import prepare_osm_networks as osm_help
from PMT_tools.PMT import table_difference

# PMT functions
from PMT_tools import PMT
from PMT_tools.PMT import (
    make_path,
    make_inmem_path,
    check_overwrite_output,
    df_to_table,
    polygons_to_points,
    extend_table_df,
    table_to_df,
    intersect_features,
    validate_directory,
    validate_geodatabase,
    validate_feature_dataset,
)

# PMT classes
from PMT_tools.PMT import ServiceAreaAnalysis

# PMT globals
from PMT_tools.PMT import (
    RAW,
    CLEANED,
    BASIC_FEATURES,
    REF,
    YEARS,
    SNAPSHOT_YEAR,
    YEAR_GDB_FORMAT,
    SR_FL_SPF,
    EPSG_FLSPF,
)
from PMT_tools.PMT import arcpy, np, pd


arcpy.env.overwriteOutput = True


def process_udb(overwrite=True):
    """Converts Urban Development Boundary line feature class to
    a polygon.

    Inputs:
        - RAW//MD_Urban_Growth_Boundary.geojson
        - RAW//Miami-Dade_County_Boundary.geojson

    Outputs:
        - CLEANED//UrbanDevelopmentBoundary.shp
        - CLEANED//PMT_BasicFeatures.gdb//BasicFeatures//UrbanGrowthBoundary
    """
    udb_fc = make_path(RAW, "MD_Urban_Growth_Boundary.geojson")
    county_fc = make_path(RAW, "Miami-Dade_County_Boundary.geojson")
    out_fc = make_path(CLEANED, "UrbanDevelopmentBoundary.shp")
    check_overwrite_output(output=out_fc, overwrite=overwrite)

    temp_line_fc = p_help.geojson_to_feature_class_arc(
        geojson_path=udb_fc, geom_type="POLYLINE"
    )
    temp_county_fc = p_help.geojson_to_feature_class_arc(
        geojson_path=county_fc, geom_type="POLYGON"
    )
    p_help.udb_line_to_polygon(
        udb_fc=temp_line_fc, county_fc=temp_county_fc, out_fc=out_fc
    )
    # copy newly created features to PMT_BasicFeatures.gdb
    where = (
        f"{arcpy.AddFieldDelimiters(datasource=out_fc, field=prep_conf.UDB_FLAG)} = 1"
    )
    arcpy.FeatureClassToFeatureClass_conversion(
        in_features=out_fc,
        out_path=BASIC_FEATURES,
        out_name=prep_conf.BASIC_UGB,
        where_clause=where,
    )


def process_basic_features(overwrite=True):
    """
    Utilizing the basic features, StationAreas, Corridors are genrated and used to generate
    SummaryAreas for the project

    Inputs:
        - BASIC_FEATURES = makePath(CLEANED, "PMT_BasicFeatures.gdb", "BasicFeatures")
        - CLEANED//BASIC_FEATURES//StationArea_presets
        - CLEANED//BASIC_FEATURES//Corridor_presets
        - CLEANED//BASIC_FEATURES//SMARTplanStations
        - CLEANED//BASIC_FEATURES//SMARTplanAlignments

    Outputs:
        - CLEANED//BASIC_FEATURES//StationAreas
        - CLEANED//BASIC_FEATURES//Corridors
        - CLEANED//BASIC_FEATURES//StationsLong
        - CLEANED//BASIC_FEATURES//SummaryAreas
    """
    # TODO: add check for existing basic features, and compare for changes
    print("MAKING BASIC FEATURES...")
    station_presets = make_path(BASIC_FEATURES, "StationArea_presets")
    p_help.make_basic_features(
        bf_gdb=BASIC_FEATURES,
        stations_fc=prep_conf.BASIC_STATIONS,
        stn_id_field=prep_conf.STN_ID_FIELD,
        stn_diss_fields=prep_conf.STN_DISS_FIELDS,
        stn_corridor_fields=prep_conf.STN_CORRIDOR_FIELDS,
        alignments_fc=prep_conf.BASIC_ALIGNMENTS,
        align_diss_fields=prep_conf.ALIGN_DISS_FIELDS,
        align_corridor_name=prep_conf.CORRIDOR_NAME_FIELD,
        stn_buff_dist=prep_conf.STN_BUFF_DIST,
        align_buff_dist=prep_conf.ALIGN_BUFF_DIST,
        stn_areas_fc=prep_conf.BASIC_STN_AREAS,
        corridors_fc=prep_conf.BASIC_CORRIDORS,
        long_stn_fc=prep_conf.BASIC_LONG_STN,
        preset_station_areas=station_presets,
        preset_station_id="Id",  # todo: spec out maybe
        preset_corridors=None,
        preset_corridor_name="Corridor",  # todo: spec out maybe
        rename_dict=prep_conf.BASIC_RENAME_DICT,
        overwrite=overwrite,
    )
    print("Making summarization features")
    p_help.make_summary_features(
        bf_gdb=BASIC_FEATURES,
        long_stn_fc=prep_conf.BASIC_LONG_STN,
        stn_areas_fc=prep_conf.BASIC_STN_AREAS,
        stn_id_field=prep_conf.STN_ID_FIELD,
        corridors_fc=prep_conf.BASIC_CORRIDORS,
        cor_name_field=prep_conf.CORRIDOR_NAME_FIELD,
        out_fc=prep_conf.BASIC_SUM_AREAS,
        stn_buffer_meters=prep_conf.STN_BUFF_METERS,
        stn_name_field=prep_conf.STN_NAME_FIELD,
        stn_cor_field=prep_conf.STN_LONG_CORRIDOR,
        overwrite=overwrite,
    )


def process_normalized_geometries(overwrite=True):
    """YEAR BY YEAR:
          - Sets up Year GDB, and 'Polygons' feature dataset
          - Adds MAZ, TAZ, Census_Blocks, Census_BlockGroups, SummaryAreas
          - for each geometry type, the year is added as an attribute
          - for NearTerm, year is set to 9998

    Inputs:
        - RAW//CENSUS//..//{census_blocks.shp}
        - RAW//CENSUS//..//{census_blockgroups.shp}
        - RAW//TAZ.shp
        - RAW//MAZ_TAZ.shp
        - CLEANED//BASIC_FEATURES//SummaryAreas

    Outputs:
        - CLEANED//PMT_{year}.gdb//Polygons\Census_Blocks;Census_Blockgroups;TAZ;MAZ;SummaryAreas
    """
    county_bounds = make_path(
        BASIC_FEATURES, "BasicFeatures", "MiamiDadeCountyBoundary"
    )
    raw_block = make_path(
        RAW, "CENSUS", "tl_2019_12_tabblock10", "tl_2019_12_tabblock10.shp"
    )
    raw_block_groups = make_path(RAW, "CENSUS", "tl_2019_12_bg", "tl_2019_12_bg.shp")
    raw_TAZ = make_path(RAW, "TAZ.shp")
    raw_MAZ = make_path(RAW, "MAZ_TAZ.shp")
    raw_sum_areas = make_path(BASIC_FEATURES, "SummaryAreas")

    # CLEANED
    blocks = "Census_Blocks"
    block_groups = "Census_BlockGroups"
    TAZ = "TAZ"
    MAZ = "MAZ"
    sum_areas = "SummaryAreas"
    print("PROCESSING NORMALIZED GEOMETRIES...")
    for year in YEARS:
        print(f"{year}")
        for raw, cleaned, cols in zip(
            [raw_block, raw_block_groups, raw_TAZ, raw_MAZ, raw_sum_areas],
            [blocks, block_groups, TAZ, MAZ, sum_areas],
            [
                prep_conf.BLOCK_COMMON_KEY,
                prep_conf.BG_COMMON_KEY,
                prep_conf.TAZ_COMMON_KEY,
                [prep_conf.MAZ_COMMON_KEY, prep_conf.TAZ_COMMON_KEY],
                prep_conf.SUMMARY_AREAS_BASIC_FIELDS,
            ],
        ):
            temp_file = make_inmem_path()
            out_path = validate_feature_dataset(
                make_path(CLEANED, f"PMT_{year}.gdb", "Polygons"), sr=SR_FL_SPF
            )
            print(f"{cleaned} normalized here: {out_path}")
            out_data = make_path(out_path, cleaned)
            p_help.prep_feature_class(
                in_fc=raw,
                geom="POLYGON",
                out_fc=temp_file,
                use_cols=cols,
                rename_dict=None,
            )
            lyr = arcpy.MakeFeatureLayer_management(
                in_features=temp_file, out_layer="lyr"
            )
            if raw in [raw_block, raw_block_groups]:
                print(f"--- Sub-setting {raw} to project AOI")
                arcpy.SelectLayerByLocation_management(
                    in_layer=lyr,
                    overlap_type="HAVE_THEIR_CENTER_IN",
                    select_features=county_bounds,
                )
            if overwrite:
                check_overwrite_output(output=out_data, overwrite=overwrite)
            print(f"--- writing out geometries and {cols} only")
            arcpy.CopyFeatures_management(in_features=lyr, out_feature_class=out_data)

            calc_year = year
            if year == "NearTerm":
                calc_year = 9998
            arcpy.CalculateField_management(
                in_table=out_data,
                field="Year",
                expression=calc_year,
                expression_type="PYTHON3",
                field_type="LONG",
            )
            arcpy.Delete_management(lyr)
            arcpy.Delete_management(temp_file)
        print("")


def process_parks(overwrite=True):
    """
    Parks - merges park polygons into one and formats both poly and point park data.

    YEAR by YEAR:
        - sets up Points FDS and year GDB (unless they exist already)
        - copies Park_Points in to each year gdb under the Points FDS
        - treat NEAR_TERM like any other year

    Inputs:
        - RAW//Municipal_Parks.geojson (polygons)
        - RAW//Federal_State_Parks.geojson (polygons)
        - RAW//County_Parks.geojson (polygons)
        - RAW//Park_Facilities.geojson (points)

    Outputs:
        - CLEANED//Park_points.shp; Park_Polys.shp
        - CLEANED//PMT_{year}.gdb//Points//Park_points
    """
    print("PROCESSING PARKS... ")
    park_polys = [
        make_path(RAW, "Municipal_Parks.geojson"),
        make_path(RAW, "Federal_State_Parks.geojson"),
        make_path(RAW, "County_Parks.geojson"),
    ]
    park_points = make_path(RAW, "Park_Facilities.geojson")
    poly_use_cols = [["FOLIO", "NAME", "TYPE"], ["NAME"], ["FOLIO", "NAME", "TYPE"]]
    poly_rename_cols = [{}, {}, {}]
    out_park_polys = make_path(CLEANED, "Park_Polys.shp")
    out_park_points = make_path(CLEANED, "Park_Points.shp")

    for output in [out_park_points, out_park_polys]:
        check_overwrite_output(output=output, overwrite=overwrite)
    print("--- cleaning park points and polys")
    p_help.prep_park_polys(
        in_fcs=park_polys,
        geom="POLYGON",
        out_fc=out_park_polys,
        use_cols=poly_use_cols,
        rename_dicts=poly_rename_cols,
        unique_id=prep_conf.PARK_POLY_COMMON_KEY,
    )
    p_help.prep_feature_class(
        in_fc=park_points,
        geom="POINT",
        out_fc=out_park_points,
        use_cols=prep_conf.PARK_POINT_COLS,
        unique_id=prep_conf.PARK_POINTS_COMMON_KEY,
    )
    for year in YEARS:
        print(f"\t--- adding park points to {year} gdb")
        out_path = validate_feature_dataset(
            fds_path=make_path(
                CLEANED, YEAR_GDB_FORMAT.replace("YEAR", str(year)), "Points"
            ),
            sr=SR_FL_SPF,
        )
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=out_park_points, out_path=out_path, out_name="Park_Points"
        )


def process_transit(overwrite=True):
    """Converts a list of transit ridership files to points with attributes cleaned.

    YEAR by YEAR:
        - cleans and consolidates transit data into Year POINTS FDS
        - if YEAR == NearTerm:
            - most recent year is copied over
    NOTE: transit folder reflects current location, needs update to reflect cleaner structure

    Inputs:
        - RAW//TRANSIT//TransitRidership_byStop//AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_{hhmm_YYYY_MMM}_standard_format.XLS
        :: these files must be acquired from DTPW :: 

    Outputs:
        - CLEANED//PMT_{year}.gdb//Points//TransitRidership
    """
    transit_folder = validate_directory(
        make_path(RAW, "TRANSIT", "TransitRidership_byStop")
    )
    transit_shape_fields = [prep_conf.TRANSIT_LONG, prep_conf.TRANSIT_LAT]
    print("PROCESSING TRANSIT RIDERSHIP... ")
    for year in YEARS:

        print(f"--- cleaning ridership for {year}")
        out_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_{year}.gdb"))
        FDS = validate_feature_dataset(
            fds_path=make_path(out_gdb, "Points"), sr=SR_FL_SPF
        )
        out_name = "TransitRidership"
        transit_out_path = make_path(FDS, out_name)
        if year == "NearTerm":
            arcpy.FeatureClassToFeatureClass_conversion(
                in_features=out_data, out_path=FDS, out_name=out_name
            )
            print(f"--- ridership cleaned for {year} and located in {transit_out_path}")
        else:
            transit_xls_file = make_path(
                transit_folder, prep_conf.TRANSIT_RIDERSHIP_TABLES[year]
            )
            check_overwrite_output(output=transit_out_path, overwrite=overwrite)
            p_help.prep_transit_ridership(
                in_table=transit_xls_file,
                rename_dict=prep_conf.TRANSIT_FIELDS_DICT,
                unique_id=prep_conf.TRANSIT_COMMON_KEY,
                shape_fields=transit_shape_fields,
                from_sr=prep_conf.IN_CRS,
                to_sr=prep_conf.OUT_CRS,
                out_fc=transit_out_path,
            )
            out_data = transit_out_path
            print(f"--- ridership cleaned for {year} and located in {transit_out_path}")


def process_parcels(overwrite=True):
    """
    YEAR by YEAR
      - cleans geometry, joins parcels from DOR to NAL table keeping appropriate columns
      - if year == NearTerm:
          previous year parcels are copied in to NearTerm gdb

    Inputs:
        - RAW//Parcels//Miami_{year}.shp
        - RAW//Parcels//NAL_{year}.shp
        :: these data are acquired from a download via the FDOR ftp site ::

    Outputs:
        - CLEANED//PMT_{year}.shp//Polygons//Parcels
    """
    print("PROCESSING PARCELS... ")
    parcel_folder = make_path(RAW, "Parcels")
    for year in YEARS:
        print(f"- {year} in process...")
        out_gdb = validate_geodatabase(make_path(CLEANED, f"PMT_{year}.gdb"))
        out_fds = validate_feature_dataset(make_path(out_gdb, "Polygons"), sr=SR_FL_SPF)
        out_fc = make_path(out_fds, "Parcels")

        # source data
        data_year = calc_year = year
        if year == "NearTerm":
            data_year = SNAPSHOT_YEAR
            calc_year = 9998

        in_fc = make_path(parcel_folder, f"Miami_{data_year}.shp")
        in_csv = make_path(parcel_folder, f"NAL_{data_year}_23Dade_F.csv")

        # input fix variables
        renames = prep_conf.PARCEL_COLS.get(data_year, {})
        usecols = prep_conf.PARCEL_USE_COLS.get(
            data_year, prep_conf.PARCEL_USE_COLS["DEFAULT"]
        )
        csv_kwargs = {"dtype": {"PARCEL_ID": str, "CENSUS_BK": str}, "usecols": usecols}

        check_overwrite_output(output=out_fc, overwrite=overwrite)
        p_help.prep_parcels(
            in_fc=in_fc,
            in_tbl=in_csv,
            out_fc=out_fc,
            fc_key_field=prep_conf.PARCEL_DOR_KEY,
            new_fc_key_field=prep_conf.PARCEL_COMMON_KEY,
            tbl_key_field=prep_conf.PARCEL_NAL_KEY,
            tbl_renames=renames,
            **csv_kwargs,
        )
        arcpy.CalculateField_management(
            in_table=out_fc,
            field="Year",
            expression=calc_year,
            expression_type="PYTHON3",
            field_type="LONG",
        )
        print("")


def process_permits(overwrite=True):
    """
    Processes Road Impact Fee report into point feature class, where points represent the
    parcel locations of active permits. The permit points are then used to generate a NearTerm
    parcel layer updating various parcel level metrics

    Inputs:
        - RAW//BUILDING_PERMITS//Road Impact Fee Collection Report -- {year}.csv
        - CLEANED//PMT_{year}.gdb//Polygons//Parcels

    Outputs:
        - CLEANED//PMT_NearTerm.gdb//Points//BuildingPermits
        - CLEANED//PMT_NearTerm.gdb//Polygons//Parcels (updated)
    """
    print("PROCESSING PERMIT DATA ... ")
    try:
        # workspaces
        permit_dir = make_path(RAW, "BUILDING_PERMITS")
        snap_gdb = validate_geodatabase(make_path(CLEANED, f"PMT_{SNAPSHOT_YEAR}.gdb"))
        near_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_NearTerm.gdb"))
        # input data
        permit_csv = make_path(
            permit_dir, "Road Impact Fee Collection Report -- 2019.csv"
        )
        snap_parcels = make_path(snap_gdb, "Polygons", "Parcels")
        # output data
        out_permits = make_path(near_gdb, "Points", "BuildingPermits")
        out_parcels = make_path(near_gdb, "Polygons", "Parcels")

        print("--- processing permit cleaning/formatting...")
        check_overwrite_output(output=out_permits, overwrite=overwrite)
        p_help.clean_permit_data(
            permit_csv=permit_csv,
            parcel_fc=snap_parcels,
            permit_key=prep_conf.PERMITS_COMMON_KEY,
            poly_key=prep_conf.PARCEL_COMMON_KEY,
            rif_lu_tbl=RIF_CAT_CODE_TBL,
            dor_lu_tbl=DOR_LU_CODE_TBL,
            out_file=out_permits,
            out_crs=EPSG_FLSPF,
        )
        unit_ref_df = p_help.create_permits_units_reference(
            parcels=snap_parcels,
            permits=out_permits,
            lu_key=prep_conf.LAND_USE_COMMON_KEY,
            parcels_living_area_key=prep_conf.PARCEL_BLD_AREA_COL,
            permit_value_key=prep_conf.PERMITS_UNITS_FIELD,
            permits_units_name=prep_conf.PERMITS_BLD_AREA_NAME,
            units_match_dict=prep_conf.PARCEL_REF_TABLE_UNITS_MATCH,
        )
        print("--- processing Near Term parcels updates/formatting...")
        temp_update = p_help.build_short_term_parcels(
            parcel_fc=snap_parcels,
            permit_fc=out_permits,
            permits_ref_df=unit_ref_df,
            parcels_id_field=prep_conf.PARCEL_COMMON_KEY,
            parcels_lu_field=prep_conf.LAND_USE_COMMON_KEY,
            parcels_living_area_field=prep_conf.PARCEL_BLD_AREA_COL,
            parcels_land_value_field=prep_conf.PARCEL_LAND_VALUE,
            parcels_total_value_field=prep_conf.PARCEL_JUST_VALUE,
            parcels_buildings_field=prep_conf.PARCEL_BUILDINGS,
            permits_id_field=prep_conf.PERMITS_ID_FIELD,
            permits_lu_field=prep_conf.PERMITS_LU_FIELD,
            permits_units_field=prep_conf.PERMITS_UNITS_FIELD,
            permits_values_field=prep_conf.PERMITS_VALUES_FIELD,
            permits_cost_field=prep_conf.PERMITS_COST_FIELD,
            units_field_match_dict=prep_conf.SHORT_TERM_PARCELS_UNITS_MATCH,
        )

        check_overwrite_output(output=out_parcels, overwrite=overwrite)
        print("--- --- writing out updated parcels with new permit data")
        out_fds = make_path(near_gdb, "Polygons")
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=temp_update, out_path=out_fds, out_name="Parcels"
        )
    except:
        raise


def enrich_block_groups(overwrite=True):
    """
    YEAR by YEAR, enrich block group with parcel data and race/commute/jobs data as table
        - if Year == "NearTerm", process as normal (parcel data have been updated to include permit updates)

    Inputs:
        - CLEANED//PMT_{year}.gdb//Polygons//Parcels
        - CLEANED//PMT_{year}.gdb//Polygons//Census_BlockGroups
        - RAW//CENSUS//ACS_{year}_race.csv
        - RAW//CENSUS//ACS_{year}_commute.csv
        - RAW//LODES//fl_wac_S000_JT00_{year}_bgrp.csv.gz

    Output:
        - CLEANED//PMT_{year}.gdb//Enrichment_census_blockgroups (table)
    """
    print(
        "Enriching block groups with parcel data and ACS tables (race/commute/jobs)..."
    )
    for year in YEARS:
        print(f"{str(year)}:")

        # data location
        gdb = make_path(CLEANED, f"PMT_{year}.gdb")
        fds = make_path(gdb, "Polygons")
        # parcels
        parcels_fc = make_path(fds, "Parcels")
        # bgs
        bg_fc = make_path(fds, "Census_BlockGroups")
        out_tbl = make_path(gdb, "Enrichment_census_blockgroups")
        # define table vars
        race_tbl = make_path(RAW, "CENSUS", f"ACS_{year}_race.csv")
        commute_tbl = make_path(RAW, "CENSUS", f"ACS_{year}_commute.csv")
        lodes_tbl = make_path(RAW, "LODES", f"fl_wac_S000_JT00_{year}_bgrp.csv.gz")

        # Enrich BGs with parcel data
        bg_df = p_help.enrich_bg_with_parcels(
            bg_fc=bg_fc,
            bg_id_field=prep_conf.BG_COMMON_KEY,
            parcels_fc=parcels_fc,
            par_id_field=prep_conf.PARCEL_COMMON_KEY,
            par_lu_field=prep_conf.LAND_USE_COMMON_KEY,
            par_bld_area=prep_conf.PARCEL_BLD_AREA_COL,
            sum_crit=prep_conf.LODES_CRITERIA,
            par_sum_fields=prep_conf.BG_PAR_SUM_FIELDS,
        )
        # Save enriched data
        print("--- saving enriched blockgroup table")
        df_to_table(df=bg_df, out_table=out_tbl, overwrite=overwrite)

        # Extend BG output with ACS/LODES data
        in_tables = [race_tbl, commute_tbl, lodes_tbl]
        in_tbl_ids = [
            prep_conf.ACS_COMMON_KEY,
            prep_conf.ACS_COMMON_KEY,
            prep_conf.LODES_COMMON_KEY,
        ]
        in_tbl_flds = [
            prep_conf.ACS_RACE_FIELDS,
            prep_conf.ACS_COMMUTE_FIELDS,
            prep_conf.LODES_FIELDS,
        ]
        for table, tbl_id, fields in zip(in_tables, in_tbl_ids, in_tbl_flds):
            _, table_name = os.path.split(table)

            if "LODES" in table:
                fld_name = "JOBS_SRC"
                data_src = '"LODES"'
            if "CENSUS" in table:
                fld_name = "DEM_SRC"
                data_src = '"ACS"'
            if not arcpy.Exists(table):
                data_src = '"EXTRAP"'
                print(
                    f"--- --- not able to enrich parcels with {table_name} doesnt exist, needs extrapolation"
                )
                arcpy.AddField_management(
                    in_table=out_tbl,
                    field_name=fld_name,
                    field_type="TEXT",
                    field_length=10,
                )
                arcpy.CalculateField_management(
                    in_table=out_tbl, field=fld_name, expression=data_src
                )
            else:
                print(f"--- --- enriching parcels with {table_name} data")
                arcpy.AddField_management(
                    in_table=out_tbl,
                    field_name=fld_name,
                    field_type="TEXT",
                    field_length=10,
                )
                arcpy.CalculateField_management(
                    in_table=out_tbl, field=fld_name, expression=data_src
                )
                p_help.enrich_bg_with_econ_demog(
                    tbl_path=out_tbl,
                    tbl_id_field=prep_conf.BG_COMMON_KEY,
                    join_tbl=table,
                    join_id_field=tbl_id,
                    join_fields=fields,
                )


def process_parcel_land_use(overwrite=True):
    """
    Generates a table mapping parcels to a human readable land use category (multiple)
    using the DOR_UC attribute

    Inputs:
        - CLEANED//PMT_{year}.gdb//Polygons//Parcels
        - REF//Land_Use_Recode.csv

    Output:
        - CLEANED//PMT_{year}.gdb//LandUseCodes_parcels (table)
    """
    for year in YEARS:
        print(year)
        year_gdb = make_path(CLEANED, f"PMT_{year}.gdb")
        # Parcels
        parcels_fc = make_path(year_gdb, "Polygons", "Parcels")
        # Reference table
        lu_table = make_path(REF, "Land_Use_Recode.csv")  # TODO: put in config?
        # Output
        out_table = make_path(year_gdb, "LandUseCodes_parcels")

        # Create combo df
        par_fields = [prep_conf.PARCEL_COMMON_KEY, "LND_SQFOOT"]
        dtype = {"DOR_UC": int}
        default_vals = {
            prep_conf.PARCEL_COMMON_KEY: "-1",
            prep_conf.LAND_USE_COMMON_KEY: 999,
        }
        par_df = p_help.prep_parcel_land_use_tbl(
            parcels_fc=parcels_fc,
            parcel_lu_field=prep_conf.LAND_USE_COMMON_KEY,
            parcel_fields=par_fields,
            lu_tbl=lu_table,
            tbl_lu_field=prep_conf.LAND_USE_COMMON_KEY,
            dtype_map=dtype,
            null_value=default_vals,
        )
        # Export result
        check_overwrite_output(output=out_table, overwrite=overwrite)
        df_to_table(df=par_df, out_table=out_table)


def process_imperviousness(overwrite=True):
    """
    Calculates impervious percentage by Census Block, and generates area estimates for
    NonDev, DevOS, DevLow, DevMed, and DevHigh intensity classes

    Inputs:
        - RAW//Imperviousness.zip
        - BASIC_FEATURES//MiamiDadeCountyBoundary

    Outputs:
        - CLEANED//PMT_{year}.gdb//Imperviousness_census_blocks
    """
    print("\nProcessing Imperviousness...")
    impervious_download = make_path(RAW, "Imperviousness.zip")
    county_boundary = make_path(
        CLEANED, "PMT_BasicFeatures.gdb", "BasicFeatures", "MiamiDadeCountyBoundary"
    )
    out_dir = validate_directory(make_path(RAW, "IMPERVIOUS"))
    # TODO: handle existing copy of the raster (dont prepare on each run if data hasnt changed)
    impv_raster = p_help.prep_imperviousness(
        zip_path=impervious_download,
        clip_path=county_boundary,
        out_dir=out_dir,
        transform_crs=EPSG_FLSPF,
    )
    print("--- --- converting raster to point")
    points = make_inmem_path(file_name="raster_to_point")
    arcpy.RasterToPoint_conversion(in_raster=impv_raster, out_point_features=points)
    print("--- grabbing impervious raster cell size")
    cellx = arcpy.GetRasterProperties_management(
        in_raster=impv_raster, property_type="CELLSIZEX"
    )
    celly = arcpy.GetRasterProperties_management(
        in_raster=impv_raster, property_type="CELLSIZEY"
    )
    cell_area = float(cellx.getOutput(0)) * float(celly.getOutput(0))
    for year in YEARS:
        print(f"\n{str(year)}:")
        year_gdb = make_path(CLEANED, f"PMT_{year}.gdb")
        block_fc = make_path(year_gdb, "Polygons", "Census_Blocks")
        parcel_fc = make_path(year_gdb, "Polygons", "Parcels")
        # capture total living area from parcels for use in later consolidations
        lvg_ar_df = p_help.agg_to_zone(
            parcel_fc=parcel_fc,
            agg_field=prep_conf.PARCEL_BLD_AREA_COL,
            zone_fc=block_fc,
            zone_id=prep_conf.BLOCK_COMMON_KEY,
        )
        impv_df = p_help.analyze_imperviousness(
            raster_points=points,
            rast_cell_area=cell_area,
            zone_fc=block_fc,
            zone_id_field=prep_conf.BLOCK_COMMON_KEY,
        )

        zone_name = os.path.split(block_fc)[1].lower()
        imp_table = make_path(year_gdb, f"Imperviousness_{zone_name}")
        df_to_table(df=impv_df, out_table=imp_table, overwrite=overwrite)
        extend_table_df(
            in_table=imp_table,
            table_match_field=prep_conf.BLOCK_COMMON_KEY,
            df=lvg_ar_df,
            df_match_field=prep_conf.BLOCK_COMMON_KEY,
        )


def process_osm_networks():
    """
    Creates bicycle and walk networks from osm-downloaded shape files and a network
    dataset template.

    Inputs:
        - RAW//OpenStreetMap//{mode}_{vintage}//edges.shp
        - REF//osm_{mode}_template.xml

    Outputs:
        - CLEANED//osm_networks//{mode}_{vintage}.gdb
        - CLEANED//PMT_{year}.gdb//Networks//edges_bike
    """
    net_versions = sorted({v[0] for v in prep_conf.NET_BY_YEAR.values()})
    for net_version in net_versions:
        # Import edges
        osm_raw = make_path(RAW, "OpenStreetMap")
        for net_type in ["bike", "walk"]:
            net_type_version = f"{net_type}{net_version}"
            # validate nets_dir
            if validate_directory(NETS_DIR):
                # Make output geodatabase
                clean_gdb = validate_geodatabase(
                    make_path(NETS_DIR, f"{net_type_version}.gdb"), overwrite=True
                )
                # make output feature dataset
                net_type_fd = validate_feature_dataset(
                    make_path(clean_gdb, "osm"), sr=SR_FL_SPF, overwrite=True
                )

            # import edges
            net_raw = make_path(osm_raw, net_type_version, "edges.shp")
            # transfer to gdb
            edges = osm_help.import_OSM_shape(
                osm_fc=net_raw, to_feature_dataset=net_type_fd, overwrite=True
            )

            if net_type == "bike":
                # Enrich features
                osm_help.classify_bikability(edges)
                # Copy bike edges to year geodatabases
                for year, nv in prep_conf.NET_BY_YEAR.items():
                    nv, model_yr = nv
                    if nv == net_version:
                        print(f"{year}:")
                        base_year = data_year = year
                        if year == "NearTerm":
                            base_year = year
                            data_year = 9998

                        out_path = validate_feature_dataset(
                            make_path(CLEANED, f"PMT_{base_year}.gdb", "Networks"),
                            sr=SR_FL_SPF,
                        )
                        out_name = "edges_bike"
                        arcpy.FeatureClassToFeatureClass_conversion(
                            in_features=edges, out_path=out_path, out_name=out_name
                        )
                        out_fc = make_path(out_path, out_name)
                        arcpy.CalculateField_management(
                            in_table=out_fc,
                            field="Year",
                            expression=str(data_year),
                            field_type="LONG",
                        )

            # Build network datasets
            template = make_path(REF, f"osm_{net_type}_template.xml")
            osm_help.make_network_dataset(
                template_xml=template,
                out_feature_dataset=net_type_fd,
                net_name="osm_ND",
            )


def process_bg_apply_activity_models(overwrite=True):
    """
    Using existing LODES and Census demographic data, a linear model is fitted to the data at the block
    group level. Modeled results are used in all years, even in those with observed data, so that there are
    clearer relationships and trends over time (mixing observed and modeled results can yield unexpected
    patterns at the temporal boundary between observed and estimated data).

    Inputs:
        - enriched Block group data (LODES, demographics) with parcel summarizations
    
    Outputs:
        - modeled version of the original enriched data
    """
    print("Modeling Block Group data...")
    model_coefficients = p_help.model_blockgroup_data(
        data_path=CLEANED,
        bg_enrich_tbl_name="Enrichment_census_blockgroups",
        bg_key="GEOID",
        fields="*",
        acs_years=prep_conf.ACS_YEARS,
        lodes_years=prep_conf.LODES_YEARS,
    )
    for year in YEARS:
        print(f"{year}: ")
        # Set the inputs based on the year
        gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        bg_enrich = make_path(gdb, "Enrichment_census_blockgroups")
        bg_geometry = make_path(gdb, "Polygons", "Census_BlockGroups")

        shares = {}
        shr_year = year
        if shr_year == "NearTerm":
            print("--- resetting share year for NearTerm")
            shr_year = datetime.datetime.now().year - 1
        # For unobserved years, set a constant share approach
        if year not in prep_conf.LODES_YEARS:
            print(f"--- getting constant LODES shares for {shr_year}")
            wl = np.argmin([abs(x - shr_year) for x in prep_conf.LODES_YEARS])
            shares["LODES"] = bg_enrich.replace(
                str(year), str(prep_conf.LODES_YEARS[wl])
            )
        if year not in prep_conf.ACS_YEARS:
            print(f"--- getting constant ACS shares for {shr_year}")
            wa = np.argmin([abs(x - shr_year) for x in prep_conf.ACS_YEARS])
            shares["ACS"] = bg_enrich.replace(str(year), str(prep_conf.ACS_YEARS[wa]))
        if len(shares.keys()) == 0:
            shares = None

        # Apply the models
        check_overwrite_output(
            output=make_path(gdb, "Modeled_blockgroups"), overwrite=overwrite
        )
        modeled_df = p_help.apply_blockgroup_model(
            year=shr_year,
            bg_enrich_path=bg_enrich,
            bg_geometry_path=bg_geometry,
            bg_id_field=prep_conf.BG_COMMON_KEY,
            model_coefficients=model_coefficients,
            shares_from=shares,
        )
        save_path = make_path(gdb, "Modeled_blockgroups")
        PMT.df_to_table(df=modeled_df, out_table=save_path)


def process_allocate_bg_to_parcels(overwrite=True):
    """
    Disaggregation of modeled block group data back to parcels, for the NearTerm time period, only parcels with
    permits are allocated to, as the other parcels are considered to be relatively static and carry over data
    from the allocation

    Inputs:
        parcel geometry, block group geometry, modeled block group tables
        - CLEANED//PMT_YYYY.gdb//Polygons//Parcels
        - CLEANED//PMT_YYYY.gdb//Polygons//Census_BlockGroups
        - CLEANED//PMT_YYYY.gdb//Modeled_blockgroups
    
    Outputs:
        - CLEANED//EconDemog_parcels (EconDemog_parcels: table of disaggregated economic and demographic data at the parcel level)
    """
    print("\nProcessing modeled data to generate allocation to parcels...")
    snap_gdb = make_path(CLEANED, f"PMT_{SNAPSHOT_YEAR}.gdb")
    for year in YEARS:
        # Set the inputs based on the year
        print(f"{year} allocation begun")
        out_gdb = make_path(CLEANED, f"PMT_{year}.gdb")
        parcel_fc = make_path(out_gdb, "Polygons", "Parcels")
        bg_geom = make_path(out_gdb, "Polygons", "Census_BlockGroups")
        bg_modeled = make_path(
            out_gdb, "Modeled_blockgroups"
        )  # TODO: read in as DF for the function

        # if nearterm, filter parcels to permitted then process bg_modeled as difference between NT and Snapshot
        if year == "NearTerm":
            # set where clause to limit parecel data to only PERMITTED changes
            wc = arcpy.AddFieldDelimiters(datasource=parcel_fc, field="PERMIT") + " = 1"
            # take the difference between NT and Snap modeled data prior to allocation
            bg_modeled_snap = make_path(snap_gdb, "Modeled_blockgroups")
            bg_modeled = table_difference(
                this_table=bg_modeled,
                base_table=bg_modeled_snap,
                idx_cols=prep_conf.BG_COMMON_KEY,
            )
        else:
            wc = ""
            bg_modeled = PMT.table_to_df(in_tbl=bg_modeled)
        # Allocate
        alloc_df = p_help.allocate_bg_to_parcels(
            bg_modeled_df=bg_modeled,
            bg_geom=bg_geom,
            bg_id_field=prep_conf.BG_COMMON_KEY,
            parcel_fc=parcel_fc,
            parcels_id=prep_conf.PARCEL_COMMON_KEY,
            parcel_wc=wc,
            parcel_lu=prep_conf.LAND_USE_COMMON_KEY,
            parcel_liv_area=prep_conf.PARCEL_BLD_AREA_COL,
        )

        if year == "NearTerm":
            # make a data frame of parcels with no change
            snap_data = make_path(snap_gdb, "EconDemog_parcels")
            snap_df = PMT.table_to_df(in_tbl=snap_data)
            snap_df["Year"] = 9998

            # mask out parcels without permits
            permit_mask = snap_df[prep_conf.PARCEL_COMMON_KEY].isin(
                alloc_df[prep_conf.PARCEL_COMMON_KEY]
            )
            masked_snap_df = snap_df.copy()[permit_mask]

            # add the rows with permitted change based on the allocation process back to the snapshot rows
            # Old data: set index and subset to numeric cols
            masked_snap_df.set_index(prep_conf.PARCEL_COMMON_KEY, inplace=True)
            masked_snap_df = masked_snap_df.select_dtypes(["number"])
            # New data: set index and subset to numeric cols
            alloc_df.set_index(prep_conf.PARCEL_COMMON_KEY, inplace=True)
            alloc_df = alloc_df.select_dtypes(["number"])
            # add Old and New data
            add_df = masked_snap_df.add(alloc_df)

            # update original data with updated allocation to permits
            snap_df.set_index(prep_conf.PARCEL_COMMON_KEY, inplace=True)
            snap_df.update(other=add_df)
            alloc_df = snap_df.reset_index()

        # For saving, we join the allocation estimates back to the ID shape we
        # initialized during spatial processing
        print("--- writing table of allocation results")
        out_path = make_path(out_gdb, "EconDemog_parcels")
        check_overwrite_output(output=out_path, overwrite=overwrite)
        PMT.df_to_table(df=alloc_df, out_table=out_path)


def process_osm_skims():
    """
    Estimated travel time by walking and biking between all MAZ origin-destination
    pairs and store in a long csv table.

    Inputs
        - CLEANED//osm_networks//{mode}_{vintage}.gdb
        - CLEANED//PMT_{year}.gdb//Polygons\MAZ

    Outputs
        - CLEANED//osm_networks//{mode}_Skim_{vintage}.csv
    """
    if arcpy.CheckExtension("network") == "Available":
        arcpy.CheckOutExtension("network")
    else:
        raise arcpy.ExecuteError("Network Analyst Extension license is not available.")
    # Create and solve OD Matrix at MAZ scale
    solved = []
    for year in YEARS:
        # Get MAZ features, create temp centroids for network loading
        maz_path = make_path(CLEANED, f"PMT_{year}.gdb", "Polygons", "MAZ")
        maz_fc = make_inmem_path()
        maz_pts = polygons_to_points(maz_path, maz_fc, prep_conf.MAZ_COMMON_KEY)
        net_suffix = prep_conf.NET_BY_YEAR[year][0]
        if net_suffix not in solved:
            # TODO: confirm whether separate walk/bike layers are needed
            #  primary concern is over chunking procedures and whether
            #  these entail selections or anything that would potentially
            #  disrupt a smooth iteration using a single layer of maz points
            modes = ["walk", "bike"]
            walk_lyr = arcpy.MakeFeatureLayer_management(maz_pts, "__walk__")
            bike_lyr = arcpy.MakeFeatureLayer_management(maz_pts, "__bike__")
            layers = [walk_lyr, bike_lyr]
            # Run each mode
            for mode, layer in zip(modes, layers):
                print(mode)
                # - Skim input/output
                nd = make_path(NETS_DIR, f"{mode}{net_suffix}.gdb", "osm", "osm_ND")
                skim = make_path(NETS_DIR, f"{mode}_Skim{net_suffix}.csv")
                if mode == "bike":
                    restrictions = prep_conf.BIKE_RESTRICTIONS
                else:
                    restrictions = None
                # - Create and load problem
                # Confirm "Year" column is included in output table
                p_help.generate_od_table(
                    origin_pts=layer,
                    origin_name_field=prep_conf.MAZ_COMMON_KEY,
                    dest_pts=maz_pts,
                    dest_name_field=prep_conf.MAZ_COMMON_KEY,
                    in_nd=nd,
                    imped_attr=prep_conf.OSM_IMPED,
                    cutoff=prep_conf.BIKE_PED_CUTOFF,
                    net_loader=prep_conf.NET_LOADER,
                    out_table=skim,
                    restrictions=restrictions,
                    use_hierarchy=False,
                    uturns="ALLOW_UTURNS",
                    o_location_fields=None,
                    d_location_fields=None,
                    o_chunk_size=1000,
                )
                # Clean up workspace
                arcpy.Delete_management(layer)
            # Mark as solved
            solved.append(net_suffix)


def process_model_se_data(overwrite=True):
    """
    Summarizing parcel level data up to MAZ and TAZ, and including SERPM model data
    for variables that cannot be readily estimated from parcel records (school enrollment, e.g.)

    Inputs: (parcel geometry, MAZ geometry (includes TAZ geometry))
        - CLEANED//PMT_YYYY.gdb//Polygons//MAZ
        - CLEANED//PMT_YYYY.gdb//Polygons//Parcels
        - CLEANED//PMT_YYYY.gdb//EconDemog_parcels
        - RAW//SERPM//maz_data_2015.csv

    Outputs: (parcel and SERPM data summarized up to MAZ and TAZ)
        - CLEANED//PMT_YYYY.gdb//EconDemog_MAZ
        - CLEANED//PMT_YYYY.gdb//EconDemog_TAZ
    """
    print("\nProcessing parcel data to MAZ/TAZ...")
    # Summarize parcel data to MAZ
    for year in YEARS:
        print(f"{str(year)}:")
        # Set output
        out_gdb = validate_geodatabase(make_path(CLEANED, f"PMT_{year}.gdb"))
        out_fds = validate_feature_dataset(make_path(out_gdb, "Polygons"), sr=SR_FL_SPF)
        out_fc = make_path(out_fds, "MAZ")
        maz_se_data = make_path(
            RAW, "SERPM", "maz_data_2015.csv"
        )  # TODO: standardize SERPM pathing
        # Summarize parcels to MAZ
        print("--- summarizing MAZ activities from parcels")
        par_fc = make_path(out_gdb, "Polygons", "Parcels")
        se_data = make_path(out_gdb, "EconDemog_parcels")
        par_data = p_help.estimate_maz_from_parcels(
            par_fc=par_fc,
            par_id_field=prep_conf.PARCEL_COMMON_KEY,
            maz_fc=out_fc,
            maz_id_field=prep_conf.MAZ_COMMON_KEY,
            taz_id_field=prep_conf.TAZ_COMMON_KEY,
            se_data=se_data,
            se_id_field=prep_conf.PARCEL_COMMON_KEY,
            agg_cols=prep_conf.MAZ_AGG_COLS,
            consolidations=prep_conf.MAZ_PAR_CONS,
        )
        # Fetch MAZ data (enrollments, etc.)
        print("--- fetching other base-year MAZ data")
        maz_data = pd.read_csv(maz_se_data)
        maz_data.rename(columns=prep_conf.SERPM_RENAMES, inplace=True)
        # Consolidate
        maz_data = p_help.consolidate_cols(
            df=maz_data,
            base_fields=[prep_conf.MAZ_COMMON_KEY, prep_conf.TAZ_COMMON_KEY],
            consolidations=prep_conf.MAZ_SE_CONS,
        )
        # Patch for full regional MAZ data
        print("--- combining parcel-based and non-parcel-based MAZ data")
        maz_data = p_help.patch_local_regional_maz(
            maz_par_df=par_data,
            maz_par_key=prep_conf.MAZ_COMMON_KEY,
            maz_df=maz_data,
            maz_key=prep_conf.MAZ_COMMON_KEY,
        )
        # Export MAZ table
        print("--- exporting MAZ socioeconomic/demographic data")
        maz_table = make_path(out_gdb, "EconDemog_MAZ")

        check_overwrite_output(output=maz_table, overwrite=overwrite)
        df_to_table(maz_data, maz_table)

        # Summarize to TAZ scale
        print("--- summarizing MAZ data to TAZ scale")
        maz_data.drop(columns=[prep_conf.MAZ_COMMON_KEY], inplace=True)
        taz_data = maz_data.groupby(prep_conf.TAZ_COMMON_KEY).sum().reset_index()
        # Export TAZ table
        print("--- exporting TAZ socioeconomic/demographic data")
        taz_table = make_path(out_gdb, "EconDemog_TAZ")

        check_overwrite_output(output=taz_table, overwrite=overwrite)
        df_to_table(taz_data, taz_table)


def process_model_skims():
    """
    For each SERPM model year, combine transit skims for local and premium
    transit into one table. Get best available transit time, eliminating false 
    connections.

        :: Assumes transit and auto skims have same fields. ::

    Inputs:
        - RAW//SERPM//AM_HWY_SKIMS_{model_year}.csv
        - RAW//SERPM//DLY_VEH_TRIPS_{model_year}.csv
        - CLEANED//SERPM//TAZ_to_TAZ_local_{model_year}.csv
        - CLEANED//SERPM//TAZ_to_TAZ_prem_{model_year}.csv

    Outputs:
        - CLEANED//SERPM/SERPM_OD_{model_year}.csv
    """
    # Get field definitions
    o_field = prep_conf.SKIM_O_FIELD
    d_field = prep_conf.SKIM_D_FIELD
    renames = {"F_TAZ": o_field, "T_TAZ": d_field, "TOT_TRIPS": "TRIPS"}
    renames.update(prep_conf.SKIM_RENAMES)

    # Clean each input/output for each model year
    for year in prep_conf.MODEL_YEARS:
        print(year)
        # Setup input/output tables
        auto_csv = PMT.make_path(RAW, "SERPM", f"AM_HWY_SKIMS_{year}.csv")
        local_csv = PMT.make_path(CLEANED, "SERPM", f"TAZ_to_TAZ_local_{year}.csv")
        prem_csv = PMT.make_path(CLEANED, "SERPM", f"TAZ_to_TAZ_prem_{year}.csv")
        trips_csv = PMT.make_path(RAW, "SERPM", f"DLY_VEH_TRIPS_{year}.csv")
        skim_out = PMT.make_path(CLEANED, "SERPM", f"SERPM_SKIM_TEMP.csv")
        temp_out = PMT.make_path(CLEANED, "SERPM", f"SERPM_OD_TEMP.csv")
        serpm_out = PMT.make_path(CLEANED, "SERPM", f"SERPM_OD_{year}.csv")

        # Combine all skims tables
        print(" - Combining all skims")
        in_tables = [auto_csv, local_csv, prem_csv]
        merge_fields = [o_field, d_field]
        suffixes = ["_AU", "_LOC", "_PRM"]
        dtypes = {o_field: int, d_field: int}
        p_help.combine_csv_dask(
            merge_fields,
            skim_out,
            *in_tables,
            suffixes=suffixes,
            how="outer",
            col_renames=renames,
            dtype=prep_conf.SKIM_DTYPES,
            thousands=",",
        )

        # Combine trips into the skims separately (helps manage field name collisions
        # that would be a bit troublesome if we combine everything at once)
        print(" - Combining trip tables")
        in_tables = [skim_out, trips_csv]
        p_help.combine_csv_dask(
            merge_fields,
            temp_out,
            *in_tables,
            how="outer",
            col_renames=renames,
            dtype=prep_conf.SKIM_DTYPES,
            thousands=",",
        )

        # Update transit timee esimates
        print(" - Getting best transit time")
        competing_cols = ["TIME_LOC", "TIME_PRM"]
        out_col = prep_conf.SKIM_IMP_FIELD + "_TR"
        replace = {0: np.inf}
        p_help.update_transit_times(
            temp_out,
            serpm_out,
            competing_cols=competing_cols,
            out_col=out_col,
            replace_vals=replace,
            chunksize=100000,
        )

        # Delete temporary tables
        arcpy.Delete_management(skim_out)
        arcpy.Delete_management(temp_out)


def process_osm_service_areas():
    """
    Estimates service area lines and polygons defining the 30-minute walkshed
    around transit stations and park facilities.

    Inputs
        - BASIC_FEATURES//SMARTplanStations
        - CLEANED//Park_Points.shp
        - CLEANED//osm_networks//walk_{vintage}.gdb

    Outputs
        - CLEANED//PMT_{year}.gdb//Networks//walk_to_parks_MERGE
        - CLEANED//PMT_{year}.gdb//Networks//walk_to_parks_NO_MERGE
        - CLEANED//PMT_{year}.gdb//Networks//walk_to_parks_NON_OVERLAP
        - CLEANED//PMT_{year}.gdb//Networks//walk_to_parks_OVERLAP
        - CLEANED//PMT_{year}.gdb//Networks//walk_to_stn_MERGE
        - CLEANED//PMT_{year}.gdb//Networks//walk_to_stn_NO_MERGE
        - CLEANED//PMT_{year}.gdb//Networks//walk_to_stn_NON_OVERLAP
        - CLEANED//PMT_{year}.gdb//Networks//walk_to_stn_OVERLAP
    """
    # Facilities
    #  - Stations
    stations = make_path(BASIC_FEATURES, "SMARTplanStations")
    station_name = "Name"
    # - Parks
    parks = make_path(CLEANED, "Park_Points.shp")
    parks_name = "NAME"

    # For each analysis year, analyze networks (avoid redundant solves)
    solved = []
    solved_years = []
    modes = ["walk"]  # ["walk", "bike"]
    dest_grp = ["stn", "parks"]
    runs = ["MERGE", "NO_MERGE", "OVERLAP", "NON_OVERLAP"]
    expected_fcs = [
        f"{mode}_to_{dg}_{run}" for mode in modes for dg in dest_grp for run in runs
    ]
    for year in YEARS:  # TODO: add appropriate print/logging statements within loop
        out_fds_path = make_path(CLEANED, f"PMT_{year}.gdb", "Networks")
        out_fds = validate_feature_dataset(out_fds_path, SR_FL_SPF, overwrite=False)
        # Network setup
        net_suffix = prep_conf.NET_BY_YEAR[year][0]
        if net_suffix in solved:
            # Copy from other year if already solved
            # Set a source to copy network analysis results from based on net_by_year
            # TODO: write function for source year setting
            target_net = prep_conf.NET_BY_YEAR[year][0]
            source_year = None
            for solved_year in solved_years:
                solved_net = prep_conf.NET_BY_YEAR[solved_year][0]
                if solved_net == target_net:
                    source_year = solved_year
                    break
            source_fds = make_path(CLEANED, f"PMT_{source_year}.gdb", "Networks")
            target_fds = make_path(CLEANED, f"PMT_{year}.gdb", "Networks")
            p_help.copy_net_result(
                source_fds=source_fds, target_fds=target_fds, fc_names=expected_fcs
            )  # TODO: embellish this function with print/logging
        else:
            # Solve this network
            print(f"\n{net_suffix}")
            for mode in modes:
                # Create separate service area problems for stations and parks
                nd = make_path(NETS_DIR, f"{mode}{net_suffix}.gdb", "osm", "osm_ND")
                stations = ServiceAreaAnalysis(
                    name=f"{mode}_to_stn",
                    network_dataset=nd,
                    facilities=stations,
                    name_field=station_name,
                    net_loader=prep_conf.NET_LOADER,
                )
                parks = ServiceAreaAnalysis(
                    name=f"{mode}_to_parks",
                    network_dataset=nd,
                    facilities=parks,
                    name_field=parks_name,
                    net_loader=prep_conf.NET_LOADER,
                )
                # Solve service area problems
                for sa_prob in [stations, parks]:
                    print(f"\n - {sa_prob.name}")
                    # Set restrictions if needed
                    if "bike" in sa_prob.name:
                        restrictions = prep_conf.BIKE_RESTRICTIONS
                    else:
                        restrictions = ""
                    # Solve (exports output to the out_fds)
                    sa_prob.solve(
                        imped_attr=prep_conf.OSM_IMPED,
                        cutoff=prep_conf.OSM_CUTOFF,
                        out_ws=out_fds,
                        restrictions=restrictions,
                        use_hierarchy=False,
                        net_location_fields="",
                    )
            # Keep track of what's already been solved
            solved.append(net_suffix)
        solved_years.append(year)


def process_centrality():
    """
    For each analysis year, analyze network centrality for all nodes in the bike
    network. Assigns a centrality score to parcels based on nearby network nodes.

    Inputs:
        - CLEANED//osm_networks//bike_{vintage}.gdb
        - CLEANED//PMT_{year}.gdb//Polygons//parcels

    Outputs:
        - CLEANED//PMT_{year}.gdb//Networks//nodes_bike
        - CLEANED//PMT_{year}.gdb//Centrality_parcels
    """
    solved = []
    solved_years = []
    node_id = "NODE_ID"
    for year in YEARS:
        if year == "NearTerm":
            calc_year = 9998
        elif year == "LongTerm":
            calc_year = 9999
        else:
            calc_year = year
        net_suffix = prep_conf.NET_BY_YEAR[year][0]
        out_gdb = make_path(CLEANED, f"PMT_{year}.gdb")
        out_fds_path = make_path(out_gdb, "Networks")
        out_fds = validate_feature_dataset(out_fds_path, SR_FL_SPF, overwrite=False)
        out_fc_name = "nodes_bike"
        out_fc = make_path(out_fds, out_fc_name)
        parcel_fc = make_path(out_gdb, "Polygons", "Parcels")
        check_overwrite_output(out_fc, overwrite=True)
        if net_suffix in solved:
            # Copy from other year if already solved
            # TODO: make source year setting a function
            target_net = prep_conf.NET_BY_YEAR[year][0]
            source_year = None
            for solved_year in solved_years:
                solved_net = prep_conf.NET_BY_YEAR[solved_year][0]
                if solved_net == target_net:
                    source_year = solved_year
                    break
            source_fds = make_path(CLEANED, f"PMT_{source_year}.gdb", "Networks")
            target_fds = make_path(CLEANED, f"PMT_{year}.gdb", "Networks")
            p_help.copy_net_result(source_fds, target_fds, fc_names=out_fc_name)
        else:
            # Get node and edge features as layers
            print(f"\n{net_suffix}")
            in_fds = make_path(NETS_DIR, f"bike{net_suffix}.gdb", "osm")
            in_nd = make_path(in_fds, "osm_ND")
            in_edges = make_path(in_fds, "edges")
            in_nodes = make_path(in_fds, "osm_ND_Junctions")
            edges = arcpy.MakeFeatureLayer_management(in_edges, "EDGES")
            nodes = arcpy.MakeFeatureLayer_management(in_nodes, "NODES")
            # Select edges by attribute - service roads
            where = arcpy.AddFieldDelimiters(datasource=edges, field="highway")
            where = where + " LIKE '%service%'"
            arcpy.SelectLayerByAttribute_management(
                in_layer_or_view=edges,
                selection_type="NEW_SELECTION",
                where_clause=where,
            )
            # Select nodes by location - nodes not touching services roads
            arcpy.SelectLayerByLocation_management(
                in_layer=nodes,
                overlap_type="INTERSECT",
                select_features=edges,
                selection_type="NEW_SELECTION",
                invert_spatial_relationship="INVERT",
            )
            # Export selected nodes to output fc
            arcpy.FeatureClassToFeatureClass_conversion(
                in_features=nodes, out_path=out_fds, out_name=out_fc_name
            )
            oid_field = arcpy.Describe(out_fc).OIDFieldName
            arcpy.AddField_management(
                in_table=out_fc, field_name=node_id, field_type="TEXT", field_length=8
            )
            arcpy.CalculateField_management(
                in_table=out_fc,
                field=node_id,
                expression=f"!{oid_field}!",
                expression_type="PYTHON",
            )
            # Calculate centrality (iterative OD solves)
            centrality_df = p_help.network_centrality(
                in_nd=in_nd,
                in_features=out_fc,
                net_loader=prep_conf.CENTRALITY_NET_LOADER,
                name_field=node_id,
                impedance_attribute=prep_conf.CENTRALITY_IMPED,
                cutoff=prep_conf.CENTRALITY_CUTOFF,
                restrictions=prep_conf.BIKE_RESTRICTIONS,
                chunk_size=1000,
            )
            # Extend out_fc
            extend_table_df(
                in_table=out_fc,
                table_match_field=node_id,
                df=centrality_df,
                df_match_field="Node",
            )
            # Delete layers to avoid name collisions
            arcpy.Delete_management(edges)
            arcpy.Delete_management(nodes)
            # Keep track of solved networks
            solved.append(net_suffix)

        # set year for nodes
        arcpy.CalculateField_management(
            out_fc, "Year", str(calc_year), field_type="LONG"
        )

        # generate CentIDX for parcels table through spatial join of closest point to parcel
        sj_temp = make_inmem_path()
        par_cent_fields = ["FOLIO", "Year", "CentIdx"]
        fmapper = arcpy.FieldMappings()
        fmapper.addTable(parcel_fc)
        fmapper.addTable(out_fc)
        for f in fmapper.fields:
            if f.name not in par_cent_fields:
                idx = fmapper.findFieldMapIndex(f.name)
                fmapper.removeFieldMap(idx)
        arcpy.SpatialJoin_analysis(
            target_features=parcel_fc,
            join_features=out_fc,
            out_feature_class=sj_temp,
            field_mapping=fmapper,
            match_option="CLOSEST",
        )
        arcpy.TableToTable_conversion(
            in_rows=sj_temp,
            out_path=out_gdb,
            out_name="Centrality_parcels",
            field_mapping=fmapper,
        )
        solved_years.append(year)


def process_walk_times():
    """
    Estimates walk times from parcels to stations and parcels to parks based on
    spatial relationships among parcel features and service area lines.

    Inputs
        - CLEANED//PMT_{year}.gdb//Polygons//parcels
        - CLEANED//PMT_{year}.gdb//Networks//walk_to_stn_MERGE
        - CLEANED//PMT_{year}.gdb//Networks//walk_to_parks_MERGE

    Outputs
        - CLEANED//PMT_{year}.gdb//WalkTime_parcels
    """
    print("\nProcessing Walk Times:")
    target_names = ["stn_walk", "park_walk"]  # , "stn_bike", "park_bike"]
    ref_fcs = [
        "walk_to_stn_NON_OVERLAP",
        "walk_to_parks_NON_OVERLAP",
    ]
    preselect_fcs = ["walk_to_stn_MERGE", "walk_to_parks_MERGE"]
    # "bike_to_stn_NON_OVERLAP", "bike_to_parks_NON_OVERLAP"]
    ref_name_field = "Name"
    ref_time_field = f"ToCumul_{prep_conf.OSM_IMPED}"
    for year in YEARS:
        print(f"{str(year)}\n--------------------")
        year_gdb = make_path(CLEANED, f"PMT_{year}.gdb")
        parcels = make_path(year_gdb, "Polygons", "Parcels")
        out_table = make_path(year_gdb, "WalkTime_parcels")
        _append_ = False
        # Iterate over targets and references
        net_fds = make_path(year_gdb, "Networks")
        for tgt_name, ref_fc, preselect_fc in zip(target_names, ref_fcs, preselect_fcs):
            print(f"- {tgt_name}")
            ref_fc = make_path(net_fds, ref_fc)
            preselect_fc = make_path(net_fds, preselect_fc)
            walk_time_df = p_help.parcel_walk_times(
                parcel_fc=parcels,
                parcel_id_field=prep_conf.PARCEL_COMMON_KEY,
                ref_fc=ref_fc,
                ref_name_field=ref_name_field,
                ref_time_field=ref_time_field,
                preselect_fc=preselect_fc,
                target_name=tgt_name,
            )
            # Dump df to output table
            if _append_:
                extend_table_df(
                    in_table=out_table,
                    table_match_field=prep_conf.PARCEL_COMMON_KEY,
                    df=walk_time_df,
                    df_match_field=prep_conf.PARCEL_COMMON_KEY,
                )
            else:
                df_to_table(df=walk_time_df, out_table=out_table, overwrite=True)
                _append_ = True
            # Add time bin field
            print("--- classifying time bins")
            bin_field = f"bin_{tgt_name}"
            min_time_field = f"min_time_{tgt_name}"
            p_help.parcel_walk_time_bin(
                in_table=out_table,
                bin_field=bin_field,
                time_field=min_time_field,
                code_block=prep_conf.TIME_BIN_CODE_BLOCK,
            )


def process_ideal_walk_times(overwrite=True):
    """
    Estimates hypothetical walk times from parcels to stations and parcels to parks based on
    spatial relationships among parcel features and stations, parks. Assumes a constant walk
    speed.

    Inputs
        - CLEANED//PMT_{year}.gdb//Polygons//parcels
        - CLEANED//Park_Points.shp
        - BASIC_FEATURES//SMARTplanStations

    Outputs
        - CLEANED//PMT_{year}.gdb//WalkTimeIdeal_parcels
    """
    print("\nProcessing Ideal Walk Times:")
    targets = ["stn", "park"]
    for year in YEARS:
        print(f"{str(year)}\n--------------------")
        # Key paths
        year_gdb = make_path(CLEANED, f"PMT_{year}.gdb")
        parcels_fc = make_path(year_gdb, "Polygons", "Parcels")
        stations_fc = make_path(BASIC_FEATURES, "SMARTplanStations")
        parks_fc = make_path(CLEANED, "Park_Points.shp")
        out_table = make_path(year_gdb, "WalkTimeIdeal_parcels")
        target_fcs = [stations_fc, parks_fc]
        # Analyze ideal walk times
        dfs = []
        for target, fc in zip(targets, target_fcs):
            print(f" - {target}")
            # field_suffix = f"{target}_ideal"
            df = p_help.parcel_ideal_walk_time(
                parcels_fc=parcels_fc,
                parcel_id_field=prep_conf.PARCEL_COMMON_KEY,
                target_fc=fc,
                target_name_field="Name",
                radius=prep_conf.IDEAL_WALK_RADIUS,
                target_name=target,
                overlap_type="HAVE_THEIR_CENTER_IN",
                sr=None,
                assumed_mph=prep_conf.IDEAL_WALK_MPH,
            )
            dfs.append(df)
        # Combine dfs, dfToTable
        combo_df = reduce(
            lambda left, right: pd.merge(
                left, right, on=prep_conf.PARCEL_COMMON_KEY, how="outer"
            ),
            dfs,
        )
        # combo_df = dfs[0].merge(right=dfs[1], how="outer", on=prep_conf.PARCEL_COMMON_KEY)
        df_to_table(df=combo_df, out_table=out_table, overwrite=overwrite)
        # Add bin fields
        for target in targets:
            min_time_field = f"min_time_{target}"
            bin_field = f"bin_{target}"
            p_help.parcel_walk_time_bin(
                in_table=out_table,
                bin_field=bin_field,
                time_field=min_time_field,
                code_block=prep_conf.TIME_BIN_CODE_BLOCK,
            )


def process_access():
    """
    Summarizes activities (jobs, school enrollments, housing units, etc.) reachable
    from zone features (MAZs for non-motorized modes, TAZs for motorized modes) by
    alternative travel modes (walk, bike, transit, auto).

    Inputs
        - CLEANED//osm_networks//{mode})Skim_{vintage}.csv
        - CLEANED//SERPM//SERPM_OD_{model_year}.csv
        - CLEANED//PMT_{year}.gdb//EconDemog_MAZ
        - CLEANED//PMT_{year}.gdb//EconDemog_TAZ

    Outputs
        - CLEANED//PMT_{year}.gdb//Access_maz_Bike
        - CLEANED//PMT_{year}.gdb//Access_maz_Walk
        - CLEANED//PMT_{year}.gdb//Access_taz_Auto
        - CLEANED//PMT_{year}.gdb//Access_taz_Transit
    """
    for year in YEARS:
        print(f"Analysis year: {year}")
        gdb = make_path(CLEANED, f"PMT_{year}.gdb")
        for mode in prep_conf.ACCESS_MODES:
            print(f"--- {mode}")
            # Get reference info from globals
            source, scale, id_field = prep_conf.MODE_SCALE_REF[mode]
            osm_year, model_year = prep_conf.NET_BY_YEAR[year]
            if source == "OSM_Networks":
                skim_year = osm_year
                imped_field = "Minutes"
                skim_data = make_path(CLEANED, source, f"{mode}_Skim{skim_year}.csv")
            else:
                skim_year = model_year
                imped_field = f"{prep_conf.SKIM_IMP_FIELD}_{mode[:2].upper()}"
                skim_data = make_path(CLEANED, source, f"SERPM_OD_{skim_year}.csv")
            # Look up zone and skim data for each mode
            zone_data = make_path(gdb, f"EconDemog_{scale}")

            # Analyze access
            atd_df = p_help.summarize_access(
                skim_table=skim_data,
                o_field=prep_conf.SKIM_O_FIELD,
                d_field=prep_conf.SKIM_D_FIELD,
                imped_field=imped_field,
                se_data=zone_data,
                id_field=id_field,
                act_fields=prep_conf.D_ACT_FIELDS,
                imped_breaks=prep_conf.ACCESS_TIME_BREAKS,
                units=prep_conf.ACCESS_UNITS,
                join_by="D",
                dtype=prep_conf.SKIM_DTYPES,
                chunk_size=100000,
            )
            afo_df = p_help.summarize_access(
                skim_table=skim_data,
                o_field=prep_conf.SKIM_O_FIELD,
                d_field=prep_conf.SKIM_D_FIELD,
                imped_field=imped_field,
                se_data=zone_data,
                id_field=id_field,
                act_fields=prep_conf.O_ACT_FIELDS,
                imped_breaks=prep_conf.ACCESS_TIME_BREAKS,
                units=prep_conf.ACCESS_UNITS,
                join_by="O",
                dtype=prep_conf.SKIM_DTYPES,
                chunk_size=100000,
            )
            # Merge tables
            atd_df.rename(columns={prep_conf.SKIM_O_FIELD: id_field}, inplace=True)
            afo_df.rename(columns={prep_conf.SKIM_D_FIELD: id_field}, inplace=True)
            full_table = atd_df.merge(right=afo_df, on=id_field)

            # Export output
            out_table = make_path(gdb, f"Access_{scale}_{mode}")
            df_to_table(full_table, out_table, overwrite=True)


def process_contiguity(overwrite=True):
    """
    Estimates contiguity of developable land year over year by removing building footprints and other non-developable
    areas from the parcel layer and calculating the area of the remaining space on each parcel

    Inputs:
        - CLEANED//BASIC_FEATURES//MiamiDadeCountyBoundary
        - CLEANED//PMT_YYYY.gdb//Polygons//Parcels
        - RAW//ENVIRONMENTAL_FEATURES//NHDPLUS_H_0309_HU4_GDB.gdb//NHDWaterbody
        - RAW//ENVIRONMENTAL_FEATURES//PADUS2_0FL.gdb//PADUS2_0Combined_DOD_Fee_Designation_Easement_FL
        - RAW//OpenStreetMap//buildings_{prefix_qX_YYYY}//OSM_Buildings_{YYYYMMDDTTTT}.shp

    Outputs:
        - CLEANED//PMT_{year}.gdb//Congiguity_parcels
    """
    county_fc = make_path(BASIC_FEATURES, "MiamiDadeCountyBoundary")
    parks = make_path(CLEANED, "Park_Polys.shp")
    water_bodies = make_path(
        RAW, "ENVIRONMENTAL_FEATURES", "NHDPLUS_H_0309_HU4_GDB.gdb", "NHDWaterbody"
    )
    pad_area = make_path(
        RAW,
        "ENVIRONMENTAL_FEATURES",
        "PADUS2_0FL.gdb",
        "PADUS2_0Combined_DOD_Fee_Designation_Easement_FL",
    )
    chunk_fishnet = p_help.generate_chunking_fishnet(
        template_fc=county_fc, out_fishnet_name="quadrats", chunks=prep_conf.CTGY_CHUNKS
    )
    for year in YEARS:
        print(f"Processing Contiguity for {year}")
        gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        parcel_fc = make_path(gdb, "Polygons", "Parcels")

        # merge mask layers and subset to County
        #   (builidngs held in year loop as future versions may take advantage of
        #       historical building footprint data via OSM attic)
        buildings = make_path(
            RAW,
            "OpenStreetMap",
            "buildings_q1_2021",
            "OSM_Buildings_20210201074346.shp",
        )
        mask = p_help.merge_and_subset(
            feature_classes=[buildings, water_bodies, pad_area, parks],
            subset_fc=county_fc,
        )

        ctgy_full = p_help.calculate_contiguity_index(
            quadrats_fc=chunk_fishnet,
            parcels_fc=parcel_fc,
            mask_fc=mask,
            parcels_id_field=prep_conf.PARCEL_COMMON_KEY,
            cell_size=prep_conf.CTGY_CELL_SIZE,
            weights=prep_conf.CTGY_WEIGHTS,
        )
        if prep_conf.CTGY_SAVE_FULL:
            full_path = make_path(gdb, "Contiguity_full_singlepart")
            df_to_table(df=ctgy_full, out_table=full_path, overwrite=True)
        ctgy_summarized = p_help.calculate_contiguity_summary(
            full_results_df=ctgy_full,
            parcels_id_field=prep_conf.PARCEL_COMMON_KEY,
            summary_funcs=prep_conf.CTGY_SUMMARY_FUNCTIONS,
            area_scaling=prep_conf.CTGY_SCALE_AREA,
        )
        summarized_path = make_path(gdb, "Contiguity_parcels")
        df_to_table(df=ctgy_summarized, out_table=summarized_path, overwrite=overwrite)


def process_bike_facilities(overwrite=True):
    """
    Combines and formats bike facility data layers previously downloaded from the MD Open Data Portal.
    A copy of the combined dataset is placed in each yearly geodatabase

    Inputs:
        - RAW//Bike_lane.geojson
        - RAW//Paved_Path.geojson
        - RAW//Paved_Shoulder.geojson
        - RAW//Wide_Curb_Lane.geojson

    Outputs:
        - RAW//bike_facilities.shp
        - CLEANED//PMT_YYYY.gdb//Network//bike_facilities
    """
    bike_lanes = make_path(RAW, "Bike_lane.geojson")
    paved_path = make_path(RAW, "Paved_Path.geojson")
    paved_shoulder = make_path(RAW, "Paved_Shoulder.geojson")
    wide_curb_lane = make_path(RAW, "Wide_Curb_Lane.geojson")
    facility_list = [bike_lanes, paved_path, paved_shoulder, wide_curb_lane]
    raw_bike_facilities = make_path(RAW, "bike_facilities.shp")

    # merge facilities
    mem_fcs = []
    for fac in facility_list:
        fac_type = os.path.splitext(os.path.basename(fac))[0].replace("_", " ")
        fac_fc = make_inmem_path()
        arcpy.JSONToFeatures_conversion(
            in_json_file=fac, out_features=fac_fc, geometry_type="Polyline"
        )
        arcpy.AddField_management(
            in_table=fac_fc, field_name=prep_conf.BIKE_FAC_COL, field_type="TEXT", field_length=50
        )
        with arcpy.da.UpdateCursor(fac_fc, prep_conf.BIKE_FAC_COL) as uc:
            for row in uc:
                row[0] = fac_type
                uc.updateRow(row)
        arcpy.AddGeometryAttributes_management(
            Input_Features=fac_fc,
            Geometry_Properties="LENGTH_GEODESIC",
            Length_Unit="MILES_US",
        )
        arcpy.AlterField_management(
            in_table=fac_fc,
            field="LENGTH_GEO",
            new_field_name=prep_conf.BIKE_MILES_COL,
            new_field_alias=prep_conf.BIKE_MILES_COL,
        )
        mem_fcs.append(fac_fc)
    check_overwrite_output(output=raw_bike_facilities, overwrite=overwrite)
    arcpy.Merge_management(inputs=mem_fcs, output=raw_bike_facilities)

    # tidy up
    keep_fields = [prep_conf.BIKE_FAC_COL, prep_conf.BIKE_MILES_COL, "NAME", "LIMIT"]
    drop_fields = [
        field.name
        for field in arcpy.ListFields(raw_bike_facilities)
        if not field.required and field.name not in keep_fields
    ]
    for drop in drop_fields:
        arcpy.DeleteField_management(in_table=raw_bike_facilities, drop_field=drop)
    oid = arcpy.Describe(raw_bike_facilities).OIDFieldName
    arcpy.CalculateField_management(
        in_table=raw_bike_facilities,
        field=prep_conf.BIKE_FAC_COMMON_KEY,
        expression=f"!{oid}!",
    )
    for year in YEARS:
        out_path = make_path(YEAR_GDB_FORMAT.replace("YEAR", str(year)), "Networks")
        check_overwrite_output(output=make_path(out_path, "bike_facilities"), overwrite=overwrite)
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=raw_bike_facilities,
            out_path=out_path,
            out_name="bike_facilities",
        )


def process_bike_miles(overwrite=True):
    """
    Intersects Summary area polygons with bike facilities and summarizes each facility
    type by miles within the summary area.

    Inputs:
        - CLEANED//PMT_YYYY.gdb//Networks//bike_facilities
        - CLEANED//PMT_YYYY.gdb//Polygons//SummaryAreas

    Outputs:
        - CLEANED//PMT_YYYY.gdb//BikeFac_summaryareas
    """
    # process bike miles at summary area (wide by facility type)
    print("\nProcessing Bike Facilities to summary areas")
    for year in YEARS:
        print(f"{str(year)}:")
        gdb = make_path(CLEANED, f"PMT_{year}.gdb")
        bike_fac_fc = make_path(gdb, "Networks", "bike_facilities")
        summ_area_fc = make_path(gdb, "Polygons", "SummaryAreas")
        out_tbl = make_path(gdb, "BikeFac_summaryareas")
        use_cols = [
            prep_conf.SUMMARY_AREAS_COMMON_KEY,
            prep_conf.BIKE_MILES_COL,
            prep_conf.BIKE_FAC_COL,
        ]
        # generate list of Facility types from
        BIKE_FAC_TYPE_COLS = [
            t.replace(" ", "_")
            for t in list(
                PMT.featureclass_to_df(in_fc=bike_fac_fc)[
                    prep_conf.BIKE_FAC_COL
                ].unique()
            )
        ]

        # grab all summArea rows by SummID and generate blank dataframe
        summ_df = PMT.featureclass_to_df(
            in_fc=summ_area_fc, keep_fields=prep_conf.SUMMARY_AREAS_COMMON_KEY
        )
        summ_df[BIKE_FAC_TYPE_COLS] = 0
        summ_df.set_index(keys=prep_conf.SUMMARY_AREAS_COMMON_KEY, inplace=True)

        # intersect bike_fac and summary areas
        print("--- intersecting summary areas with bike facilities")
        fac_int = intersect_features(
            summary_fc=summ_area_fc, disag_fc=bike_fac_fc, full_geometries=True
        )
        # update Bike_miles based on intersection
        print("--- recalculating length of facilities within each summary area")
        arcpy.CalculateGeometryAttributes_management(
            in_features=fac_int,
            geometry_property=f"{prep_conf.BIKE_MILES_COL} LENGTH_GEODESIC",
            length_unit="MILES_US",
        )
        fac_int_df = PMT.featureclass_to_df(in_fc=fac_int, keep_fields=use_cols)
        col_rename = {
            old: old.replace(" ", "_")
            for old in fac_int_df[prep_conf.BIKE_FAC_COL].unique()
        }
        print("--- summarizing bike miles to summary area by Facility Type")
        pivot = pd.pivot_table(
            data=fac_int_df,
            values=prep_conf.BIKE_MILES_COL,
            index=prep_conf.SUMMARY_AREAS_COMMON_KEY,
            columns=[prep_conf.BIKE_FAC_COL],
            aggfunc=np.sum,
            fill_value=0.0,
        )
        pivot.rename(columns=col_rename, inplace=True)
        summ_df.update(other=pivot)
        summ_df.reset_index(inplace=True)
        df_to_table(df=summ_df, out_table=out_tbl, overwrite=overwrite)


def process_lu_diversity(overwrite=True):
    """
    YEAR OVER YEAR: calculates land use diversity within aggregate geometries using parcels

    The diversity measures are defined as followed:
        1. Simpson index: mathematically, the probability that a random draw of one unit of land use A would be
            followed by a random draw of one unit of land use B. Ranges from 0 (only one land use present) to
            1 (all land uses present in equal abundance)
        2. Shannon index: borrowing from information theory, Shannon quantifies the uncertainty in predicting the
            land use of a random one unit draw. The higher the uncertainty, the higher the diversity. Ranges from 0
           (only one land use present) to -log(1/|land uses|) (all land uses present in equal abundance)
        3. Berger-Parker index: the maximum proportional abundance, giving a measure of dominance. Ranges from 1
            (only one land use present) to 1/|land uses| (all land uses present in equal abundance). Lower values
            indicate a more even spread, while high values indicate the dominance of one land use.
        4. Effective number of parties (ENP): a count of land uses, as weighted by their proportional abundance.
            A land use contributes less to ENP if it is relatively rare, and more if it is relatively common. Ranges
            from 1 (only one land use present) to |land uses| (all land uses present in equal abunance)
        5. Chi-squared goodness of fit: the ratio of an observed chi-squared goodness of fit test statistic to a
            "worst case scenario" chi-squared goodness of fit test statistic. The goodness of fit test requires
            the definition of an "optimal" land use distribution ("optimal" is assumed  to be equal abundance of
            all land uses, but can be specified by the user). The "worst case scenario" defines the highest possible
            chi-squared statistic that could be observed under the optimal land use distribution. In practice, this
            "worst case scenario" is the equivalent of the least likely land use [according to the optimal
            distribution] comprising the entire area. Ranges from 0 (all land uses present in equal abundance)
            to 1 (only one land use present)

    Inputs:
        - CLEANED//PMT_BasicFeatures.gdb//SummaryAreas (geometry)
        - REF//Land_Use_Recode.csv
        - CLEANED//PMT_YYYY.gdb//Polygons//Parcels

    Outputs:
        - CLEANED//PMT_YYYY.gdb//Diversity_summaryareas
    """
    print("\nProcessing Land Use Diversity...")
    summary_areas_fc = make_path(BASIC_FEATURES, "SummaryAreas")
    lu_recode_table = make_path(REF, "Land_Use_Recode.csv")
    usecols = [prep_conf.LAND_USE_COMMON_KEY, prep_conf.LU_RECODE_FIELD]
    recode_df = pd.read_csv(lu_recode_table, usecols=usecols)

    # Filter recode table
    fltr = np.in1d(
        recode_df[prep_conf.LU_RECODE_FIELD], prep_conf.DIV_RELEVANT_LAND_USES
    )
    recode_df = recode_df[fltr].copy()
    # Iterate over analysis years
    for year in YEARS:
        print(year)
        gdb = make_path(CLEANED, f"PMT_{year}.gdb")
        parcel_fc = make_path(gdb, "Polygons", "Parcels")
        out_fc = make_path(gdb, "Diversity_summaryareas")

        # Intersect parcels and summary areas
        print(" - intersecting parcels with summary areas")
        par_fields = [
            prep_conf.PARCEL_COMMON_KEY,
            prep_conf.LAND_USE_COMMON_KEY,
            prep_conf.PARCEL_BLD_AREA_COL,
        ]
        par_sa_int = p_help.assign_features_to_agg_area(
            in_features=parcel_fc,
            agg_features=summary_areas_fc,
            in_fields=par_fields,
            buffer=None,
            as_df=True,
        )

        # Intersect can alter field name
        col_rename = {
            f"{prep_conf.SUMMARY_AREAS_COMMON_KEY}_": prep_conf.SUMMARY_AREAS_COMMON_KEY
        }
        par_sa_int.rename(columns=col_rename, inplace=True)

        # Merge generalized land uses
        in_df = par_sa_int.merge(
            recode_df, how="inner", on=prep_conf.LAND_USE_COMMON_KEY
        )
        # Adjust floor area since sqft are large numbers
        in_df[prep_conf.PARCEL_BLD_AREA_COL] /= 1000

        # Calculate div indices
        print("--- calculating diversity indices")
        div_funcs = [
            p_help.simpson_diversity,
            p_help.shannon_diversity,
            p_help.berger_parker_diversity,
            p_help.enp_diversity,
            # P_HELP.chi_squared_diversity
        ]
        count_lu = len(prep_conf.DIV_RELEVANT_LAND_USES)
        div_df = p_help.lu_diversity(
            in_df=in_df,
            groupby_field=prep_conf.SUMMARY_AREAS_COMMON_KEY,
            lu_field=prep_conf.LU_RECODE_FIELD,
            div_funcs=div_funcs,
            weight_field=prep_conf.PARCEL_BLD_AREA_COL,
            count_lu=count_lu,
            regional_comp=True,
        )

        # Export results
        print(" - exporting results")
        df_to_table(div_df, out_fc, overwrite=overwrite)


def process_travel_stats(overwrite=True):
    """
    Estimates rates of vehicle miles of travel (VMT) per capita and per job
    based on vehicle trip estimates, estimated travel distances, and 
    jobs and housing estimates from SERPM. These estimates are then applied
    year over year to parcel-based estimates of jobs and housing to approximate
    daily vehicle trips and VMT generated by each TAZ.
    
    Inputs:
        - CLEANED//SERPM//SERPM_OD_{model_year}.csv
        - CLEANED//PMT_YYYY.gdb//EconDemog_TAZ
    
    Outputs:
        - CLEANED//PMT_YYYY.gdb//TripStates_TAZ
    """
    rates = {}
    hh_field = "HH"
    jobs_field = "TotalJobs"

    # Apply per cap/job rates to analysis years
    for year in YEARS:
        # Get SE DATA
        year_gdb = make_path(CLEANED, f"PMT_{year}.gdb")
        taz_table = make_path(year_gdb, "EconDemog_TAZ")
        out_table = make_path(year_gdb, "TripStats_TAZ")
        taz_df = table_to_df(taz_table, keep_fields="*")

        # Get OD reference
        model_year = prep_conf.NET_BY_YEAR[year][1]
        if model_year not in rates:
            # Calculate per cap/per job vmt rates
            skim_csv = make_path(CLEANED, "SERPM", f"SERPM_OD_{model_year}.csv")
            taz_ref_csv = make_path(CLEANED, f"PMT_{model_year}.gdb", "EconDemog_TAZ")
            taz_ref = PMT.table_to_df(taz_ref_csv, keep_fields="*")
            trips_field = "TRIPS"
            auto_time_field = prep_conf.SKIM_IMP_FIELD + "_AU"
            #tran_time_field = prep_conf.SKIM_IMP_FIELD + "_TR"
            dist_field = "DIST"
            rates_df = p_help.taz_travel_stats(
                od_table=skim_csv,
                o_field=prep_conf.SKIM_O_FIELD,
                d_field=prep_conf.SKIM_D_FIELD,
                veh_trips_field=trips_field,
                auto_time_field=auto_time_field,
                dist_field=dist_field,
                taz_df=taz_ref,
                taz_id_field=prep_conf.TAZ_COMMON_KEY,
                hh_field=hh_field,
                jobs_field=jobs_field,
            )
            rates[model_year] = rates_df

        # Multiply rates by TAZ activity
        rates_df = rates[model_year]
        taz_fields = [prep_conf.TAZ_COMMON_KEY, hh_field, jobs_field]
        loaded_df = rates_df.merge(
            taz_df[taz_fields], how="inner", on=prep_conf.TAZ_COMMON_KEY
        )
        loaded_df["__activity__"] = loaded_df[[hh_field, jobs_field]].sum(axis=1)
        loaded_df["VMT_FROM"] = loaded_df.VMT_PER_ACT_FROM * loaded_df.__activity__
        loaded_df["VMT_TO"] = loaded_df.VMT_PER_ACT_TO * loaded_df.__activity__
        loaded_df["VMT_ALL"] = loaded_df[["VMT_FROM", "VMT_TO"]].mean(axis=1)

        # Export results
        loaded_df = loaded_df.drop(columns=[hh_field, jobs_field, "__activity__"])
        df_to_table(df=loaded_df, out_table=out_table, overwrite=overwrite)


def process_walk_to_transit_skim():
    """
    Estimates the time required to walk between SERPM TAZ centroids and SERPM
    TAP (transit access point) nodes using the OSM walk network (for Miami-Dade
    County) or based on simple spatial relationships (coarser estimate outside MDC).

    Inputs:
        - RAW//SERPM//SERPM_TAZ_Centroids.shp
        - RAW//SERPM//SERPM_TAP_Nodes.shp
        - NETS_DIR//Walk{net suffix for analysis year}.gdb//osm/osm_ND
    Outputs:
        - CLEANED//SERPM/TAZ_to_TAP{net suffix for analysis year}.csv
    """
    # Create OD table of TAZ centroids to TAP nodes
    serpm_raw = make_path(RAW, "SERPM")
    serpm_clean = make_path(CLEANED, "SERPM")
    taz_centroids = make_path(serpm_raw, "SERPM_TAZ_Centroids.shp")
    tap_nodes = make_path(serpm_raw, "SERPM_TAP_Nodes.shp")
    tap_id = "TAP"
    tap_cutoff = "15"  # minutes
    solved = []
    for year in YEARS:
        net_suffix, model_year = prep_conf.NET_BY_YEAR[year]
        if model_year not in solved:
            print(f"Preparing TAP to TAZ skims for model year {model_year}")
            # Get TAZ to TAP OD table
            # - Skim input
            nd = make_path(NETS_DIR, f"Walk{net_suffix}.gdb", "osm", "osm_ND")
            skim = make_path(serpm_clean, f"TAZ_to_TAP{net_suffix}.csv")
            restrictions = None
            # - Create and load problem
            print(" - Network-based")
            p_help.generate_od_table(
                origin_pts=taz_centroids,
                origin_name_field=prep_conf.TAZ_COMMON_KEY,
                dest_pts=tap_nodes,
                dest_name_field=tap_id,
                in_nd=nd,
                imped_attr=prep_conf.OSM_IMPED,
                cutoff=tap_cutoff,
                net_loader=prep_conf.NET_LOADER,
                out_table=skim,
                restrictions=restrictions,
                use_hierarchy=False,
                uturns="ALLOW_UTURNS",
                o_location_fields=None,
                d_location_fields=None,
                o_chunk_size=None,
            )

            # Estimate simple spatial distance TAZ to TAP for TAZs outside extents of osm network
            print(" - Spatial-based")
            taz_layer = arcpy.MakeFeatureLayer_management(taz_centroids, "TAZ")
            tap_layer = arcpy.MakeFeatureLayer_management(tap_nodes, "TAP")
            edges = make_path(NETS_DIR, f"Walk{net_suffix}.gdb", "osm", "edges")
            net_layer = arcpy.MakeFeatureLayer_management(edges, "edges")
            # Set spatial reference
            sr = arcpy.Describe(edges).spatialReference
            mpu = float(sr.metersPerUnit)
            # Get distances and estimate times
            out_rows = []
            try:
                # Select TAZ's that wouldn't load on network
                arcpy.SelectLayerByLocation_management(
                    in_layer=taz_layer,
                    overlap_type="INTERSECT",
                    select_features=edges,
                    search_distance=prep_conf.NET_LOADER.search_tolerance,
                    selection_type="NEW_SELECTION",
                    invert_spatial_relationship=True,
                )
                # Iterate over TAZs
                with arcpy.da.SearchCursor(
                    taz_layer,
                    ["SHAPE@", prep_conf.TAZ_COMMON_KEY],
                    spatial_reference=sr,
                ) as taz_c:
                    for taz_r in taz_c:
                        taz_point, taz_id = taz_r
                        # Select TAP's that are within potential walking distance of selected TAZ's
                        arcpy.SelectLayerByLocation_management(
                            in_layer=tap_layer,
                            overlap_type="INTERSECT",
                            select_features=taz_point,
                            search_distance=prep_conf.IDEAL_WALK_RADIUS,
                            selection_type="NEW_SELECTION",
                            invert_spatial_relationship=False,
                        )
                        # Iterate over taps and estimate walk time
                        with arcpy.da.SearchCursor(
                            tap_layer, ["SHAPE@", tap_id], spatial_reference=sr
                        ) as tap_c:
                            for tap_r in tap_c:
                                tap_point, tap_n = tap_r
                                grid_dist = abs(
                                    tap_point.centroid.X - taz_point.centroid.X
                                )
                                grid_dist += abs(
                                    tap_point.centroid.Y - taz_point.centroid.Y
                                )
                                grid_meters = grid_dist * mpu
                                grid_minutes = (grid_meters * 60) / (
                                    prep_conf.IDEAL_WALK_MPH * 1609.344
                                )
                                if grid_minutes <= float(tap_cutoff):
                                    out_rows.append(
                                        [
                                            f"{taz_id} - {tap_n}",
                                            grid_minutes,
                                            taz_id,
                                            tap_n,
                                        ]
                                    )
                # Update output csv
                out_df = pd.DataFrame(
                    out_rows, columns=["Name", prep_conf.OSM_IMPED, "OName", "DName"]
                )
                out_df.to_csv(skim, mode="a", header=False, index=False)
            except:
                raise
            finally:
                arcpy.Delete_management(taz_layer)
                arcpy.Delete_management(tap_layer)
                arcpy.Delete_management(net_layer)

            # Mark as solved
            solved.append(model_year)


def process_serpm_transit():
    """
    Combines estimates of TAP to TAP and TAZ to TAP travel times (in minutes) to create
    an OD table of TAZ to TAZ travel time estimates.

    Inputs:
        - RAW//SERPM//SERPM_TAZ_Centroids.shp
        - RAW//SERPM//TAP_to_TAP_{skim version}_{model_year}.csv
        - CLEANED//SERPM//TAZ_to_TAP_{net suffix for analysis year}.csv
    Outputs:
        - CLEANED//SERPM//TAP_to_TAP_{skim version}_{model_year}_clean.csv
        - CLEANED//SERPM//TAZ_to_TAZ_{skim version}_{model_year}.csv
    """
    # Make a graph from TAP to TAP skim
    serpm_raw = make_path(RAW, "SERPM")
    serpm_clean = make_path(CLEANED, "SERPM")
    skim_versions = ["local", "prem"]
    cutoff = 60  # TODO: move to prep_conf?
    solved = []
    for year in YEARS:
        net_suffix, model_year = prep_conf.NET_BY_YEAR[year]
        taz_to_tap = make_path(serpm_clean, f"TAZ_to_TAP{net_suffix}.csv")
        tazs = make_path(serpm_raw, "SERPM_TAZ_Centroids.shp")
        # Get TAZ nodes
        mdc_wc = arcpy.AddFieldDelimiters(tazs, "IN_MDC") + "=1"
        with arcpy.da.SearchCursor(
            tazs, prep_conf.TAZ_COMMON_KEY, where_clause=mdc_wc
        ) as c:
            taz_nodes = sorted({r[0] for r in c})
        with arcpy.da.SearchCursor(tazs, prep_conf.TAZ_COMMON_KEY) as c:
            all_tazs = sorted({r[0] for r in c})
        # Analyze zone to zone times
        if model_year not in solved:
            print(f"Estimating TAZ to TAZ times for model year {model_year}")
            for skim_version in skim_versions:
                print(f"- transit submode: {skim_version}")
                tap_to_tap = make_path(
                    serpm_raw, f"TAP_to_TAP_{skim_version}_{model_year}.csv"
                )
                # Clean the tap to tap skim
                print(f" - - cleaning TAP to TAP skim")
                tap_to_tap_clean = make_path(
                    serpm_clean, f"TAP_to_TAP_{skim_version}_{model_year}_clean.csv"
                )
                tap_renames = {
                    "orig": "OName",
                    "dest": "DName",
                    "flow": "Minutes",
                }  # TODO move to prep_conf?
                p_help.clean_skim_csv(
                    in_file=tap_to_tap,
                    out_file=tap_to_tap_clean,
                    imp_field="Minutes",
                    drop_val=0,
                    node_fields=["OName", "DName"],
                    node_offset=5000,
                    renames=tap_renames,
                )
                # TODO: tack taz_to_tap onto the end of tap_to_tap to eliminate the need to run Compose?
                # # Combine skims to build zone-to-zone times
                # taz_to_taz = makePath(serpm_clean, f"TAZ_to_TAZ_{skim_version}_{model_year}.csv")
                # P_HELP.transit_skim_joins(taz_to_tap, tap_to_tap_clean, out_skim=taz_to_taz,
                #                           o_col="OName", d_col="DName", imp_col="Minutes",
                #                           origin_zones=taz_nodes, total_cutoff=cutoff)
                # Make tap to tap network
                print(" - - building TAZ to TAZ graph")
                taz_to_taz = PMT.make_path(serpm_clean, f"TAZ_to_TAZ_{skim_version}_{model_year}.csv")
                p_help.full_skim(
                    # clean_serpm_dir=make_path(CLEANED, "SERPM"),
                    tap_to_tap=tap_to_tap_clean,
                    taz_to_tap=taz_to_tap,
                    taz_to_taz = taz_to_taz,
                    cutoff=cutoff,
                    # model_year=model_year,
                    # skim_version=skim_version,
                    taz_nodes=taz_nodes,
                    all_tazs=all_tazs,
                    impedance_attr="Minutes"
                )
                solved.append(model_year)


def run(args):
    if args.overwrite:
        overwrite = True
    if args.setup:
        setup_download_folder(dl_folder=RAW)
    if args.urls:
        download_urls(overwrite=overwrite)
    if args.osm:
        download_osm_data(overwrite=overwrite)
    if args.census_geo:
        download_census_geo(overwrite=overwrite)
    if args.commutes:
        download_commute_data(overwrite=overwrite)
    if args.race:
        download_race_data(overwrite=overwrite)
    if args.lodes:
        download_lodes_data(overwrite=overwrite)


def main():
    # todo: add more utility to this, making the download script executable
    import argparse
    parser = argparse.ArgumentParser(prog="downloader",
                                     description="Download RAW data...")
    parser.add_argument("-x", "--overwrite",    dest="overwrite",   action="store_false")
    parser.add_argument("-s", "--setup",        dest="setup",       action="store_false")
    parser.add_argument("-u", "--urls",         dest="urls",        action="store_true")
    parser.add_argument("-o", "--osm",          dest="osm",         action="store_false")
    parser.add_argument("-g", "--census_geo",   dest="census_geo",  action="store_false")
    parser.add_argument("-c", "--commutes",     dest="commutes",    action="store_false")
    parser.add_argument("-r", "--race",         dest="race",        action="store_false")
    parser.add_argument("-l", "--lodes",        dest="lodes",       action="store_false")
    args = parser.parse_args()
    run(args)


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
        RAW = validate_directory(directory=make_path(ROOT, "RAW"))
        CLEANED = validate_directory(directory=make_path(ROOT, "CLEANED"))
        NETS_DIR = make_path(CLEANED, "osm_networks")
        DATA = ROOT
        BASIC_FEATURES = make_path(CLEANED, "PMT_BasicFeatures.gdb")
        YEAR_GDB_FORMAT = make_path(CLEANED, "PMT_YEAR.gdb")
        REF = make_path(ROOT, "Reference")
        RIF_CAT_CODE_TBL = make_path(REF, "road_impact_fee_cat_codes.csv")
        DOR_LU_CODE_TBL = make_path(REF, "Land_Use_Recode.csv")
        YEARS = PMT.YEARS

    ###################################################################
    # ------------------- SETUP CLEAN DATA ----------------------------
    ###################################################################
    # UDB might be ignored as this isnt likely to change and can be updated ad-hoc
    # process_udb()  # TODO: add print statements
    #
    process_basic_features()
    #
    # # MERGES PARK DATA INTO A SINGLE POINT FEATURESET AND POLYGON FEARTURESET
    # process_parks()
    #
    # # CLEANS AND GEOCODES TRANSIT INTO INCLUDED LAT/LON
    # process_transit()
    #
    # # SETUP ANY BASIC NORMALIZED GEOMETRIES
    # process_normalized_geometries()

    # COPIES DOWNLOADED PARCEL DATA AND ONLY MINIMALLY NECESSARY ATTRIBUTES INTO YEARLY GDB
    # process_parcels()
    #
    # # CLEANS AND GEOCODES PERMITS TO ASSOCIATED PARCELS AND
    # #   GENERATES A NEAR TERM PARCELS LAYER WITH PERMIT INFO
    # process_permits()  # TESTED CR 03/01/21

    ###################################################################
    # ---------------------- ENRICH DATA------------------------------
    ###################################################################
    # ADD VARIOUS BLOCK GROUP LEVEL DEMOGRAPHIC, EMPLOYMENT AND COMMUTE DATA AS TABLE
    # enrich_block_groups()  # TESTED CR 03/12/21 added src attributes for enrichement data

    # MODELS MISSING DATA WHERE APPROPRIATE AND DISAGGREGATES BLOCK LEVEL DATA DOWN TO PARCEL LEVEL
    # process_bg_apply_activity_models()  # TESTED CR 03/02/21
    # process_allocate_bg_to_parcels()

    # ADDS LAND USE TABLE FOR PARCELS INCLUDING VACANT, RES AND NRES AREA
    # process_parcel_land_use()  # Tested by CR 3/11/21 verify NearTerm year works

    # prepare maz and taz socioeconomic/demographic data
    # process_model_se_data()  # TESTED 3/16/21   # TODO: standardize the SERPM pathing and clean up any clutter

    ###################################################################
    # ------------------ NETWORK ANALYSES -----------------------------
    ###################################################################
    # TODO: verify for NearTerm only copies are made processing time is exorbitant and unnecessary to rerun
    # BUILD OSM NETWORKS FROM TEMPLATES
    # process_osm_networks()  # TESTED by CR 03/18/21 added nearterm

    # ASSESS NETWORK CENTRALITY FOR EACH BIKE NETWORK
    # process_centrality()  # TESTED by CR 03/18/21 added nearterm

    # ANALYZE OSM NETWORK SERVICE AREAS
    # process_osm_service_areas()  # TESTED by CR 03/18/21 added nearterm

    # ANALYZE WALK/BIKE TIMES AMONG MAZS
    # process_osm_skims()  # TESTED by CR 03/18/21 added nearterm

    # RECORD PARCEL WALK TIMES
    # process_walk_times()  # TESTED by CR 03/19/21 added nearterm

    # RECORD PARCEL IDEAL WALK TIMES
    # process_ideal_walk_times()  # Tested by AB 3/2/21 NEAR TERM just use parcel geometry so

    # PREPARE SERPM TRANSIT SKIMS
    # - Walk access to transit
    # process_walk_to_transit_skim() # TESTED by AB 4/10/21
    # process_serpm_transit() # TESTED by AB 4/11/21

    # prepare serpm taz-level travel skims
    # process_model_skims()  # TESTED by AB 4/11/21 with transit skim

    ###################################################################
    # -----------------DEPENDENT ANALYSIS------------------------------
    ###################################################################
    # # ANALYZE ACCESS BY MAZ, TAZ
    # process_access()  # TESTED by AB 4/11/21 with transit skim
    #
    # # PREPARE TAZ TRIP LENGTH AND VMT RATES
    # process_travel_stats()  # Tested by AB 4/1/21
    #
    # # ONLY UPDATED WHEN NEW IMPERVIOUS DATA ARE MADE AVAILABLE
    # process_imperviousness()  # TESTED by CR 3/21/21 Added NearTerm
    #
    # # make a wide table of bike facilities
    # process_bike_facilities()
    # process_bike_miles()
    # process_lu_diversity()  # TESTED by CR 3/21/21 Added NearTerm

    # generate contiguity index for all years
    # process_contiguity()


""" deprecated """
# cleans and geocodes crashes to included Lat/Lon
# process_crashes()
# def process_crashes():
#     """ crashes """
#     crash_json = makePath(RAW, "Safety_Security", "bike_ped.geojson")
#     all_features = P_HELP.geojson_to_feature_class_arc(
#         geojson_path=crash_json, geom_type="POINT"
#     )
#     arcpy.FeatureClassToFeatureClass_conversion(all_features, RAW, "DELETE_crashes.shp")
#     # reformat attributes and keep only useful
#     P_HELP.clean_and_drop(
#         feature_class=all_features,
#         use_cols=prep_conf.USE_CRASH,
#         rename_dict=prep_conf.CRASH_FIELDS_DICT,
#     )
#     for year in YEARS:
#         # use year variable to setup outputs
#         out_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_{year}.gdb"))
#         FDS = validate_feature_dataset(makePath(out_gdb, "Points"), sr=SR_FL_SPF)
#         out_name = "BikePedCrashes"
#         year_wc = f'"YEAR" = {year}'
#         # clean and format crash data
#         P_HELP.prep_bike_ped_crashes(
#             in_fc=all_features, out_path=FDS, out_name=out_name, where_clause=year_wc
#         )
#     arcpy.Delete_management(in_data=all_features)


# updates parcels based on permits for near term analysis
# process_short_term_parcels()
#   - record parcel land use groupings and multipliers/overwrites
#   - replace relevant attributes for parcels with current permits

# def process_short_term_parcels():
#     """
#     try:
#         1) set up parcels --> returns parcel_df, and copy of parcels path (shape, FOLIO, UID)
#         2) process permit --> returns permits_df with appropriate fields
#         3) update permit data --> returns df
#         4) update parcels data --> returns nothing, parcel copy is filled with new data extendTableDf
#         5) delete features of original parcel layer
#         6) append parcel copy
#         7) delete parcel copy
#     except:
#         raise error and delete parcel copy
#     """
#     parcels = makePath(YEAR_GDB_FORMAT.replace("YEAR", "NearTerm"), "Polygons", "Parcels")
#     permits = makePath(YEAR_GDB_FORMAT.replace("YEAR", "NearTerm"), "Points", "BuildingPermits")
#     # save_gdb = validate_geodatabase(makePath(ROOT, CLEANED, "near_term_parcels.gdb"))
#     # permits_ref_df = create_permits_units_reference(parcels=parcels, permits=permits,
#     #                                                 lu_key=prep_conf.LAND_USE_COMMON_KEY,
#     #                                                 parcels_living_area_key=prep_conf.PARCEL_BLD_AREA_COL,
#     #                                                 permit_value_key=prep_conf.PERMITS_UNITS_FIELD,
#     #                                                 permits_units_name=prep_conf.PERMITS_BLD_AREA_NAME,
#     #                                                 units_match_dict=prep_conf.PARCEL_REF_TABLE_UNITS_MATCH)
#     build_short_term_parcels(parcel_fc=parcels, permit_fc=permits,
#                              permits_ref_df=permits_ref_df, parcels_id_field=prep_conf.PARCEL_COMMON_KEY,
#                              parcels_lu_field=prep_conf.LAND_USE_COMMON_KEY,
#                              parcels_living_area_field=prep_conf.PARCEL_BLD_AREA_COL,
#                              parcels_land_value_field=prep_conf.PARCEL_LAND_VALUE,
#                              parcels_total_value_field=prep_conf.PARCEL_JUST_VALUE,
#                              parcels_buildings_field=prep_conf.PARCEL_BUILDINGS,
#                              permits_id_field=prep_conf.PERMITS_ID_FIELD,
#                              permits_lu_field=prep_conf.PERMITS_LU_FIELD,
#                              permits_units_field=prep_conf.PERMITS_UNITS_FIELD,
#                              permits_values_field=prep_conf.PERMITS_VALUES_FIELD,
#                              permits_cost_field=prep_conf.PERMITS_COST_FIELD,
#                              save_gdb_location=save_gdb,
#                              units_field_match_dict=prep_conf.SHORT_TERM_PARCELS_UNITS_MATCH)

# def process_bg_estimate_activity_models():
#     bg_enrich = makePath(YEAR_GDB_FORMAT, "Enrichment_census_blockgroups")
#     save_path = P_HELP.analyze_blockgroup_model(
#         bg_enrich_tbl_name="Enrichment_census_blockgroups",
#         bg_key="GEOID",
#         fields="*",
#         acs_years=prep_conf.ACS_YEARS,
#         lodes_years=prep_conf.LODES_YEARS,
#         save_directory=REF,
#     )
#     return save_path
