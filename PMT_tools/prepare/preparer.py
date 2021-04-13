"""
preparation scripts used set up cleaned geodatabases
"""
import datetime
import os
import sys
import warnings
import csv
import networkx as nx
from functools import reduce

warnings.filterwarnings("ignore")

sys.path.insert(0, os.getcwd())
# config global variables
from PMT_tools.config import prepare_config as prep_conf

# prep/clean helper functions
import PMT_tools.prepare.prepare_helpers as P_HELP
import PMT_tools.prepare.prepare_osm_networks as OSM_HELP

# PMT functions
from PMT_tools import PMT
from PMT_tools.PMT import (makePath, make_inmem_path, checkOverwriteOutput,
                           dfToTable, polygonsToPoints, extendTableDf, table_to_df, intersectFeatures)
from PMT_tools.PMT import validate_directory, validate_geodatabase, validate_feature_dataset
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
from PMT_tools.PMT import arcpy, np, pd, tempfile

import PMT_tools.logger as log

logger = log.Logger(
    add_logs_to_arc_messages=True
)  # TODO: only initialize logger if running as main?

arcpy.env.overwriteOutput = True

NETS_DIR = makePath(CLEANED, "osm_networks")
DEBUG = True
if DEBUG:
    '''
    if DEBUG is True, you can change the path of the root directory and test any
    changes to the code you might need to handle without munging the existing data
    '''
    ROOT = r"C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT_link\Data"
    RAW = validate_directory(directory=makePath(ROOT, 'PROCESSING_TEST_local', "RAW"))
    CLEANED = validate_directory(directory=makePath(ROOT, 'PROCESSING_TEST_local', "CLEANED"))
    NETS_DIR = makePath(CLEANED, "osm_networks")
    DATA = ROOT
    BASIC_FEATURES = makePath(CLEANED, "PMT_BasicFeatures.gdb")
    YEAR_GDB_FORMAT = makePath(CLEANED, "PMT_YEAR.gdb")
    REF = makePath(ROOT, "Reference")
    RIF_CAT_CODE_TBL = makePath(REF, "road_impact_fee_cat_codes.csv")
    DOR_LU_CODE_TBL = makePath(REF, "Land_Use_Recode.csv")
    YEARS = YEARS + ["NearTerm"]


def process_normalized_geometries(overwrite=True):
    """ Census Data and TAZ/MAZ
        - these are geometries that only consist of a dataset key
        - copies census block groups and blocks into yearly GDB, subsets to county and tags with year
        - copied TAZ and MAZ into yearly GDB, subsets to county and tags with year
    """
    # AOI
    county_bounds = makePath(BASIC_FEATURES, "BasicFeatures", "MiamiDadeCountyBoundary")
    # RAW
    raw_block = makePath(
        RAW, "CENSUS", "tl_2019_12_tabblock10", "tl_2019_12_tabblock10.shp"
    )
    raw_block_groups = makePath(RAW, "CENSUS", "tl_2019_12_bg", "tl_2019_12_bg.shp")
    raw_TAZ = makePath(RAW, "TAZ.shp")
    raw_MAZ = makePath(RAW, "MAZ_TAZ.shp")
    raw_sum_areas = makePath(BASIC_FEATURES, "SummaryAreas")

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
                makePath(CLEANED, f"PMT_{year}.gdb", "Polygons"), sr=SR_FL_SPF
            )
            logger.log_msg(f"- {cleaned} normalized here: {out_path}")
            out_data = makePath(out_path, cleaned)
            P_HELP.prep_feature_class(
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
                checkOverwriteOutput(output=out_data, overwrite=overwrite)
            logger.log_msg(f"--- writing out geometries and {cols} only")
            arcpy.CopyFeatures_management(in_features=lyr, out_feature_class=out_data)
            if year == "NearTerm":
                calc_year = 9998
            elif year == "LongTerm":
                # unused currently but if we get to this we want long term data year represented as 9999
                calc_year = 9999
            else:
                calc_year = year
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


def process_basic_features():
    # TODO: add check for existing basic features, and compare for changes
    print("Making basic features")
    station_presets = makePath(BASIC_FEATURES, "StationArea_presets")
    corridor_presets = makePath(BASIC_FEATURES, "Corridor_presets")
    P_HELP.makeBasicFeatures(bf_gdb=BASIC_FEATURES, stations_fc=prep_conf.BASIC_STATIONS, stn_id_field="Id", # TODO global
                      stn_diss_fields=prep_conf.STN_DISS_FIELDS, stn_corridor_fields=prep_conf.STN_CORRIDOR_FIELDS,
                      alignments_fc=prep_conf.BASIC_ALIGNMENTS, align_diss_fields=prep_conf.ALIGN_DISS_FIELDS,
                      align_corridor_name="Corridor", # TODO: global
                      stn_buff_dist=prep_conf.STN_BUFF_DIST, align_buff_dist=prep_conf.ALIGN_BUFF_DIST,
                      stn_areas_fc=prep_conf.BASIC_STN_AREAS, corridors_fc=prep_conf.BASIC_CORRIDORS,
                      long_stn_fc=prep_conf.BASIC_LONG_STN,
                      preset_station_areas=station_presets, preset_station_id="Id", 
                      preset_corridors=corridor_presets, preset_corridor_name="Corridor",
                      rename_dict=prep_conf.BASIC_RENAME_DICT, overwrite=True)
    print("Making summarization features")
    P_HELP.makeSummaryFeatures(bf_gdb=BASIC_FEATURES, long_stn_fc=prep_conf.BASIC_LONG_STN,
                        stn_areas_fc=prep_conf.BASIC_STN_AREAS, stn_id_field="Id", # TODO: global
                        corridors_fc=prep_conf.BASIC_CORRIDORS,
                        cor_name_field=prep_conf.CORRIDOR_NAME_FIELD, out_fc=prep_conf.BASIC_SUM_AREAS,
                        stn_buffer_meters=prep_conf.STN_BUFF_METERS, stn_name_field=prep_conf.STN_NAME_FIELD,
                        stn_cor_field=prep_conf.STN_LONG_CORRIDOR, overwrite=True)



def process_parks(overwrite=True):
    """
    parks - merges park polygons into one and formats both poly and point park data.
    """
    print("PARKS:")
    park_polys = [
        makePath(RAW, "Municipal_Parks.geojson"),
        makePath(RAW, "Federal_State_Parks.geojson"),
        makePath(RAW, "County_Parks.geojson"),
    ]
    park_points = makePath(RAW, "Park_Facilities.geojson")
    poly_use_cols = [["FOLIO", "NAME", "TYPE"], ["NAME"], ["FOLIO", "NAME", "TYPE"]]
    poly_rename_cols = [{}, {}, {}]
    out_park_polys = makePath(CLEANED, "Park_Polys.shp")
    out_park_points = makePath(CLEANED, "Park_Points.shp")
    if overwrite:
        for output in [out_park_points, out_park_polys]:
            checkOverwriteOutput(output=output, overwrite=overwrite)
    print("--- cleaning park points and polys")
    P_HELP.prep_park_polys(
        in_fcs=park_polys,
        geom="POLYGON",
        out_fc=out_park_polys,
        use_cols=poly_use_cols,
        rename_dicts=poly_rename_cols,
        unique_id=prep_conf.PARK_POLY_COMMON_KEY,
    )
    P_HELP.prep_feature_class(
        in_fc=park_points,
        geom="POINT",
        out_fc=out_park_points,
        use_cols=prep_conf.PARK_POINT_COLS,
        unique_id=prep_conf.PARK_POINTS_COMMON_KEY,
    )
    for year in YEARS:
        print(f"\t--- adding park points to {year} gdb")
        out_path = validate_feature_dataset(
            fds_path=makePath(
                CLEANED, YEAR_GDB_FORMAT.replace("YEAR", str(year)), "Points"
            ),
            sr=SR_FL_SPF,
        )
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=out_park_points, out_path=out_path, out_name="Park_Points"
        )


def process_udb():
    """ UDB """
    udb_fc = makePath(RAW, "MD_Urban_Growth_Boundary.geojson")
    county_fc = makePath(RAW, "CensusGeo", "Miami-Dade_Boundary.geojson")
    out_fc = makePath(CLEANED, "UrbanDevelopmentBoundary.shp")

    temp_fc = P_HELP.geojson_to_feature_class_arc(
        geojson_path=udb_fc, geom_type="POLYLINE"
    )
    P_HELP.udbLineToPolygon(udb_fc=temp_fc, county_fc=county_fc, out_fc=out_fc)


def process_transit(overwrite=True):
    """ Transit Ridership
        - converts a list of ridership files to points with attributes cleaned
        YEAR over YEARS
            - cleans and consolidates transit data into Year POINTS FDS
            - if YEAR == NearTerm:
            -     most recent year is copied over
        - NOTE: transit folder reflects current location, needs update to reflect cleaner structure
    """
    transit_folder = validate_directory(
        makePath(RAW, "TRANSIT", "TransitRidership_byStop")
    )
    transit_shape_fields = [prep_conf.TRANSIT_LONG, prep_conf.TRANSIT_LAT]
    print("PROCESSING TRANSIT RIDERSHIP... ")
    for year in YEARS:
        print(f"--- cleaning ridership for {year}")
        out_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_{year}.gdb"))
        FDS = validate_feature_dataset(
            fds_path=makePath(out_gdb, "Points"), sr=SR_FL_SPF
        )
        out_name = "TransitRidership"
        transit_xls_file = makePath(
            transit_folder, prep_conf.TRANSIT_RIDERSHIP_TABLES[year]
        )
        transit_out_path = makePath(FDS, out_name)
        if overwrite:
            checkOverwriteOutput(output=transit_out_path, overwrite=overwrite)
        P_HELP.prep_transit_ridership(
            in_table=transit_xls_file,
            rename_dict=prep_conf.TRANSIT_FIELDS_DICT,
            unique_id=prep_conf.TRANSIT_COMMON_KEY,
            shape_fields=transit_shape_fields,
            from_sr=prep_conf.IN_CRS,
            to_sr=prep_conf.OUT_CRS,
            out_fc=transit_out_path,
        )
        print(f"--- ridership cleaned for {year} and located in {transit_out_path}")


def process_crashes():
    """ crashes """
    crash_json = makePath(RAW, "Safety_Security", "bike_ped.geojson")
    all_features = P_HELP.geojson_to_feature_class_arc(
        geojson_path=crash_json, geom_type="POINT"
    )
    arcpy.FeatureClassToFeatureClass_conversion(all_features, RAW, "DELETE_crashes.shp")
    # reformat attributes and keep only useful
    P_HELP.clean_and_drop(
        feature_class=all_features,
        use_cols=prep_conf.USE_CRASH,
        rename_dict=prep_conf.CRASH_FIELDS_DICT,
    )
    for year in YEARS:
        # use year variable to setup outputs
        out_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_{year}.gdb"))
        FDS = validate_feature_dataset(makePath(out_gdb, "Points"), sr=SR_FL_SPF)
        out_name = "BikePedCrashes"
        year_wc = f'"YEAR" = {year}'
        # clean and format crash data
        P_HELP.prep_bike_ped_crashes(
            in_fc=all_features, out_path=FDS, out_name=out_name, where_clause=year_wc
        )
    arcpy.Delete_management(in_data=all_features)


def process_permits(overwrite=True):
    print("PROCESSING PERMIT DATA ... ")
    try:
        permit_csv = makePath(
            RAW, "BUILDING_PERMITS", "Road Impact Fee Collection Report -- 2019.csv"
        )
        out_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_NearTerm.gdb"))
        in_fds = validate_feature_dataset(
            fds_path=makePath(out_gdb, "Polygons"), sr=SR_FL_SPF
        )
        parcels = makePath(in_fds, "Parcels")
        out_fds = validate_feature_dataset(makePath(out_gdb, "Points"), sr=SR_FL_SPF)
        permits_out = makePath(out_fds, "BuildingPermits")
        if overwrite:
            checkOverwriteOutput(output=permits_out, overwrite=overwrite)
        P_HELP.clean_permit_data(
            permit_csv=permit_csv,
            parcel_fc=parcels,
            permit_key=prep_conf.PERMITS_COMMON_KEY,
            poly_key=prep_conf.PARCEL_COMMON_KEY,
            rif_lu_tbl=RIF_CAT_CODE_TBL,
            dor_lu_tbl=DOR_LU_CODE_TBL,
            out_file=permits_out,
            out_crs=EPSG_FLSPF,
        )
        unit_ref_df = P_HELP.create_permits_units_reference(
            parcels=parcels,
            permits=permits_out,
            lu_key=prep_conf.LAND_USE_COMMON_KEY,
            parcels_living_area_key=prep_conf.PARCEL_BLD_AREA_COL,
            permit_value_key=prep_conf.PERMITS_UNITS_FIELD,
            permits_units_name=prep_conf.PERMITS_BLD_AREA_NAME,
            units_match_dict=prep_conf.PARCEL_REF_TABLE_UNITS_MATCH,
        )

        temp_update = P_HELP.build_short_term_parcels(
            parcel_fc=parcels,
            permit_fc=permits_out,
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
        if overwrite:
            checkOverwriteOutput(output=parcels, overwrite=overwrite)
        print("--- --- writing out updated parcels with new permit data")
        arcpy.FeatureClassToFeatureClass_conversion(
            in_features=temp_update, out_path=in_fds, out_name="Parcels"
        )
    except:
        raise


def process_parcels(overwrite=True):
    print("PROCESSING PARCELS... ")
    parcel_folder = makePath(RAW, "Parcels")
    for year in YEARS:
        print(f"- {year} in process...")
        out_gdb = validate_geodatabase(makePath(CLEANED, f"PMT_{year}.gdb"))
        out_fds = validate_feature_dataset(makePath(out_gdb, "Polygons"), sr=SR_FL_SPF)
        out_fc = makePath(out_fds, "Parcels")

        # source data
        if year == "NearTerm":
            data_year = SNAPSHOT_YEAR
            calc_year = 9998
        else:
            data_year = calc_year = year
        in_fc = makePath(parcel_folder, f"Miami_{data_year}.shp")
        in_csv = makePath(parcel_folder, f"NAL_{data_year}_23Dade_F.csv")

        # input fix variables
        renames = prep_conf.PARCEL_COLS.get(data_year, {})
        usecols = prep_conf.PARCEL_USE_COLS.get(
            data_year, prep_conf.PARCEL_USE_COLS["DEFAULT"]
        )
        csv_kwargs = {"dtype": {"PARCEL_ID": str, "CENSUS_BK": str}, "usecols": usecols}
        if overwrite:
            checkOverwriteOutput(output=out_fc, overwrite=overwrite)
        P_HELP.prep_parcels(
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


def enrich_block_groups(overwrite=True):
    # For all years, enrich block groups with parcels
    for year in YEARS:
        if year == "NearTerm":
            print()

        # Define inputs/outputs
        gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        fds = makePath(gdb, "Polygons")
        parcels_fc = makePath(fds, "Parcels")
        bg_fc = makePath(fds, "Census_BlockGroups")
        out_tbl = makePath(gdb, "Enrichment_census_blockgroups")
        # define table vars
        race_tbl = makePath(RAW, "CENSUS", f"ACS_{year}_race.csv")
        commute_tbl = makePath(RAW, "CENSUS", f"ACS_{year}_commute.csv")
        lodes_tbl = makePath(RAW, "LODES", f"fl_wac_S000_JT00_{year}_bgrp.csv.gz")

        # Enrich BGs with parcel data
        bg_df = P_HELP.enrich_bg_with_parcels(
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
        dfToTable(df=bg_df, out_table=out_tbl, overwrite=overwrite)
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
            data_src = '"EXTRAP"'
            if not arcpy.Exists(table):
                print(
                    f"--- not able to enrich parcels with {table_name} doesnt exist, needs extrapolation"
                )
                if "LODES" in table:
                    fld_name = "JOBS_SRC"
                if "CENSUS" in table:
                    fld_name = "DEM_SRC"
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
                if "LODES" in table:
                    data_src = '"LODES"'
                    fld_name = "JOBS_SRC"
                if "CENSUS" in table:
                    data_src = '"ACS"'
                    fld_name = "DEM_SRC"
                arcpy.AddField_management(
                    in_table=out_tbl,
                    field_name=fld_name,
                    field_type="TEXT",
                    field_length=10,
                )
                arcpy.CalculateField_management(
                    in_table=out_tbl, field=fld_name, expression=data_src
                )
                print(f"--- enriching parcels with {table_name} data")
                P_HELP.enrich_bg_with_econ_demog(
                    tbl_path=out_tbl,
                    tbl_id_field=prep_conf.BG_COMMON_KEY,
                    join_tbl=table,
                    join_id_field=tbl_id,
                    join_fields=fields,
                )


def process_parcel_land_use(overwrite=True):
    for year in YEARS:
        print(year)
        year_gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        # Parcels
        parcels_fc = makePath(year_gdb, "Polygons", "Parcels")
        # Reference table
        lu_table = makePath(REF, "Land_Use_Recode.csv")  # TODO: put in config?
        # Output
        out_table = makePath(year_gdb, "LandUseCodes_parcels")

        # Create combo df
        par_lu_field = "DOR_UC"
        par_fields = [prep_conf.PARCEL_COMMON_KEY, "LND_SQFOOT"]
        tbl_lu_field = "DOR_UC"
        dtype = {"DOR_UC": int}
        default_vals = {prep_conf.PARCEL_COMMON_KEY: "-1", prep_conf.PARCEL_LU_COL: 999}
        par_df = P_HELP.prep_parcel_land_use_tbl(
            parcels_fc=parcels_fc,
            parcel_lu_field=prep_conf.PARCEL_LU_COL,
            parcel_fields=par_fields,
            lu_tbl=lu_table,
            tbl_lu_field=prep_conf.PARCEL_LU_COL,
            dtype_map=dtype,
            null_value=default_vals,
        )
        # Export result
        checkOverwriteOutput(output=out_table, overwrite=overwrite)
        dfToTable(df=par_df, out_table=out_table)


def process_imperviousness(overwrite=True):
    impervious_download = makePath(RAW, "Imperviousness.zip")
    county_boundary = makePath(
        CLEANED, "PMT_BasicFeatures.gdb", "BasicFeatures", "MiamiDadeCountyBoundary"
    )
    out_dir = validate_directory(makePath(RAW, "IMPERVIOUS"))
    # TODO: handle existing copy of the raster (dont prepare on each run if data hasnt changed)
    impv_raster = P_HELP.prep_imperviousness(
        zip_path=impervious_download,
        clip_path=county_boundary,
        out_dir=out_dir,
        transform_crs=EPSG_FLSPF,
    )
    logger.log_msg("--- --- converting raster to point")
    points = make_inmem_path(file_name="raster_to_point")
    arcpy.RasterToPoint_conversion(in_raster=impv_raster, out_point_features=points)
    logger.log_msg("--- grabbing impervious raster cell size")
    cellx = arcpy.GetRasterProperties_management(
        in_raster=impv_raster, property_type="CELLSIZEX"
    )
    celly = arcpy.GetRasterProperties_management(
        in_raster=impv_raster, property_type="CELLSIZEY"
    )
    cell_area = float(cellx.getOutput(0)) * float(celly.getOutput(0))
    for year in YEARS:
        print(f"\n{str(year)}:")
        year_gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        block_fc = makePath(year_gdb, "Polygons", "Census_Blocks")
        parcel_fc = makePath(year_gdb, "Polygons", "Parcels")
        # capture total living area from parcels for use in later consolidations
        lvg_ar_df = agg_to_zone(
            parcel_fc=parcel_fc,
            agg_field=prep_conf.PARCEL_BLD_AREA_COL,
            zone_fc=block_fc,
            zone_id=prep_conf.BLOCK_COMMON_KEY,
        )
        impv_df = P_HELP.analyze_imperviousness(
            raster_points=points,
            rast_cell_area=cell_area,
            zone_fc=block_fc,
            zone_id_field=prep_conf.BLOCK_COMMON_KEY,
        )

        zone_name = os.path.split(block_fc)[1].lower()
        imp_table = makePath(year_gdb, f"Imperviousness_{zone_name}")
        dfToTable(df=impv_df, out_table=imp_table, overwrite=overwrite)
        extendTableDf(
            in_table=imp_table,
            table_match_field=prep_conf.BLOCK_COMMON_KEY,
            df=lvg_ar_df,
            df_match_field=prep_conf.BLOCK_COMMON_KEY,
        )


def agg_to_zone(parcel_fc, agg_field, zone_fc, zone_id):
    parcel_pts = polygonsToPoints(
        in_fc=parcel_fc, out_fc=make_inmem_path(), fields=[agg_field], null_value=0.0
    )
    block_par = arcpy.SpatialJoin_analysis(
        target_features=zone_fc,
        join_features=parcel_pts,
        join_operation="JOIN_ONE_TO_MANY",
        out_feature_class=make_inmem_path(),
    )
    block_par = arcpy.Dissolve_management(
        in_features=block_par,
        out_feature_class=make_inmem_path(),
        dissolve_field=zone_id,
        statistics_fields=f"{agg_field} SUM",
    )
    arcpy.AlterField_management(
        in_table=block_par,
        field=f"SUM_{agg_field}",
        new_field_name=agg_field,
        new_field_alias=agg_field,
    )
    df = featureclass_to_df(
        in_fc=block_par, keep_fields=[zone_id, agg_field], null_val=0.0
    )
    return df


def process_osm_networks():
    """
    - copy bike edges and walk edges to osm version database (formatting if field mapping is passed)
    - Year over Year, copy the bike_edges to the Networks FDS
    - generate NetworkDataset for both walk and bike networks
    Returns:
        intermediate GDB for bike and walk networks containing the ND
        bike and walk edges in the year gdb Network FDS
    """
    net_versions = sorted({v[0] for v in prep_conf.NET_BY_YEAR.values()})
    for net_version in net_versions:
        # Import edges
        osm_raw = makePath(RAW, "OpenStreetMap")
        for net_type in ["bike", "walk"]:
            net_type_version = f"{net_type}{net_version}"
            # validate nets_dir
            if validate_directory(NETS_DIR):
                # Make output geodatabase
                clean_gdb = validate_geodatabase(
                    makePath(NETS_DIR, f"{net_type_version}.gdb"), overwrite=True
                )
                # make output feature dataset
                net_type_fd = validate_feature_dataset(
                    makePath(clean_gdb, "osm"), sr=SR_FL_SPF, overwrite=True
                )

            # import edges
            net_raw = makePath(osm_raw, net_type_version, "edges.shp")
            # transfer to gdb
            edges = OSM_HELP.importOSMShape(
                osm_fc=net_raw, to_feature_dataset=net_type_fd, overwrite=True
            )

            if net_type == "bike":
                # Enrich features
                OSM_HELP.classifyBikability(edges)
                # Copy bike edges to year geodatabases
                for year, nv in prep_conf.NET_BY_YEAR.items():
                    nv, model_yr = nv
                    if nv == net_version:
                        print(f"{year}:")
                        if year == "NearTerm":
                            base_year = year
                            data_year = 9998
                        elif year == "LongTerm":
                            base_year = year
                            data_year = 9999
                        else:
                            base_year = data_year = year
                        out_path = validate_feature_dataset(
                            makePath(CLEANED, f"PMT_{base_year}.gdb", "Networks"),
                            sr=SR_FL_SPF,
                        )
                        out_name = "edges_bike"
                        arcpy.FeatureClassToFeatureClass_conversion(
                            in_features=edges, out_path=out_path, out_name=out_name
                        )
                        out_fc = makePath(out_path, out_name)
                        arcpy.CalculateField_management(
                            in_table=out_fc,
                            field="Year",
                            expression=str(data_year),
                            field_type="LONG",
                        )

            # Build network datasets
            template = makePath(REF, f"osm_{net_type}_template.xml")
            P_HELP.makeNetworkDataset(
                template_xml=template,
                out_feature_dataset=net_type_fd,
                net_name="osm_ND",
            )


def process_bg_estimate_activity_models():
    bg_enrich = makePath(YEAR_GDB_FORMAT, "Enrichment_census_blockgroups")
    save_path = P_HELP.analyze_blockgroup_model(
        bg_enrich_path=bg_enrich,
        bg_key="GEOID",
        fields="*",
        acs_years=prep_conf.ACS_YEARS,
        lodes_years=prep_conf.LODES_YEARS,
        save_directory=REF,
    )
    return save_path


def process_bg_apply_activity_models(overwrite=True):
    print("Modeling Block Group data...")
    for year in YEARS:
        print(f"{year}: ")
        # Set the inputs based on the year
        gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        bg_enrich = makePath(gdb, "Enrichment_census_blockgroups")
        bg_geometry = makePath(gdb, "Polygons", "Census_BlockGroups")
        model_coefficients = makePath(REF, "block_group_model_coefficients.csv")
        save_gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))

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
        if overwrite:
            checkOverwriteOutput(
                output=makePath(save_gdb, "Modeled_blockgroups"), overwrite=overwrite
            )
        P_HELP.analyze_blockgroup_apply(
            year=shr_year,
            bg_enrich_path=bg_enrich,
            bg_geometry_path=bg_geometry,
            bg_id_field=prep_conf.BG_COMMON_KEY,
            model_coefficients_path=model_coefficients,
            save_gdb_location=save_gdb,
            shares_from=shares,
        )


def process_allocate_bg_to_parcels(overwrite=True):
    for year in YEARS:
        if year == 2019:
            print("holdup")
        # Set the inputs based on the year
        print(f"{year} allocation begun")
        out_gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        parcel_fc = makePath(out_gdb, "Polygons", "Parcels")
        bg_modeled = makePath(out_gdb, "Modeled_blockgroups")
        bg_geom = makePath(out_gdb, "Polygons", "Census_BlockGroups")

        # Allocate
        if overwrite:
            checkOverwriteOutput(
                output=makePath(out_gdb, "EconDemog_parcels"), overwrite=overwrite
            )
        P_HELP.analyze_blockgroup_allocate(
            out_gdb=out_gdb,
            bg_modeled=bg_modeled,
            bg_geom=bg_geom,
            bg_id_field=prep_conf.BG_COMMON_KEY,
            parcel_fc=parcel_fc,
            parcels_id=prep_conf.PARCEL_COMMON_KEY,
            parcel_lu=prep_conf.LAND_USE_COMMON_KEY,
            parcel_liv_area="TOT_LVG_AREA",
        )


def process_osm_skims():
    if arcpy.CheckExtension("network") == "Available":
        arcpy.CheckOutExtension("network")
    else:
        raise arcpy.ExecuteError("Network Analyst Extension license is not available.")
    # Create and solve OD Matrix at MAZ scale
    solved = []
    for year in YEARS:
        # Get MAZ features, create temp centroids for network loading
        maz_path = makePath(CLEANED, f"PMT_{year}.gdb", "Polygons", "MAZ")
        maz_fc = make_inmem_path()
        maz_pts = polygonsToPoints(maz_path, maz_fc, prep_conf.MAZ_COMMON_KEY)
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
                nd = makePath(NETS_DIR, f"{mode}{net_suffix}.gdb", "osm", "osm_ND")
                skim = makePath(NETS_DIR, f"{mode}_Skim{net_suffix}.csv")
                if mode == "bike":
                    restrictions = prep_conf.BIKE_RESTRICTIONS
                else:
                    restrictions = None
                # - Create and load problem
                # Confirm "Year" column is included in output table
                P_HELP.genODTable(
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


def process_model_se_data(
        overwrite=True,
):  # TODO: incorporate print statements for logging later
    # Summarize parcel data to MAZ
    for year in YEARS:
        # Set output
        out_gdb = validate_geodatabase(makePath(CLEANED, f"PMT_{year}.gdb"))
        out_fds = validate_feature_dataset(makePath(out_gdb, "Polygons"), sr=SR_FL_SPF)
        out_fc = makePath(out_fds, "MAZ")
        maz_se_data = makePath(
            RAW, "SERPM", "V7", "maz_data.csv"
        )  # TODO: standardize SERPM pathing
        # Summarize parcels to MAZ
        print("--- summarizing MAZ activities from parcels")
        par_fc = makePath(out_gdb, "Polygons", "Parcels")
        se_data = makePath(out_gdb, "EconDemog_parcels")
        # TODO: confirm we can use common keys here?
        par_data = P_HELP.estimate_maz_from_parcels(
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
        maz_data = P_HELP.consolidate_cols(
            df=maz_data,
            base_fields=[prep_conf.MAZ_COMMON_KEY, prep_conf.TAZ_COMMON_KEY],
            consolidations=prep_conf.MAZ_SE_CONS,
        )
        # Patch for full regional MAZ data
        print("--- combining parcel-based and non-parcel-based MAZ data")
        maz_data = P_HELP.patch_local_regional_maz(
            maz_par_df=par_data,
            maz_par_key=prep_conf.MAZ_COMMON_KEY,
            maz_df=maz_data,
            maz_key=prep_conf.MAZ_COMMON_KEY,
        )
        # Export MAZ table
        print("--- exporting MAZ socioeconomic/demographic data")
        maz_table = makePath(out_gdb, "EconDemog_MAZ")
        if overwrite:
            checkOverwriteOutput(output=maz_table, overwrite=overwrite)
        dfToTable(maz_data, maz_table)

        # Summarize to TAZ scale
        print("--- summarizing MAZ data to TAZ scale")
        maz_data.drop(columns=[prep_conf.MAZ_COMMON_KEY], inplace=True)
        taz_data = maz_data.groupby(prep_conf.TAZ_COMMON_KEY).sum().reset_index()
        # Export TAZ table
        print("--- exporting TAZ socioeconomic/demographic data")
        taz_table = makePath(out_gdb, "EconDemog_TAZ")
        if overwrite:
            checkOverwriteOutput(output=taz_table, overwrite=overwrite)
        dfToTable(taz_data, taz_table)


def process_model_skims():
    """
    Assumes transit and auto skims have same fields.

    Combine transit skims for local and premium transit into one table.
    Get best available transit time, eliminating false connections
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
        auto_csv = PMT.makePath(RAW, "SERPM", f"AM_HWY_SKIMS_{year}.csv")
        local_csv = PMT.makePath(CLEANED, "SERPM", f"TAZ_to_TAZ_local_{year}.csv")
        prem_csv = PMT.makePath(CLEANED, "SERPM", f"TAZ_to_TAZ_prem_{year}.csv")
        trips_csv = PMT.makePath(RAW, "SERPM", f"DLY_VEH_TRIPS_{year}.csv")
        skim_out = PMT.makePath(CLEANED, "SERPM", f"SERPM_SKIM_TEMP.csv")
        temp_out = PMT.makePath(CLEANED, "SERPM", f"SERPM_OD_TEMP.csv")
        serpm_out = PMT.makePath(CLEANED, "SERPM", f"SERPM_OD_{year}.csv")
        
        # Combine all skims tables
        print(" - Combining all skims")
        in_tables = [auto_csv, local_csv, prem_csv]
        merge_fields = [o_field, d_field]
        suffixes = ["_AU", "_LOC", "_PRM"]
        dtypes = {o_field: int, d_field: int}
        P_HELP.combine_csv_dask(merge_fields, skim_out, *in_tables, suffixes=suffixes, how="outer",
                                col_renames=renames, dtype=prep_conf.SKIM_DTYPES,
                                thousands=",")

        # Combine trips into the skims separately (helps manage field name collisions
        # that would be a bit troublesome if we combine everything at once)
        print(" - Combining trip tables")
        in_tables = [skim_out, trips_csv]
        P_HELP.combine_csv_dask(merge_fields, temp_out, *in_tables, how="outer",
                                col_renames=renames, dtype=prep_conf.SKIM_DTYPES,
                                thousands=",")

        # Update transit timee esimates
        print(" - Getting best transit time")
        competing_cols = ["TIME_LOC", "TIME_PRM"]
        out_col = prep_conf.SKIM_IMP_FIELD + "_TR"
        replace = {0: np.inf}
        P_HELP.update_transit_times(temp_out, serpm_out, competing_cols=competing_cols, out_col=out_col,
                                    replace_vals=replace, chunksize=100000)

        # Delete temporary tables
        arcpy.Delete_management(skim_out)
        arcpy.Delete_management(temp_out)


def process_osm_service_areas():
    # Facilities
    #  - Stations
    stations = makePath(BASIC_FEATURES, "SMARTplanStations")
    station_name = "Name"
    # - Parks
    parks = makePath(CLEANED, "Park_Points.shp")
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
        out_fds_path = makePath(CLEANED, f"PMT_{year}.gdb", "Networks")
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
            source_fds = makePath(CLEANED, f"PMT_{source_year}.gdb", "Networks")
            target_fds = makePath(CLEANED, f"PMT_{year}.gdb", "Networks")
            P_HELP.copy_net_result(
                source_fds=source_fds, target_fds=target_fds, fc_names=expected_fcs
            )  # TODO: embellish this function with print/logging
        else:
            # Solve this network
            print(f"\n{net_suffix}")
            for mode in modes:
                # Create separate service area problems for stations and parks
                nd = makePath(NETS_DIR, f"{mode}{net_suffix}.gdb", "osm", "osm_ND")
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
    # For each analysis year, analyze networks (avoid redundant solves)
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
        out_gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        out_fds_path = makePath(out_gdb, "Networks")
        out_fds = validate_feature_dataset(out_fds_path, SR_FL_SPF, overwrite=False)
        out_fc_name = "nodes_bike"
        out_fc = makePath(out_fds, out_fc_name)
        parcel_fc = makePath(out_gdb, "Polygons", "Parcels")
        checkOverwriteOutput(out_fc, overwrite=True)
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
            source_fds = makePath(CLEANED, f"PMT_{source_year}.gdb", "Networks")
            target_fds = makePath(CLEANED, f"PMT_{year}.gdb", "Networks")
            P_HELP.copy_net_result(source_fds, target_fds, fc_names=out_fc_name)
        else:
            # Get node and edge features as layers
            print(f"\n{net_suffix}")
            in_fds = makePath(NETS_DIR, f"bike{net_suffix}.gdb", "osm")
            in_nd = makePath(in_fds, "osm_ND")
            in_edges = makePath(in_fds, "edges")
            in_nodes = makePath(in_fds, "osm_ND_Junctions")
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
            centrality_df = P_HELP.network_centrality(
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
            extendTableDf(
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
        year_gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        parcels = makePath(year_gdb, "Polygons", "Parcels")
        out_table = makePath(year_gdb, "WalkTime_parcels")
        _append_ = False
        # Iterate over targets and references
        net_fds = makePath(year_gdb, "Networks")
        for tgt_name, ref_fc, preselect_fc in zip(target_names, ref_fcs, preselect_fcs):
            print(f"- {tgt_name}")
            ref_fc = makePath(net_fds, ref_fc)
            preselect_fc = makePath(net_fds, preselect_fc)
            walk_time_df = P_HELP.parcel_walk_times(
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
                extendTableDf(
                    in_table=out_table,
                    table_match_field=prep_conf.PARCEL_COMMON_KEY,
                    df=walk_time_df,
                    df_match_field=prep_conf.PARCEL_COMMON_KEY,
                )
            else:
                dfToTable(df=walk_time_df, out_table=out_table, overwrite=True)
                _append_ = True
            # Add time bin field
            print("--- classifying time bins")
            bin_field = f"bin_{tgt_name}"
            min_time_field = f"min_time_{tgt_name}"
            P_HELP.parcel_walk_time_bin(
                in_table=out_table,
                bin_field=bin_field,
                time_field=min_time_field,
                code_block=prep_conf.TIME_BIN_CODE_BLOCK,
            )


def process_ideal_walk_times(overwrite=True):
    print("\nProcessing Ideal Walk Times:")
    targets = ["stn", "park"]
    for year in YEARS:
        print(f"{str(year)}\n--------------------")
        # Key paths
        year_gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        parcels_fc = makePath(year_gdb, "Polygons", "Parcels")
        stations_fc = makePath(BASIC_FEATURES, "SMARTplanStations")
        parks_fc = makePath(CLEANED, "Park_Points.shp")
        out_table = makePath(year_gdb, "WalkTimeIdeal_parcels")
        target_fcs = [stations_fc, parks_fc]
        # Analyze ideal walk times
        dfs = []
        for target, fc in zip(targets, target_fcs):
            print(f" - {target}")
            # field_suffix = f"{target}_ideal"
            df = P_HELP.parcel_ideal_walk_time(
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
        dfToTable(df=combo_df, out_table=out_table, overwrite=overwrite)
        # Add bin fields
        for target in targets:
            min_time_field = f"min_time_{target}"
            bin_field = f"bin_{target}"
            P_HELP.parcel_walk_time_bin(
                in_table=out_table,
                bin_field=bin_field,
                time_field=min_time_field,
                code_block=prep_conf.TIME_BIN_CODE_BLOCK,
            )


def process_access():
    for year in YEARS:
        print(f"Analysis year: {year}")
        gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        for mode in prep_conf.ACCESS_MODES:
            print(f"--- {mode}")
            # Get reference info from globals
            source, scale, id_field = prep_conf.MODE_SCALE_REF[mode]
            osm_year, model_year = prep_conf.NET_BY_YEAR[year]
            if source == "OSM_Networks":
                skim_year = osm_year
                imped_field = "Minutes"
                skim_data = makePath(CLEANED, source, f"{mode}_Skim{skim_year}.csv")
            else:
                skim_year = model_year
                imped_field = f"{prep_conf.SKIM_IMP_FIELD}_{mode[:2].upper()}"
                skim_data = makePath(CLEANED, source, f"SERPM_OD_{skim_year}.csv")
            # Look up zone and skim data for each mode
            zone_data = makePath(gdb, f"EconDemog_{scale}")

            # Analyze access
            atd_df = P_HELP.summarizeAccess(
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
            afo_df = P_HELP.summarizeAccess(
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
            out_table = makePath(gdb, f"Access_{scale}_{mode}")
            dfToTable(full_table, out_table, overwrite=True)


def get_filename(file_path):
    basename, ext = os.path.splitext(os.path.split(file_path)[1])
    return basename


# TODO: move this to prepare_helper.py
def merge_and_subset(fcs, subset_fc):
    if isinstance(fcs, str):
        fcs = [fcs]
    project_crs = arcpy.Describe(subset_fc).spatialReference
    temp_dir = tempfile.TemporaryDirectory()
    prj_fcs = []
    for fc in fcs:
        print(f"projecting and clipping {fc}")
        arcpy.env.outputCoordinateSystem = project_crs
        basename = get_filename(fc)
        prj_fc = makePath(temp_dir.name, f"{basename}.shp")
        arcpy.Clip_analysis(
            in_features=fc, clip_features=subset_fc, out_feature_class=prj_fc
        )
        prj_fcs.append(prj_fc)

    merge_fc = make_inmem_path()
    arcpy.Merge_management(inputs=prj_fcs, output=merge_fc)
    return merge_fc


def process_contiguity(overwrite=True):
    county_fc = makePath(BASIC_FEATURES, "MiamiDadeCountyBoundary")
    parks = makePath(CLEANED, "Park_Polys.shp")
    water_bodies = makePath(
        RAW, "ENVIRONMENTAL_FEATURES", "NHDPLUS_H_0309_HU4_GDB.gdb", "NHDWaterbody"
    )
    pad_area = makePath(
        RAW,
        "ENVIRONMENTAL_FEATURES",
        "PADUS2_0FL.gdb",
        "PADUS2_0Combined_DOD_Fee_Designation_Easement_FL",
    )
    chunk_fishnet = P_HELP.generate_chunking_fishnet(
        template_fc=county_fc, out_fishnet_name="quadrats", chunks=prep_conf.CTGY_CHUNKS
    )
    for year in YEARS:
        print(f"Processing Contiguity for {year}")
        gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        parcel_fc = makePath(gdb, "Polygons", "Parcels")

        # merge mask layers and subset to County (builidngs held in year loop as future versions may take advantage of
        #   historical building footprint data via OSM attic
        buildings = makePath(
            RAW,
            "OpenStreetMap",
            "buildings_q1_2021",
            "OSM_Buildings_20210201074346.shp",
        )
        mask = merge_and_subset(
            fcs=[buildings, water_bodies, pad_area, parks], subset_fc=county_fc
        )

        ctgy_full = P_HELP.contiguity_index(
            quadrats_fc=chunk_fishnet,
            parcels_fc=parcel_fc,
            mask_fc=mask,
            parcels_id_field=prep_conf.PARCEL_COMMON_KEY,
            cell_size=prep_conf.CTGY_CELL_SIZE,
            weights=prep_conf.CTGY_WEIGHTS,
        )
        if prep_conf.CTGY_SAVE_FULL:
            full_path = makePath(gdb, "Contiguity_full_singlepart")
            dfToTable(df=ctgy_full, out_table=full_path, overwrite=True)
        ctgy_summarized = P_HELP.contiguity_summary(
            full_results_df=ctgy_full,
            parcels_id_field=prep_conf.PARCEL_COMMON_KEY,
            summary_funcs=prep_conf.CTGY_SUMMARY_FUNCTIONS,
            area_scaling=prep_conf.CTGY_SCALE_AREA,
        )
        summarized_path = makePath(gdb, "Contiguity_parcels")
        dfToTable(df=ctgy_summarized, out_table=summarized_path, overwrite=overwrite)


def process_lu_diversity():
    summary_areas_fc = makePath(BASIC_FEATURES, "SummaryAreas")
    lu_recode_table = makePath(REF, "Land_Use_Recode.csv")
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
        gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        parcel_fc = makePath(gdb, "Polygons", "Parcels")
        out_fc = makePath(gdb, "Diversity_summaryareas")

        # Intersect parcels and summary areas
        print(" - intersecting parcels with summary areas")
        par_fields = [
            prep_conf.PARCEL_COMMON_KEY,
            prep_conf.LAND_USE_COMMON_KEY,
            prep_conf.PARCEL_BLD_AREA_COL,
        ]
        par_sa_int = P_HELP.assign_features_to_agg_area(
            parcel_fc,
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
        print(" - calculating diversity indices")
        div_funcs = [
            P_HELP.simpson_diversity,
            P_HELP.shannon_diversity,
            P_HELP.berger_parker_diversity,
            P_HELP.enp_diversity,
            # P_HELP.chi_squared_diversity
        ]
        count_lu = len(prep_conf.DIV_RELEVANT_LAND_USES)
        div_df = P_HELP.lu_diversity(
            in_df,
            prep_conf.SUMMARY_AREAS_COMMON_KEY,
            prep_conf.LU_RECODE_FIELD,
            div_funcs,
            weight_field=prep_conf.PARCEL_BLD_AREA_COL,
            count_lu=count_lu,
            regional_comp=True,
        )

        # Export results
        print(" - exporting results")
        dfToTable(div_df, out_fc, overwrite=True)


def process_travel_stats():
    rates = {}
    hh_field = "HH"
    jobs_field = "TotalJobs"

    # Apply per cap/job rates to analysis years
    for year in YEARS:
        # Get SE DATA
        year_gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        taz_table = makePath(year_gdb, "EconDemog_TAZ")
        out_table = makePath(year_gdb, "TripStats_TAZ")
        taz_df = table_to_df(taz_table, keep_fields="*")

        # Get OD reference
        model_year = prep_conf.NET_BY_YEAR[year][1]
        if model_year not in rates:
            # Calculate per cap/per job vmt rates
            skim_csv = makePath(CLEANED, "SERPM", f"SERPM_OD_{model_year}.csv")
            taz_ref_csv = makePath(CLEANED, f"PMT_{model_year}.gdb", "EconDemog_TAZ")
            taz_ref = PMT.table_to_df(taz_ref_csv, keep_fields="*")
            trips_field = "TRIPS"
            auto_time_field = prep_conf.SKIM_IMP_FIELD + "_AU"
            tran_time_field = prep_conf.SKIM_IMP_FIELD + "_TR"
            dist_field = "DIST"
            rates_df = P_HELP.taz_travel_stats(skim_csv, o_field=prep_conf.SKIM_O_FIELD, d_field=prep_conf.SKIM_D_FIELD,
                                        veh_trips_field=trips_field, auto_time_field=auto_time_field, dist_field=dist_field,
                                        taz_df=taz_ref, taz_id_field=prep_conf.TAZ_COMMON_KEY, hh_field=hh_field,
                                        jobs_field=jobs_field)
            rates[model_year] = rates_df

        # Multiply rates by TAZ activity
        rates_df = rates[model_year]
        taz_fields = [prep_conf.TAZ_COMMON_KEY, hh_field, jobs_field]
        loaded_df = rates_df.merge(taz_df[taz_fields], how="inner", on=prep_conf.TAZ_COMMON_KEY)
        loaded_df["__activity__"] = loaded_df[[hh_field, jobs_field]].sum(axis=1)
        loaded_df["VMT_FROM"] = loaded_df.VMT_PER_ACT_FROM * loaded_df.__activity__
        loaded_df["VMT_TO"] = loaded_df.VMT_PER_ACT_TO * loaded_df.__activity__
        loaded_df["VMT_ALL"] = loaded_df[["VMT_FROM", "VMT_TO"]].mean(axis=1)

        # Export results
        loaded_df = loaded_df.drop(columns=[hh_field, jobs_field, "__activity__"])
        dfToTable(df=loaded_df, out_table=out_table, overwrite=True)


def process_walk_to_transit_skim():
    # Create OD table of TAZ centroids to TAP nodes
    serpm_raw = makePath(RAW, "SERPM")
    serpm_clean = makePath(CLEANED, "SERPM")
    taz_centroids = makePath(serpm_raw, "SERPM_TAZ_Centroids.shp")
    tap_nodes = makePath(serpm_raw, "SERPM_TAP_Nodes.shp")
    tap_id = "TAP"
    tap_cutoff = "15" #minutes
    solved = []
    for year in YEARS:
        net_suffix, model_year = prep_conf.NET_BY_YEAR[year]
        if model_year not in solved:
            print(f"Preparing TAP to TAZ skims for model year {model_year}")
            # Get TAZ to TAP OD table
            # - Skim input
            nd = makePath(NETS_DIR, f"Walk{net_suffix}.gdb", "osm", "osm_ND")
            skim = makePath(serpm_clean, f"TAZ_to_TAP{net_suffix}.csv")
            restrictions = None
            # - Create and load problem
            print(" - Network-based")
            P_HELP.genODTable(origin_pts=taz_centroids, origin_name_field=prep_conf.TAZ_COMMON_KEY,
                        dest_pts=tap_nodes, dest_name_field=tap_id,
                        in_nd=nd, imped_attr=prep_conf.OSM_IMPED, cutoff=tap_cutoff,
                        net_loader=prep_conf.NET_LOADER,
                        out_table=skim, restrictions=restrictions, use_hierarchy=False, uturns="ALLOW_UTURNS",
                        o_location_fields=None, d_location_fields=None, o_chunk_size=None)
    
            # Estimate simple spatial distance TAZ to TAP for TAZs outside extends of osm network
            print(" - Spatial-based")
            taz_layer = arcpy.MakeFeatureLayer_management(taz_centroids, "TAZ")
            tap_layer = arcpy.MakeFeatureLayer_management(tap_nodes, "TAP")
            edges = makePath(NETS_DIR, f"Walk{net_suffix}.gdb", "osm", "edges")
            net_layer = arcpy.MakeFeatureLayer_management(edges, "edges")
            # Set spatial reference
            sr = arcpy.Describe(edges).spatialReference
            mpu = float(sr.metersPerUnit)
            # Get distances and estimate times
            out_rows = []
            try:
                # Select TAZ's that wouldn't load on network
                arcpy.SelectLayerByLocation_management(
                    in_layer=taz_layer, overlap_type="INTERSECT", select_features=edges,
                    search_distance=prep_conf.NET_LOADER.search_tolerance, selection_type="NEW_SELECTION",
                    invert_spatial_relationship=True)
                # Iterate over TAZs
                with arcpy.da.SearchCursor(
                    taz_layer, ["SHAPE@", prep_conf.TAZ_COMMON_KEY], spatial_reference=sr) as taz_c:
                    for taz_r in taz_c:
                        taz_point, taz_id = taz_r
                        # Select TAP's that are within potential walking distance of selected TAZ's
                        arcpy.SelectLayerByLocation_management(
                            in_layer=tap_layer, overlap_type="INTERSECT", select_features=taz_point,
                            search_distance=prep_conf.IDEAL_WALK_RADIUS, selection_type="NEW_SELECTION",
                            invert_spatial_relationship=False)
                        # Iterate over taps and estimate walk time
                        with arcpy.da.SearchCursor(
                            tap_layer, ["SHAPE@", tap_id], spatial_reference=sr) as tap_c:
                            for tap_r in tap_c:
                                tap_point, tap_n = tap_r
                                grid_dist = abs(tap_point.centroid.X - taz_point.centroid.X)
                                grid_dist += abs(tap_point.centroid.Y - taz_point.centroid.Y)
                                grid_meters = grid_dist * mpu
                                grid_minutes = (grid_meters * 60) / (prep_conf.IDEAL_WALK_MPH * 1609.344)
                                if grid_minutes <= float(tap_cutoff):
                                    out_rows.append([f"{taz_id} - {tap_n}", grid_minutes, taz_id, tap_n])
                # Update output csv
                out_df = pd.DataFrame(out_rows, columns=["Name", prep_conf.OSM_IMPED, "OName", "DName"])
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
    # Make a graph from TAP to TAP skim
    serpm_raw = makePath(RAW, "SERPM")
    serpm_clean = makePath(CLEANED, "SERPM")
    skim_versions = ["local", "prem"]
    cutoff = 60 #TODO: move to prep_conf?
    solved = []
    for year in YEARS:
        net_suffix, model_year = prep_conf.NET_BY_YEAR[year]
        taz_to_tap = makePath(serpm_clean, f"TAZ_to_TAP{net_suffix}.csv")
        tazs = makePath(serpm_raw, "SERPM_TAZ_Centroids.shp")
        # Get TAZ nodes
        mdc_wc = arcpy.AddFieldDelimiters(tazs, "IN_MDC") + "=1"
        with arcpy.da.SearchCursor(tazs, prep_conf.TAZ_COMMON_KEY, where_clause=mdc_wc) as c:
            taz_nodes = sorted({r[0] for r in c})
        with arcpy.da.SearchCursor(tazs, prep_conf.TAZ_COMMON_KEY) as c:
            all_tazs = sorted({r[0] for r in c})
        # Analyze zone to zone times
        if model_year not in solved:
            print(f"Estimating TAZ to TAZ times for model year {model_year}")
            for skim_version in skim_versions:
                print(f"- transit submode: {skim_version}")
                tap_to_tap = makePath(serpm_raw, f"TAP_to_TAP_{skim_version}_{model_year}.csv")
                # Clean the tap to tap skim
                print(f" - - cleaning TAP to TAP skim")
                tap_to_tap_clean = makePath(serpm_clean, f"TAP_to_TAP_{skim_version}_{model_year}_clean.csv")
                tap_renames = {"orig": "OName", "dest": "DName", "flow": "Minutes"} # TODO move to prep_conf?
                P_HELP.clean_skim_csv(in_file=tap_to_tap, out_file=tap_to_tap_clean, imp_field="Minutes",
                                    drop_val=0, node_fields=["OName", "DName"], node_offset=5000, renames=tap_renames)
                # TODO: tack taz_to_tap onto the end of tap_to_tap to eliminate the need to run Compose?
                # # Combine skims to build zone-to-zone times
                # taz_to_taz = makePath(serpm_clean, f"TAZ_to_TAZ_{skim_version}_{model_year}.csv")
                # P_HELP.transit_skim_joins(taz_to_tap, tap_to_tap_clean, out_skim=taz_to_taz,
                #                           o_col="OName", d_col="DName", imp_col="Minutes",
                #                           origin_zones=taz_nodes, total_cutoff=cutoff)
                # Make tap to tap network
                print(" - - building TAP to TAP graph")
                full_skim(tap_to_tap_clean, taz_to_tap, cutoff, model_year, skim_version, taz_nodes, all_tazs)
                solved.append(model_year)

def full_skim(tap_to_tap_clean, taz_to_tap, cutoff, model_year, skim_version, taz_nodes, all_tazs):
    serpm_clean = makePath(CLEANED, "SERPM")
    G = P_HELP.skim_to_graph(tap_to_tap_clean, source="OName", target="DName", attrs="Minutes",
                            create_using=nx.DiGraph)
    # Make tap to taz network (as base graph, converted to digraph)
    print(" - - building TAZ to TAP graph")
    H = P_HELP.skim_to_graph(taz_to_tap, source="OName", target="DName", attrs="Minutes",
                            create_using=nx.Graph, renames={}).to_directed()
    # Combine networks and solve taz to taz
    print(" - - combining graphs")
    FULL = nx.compose(G, H)
    print(f" - - solving TAZ to TAZ for {len(taz_nodes)} origins (of {len(all_tazs)} taz's)")
    taz_to_taz = makePath(serpm_clean, f"TAZ_to_TAZ_{skim_version}_{model_year}.csv")
    
    with open(taz_to_taz, "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([prep_conf.SKIM_O_FIELD, prep_conf.SKIM_D_FIELD, prep_conf.SKIM_IMP_FIELD])
        for i in taz_nodes:
            if FULL.has_node(i):
                i_dict = nx.single_source_dijkstra_path_length(
                    G=FULL, source=i, cutoff=cutoff, weight="Minutes")
                out_rows = []
                for j, time in i_dict.items():
                    if j in all_tazs:
                        out_row = (i, j, time)
                        out_rows.append(out_row)
                writer.writerows(out_rows)


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
        RAW = validate_directory(directory=makePath(ROOT, "RAW"))
        CLEANED = validate_directory(directory=makePath(ROOT, "CLEANED"))
        NETS_DIR = makePath(CLEANED, "osm_networks")
        DATA = ROOT
        BASIC_FEATURES = makePath(CLEANED, "PMT_BasicFeatures.gdb")
        YEAR_GDB_FORMAT = makePath(CLEANED, "PMT_YEAR.gdb")
        REF = makePath(ROOT, "Reference")
        RIF_CAT_CODE_TBL = makePath(REF, "road_impact_fee_cat_codes.csv")
        DOR_LU_CODE_TBL = makePath(REF, "Land_Use_Recode.csv")
        YEARS = YEARS + ["NearTerm"]

    # SETUP CLEAN DATA
    # -----------------------------------------------
    # UDB might be ignored as this isnt likely to change and can be updated ad-hoc
    # process_udb() # TODO: udbToPolygon failing to create a feature class to store the output (likley an arcpy overrun)

    # process_basic_features() # TESTED


    # MERGES PARK DATA INTO A SINGLE POINT FEATURESET AND POLYGON FEARTURESET
    # process_parks()  # TESTED UPDATES 03/10/21 CR
    #   YEAR over YEARS
    #   - sets up Points FDS and year GDB(unless they exist already
    #   - copies Park_Points in to each year gdb under the Points FDS
    #   - treat NEAR_TERM like any other year

    # CLEANS AND GEOCODES TRANSIT INTO INCLUDED LAT/LON
    # process_transit()  # TESTED 02/26/21 CR
    #   YEAR over YEARS
    #     - cleans and consolidates transit data into Year POINTS FDS
    #     - if YEAR == NearTerm:
    #     -     most recent year is copied over

    # SETUP ANY BASIC NORMALIZED GEOMETRIES
    # process_normalized_geometries()  # TESTED 03/11/21 updated names to standarization
    #    YEAR BY YEAR
    #   - Sets up Year GDB, and Polygons FDS
    #   - Adds MAZ, TAZ, Census_Blocks, Census_BlockGroups, SummaryAreas
    #   - for each geometry type, the year is added as an attribute
    #   - for NearTerm, year is set to 9998 (allows for LongTerm to be 9999)

    # COPIES DOWNLOADED PARCEL DATA AND ONLY MINIMALLY NECESSARY ATTRIBUTES INTO YEARLY GDB
    # process_parcels()  # TESTED 03/11/21 CR
    #   YEAR over YEARS
    #   - procedure joins parcels from DOR to NAL table keeping appropriate columns
    #   - if year == NearTerm:
    #       previous year parcels are copied in

    # CLEANS AND GEOCODES PERMITS TO ASSOCIATED PARCELS AND
    #   GENERATES A NEAR TERM PARCELS LAYER WITH PERMIT INFO
    process_permits()  # TESTED CR 03/01/21

    # updates parcels based on permits for near term analysis
    # process_short_term_parcels()  # TESTED 3/1/21 #TODO: needs to be broken down into smaller functions
    #   - record parcel land use groupings and multipliers/overwrites
    #   - replace relevant attributes for parcels with current permits
    #   update this procedure to pull from NearTerm and update parcel layer

    # -----------------ENRICH DATA------------------------------
    # ADD VARIOUS BLOCK GROUP LEVEL DEMOGRAPHIC, EMPLOYMENT AND COMMUTE DATA AS TABLE
    # enrich_block_groups()  # TESTED CR 03/12/21 added src attributes for enrichement data
    #   YEAR over YEARS
    #   - enrich block group with parcel data and race/commute/jobs data as table
    #   - if Year == NearTerm:
    #       process as normal (parcel data have been updated to include permit updates)

    # MODELS MISSING DATA WHERE APPROPRIATE AND DISAGGREGATES BLOCK LEVEL DATA DOWN TO PARCEL LEVEL
    # process_bg_estimate_activity_models()  # TESTED CR 03/02/21
    #   - creates linear model at block group-level for total employment, population, and commutes
    #       TODO: pull out of this function and insert to bg_apply and process once
    # process_bg_apply_activity_models()  # TESTED CR 03/02/21
    #   YEAR over YEARS
    #   - applies linear model to block groups and estimates shares for employment, population and commute classes
    #     - if Year == NearTerm:
    #     -   process as normal (enrichment table generated from near_term parcels updated with permits)
    #       TODO: pull this into the allocate_bg function process
    # process_allocate_bg_to_parcels()
    #   YEAR over YEARS
    #   - allocates the modeled data from previous step to parcels as table
    #   - if Year == NearTerm:
    #   -   process as normal (modeled block group data will be generated from near_term parcels with permit updates)

    # ADDS LAND USE TABLE FOR PARCELS INCLUDING VACANT, RES AND NRES AREA
    # process_parcel_land_use()  # Tested by CR 3/11/21 verify NearTerm year works
    #   YEAR over YEARS
    #   - creates table of parcel records with land use and areas

    """ start here  use YEARS = [2019, "NearTerm"] """
    # prepare maz and taz socioeconomic/demographic data
    # process_model_se_data()  # TESTED 3/16/21

    # ------------------NETWORK ANALYSES-----------------------------
    """ for NearTerm make copies, processing time is exorbitant and unnecessary to rerun """
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

    # -----------------DEPENDENT ANALYSIS------------------------------
    # ANALYZE ACCESS BY MAZ, TAZ
    # process_access()  # TESTED by AB 4/11/21 with transkt skim

    # PREPARE TAZ TRIP LENGTH AND VMT RATES
    process_travel_stats() #  Tested by AB 4/1/21

    # ONLY UPDATED WHEN NEW IMPERVIOUS DATA ARE MADE AVAILABLE
    # process_imperviousness()  # TESTED by CR 3/21/21 Added NearTerm

    # process_lu_diversity()  # TESTED by CR 3/21/21 Added NearTerm

    # generate contiguity index for all years
    # process_contiguity()

# TODO: !!! incorporate a project setup script or at minimum a yearly build geodatabase function/logic !!!
# TODO: add logging/print statements for procedure tracking (low priority)

""" deprecated """
# cleans and geocodes crashes to included Lat/Lon
# process_crashes()

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
