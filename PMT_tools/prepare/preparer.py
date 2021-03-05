"""
preparation scripts used set up cleaned geodatabases
"""
import os, sys

sys.path.insert(0, os.getcwd())
# TODO: move these functions to a general helper file as they apply more broadly
from PMT_tools.download.download_helper import (validate_directory, validate_geodatabase, validate_feature_dataset)
# config global variables
from PMT_tools.config.prepare_config import IN_CRS, OUT_CRS
from PMT_tools.config.prepare_config import (BASIC_STATIONS, STN_NAME_FIELD, STN_BUFF_DIST, STN_BUFF_METERS,
                                             STN_DISS_FIELDS, STN_CORRIDOR_FIELDS)
from PMT_tools.config.prepare_config import BASIC_ALIGNMENTS, ALIGN_BUFF_DIST, ALIGN_DISS_FIELDS, CORRIDOR_NAME_FIELD
from PMT_tools.config.prepare_config import (BASIC_STN_AREAS, BASIC_CORRIDORS, BASIC_LONG_STN,
                                             BASIC_SUM_AREAS, BASIC_RENAME_DICT, STN_LONG_CORRIDOR)
from PMT_tools.config.prepare_config import (CRASH_FIELDS_DICT, USE_CRASH)
from PMT_tools.config.prepare_config import TRANSIT_RIDERSHIP_TABLES, TRANSIT_FIELDS_DICT, TRANSIT_LONG, TRANSIT_LAT
from PMT_tools.config.prepare_config import LODES_YEARS, ACS_YEARS
from PMT_tools.config.prepare_config import (PARCEL_COMMON_KEY, PARCEL_DOR_KEY, PARCEL_NAL_KEY, PARCEL_COLS,
                                             PARCEL_USE_COLS, PARCEL_AREA_COL, PARCEL_LU_AREAS, PARCEL_BLD_AREA)
from PMT_tools.config.prepare_config import BG_COMMON_KEY, ACS_COMMON_KEY, LODES_COMMON_KEY, MAZ_COMMON_KEY, TAZ_COMMON_KEY
from PMT_tools.config.prepare_config import (BG_PAR_SUM_FIELDS, ACS_RACE_FIELDS, ACS_COMMUTE_FIELDS, LODES_FIELDS,
                                             LODES_CRITERIA)
from PMT_tools.config.prepare_config import SKIM_O_FIELD, SKIM_D_FIELD, SKIM_IMP_FIELD, SKIM_DTYPES, SKIM_RENAMES
from PMT_tools.config.prepare_config import OSM_IMPED, OSM_CUTOFF, BIKE_PED_CUTOFF
from PMT_tools.config.prepare_config import (NETS_DIR, SEARCH_CRITERIA, SEARCH_QUERY, NET_LOADER,
                                             NET_BY_YEAR, BIKE_RESTRICTIONS)
from PMT_tools.config.prepare_config import (MAZ_AGG_COLS, MAZ_PAR_CONS, SERPM_RENAMES,
                                             MAZ_SE_CONS, MODEL_YEARS, MAZ_COMMON_KEY, TAZ_COMMON_KEY)
from PMT_tools.config.prepare_config import CENTRALITY_IMPED, CENTRALITY_CUTOFF, CENTRALITY_NET_LOADER
from PMT_tools.config.prepare_config import TIME_BIN_CODE_BLOCK, IDEAL_WALK_MPH, IDEAL_WALK_RADIUS
from PMT_tools.config.prepare_config import (ACCESS_MODES, MODE_SCALE_REF, ACCESS_TIME_BREAKS,
                                             ACCESS_UNITS, O_ACT_FIELDS, D_ACT_FIELDS)
from PMT_tools.config.prepare_config import (CTGY_CHUNKS, CTGY_CELL_SIZE, CTGY_WEIGHTS, CTGY_SAVE_FULL,
                                             CTGY_SUMMARY_FUNCTIONS, CTGY_SCALE_AREA, BUILDINGS_PATH)
from PMT_tools.config.prepare_config import DIV_ON_FIELD, LU_RECODE_TABLE, LU_RECODE_FIELD
from PMT_tools.config.prepare_config import DIV_AGG_GEOM_FORMAT, DIV_AGG_GEOM_ID, DIV_AGG_GEOM_BUFFER
from PMT_tools.config.prepare_config import (DIV_RELEVANT_LAND_USES, DIV_METRICS, DIV_CHISQ_PROPS,
                                             DIV_REGIONAL_ADJ, DIV_REGIONAL_CONSTS)
from PMT_tools.config.prepare_config import ZONE_GEOM_FORMAT, ZONE_GEOM_ID
from PMT_tools.config.prepare_config import (PERMITS_PATH, PERMITS_REF_TABLE_PATH, PARCEL_REF_TABLE_UNITS_MATCH,
                                             SHORT_TERM_PARCELS_UNITS_MATCH)
from PMT_tools.config.prepare_config import (PERMITS_VALUES_FIELD, PERMITS_UNITS_FIELD, PERMITS_BLD_AREA_NAME,
                                             PERMITS_ID_FIELD, PERMITS_LU_FIELD, PERMITS_VALUES_FIELD,
                                             PERMITS_COST_FIELD)
from PMT_tools.config.prepare_config import PARCEL_LAND_VALUE, PARCEL_JUST_VALUE, PARCEL_BUILDINGS, PARCEL_LU_COL

# prep/clean helper functions
from PMT_tools.prepare.prepare_helpers import *
from PMT_tools.prepare.prepare_osm_networks import *
# PMT functions
from PMT_tools.PMT import makePath, SR_FL_SPF, EPSG_FLSPF, checkOverwriteOutput, dfToTable, polygonsToPoints
# PMT classes
from PMT_tools.PMT import ServiceAreaAnalysis
# PMT globals
from PMT_tools.PMT import (RAW, CLEANED, BASIC_FEATURES, REF, YEARS, YEAR_GDB_FORMAT)
from PMT_tools.PMT import arcpy

import PMT_tools.logger as log
import os

logger = log.Logger(add_logs_to_arc_messages=True)

arcpy.env.overwriteOutput = True

DEBUG = True
if DEBUG:
    '''
    if DEBUG is True, you can change the path of the root directory and test any
    changes to the code you might need to handle without munging the existing data
    '''
    ROOT = r'C:\OneDrive_RP\OneDrive - Renaissance Planning Group\SHARE\PMT\Data'
    RAW = validate_directory(directory=makePath(ROOT, 'PROCESSING_TEST', "RAW"))
    CLEANED = validate_directory(directory=makePath(ROOT, 'PROCESSING_TEST', "CLEANED"))
    DATA = ROOT
    BASIC_FEATURES = makePath(CLEANED, "PMT_BasicFeatures.gdb")
    REF = makePath(ROOT, "Reference")
    RIF_CAT_CODE_TBL = makePath(REF, "road_impact_fee_cat_codes.csv")
    DOR_LU_CODE_TBL = makePath(REF, "Land_Use_Recode.csv")
    YEAR_GDB_FORMAT = makePath(CLEANED, "PMT_YEAR.gdb")


def process_normalized_geometries(overwrite=True):
    ''' Census Data and TAZ/MAZ
        - these are geometries that only consist of a dataset key
        - copies census block groups and blocks into yearly GDB, subsets to county and tags with year
        - copied TAZ and MAZ into yearly GDB, subsets to county and tags with year

    '''
    # RAW
    raw_block = makePath(RAW, "CENSUS", "tl_2019_12_tabblock10", "tl_2019_12_tabblock10.shp")
    raw_block_groups = makePath(RAW, "CENSUS", "tl_2019_12_bg", "tl_2019_12_bg.shp")
    raw_TAZ = makePath(RAW, "TAZ.shp")
    raw_MAZ = makePath(RAW, "MAZ.shp")
    # CLEANED
    blocks = "Census_Blocks"
    block_groups = "Census_BlockGroups"
    TAZ = "TAZ"
    MAZ = "MAZ"
    county = makePath(BASIC_FEATURES, "BasicFeatures", "MiamiDadeCountyBoundary")
    for year in YEARS:
        for raw, cleaned, cols in zip([raw_block, raw_block_groups, raw_TAZ, raw_MAZ],
                                      [blocks, block_groups, TAZ, MAZ],
                                      ["GEOID10", "GEOID", TAZ, MAZ]):
            temp_file = make_inmem_path()
            out_path = validate_feature_dataset(makePath(CLEANED, f"PMT_{year}.gdb", "Polygons"), sr=SR_FL_SPF)
            logger.log_msg(f"--- building normalized {cleaned} in {out_path}")
            out_data = makePath(out_path, cleaned)
            prep_feature_class(in_fc=raw, geom="POLYGON", out_fc=temp_file, use_cols=cols, rename_dict=None)
            lyr = arcpy.MakeFeatureLayer_management(in_features=temp_file, out_layer="lyr")
            # drop features not in county boundary except for MAZ/TAZ
            if raw not in [raw_MAZ, raw_TAZ]:
                arcpy.SelectLayerByLocation_management(in_layer=lyr, overlap_type="HAVE_THEIR_CENTER_IN",
                                                       select_features=county)
            if overwrite:
                checkOverwriteOutput(output=out_data, overwrite=overwrite)
            logger.log_msg(f"--- outputing geometries and {cols} only")
            arcpy.CopyFeatures_management(in_features=lyr, out_feature_class=out_data)
            arcpy.CalculateField_management(in_table=out_data, field="Year", expression=year,
                                            expression_type="PYTHON3", field_type="LONG")


def process_basic_features():
    # TODO: add check for existing basic features, and compare for changes
    print("Making basic features")
    makeBasicFeatures(bf_gdb=BASIC_FEATURES, stations_fc=BASIC_STATIONS, stn_diss_fields=STN_DISS_FIELDS,
                      stn_corridor_fields=STN_CORRIDOR_FIELDS, alignments_fc=BASIC_ALIGNMENTS,
                      align_diss_fields=ALIGN_DISS_FIELDS, stn_buff_dist=STN_BUFF_DIST,
                      align_buff_dist=ALIGN_BUFF_DIST, stn_areas_fc=BASIC_STN_AREAS,
                      corridors_fc=BASIC_CORRIDORS, long_stn_fc=BASIC_LONG_STN,
                      rename_dict=BASIC_RENAME_DICT, overwrite=True)

    print("Making summarization features")
    makeSummaryFeatures(bf_gdb=BASIC_FEATURES, long_stn_fc=BASIC_LONG_STN, corridors_fc=BASIC_CORRIDORS,
                        cor_name_field=CORRIDOR_NAME_FIELD, out_fc=BASIC_SUM_AREAS, stn_buffer_meters=STN_BUFF_METERS,
                        stn_name_field=STN_NAME_FIELD, stn_cor_field=STN_LONG_CORRIDOR, overwrite=True)


def process_parks(overwrite=True):
    """
    parks - merges park polygons into one and formats both poly and point park data.
    """
    park_polys = [makePath(RAW, "Municipal_Parks.geojson"),
                  makePath(RAW, "Federal_State_Parks.geojson"),
                  makePath(RAW, "County_Parks.geojson")]
    park_points = makePath(RAW, "Park_Facilities.geojson")
    poly_use_cols = [["FOLIO", "NAME", "TYPE"], ["NAME"], ["FOLIO", "NAME", "TYPE"]]
    poly_rename_cols = [{}, {}, {}]
    out_park_polys = makePath(CLEANED, "Park_Polys.shp")
    out_park_points = makePath(CLEANED, "Park_Points.shp")
    if overwrite:
        for output in [out_park_points, out_park_polys]:
            checkOverwriteOutput(output=output, overwrite=overwrite)
    prep_park_polys(in_fcs=park_polys, geom="POLYGON", out_fc=out_park_polys,
                    use_cols=poly_use_cols, rename_dicts=poly_rename_cols)
    prep_feature_class(in_fc=park_points, geom="POINT", out_fc=out_park_points)


def process_crashes():
    ''' crashes '''
    crash_json = makePath(RAW, "Safety_Security", "bike_ped.geojson")
    all_features = geojson_to_feature_class_arc(geojson_path=crash_json, geom_type='POINT')
    arcpy.FeatureClassToFeatureClass_conversion(all_features, RAW, "DELETE_crashes.shp")
    # reformat attributes and keep only useful
    clean_and_drop(feature_class=all_features, use_cols=USE_CRASH, rename_dict=CRASH_FIELDS_DICT)
    for year in YEARS:
        # use year variable to setup outputs
        out_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_{year}.gdb"))
        FDS = validate_feature_dataset(makePath(out_gdb, "Points"), sr=SR_FL_SPF)
        out_name = 'BikePedCrashes'
        year_wc = f'"YEAR" = {year}'
        # clean and format crash data
        prep_bike_ped_crashes(in_fc=all_features, out_path=FDS, out_name=out_name,
                              where_clause=year_wc)
    arcpy.Delete_management(in_data=all_features)


def process_permits(overwrite=True):
    ''' permits '''
    permit_csv = makePath(RAW, "BUILDING_PERMITS", "Road Impact Fee Collection Report -- 2019.csv")

    out_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_2019.gdb"))
    in_fds = validate_feature_dataset(fds_path=makePath(out_gdb, "Polygons"), sr=SR_FL_SPF)
    parcels = makePath(in_fds, "Parcels")
    out_fds = validate_feature_dataset(makePath(out_gdb, "Points"), sr=SR_FL_SPF)
    permits_out = makePath(out_fds, "BuildingPermits")
    if overwrite:
        checkOverwriteOutput(output=permits_out, overwrite=overwrite)
    clean_permit_data(permit_csv=permit_csv, poly_features=parcels,
                      permit_key="FOLIO", poly_key="FOLIO",
                      out_file=permits_out, out_crs=EPSG_FLSPF)


def process_udb():
    ''' UDB '''
    udb_fc = makePath(RAW, "MD_Urban_Growth_Boundary.geojson")
    county_fc = makePath(RAW, "CensusGeo", "Miami-Dade_Boundary.geojson")
    out_fc = makePath(CLEANED, "UrbanDevelopmentBoundary.shp")

    temp_fc = geojson_to_feature_class_arc(geojson_path=udb_fc, geom_type="POLYLINE")
    udbLineToPolygon(udb_fc=temp_fc, county_fc=county_fc, out_fc=out_fc)


def process_transit(overwrite=True):
    ''' Transit Ridership
        - converts a list of ridership files to points with attributes cleaned
        - NOTE: transit folder reflects current location, needs update to reflect cleaner structure
    '''
    transit_folder = validate_directory(makePath(RAW, "TRANSIT", "TransitRidership_byStop"))
    transit_shape_fields = [TRANSIT_LONG, TRANSIT_LAT]
    print("PROCESSING TRANSIT RIDERSHIP... ")
    for year in YEARS:
        print(f"--- cleaning ridership for {year}")
        out_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_{year}.gdb"))
        FDS = validate_feature_dataset(fds_path=makePath(out_gdb, "Points"), sr=SR_FL_SPF)
        out_name = 'TransitRidership'
        transit_xls_file = makePath(transit_folder, TRANSIT_RIDERSHIP_TABLES[year])
        transit_out_path = makePath(FDS, out_name)
        if overwrite:
            checkOverwriteOutput(output=transit_out_path, overwrite=overwrite)
        prep_transit_ridership(in_table=transit_xls_file, rename_dict=TRANSIT_FIELDS_DICT,
                               shape_fields=transit_shape_fields, from_sr=IN_CRS,
                               to_sr=OUT_CRS, out_fc=transit_out_path)
        print(f"--- ridership cleaned for {year} and located in {transit_out_path}")


def process_parcels(overwrite=True):
    print("PROCESSING PARCELS... ")
    parcel_folder = makePath(RAW, "Parcels")
    for year in YEARS:
        print(f"Setting up parcels for {year}")
        out_gdb = validate_geodatabase(makePath(CLEANED, f"PMT_{year}.gdb"))
        out_fds = validate_feature_dataset(makePath(out_gdb, "Polygons"), sr=SR_FL_SPF)
        # source data
        in_fc = makePath(parcel_folder, f"Miami_{year}.shp")
        in_csv = makePath(parcel_folder, f"NAL_{year}_23Dade_F.csv")
        # output feature
        out_fc = makePath(out_fds, "Parcels")
        # input fix variables
        renames = PARCEL_COLS.get(year, {})
        usecols = PARCEL_USE_COLS.get(year, PARCEL_USE_COLS["DEFAULT"])
        csv_kwargs = {"dtype": {"PARCEL_ID": str, "CENSUS_BK": str},
                      "usecols": usecols}
        if overwrite:
            checkOverwriteOutput(output=out_fc, overwrite=overwrite)
        prep_parcels(in_fc=in_fc, in_tbl=in_csv, out_fc=out_fc, fc_key_field=PARCEL_DOR_KEY,
                     new_fc_key_field=PARCEL_COMMON_KEY, tbl_key_field=PARCEL_NAL_KEY,
                     tbl_renames=renames, **csv_kwargs)
        arcpy.CalculateField_management(in_table=out_fc, field="Year", expression=year,
                                        expression_type="PYTHON3", field_type="LONG")


def enrich_block_groups():
    # For all years, enrich block groups with parcels
    for year in PMT.YEARS:
        print(year)
        # Define inputs/outputs
        gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        fds = makePath(gdb, "Polygons")
        parcels_fc = makePath(fds, "Parcels")
        bg_fc = makePath(fds, "Census_BlockGroups")
        out_tbl = makePath(gdb, "Enrichment_blockgroups")
        # define table vars
        race_tbl = makePath(RAW, "CENSUS", f"ACS_{year}_race.csv")
        commute_tbl = makePath(RAW, "CENSUS", f"ACS_{year}_commute.csv")
        lodes_tbl = makePath(RAW, "LODES", f"fl_wac_S000_JT00_{year}_bgrp.csv.gz")
        # Enrich BGs with parcel data
        bg_df = enrich_bg_with_parcels(bg_fc=bg_fc, bg_id_field=BG_COMMON_KEY,
                                       parcels_fc=parcels_fc, par_id_field=PARCEL_COMMON_KEY,
                                       par_lu_field=PARCEL_LU_COL, par_bld_area=PARCEL_BLD_AREA,
                                       sum_crit=LODES_CRITERIA, par_sum_fields=BG_PAR_SUM_FIELDS)
        # Save enriched data
        dfToTable(bg_df, out_tbl, overwrite=True)
        # Extend BG output with ACS/LODES data
        in_tables = [race_tbl, commute_tbl, lodes_tbl]
        in_tbl_ids = [ACS_COMMON_KEY, ACS_COMMON_KEY, LODES_COMMON_KEY]
        in_tbl_flds = [ACS_RACE_FIELDS, ACS_COMMUTE_FIELDS, LODES_FIELDS]
        for table, tbl_id, fields in zip(in_tables, in_tbl_ids, in_tbl_flds):
            if arcpy.Exists(table):
                print(f"--- enriching parcels with {table} data")
                enrich_bg_with_econ_demog(tbl_path=out_tbl, tbl_id_field=BG_COMMON_KEY,
                                          join_tbl=table, join_id_field=tbl_id, join_fields=fields)


def process_parcel_land_use():
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
        par_fields = [PARCEL_COMMON_KEY, "LND_SQFOOT"]
        tbl_lu_field = "DOR_UC"
        dtype = {"DOR_UC": int}
        par_df = prep_parcel_land_use_tbl(parcels_fc, par_lu_field, par_fields,
                                          lu_table, tbl_lu_field, dtype_map=dtype)
        # Calculate area columns
        for par_lu_col in PARCEL_LU_AREAS.keys():
            ref_col, crit = PARCEL_LU_AREAS[par_lu_col]
            par_df[par_lu_col] = np.select(
                [par_df[ref_col] == crit], [par_df[PARCEL_AREA_COL]], 0.0
            )
        # Export result
        checkOverwriteOutput(out_table, overwrite=True)
        dfToTable(par_df, out_table)


def process_imperviousness():
    impervious_download = makePath(RAW, "Imperviousness.zip")
    county_boundary = makePath(DATA, "PMT_BasicFeatures.gdb", "BasicFeatures", "MiamiDadeCountyBoundary")
    out_dir = validate_directory(makePath(CLEANED, "IMPERVIOUS"))
    impv_raster = prep_imperviousness(zip_path=impervious_download, clip_path=county_boundary, out_dir=out_dir,
                                      transform_crs=EPSG_FLSPF)
    for year in YEARS:
        gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        if "{year}" in ZONE_GEOM_FORMAT:
            zone_fc = ZONE_GEOM_FORMAT.replace("{year}", str(year))
        else:
            zone_fc = ZONE_GEOM_FORMAT
        impv = analyze_imperviousness(impervious_path=impv_raster,
                                      zone_geometries_path=zone_fc,
                                      zone_geometries_id_field=ZONE_GEOM_ID)
        zone_name = os.path.split(zone_fc)[1].lower()
        write_to = makePath(gdb, ''.join(["Imperviousness_", zone_name]))
        dfToTable(impv, write_to)


def process_osm_networks():
    net_versions = sorted({v[0] for v in NET_BY_YEAR.values()})
    # TODO: DROP DEBUG PLACEHOLDER default of net_versions
    # ********
    net_versions = ["q1_2021"]
    # ********
    for net_version in net_versions:
        # Import edges
        osm_raw = PMT.makePath(RAW, "OpenStreetMap")
        for net_type in ["bike", "walk"]:
            net_type_version = f"{net_type}_{net_version}"
            # Make output geodatabase
            clean_gdb = validate_geodatabase(
                makePath(NETS_DIR, f"{net_type_version}.gdb"),
                overwrite=True
                )
            # make output feature dataset
            net_type_fd = validate_feature_dataset(
                makePath(clean_gdb, "osm"),
                sr=OUT_CRS,
                overwrite=True
                )

            # import edges
            net_raw = PMT.makePath(osm_raw, net_type_version, "edges.shp")
            # transfer to gdb
            edges = importOSMShape(net_raw, net_type_fd, overwrite=True)

            if net_type == "bike":
                # Enrich features
                classifyBikability(edges)

            # Build network datasets
            template = makePath(REF, f"osm_{net_type}_template.xml")
            makeNetworkDataset(template, net_type_fd, "osm_ND")


def process_bg_estimate_activity_models():
    bg_enrich = makePath(YEAR_GDB_FORMAT, "Enrichment_blockgroups")
    save_path = analyze_blockgroup_model(bg_enrich_path=bg_enrich, bg_key="GEOID", fields="*",
                                         acs_years=ACS_YEARS, lodes_years=LODES_YEARS, save_directory=REF)
    return save_path


def process_bg_apply_activity_models():
    for year in YEARS:
        # Set the inputs based on the year
        gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        bg_enrich = makePath(gdb, "Enrichment_blockgroups")
        bg_geometry = makePath(gdb, "Polygons", "Census_BlockGroups")
        model_coefficients = makePath(REF, "block_group_model_coefficients.csv")
        save_gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))

        # For unobserved years, set a constant share approach
        shares = {}
        if year not in LODES_YEARS:
            wl = np.argmin([abs(x - year) for x in LODES_YEARS])
            shares["LODES"] = bg_enrich.replace(str(year), str(LODES_YEARS[wl]))
        if year not in ACS_YEARS:
            wa = np.argmin([abs(x - year) for x in ACS_YEARS])
            shares["ACS"] = bg_enrich.replace(str(year), str(ACS_YEARS[wa]))
        if len(shares.keys()) == 0:
            shares = None

        # Apply the models
        analyze_blockgroup_apply(year=year, bg_enrich_path=bg_enrich, bg_geometry_path=bg_geometry, bg_id_field="GEOID",
                                 model_coefficients_path=model_coefficients, save_gdb_location=save_gdb,
                                 shares_from=shares)


def process_allocate_bg_to_parcels(overwrite=True):
    for year in YEARS:
        if year == 2019:
            print('holdup')
        # Set the inputs based on the year
        print(f"{year} allocation begun")
        out_gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        parcel_fc = makePath(out_gdb, "Polygons", "Parcels")
        bg_modeled = makePath(out_gdb, "Modeled_blockgroups")
        bg_geom = makePath(out_gdb, "Polygons", "Census_BlockGroups")

        # Allocate
        if overwrite:
            checkOverwriteOutput(output=makePath(out_gdb, "EconDemog_parcels"), overwrite=overwrite)
        analyze_blockgroup_allocate(out_gdb=out_gdb,
                                    bg_modeled=bg_modeled, bg_geom=bg_geom, bg_id_field=BG_COMMON_KEY,
                                    parcel_fc=parcel_fc, parcels_id="FOLIO",
                                    parcel_lu="DOR_UC", parcel_liv_area="TOT_LVG_AREA")


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
        maz_pts = polygonsToPoints(maz_path, maz_fc, MAZ_COMMON_KEY)
        net_suffix = NET_BY_YEAR[year][0]
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
                nd = makePath(
                    NETS_DIR, f"{mode}{net_suffix}.gdb", "osm", "osm_ND")
                skim = PMT.makePath(
                    NETS_DIR, f"{mode}_Skim{net_suffix}.csv")
                if mode == "bike":
                    restrictions = BIKE_RESTRICTIONS
                else:
                    restrictions = None
                # - Create and load problem
                # Confirm "Year" column is included in output table
                genODTable(origin_pts=layer, origin_name_field=MAZ_COMMON_KEY,
                           dest_pts=maz_pts, dest_name_field=MAZ_COMMON_KEY,
                           in_nd=nd, imped_attr=OSM_IMPED, cutoff=BIKE_PED_CUTOFF, net_loader=NET_LOADER,
                           out_table=skim, restrictions=restrictions, use_hierarchy=False, uturns="ALLOW_UTURNS",
                           o_location_fields=None, d_location_fields=None, o_chunk_size=1000)
                # Clean up workspace
                arcpy.Delete_management(layer)
            # Mark as solved
            solved.append(net_suffix)


def process_model_se_data():
    # Summarize parcel data to MAZ
    for year in YEARS:
        # Set output
        out_gdb = validate_geodatabase(makePath(CLEANED, f"PMT_{year}.gdb"))
        out_fds = validate_feature_dataset(makePath(out_gdb, "Polygons"), sr=SR_FL_SPF)
        out_fc = makePath(out_fds, "MAZ")
        maz_se_data = makePath(RAW, "SERPM", "V7", "maz_data.csv")  # TODO: standardize SERPM pathing
        # Summarize parcels to MAZ
        print("--- summarizing MAZ activities from parcels")
        par_fc = makePath(out_gdb, "Polygons", "Parcels")
        se_data = makePath(out_gdb, "EconDemog_parcels")
        # TODO: confirm we can use common keys here?
        par_data = estimate_maz_from_parcels(par_fc=par_fc, par_id_field=PARCEL_COMMON_KEY,
                                             maz_fc=out_fc, maz_id_field=MAZ_COMMON_KEY,
                                             taz_id_field=TAZ_COMMON_KEY, se_data=se_data,
                                             se_id_field=PARCEL_COMMON_KEY, agg_cols=MAZ_AGG_COLS,
                                             consolidations=MAZ_PAR_CONS)
        # Fetch MAZ data (enrollments, etc.)
        print("--- fetching other base-year MAZ data")
        maz_data = pd.read_csv(maz_se_data)
        maz_data.rename(columns=SERPM_RENAMES, inplace=True)
        # Consolidate
        maz_data = consolidate_cols(maz_data, [MAZ_COMMON_KEY, TAZ_COMMON_KEY], MAZ_SE_CONS)
        # Patch for full regional MAZ data
        print("--- combining parcel-based and non-parcel-based MAZ data")
        maz_data = patch_local_regional_maz(par_data, maz_data)
        # Export MAZ table
        print("--- exporting MAZ socioeconomic/demographic data")
        maz_table = makePath(out_gdb, "EconDemog_MAZ")
        dfToTable(maz_data, maz_table)
        # Summarize to TAZ scale
        print("--- summarizing MAZ data to TAZ scale")
        maz_data.drop(columns=[MAZ_COMMON_KEY], inplace=True)
        taz_data = maz_data.groupby(TAZ_COMMON_KEY).sum().reset_index()
        # Export TAZ table
        print("--- exporting TAZ socioeconomic/demographic data")
        taz_table = makePath(out_gdb, "EconDemog_TAZ")
        dfToTable(taz_data, taz_table)


def process_model_skims():
    """
    Assumes transit and auto skims have same fields
    """
    # Get field definitions
    o_field = [k for k in SKIM_RENAMES.keys() if SKIM_RENAMES[k] == "OName"][0]
    d_field = [k for k in SKIM_RENAMES.keys() if SKIM_RENAMES[k] == "DName"][0]

    # Clean each input/output for each model year
    for year in MODEL_YEARS:
        print(year)
        # Setup input/output tables
        # TODO: add trip tables()
        auto_csv = PMT.makePath(RAW, "SERPM", f"GP_Skims_AM_{year}.csv")
        auto_out = PMT.makePath(CLEANED, "SERPM", f"Auto_Skim_{year}.csv")
        transit_csv = PMT.makePath(RAW, "SERPM", f"Tran_Skims_AM_{year}.csv")
        transit_out = PMT.makePath(CLEANED, "SERPM", f"Transit_Skim_{year}.csv")
        inputs = [auto_csv, transit_csv]
        outputs = [auto_out, transit_out]
        for i, o in zip(inputs, outputs):
            print(f"--- cleaning skim {i}")
            clean_skim(in_csv=i, o_field=o_field, d_field=d_field, imp_fields=SKIM_IMP_FIELD, out_csv=o,
                       rename=SKIM_RENAMES, chunksize=100000, thousands=",", dtype=SKIM_DTYPES)


def process_osm_service_areas():
    # Facilities
    #  - Stations
    stations = makePath(BASIC_FEATURES, "SMART_Plan_Stations")
    station_name = "Name"
    # - Parks
    parks = PMT.makePath(CLEANED, "Park_Points.shp")
    parks_name = "NAME"

    # For each analysis year, analyze networks (avoid redundant solves)
    solved = []
    solved_years = []
    modes = ["walk"] #["walk", "bike"]
    dest_grp = ["stn", "parks"]
    runs = ["MERGE", "NO_MERGE", "OVERLAP", "NON_OVERLAP"]
    expected_fcs = [f"{mode}_to_{dg}_{run}"
                    for mode in modes
                    for dg in dest_grp
                    for run in runs
                    ]
    for year in YEARS:
        out_fds_path = makePath(CLEANED, f"PMT_{year}.gdb", "Networks")
        out_fds = validate_feature_dataset(out_fds_path, SR_FL_SPF, overwrite=False)
        # Network setup
        net_suffix = NET_BY_YEAR[year][0]
        if net_suffix in solved:
            # Copy from other year if already solved
            # Set a source to copy network analysis results from based on net_by_year
            # TODO: functionalize source year setting
            target_net = NET_BY_YEAR[year][0]
            source_year = None
            for solved_year in solved_years:
                solved_net = NET_BY_YEAR[solved_year][0]
                if solved_net == target_net:
                    source_year = solved_year
                    break
            source_fds = makePath(CLEANED, f"PMT_{source_year}.gdb", "Networks")
            target_fds = makePath(CLEANED, f"PMT_{year}.gdb", "Networks")
            copy_net_result(source_fds, target_fds, fc_names=expected_fcs)
        else:
            # Solve this network
            print(f"\n{net_suffix}")
            for mode in modes:
                # Create separate service area problems for stations and parks
                nd = makePath(NETS_DIR, f"{mode}{net_suffix}.gdb", "osm", "osm_ND")
                stns = ServiceAreaAnalysis(name=f"{mode}_to_stn", network_dataset=nd, facilities=stations,
                                           name_field=station_name, net_loader=NET_LOADER)
                parks = ServiceAreaAnalysis(name=f"{mode}_to_parks", network_dataset=nd, facilities=parks,
                                            name_field=parks_name, net_loader=NET_LOADER)
                # Solve service area problems
                for sa_prob in [stns, parks]:
                    print(f"\n - {sa_prob.name}")
                    # Set restrictions if needed
                    if "bike" in sa_prob.name:
                        restrictions = BIKE_RESTRICTIONS
                    else:
                        restrictions = ""
                    # Solve (exports output to the out_fds)
                    sa_prob.solve(imped_attr=OSM_IMPED, cutoff=OSM_CUTOFF, out_ws=out_fds,
                                  restrictions=restrictions, use_hierarchy=False, net_location_fields="")
            # Keep track of what's already been solved
            solved.append(net_suffix)
        solved_years.append(year)


def process_centrality():
    # For each analysis year, analyze networks (avoid redundant solves)
    solved = []
    solved_years = []
    node_id = "NODE_ID"
    for year in YEARS:
        net_suffix = NET_BY_YEAR[year][0]
        out_fds_path = makePath(CLEANED, f"PMT_{year}.gdb", "Networks")
        out_fds = validate_feature_dataset(out_fds_path, SR_FL_SPF, overwrite=False)
        out_fc_name = "nodes_bike"
        out_fc = makePath(out_fds, out_fc_name)
        checkOverwriteOutput(out_fc, overwrite=True)
        if net_suffix in solved:
            # Copy from other year if already solved
            # TODO: functionalize source year setting
            target_net = NET_BY_YEAR[year][0]
            source_year = None
            for solved_year in solved_years:
                solved_net = NET_BY_YEAR[solved_year][0]
                if solved_net == target_net:
                    source_year = solved_year
                    break
            source_fds = makePath(CLEANED, f"PMT_{source_year}.gdb", "Networks")
            target_fds = makePath(CLEANED, f"PMT_{year}.gdb", "Networks")
            copy_net_result(source_fds, target_fds, fc_names=out_fc_name)
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
            arcpy.SelectLayerByAttribute_management(in_layer_or_view=edges, selection_type="NEW_SELECTION",
                                                    where_clause=where)
            # Select nodes by location - nodes not touching services roads
            arcpy.SelectLayerByLocation_management(in_layer=nodes, overlap_type="INTERSECT", select_features=edges,
                                                   selection_type="NEW_SELECTION", invert_spatial_relationship="INVERT")
            # Export selected nodes to output fc
            arcpy.FeatureClassToFeatureClass_conversion(in_features=nodes, out_path=out_fds, out_name=out_fc_name)
            oid_field = arcpy.Describe(out_fc).OIDFieldName
            arcpy.AddField_management(in_table=out_fc, field_name=node_id, field_type="TEXT", field_length=8)
            arcpy.CalculateField_management(in_table=out_fc, field=node_id, expression=f"!{oid_field}!",
                                            expression_type="PYTHON")
            # Calculate centrality (iterative OD solves)
            centrality_df = network_centrality(in_nd=in_nd, in_features=out_fc, net_loader=CENTRALITY_NET_LOADER,
                                               name_field=node_id, impedance_attribute=CENTRALITY_IMPED,
                                               cutoff=CENTRALITY_CUTOFF, restrictions=BIKE_RESTRICTIONS, chunksize=1000)
            # Extend out_fc
            PMT.extendTableDf(in_table=out_fc, table_match_field=node_id,
                              df=centrality_df, df_match_field="Node")
            # Delete layers to avoid name collisions
            arcpy.Delete_management(edges)
            arcpy.Delete_management(nodes)
            # Keep track of solved networks
            solved.append(net_suffix)
        solved_years.append(year)


def process_walk_times():
    target_names = ["stn_walk", "park_walk"]  # , "stn_bike", "park_bike"]
    ref_fcs = [
        "walk_to_stn_NON_OVERLAP",
        "walk_to_parks_NON_OVERLAP",
        # "bike_to_stn_NON_OVERLAP",
        # "bike_to_parks_NON_OVERLAP"
    ]
    ref_name_field = "Name"
    ref_time_field = f"ToCumul_{OSM_IMPED}"

    for year in YEARS:
        print(year)
        year_gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        parcels = makePath(year_gdb, "Polygons", "Parcels")
        out_table = makePath(year_gdb, "WalkTime_parcels")
        _append_ = False
        # Iterate over targets and references
        net_fds = makePath(year_gdb, "Networks")
        for tgt_name, ref_fc in zip(target_names, ref_fcs):
            print(f"- {tgt_name}")
            ref_fc = makePath(net_fds, ref_fc)
            walk_time_df = parcel_walk_times(parcel_fc=parcels, parcel_id_field=PARCEL_COMMON_KEY, ref_fc=ref_fc,
                                             ref_name_field=ref_name_field, ref_time_field=ref_time_field,
                                             target_name=tgt_name)
            # Dump df to output table
            if _append_:
                extendTableDf(out_table, PARCEL_COMMON_KEY, walk_time_df, PARCEL_COMMON_KEY)
            else:
                dfToTable(walk_time_df, out_table, overwrite=True)
                _append_ = True
            # Add time bin field
            print("--- classifying time bins")
            bin_field = f"bin_{tgt_name}"
            min_time_field = f"min_time_{tgt_name}"
            parcel_walk_time_bin(in_table=out_table, bin_field=bin_field,
                                 time_field=min_time_field, code_block=TIME_BIN_CODE_BLOCK)


def process_ideal_walk_times():
    targets = ["stn", "parks"]
    for year in YEARS:
        print(year)
        # Key paths
        year_gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        parcels_fc = makePath(year_gdb, "Polygons", "Parcels")
        stations_fc = makePath(BASIC_FEATURES, "SMART_Plan_Stations")
        parks_fc = makePath(CLEANED, "Park_Points.shp")
        out_table = makePath(year_gdb, "IdealWalkTime_parcels")
        target_fcs = [stations_fc, parks_fc]
        # Analyze ideal walk times
        dfs = []
        for target, fc in zip(targets, target_fcs):
            print(f" - {target}")
            # field_suffix = f"{target}_ideal"
            df = parcel_ideal_walk_time(parcels_fc=parcels_fc, parcel_id_field=PARCEL_COMMON_KEY, target_fc=fc,
                                        target_name_field="Name", radius=IDEAL_WALK_RADIUS, target_name=target,
                                        overlap_type="HAVE_THEIR_CENTER_IN", sr=None, assumed_mph=IDEAL_WALK_MPH)
            dfs.append(df)
        # Combine dfs, dfToTable
        # TODO: This assumes only 2 data frames, but could be generalized to merge multiple frames
        combo_df = dfs[0].merge(right=dfs[1], how="outer", on=PARCEL_COMMON_KEY)
        dfToTable(combo_df, out_table)
        # Add bin fields
        for target in targets:
            min_time_field = f"min_time_{target}"
            bin_field = f"bin_{target}"
            parcel_walk_time_bin(in_table=out_table, bin_field=bin_field,
                                 time_field=min_time_field, code_block=TIME_BIN_CODE_BLOCK)


def process_access():
    for year in YEARS:
        print(f"Analysis year: {year}")
        gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        for mode in ACCESS_MODES:
            print(f"--- {mode}")
            # Get reference info from globals
            source, scale, id_field = MODE_SCALE_REF[mode]
            osm_year, model_year = NET_BY_YEAR[year]
            if source == "OSM_Networks":
                skim_year = osm_year
            else:
                skim_year = model_year
            # Look up zone and skim data for each mode
            zone_data = makePath(gdb, f"EconDemog_{scale}")
            skim_data = makePath(CLEANED, source, f"{mode}_Skim_{skim_year}.csv")
            # Analyze access
            atd_df = summarizeAccess(skim_table=skim_data, o_field=SKIM_O_FIELD, d_field=SKIM_D_FIELD,
                                     imped_field=SKIM_IMP_FIELD, se_data=zone_data, id_field=id_field,
                                     act_fields=D_ACT_FIELDS, imped_breaks=ACCESS_TIME_BREAKS,
                                     units=ACCESS_UNITS, join_by="D",
                                     dtype=SKIM_DTYPES, chunk_size=100000
                                     )
            afo_df = summarizeAccess(skim_table=skim_data, o_field=SKIM_O_FIELD, d_field=SKIM_D_FIELD,
                                     imped_field=SKIM_IMP_FIELD, se_data=zone_data, id_field=id_field,
                                     act_fields=O_ACT_FIELDS, imped_breaks=ACCESS_TIME_BREAKS,
                                     units=ACCESS_UNITS, join_by="O",
                                     dtype=SKIM_DTYPES, chunk_size=100000
                                     )
            # Merge tables
            atd_df.rename(columns={SKIM_O_FIELD: id_field}, inplace=True)
            afo_df.rename(columns={SKIM_D_FIELD: id_field}, inplace=True)
            full_table = atd_df.merge(right=afo_df, on=id_field)

            # Export output
            out_table = makePath(gdb, f"Access_{scale}_{mode}")
            dfToTable(full_table, out_table, overwrite=True)


def process_contiguity(overwrite=True):
    county_fc = makePath(BASIC_FEATURES, "MiamiDadeCountyBoundary")
    chunk_fishnet = generate_chunking_fishnet(template_fc=county_fc, out_fishnet_name="quadrats", chunks=CTGY_CHUNKS)
    for year in YEARS:
        print(f"Processing Contiguity for {year}")
        gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        parcel_fc = makePath(gdb, "Polygons", "Parcels")
        buildings = makePath(RAW, "OpenStreetMap", "buildings_q1_2021", "OSM_Buildings_20210201074346.shp")

        ctgy_full = contiguity_index(quadrats_fc=chunk_fishnet, parcels_fc=parcel_fc, buildings_fc=buildings,
                                     parcels_id_field=PARCEL_COMMON_KEY,
                                     cell_size=CTGY_CELL_SIZE, weights=CTGY_WEIGHTS)
        if CTGY_SAVE_FULL:
            full_path = makePath(gdb, "Contiguity_full_singlepart")
            dfToTable(df=ctgy_full, out_table=full_path, overwrite=True)
        ctgy_summarized = contiguity_summary(full_results_df=ctgy_full, parcels_id_field=PARCEL_COMMON_KEY,
                                             summary_funcs=CTGY_SUMMARY_FUNCTIONS, area_scaling=CTGY_SCALE_AREA)
        summarized_path = makePath(gdb, "Contiguity_parcels")
        dfToTable(df=ctgy_summarized, out_table=summarized_path, overwrite=overwrite)


def process_lu_diversity():
    for year in YEARS:
        gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        parcel_fc = makePath(gdb, "Polygons", "Parcels")
        if "{year}" in DIV_AGG_GEOM_FORMAT:
            agg_fc = DIV_AGG_GEOM_FORMAT.replace("{year}", str(year))
        else:
            agg_fc = DIV_AGG_GEOM_FORMAT
        lu_dict = lu_diversity(parcels_path=parcel_fc, parcels_id_field=PARCEL_COMMON_KEY,
                               parcels_land_use_field=PARCEL_LU_COL,
                               land_use_recode_path=LU_RECODE_TABLE, land_use_recode_field=LU_RECODE_FIELD,
                               on_field=DIV_ON_FIELD,
                               aggregate_geometry_path=agg_fc, aggregate_geometry_id_field=DIV_AGG_GEOM_ID,
                               buffer_diversity=DIV_AGG_GEOM_BUFFER, relevant_land_uses=DIV_RELEVANT_LAND_USES,
                               how=DIV_METRICS, chisq_props=DIV_CHISQ_PROPS,
                               regional_adjustment=DIV_REGIONAL_ADJ, regional_constants=DIV_REGIONAL_CONSTS)
        for key, value in lu_dict.items():
            write_to = makePath(gdb, ''.join(["Diversity_", key]))
            dfToTable(df=value, out_table=write_to)


def process_permits_units_reference():
    parcels = makePath(YEAR_GDB_FORMAT.replace("YEAR", str(max(YEARS))), "Polygons", "Parcels")
    permits = makePath(YEAR_GDB_FORMAT.replace("YEAR", str(max(YEARS))), "Points", "BuildingPermits")
    purt = prep_permits_units_reference(parcels_path=parcels, permits_path=permits,
                                        lu_match_field=PARCEL_LU_COL,
                                        parcels_living_area_field=PARCEL_BLD_AREA,
                                        permits_units_field=PERMITS_UNITS_FIELD,
                                        permits_living_area_name=PERMITS_BLD_AREA_NAME,
                                        units_match_dict=PARCEL_REF_TABLE_UNITS_MATCH)
    save_to = makePath(REF, "permits_units_reference_table.csv")
    purt.to_csv(save_to)
    # dfToTable(df=purt, out_table=save_to, overwrite=True)


def process_short_term_parcels():
    parcels = makePath(YEAR_GDB_FORMAT.replace("YEAR", str(max(YEARS))), "Polygons", "Parcels")
    permits = makePath(YEAR_GDB_FORMAT.replace("YEAR", str(max(YEARS))), "Points", "BuildingPermits")
    # TODO: needs a permenant solution for this temporary dataset
    # TODO: assess whether this needs to be done in builder.py
    save_gdb = validate_geodatabase(makePath(ROOT, CLEANED, "near_term_parcels.gdb"))
    permits_ref_df = prep_permits_units_reference(parcels_path=parcels, permits_path=permits,
                                                  lu_match_field=PARCEL_LU_COL,
                                                  parcels_living_area_field=PARCEL_BLD_AREA,
                                                  permits_units_field=PERMITS_UNITS_FIELD,
                                                  permits_living_area_name=PERMITS_BLD_AREA_NAME,
                                                  units_match_dict=PARCEL_REF_TABLE_UNITS_MATCH)
    build_short_term_parcels(parcels_path=parcels, permits_path=permits,
                             permits_ref_df=permits_ref_df, parcels_id_field=PARCEL_COMMON_KEY,
                             parcels_lu_field=PARCEL_LU_COL, parcels_living_area_field=PARCEL_BLD_AREA,
                             parcels_land_value_field=PARCEL_LAND_VALUE, parcels_total_value_field=PARCEL_JUST_VALUE,
                             parcels_buildings_field=PARCEL_BUILDINGS, permits_id_field=PERMITS_ID_FIELD,
                             permits_lu_field=PERMITS_LU_FIELD, permits_units_field=PERMITS_UNITS_FIELD,
                             permits_values_field=PERMITS_VALUES_FIELD, permits_cost_field=PERMITS_COST_FIELD,
                             save_gdb_location=save_gdb, units_field_match_dict=SHORT_TERM_PARCELS_UNITS_MATCH)


if __name__ == "__main__":
    # setup basic features
    # SETUP CLEAN DATA
    # -----------------------------------------------
    # UDB might be ignored as this isnt likely to change and can be updated ad-hoc
    # process_udb() # TODO: udbToPolygon failing to create a feature class to store the output (likley an arcpy overrun)

    # process_basic_features() # TESTED # TODO: include the status field to drive selector widget

    # setup any basic normalized geometries
    # process_normalized_geometries()  # TESTED # TODO: standardize column names

    # copies downloaded parcel data and only minimally necessary attributes into yearly gdb
    # process_parcels() # TESTED

    # cleans and geocodes permits to associated parcels
    # process_permits() # TESTED CR 03/01/21

    # merges park data into a single point featureset and polygon feartureset
    # process_parks() # TESTED 02/26/21 CR

    # cleans and geocodes crashes to included Lat/Lon
    # process_crashes() ''' deprecated '''

    # cleans and geocodes transit into included Lat/Lon
    # process_transit() # TESTED 02/26/21 CR

    # ENRICH DATA
    # -----------------------------------------------
    # Updates parcels based on permits for near term analysis
    # process_permits_units_reference() # deprecated, the output df is now just part of the following process
    # process_short_term_parcels() # TESTED 3/1/21 # TODO: needs to be broken down into smaller functions

    # prepare near term parcels
    # enrich_block_groups() # TESTED CR 03/01/21

    # process_bg_estimate_activity_models() # TESTED CR 03/02/21
    # process_bg_apply_activity_models()      # TESTED CR 03/02/21
    # process_allocate_bg_to_parcels()

    # TODO: check use validate_fds method instead of makePath in these and helper funcs
    # record parcel land use groupings and multipliers
    # process_parcel_land_use()

    # prepare MAZ and TAZ socioeconomic/demographic data
    # process_model_se_data()

    # generate contiguity index for all years
    process_contiguity()

    # NETWORK ANALYSES
    # -----------------------------------------------
    # build osm networks from templates
    # process_osm_networks() #Tested by AB 2/26/21

    # assess network centrality for each bike network
    # process_centrality() # Tested by AB 3/2/21

    # analyze osm network service areas
    # process_osm_service_areas() # Tested by AB 2/28/21

    # analyze walk/bike times among MAZs
    # process_osm_skims() #Tested by AB 3/2/21

    # record parcel walk times
    # process_walk_times() # Tested by AB 3/2/21

    # record parcel ideal walk times
    # process_ideal_walk_times() # Tested by AB 3/2/21

    # prepare serpm TAZ-level travel skims
    # process_model_skims()

    # DEPENDENT ANALYSIS
    # -----------------------------------------------
    # analyze access by MAZ, TAZ
    # process_access()

    # prepare TAZ trip length and VMT rates
    # process_travel_stats() #AB
    # TODO: script to calculate rates so year-over-year diffs can be estimated

    # only updated when new impervious data are made available
    # process_imperviousness() # AW
    # TODO: ISGM for year-over-year changes? (low priority)

# TODO: incorporate a project setup script or at minimum a yearly build geodatabase function/logic
# TODO: handle multi-year data as own function
# TODO: add logging/print statements for procedure tracking (low priority)
