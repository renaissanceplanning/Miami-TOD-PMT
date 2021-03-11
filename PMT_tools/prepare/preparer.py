"""
preparation scripts used set up cleaned geodatabases
"""
import os, sys

sys.path.insert(0, os.getcwd())
# config global variables
import PMT_tools.config.prepare_config as prep_conf

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
    RAW = PMT.validate_directory(directory=PMT.makePath(ROOT, 'PROCESSING_TEST', "RAW"))
    CLEANED = PMT.validate_directory(directory=PMT.makePath(ROOT, 'PROCESSING_TEST', "CLEANED"))
    DATA = ROOT
    BASIC_FEATURES = PMT.makePath(CLEANED, "PMT_BasicFeatures.gdb")
    YEAR_GDB_FORMAT = PMT.makePath(CLEANED, "PMT_YEAR.gdb")
    REF = PMT.makePath(ROOT, "Reference")
    RIF_CAT_CODE_TBL = PMT.makePath(REF, "road_impact_fee_cat_codes.csv")
    DOR_LU_CODE_TBL = PMT.makePath(REF, "Land_Use_Recode.csv")
    YEARS = YEARS + ["NearTerm"]


def process_normalized_geometries(overwrite=True):
    """ Census Data and TAZ/MAZ
        - these are geometries that only consist of a dataset key
        - copies census block groups and blocks into yearly GDB, subsets to county and tags with year
        - copied TAZ and MAZ into yearly GDB, subsets to county and tags with year
    """
    # RAW
    raw_block = makePath(RAW, "CENSUS", "tl_2019_12_tabblock10", "tl_2019_12_tabblock10.shp")
    raw_block_groups = makePath(RAW, "CENSUS", "tl_2019_12_bg", "tl_2019_12_bg.shp")
    raw_TAZ = makePath(RAW, "TAZ.shp")
    raw_MAZ = makePath(RAW, "MAZ.shp")
    raw_sum_areas = makePath(BASIC_FEATURES, "SummaryAreas")

    # CLEANED
    blocks = "Census_Blocks"
    block_groups = "Census_BlockGroups"
    TAZ = "TAZ"
    MAZ = "MAZ"
    sum_areas = "SummaryAreas"
    for year in YEARS:
        for raw, cleaned, cols in zip([raw_block, raw_block_groups, raw_TAZ, raw_MAZ, raw_sum_areas],
                                      [blocks, block_groups, TAZ, MAZ, sum_areas],
                                      [prep_conf.BLOCK_COMMON_KEY, prep_conf.BG_COMMON_KEY, prep_conf.TAZ_COMMON_KEY,
                                       prep_conf.MAZ_COMMON_KEY, prep_conf.SUMMARY_AREAS_COMMON_KEY]):
            temp_file = make_inmem_path()
            out_path = PMT.validate_feature_dataset(makePath(CLEANED, f"PMT_{year}.gdb", "Polygons"), sr=SR_FL_SPF)
            logger.log_msg(f"--- building normalized {cleaned} in {out_path}")
            out_data = makePath(out_path, cleaned)
            prep_feature_class(in_fc=raw, geom="POLYGON", out_fc=temp_file, use_cols=cols, rename_dict=None)
            lyr = arcpy.MakeFeatureLayer_management(in_features=temp_file, out_layer="lyr")
            if overwrite:
                checkOverwriteOutput(output=out_data, overwrite=overwrite)
            logger.log_msg(f"--- writing out geometries and {cols} only")
            arcpy.CopyFeatures_management(in_features=lyr, out_feature_class=out_data)
            arcpy.CalculateField_management(in_table=out_data, field="Year", expression=year,
                                            expression_type="PYTHON3", field_type="LONG")


def process_basic_features():
    # TODO: add check for existing basic features, and compare for changes
    print("Making basic features")
    makeBasicFeatures(bf_gdb=BASIC_FEATURES, stations_fc=prep_conf.BASIC_STATIONS,
                      stn_diss_fields=prep_conf.STN_DISS_FIELDS, stn_corridor_fields=prep_conf.STN_CORRIDOR_FIELDS,
                      alignments_fc=prep_conf.BASIC_ALIGNMENTS, align_diss_fields=prep_conf.ALIGN_DISS_FIELDS,
                      stn_buff_dist=prep_conf.STN_BUFF_DIST, align_buff_dist=prep_conf.ALIGN_BUFF_DIST,
                      stn_areas_fc=prep_conf.BASIC_STN_AREAS, corridors_fc=prep_conf.BASIC_CORRIDORS,
                      long_stn_fc=prep_conf.BASIC_LONG_STN,
                      rename_dict=prep_conf.BASIC_RENAME_DICT, overwrite=True)

    print("Making summarization features")
    makeSummaryFeatures(bf_gdb=prep_conf.BASIC_FEATURES, long_stn_fc=prep_conf.BASIC_LONG_STN,
                        corridors_fc=prep_conf.BASIC_CORRIDORS,
                        cor_name_field=prep_conf.CORRIDOR_NAME_FIELD, out_fc=prep_conf.BASIC_SUM_AREAS,
                        stn_buffer_meters=prep_conf.STN_BUFF_METERS, stn_name_field=prep_conf.STN_NAME_FIELD,
                        stn_cor_field=prep_conf.STN_LONG_CORRIDOR, overwrite=True)


def process_parks(overwrite=True):
    """
    parks - merges park polygons into one and formats both poly and point park data.
    """
    print('PARKS:')
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
    print("--- cleaning park points and polys")
    prep_park_polys(in_fcs=park_polys, geom="POLYGON", out_fc=out_park_polys,
                    use_cols=poly_use_cols, rename_dicts=poly_rename_cols, unique_id=prep_conf.PARK_POLY_COMMON_KEY)
    prep_feature_class(in_fc=park_points, geom="POINT", out_fc=out_park_points,
                       use_cols=prep_conf.PARK_POINT_COLS, unique_id=prep_conf.PARK_POINTS_COMMON_KEY)
    for year in YEARS:
        print(f"\t--- adding park points to {year} gdb")
        out_path = PMT.validate_feature_dataset(fds_path=makePath(CLEANED,
                                                                  YEAR_GDB_FORMAT.replace("YEAR", str(year)),
                                                                  "Points"),
                                                sr=SR_FL_SPF)
        arcpy.FeatureClassToFeatureClass_conversion(in_features=out_park_points, out_path=out_path,
                                                    out_name="Park_Points")


def process_crashes():
    ''' crashes '''
    crash_json = makePath(RAW, "Safety_Security", "bike_ped.geojson")
    all_features = geojson_to_feature_class_arc(geojson_path=crash_json, geom_type='POINT')
    arcpy.FeatureClassToFeatureClass_conversion(all_features, RAW, "DELETE_crashes.shp")
    # reformat attributes and keep only useful
    clean_and_drop(feature_class=all_features, use_cols=prep_conf.USE_CRASH, rename_dict=prep_conf.CRASH_FIELDS_DICT)
    for year in YEARS:
        # use year variable to setup outputs
        out_gdb = PMT.validate_geodatabase(os.path.join(CLEANED, f"PMT_{year}.gdb"))
        FDS = PMT.validate_feature_dataset(makePath(out_gdb, "Points"), sr=SR_FL_SPF)
        out_name = 'BikePedCrashes'
        year_wc = f'"YEAR" = {year}'
        # clean and format crash data
        prep_bike_ped_crashes(in_fc=all_features, out_path=FDS, out_name=out_name,
                              where_clause=year_wc)
    arcpy.Delete_management(in_data=all_features)


def process_permits(overwrite=True):
    ''' permits '''
    permit_csv = makePath(RAW, "BUILDING_PERMITS", "Road Impact Fee Collection Report -- 2019.csv")

    out_gdb = PMT.validate_geodatabase(os.path.join(CLEANED, f"PMT_2019.gdb"))
    in_fds = PMT.validate_feature_dataset(fds_path=makePath(out_gdb, "Polygons"), sr=SR_FL_SPF)
    parcels = makePath(in_fds, "Parcels")
    out_fds = PMT.validate_feature_dataset(makePath(out_gdb, "Points"), sr=SR_FL_SPF)
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
    """ Transit Ridership
        - converts a list of ridership files to points with attributes cleaned
        YEAR over YEARS
            - cleans and consolidates transit data into Year POINTS FDS
            - if YEAR == NearTerm:
            -     most recent year is copied over
        - NOTE: transit folder reflects current location, needs update to reflect cleaner structure
    """
    transit_folder = PMT.validate_directory(makePath(RAW, "TRANSIT", "TransitRidership_byStop"))
    transit_shape_fields = [prep_conf.TRANSIT_LONG, prep_conf.TRANSIT_LAT]
    print("PROCESSING TRANSIT RIDERSHIP... ")
    for year in YEARS:
        print(f"--- cleaning ridership for {year}")
        out_gdb = PMT.validate_geodatabase(os.path.join(CLEANED, f"PMT_{year}.gdb"))
        FDS = PMT.validate_feature_dataset(fds_path=makePath(out_gdb, "Points"), sr=SR_FL_SPF)
        out_name = 'TransitRidership'
        transit_xls_file = makePath(transit_folder, prep_conf.TRANSIT_RIDERSHIP_TABLES[year])
        transit_out_path = makePath(FDS, out_name)
        if overwrite:
            checkOverwriteOutput(output=transit_out_path, overwrite=overwrite)
        prep_transit_ridership(in_table=transit_xls_file, rename_dict=prep_conf.TRANSIT_FIELDS_DICT,
                               unique_id=prep_conf.TRANSIT_COMMON_KEY, shape_fields=transit_shape_fields,
                               from_sr=prep_conf.IN_CRS, to_sr=prep_conf.OUT_CRS, out_fc=transit_out_path)
        print(f"--- ridership cleaned for {year} and located in {transit_out_path}")


def process_parcels(overwrite=True):
    print("PROCESSING PARCELS... ")
    parcel_folder = makePath(RAW, "Parcels")
    for year in YEARS:
        print(f"Setting up parcels for {year}")
        out_gdb = PMT.validate_geodatabase(makePath(CLEANED, f"PMT_{year}.gdb"))
        out_fds = PMT.validate_feature_dataset(makePath(out_gdb, "Polygons"), sr=SR_FL_SPF)
        # source data
        in_fc = makePath(parcel_folder, f"Miami_{year}.shp")
        in_csv = makePath(parcel_folder, f"NAL_{year}_23Dade_F.csv")
        # output feature
        out_fc = makePath(out_fds, "Parcels")
        # input fix variables
        renames = prep_conf.PARCEL_COLS.get(year, {})
        usecols = prep_conf.PARCEL_USE_COLS.get(year, prep_conf.PARCEL_USE_COLS["DEFAULT"])
        csv_kwargs = {"dtype": {"PARCEL_ID": str, "CENSUS_BK": str},
                      "usecols": usecols}
        if overwrite:
            checkOverwriteOutput(output=out_fc, overwrite=overwrite)
        prep_parcels(in_fc=in_fc, in_tbl=in_csv, out_fc=out_fc, fc_key_field=prep_conf.PARCEL_DOR_KEY,
                     new_fc_key_field=prep_conf.PARCEL_COMMON_KEY, tbl_key_field=prep_conf.PARCEL_NAL_KEY,
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
        bg_df = enrich_bg_with_parcels(bg_fc=bg_fc, bg_id_field=prep_conf.BG_COMMON_KEY,
                                       parcels_fc=parcels_fc, par_id_field=prep_conf.PARCEL_COMMON_KEY,
                                       par_lu_field=prep_conf.LAND_USE_COMMON_KEY, par_bld_area=prep_conf.PARCEL_BLD_AREA_COL,
                                       sum_crit=prep_conf.LODES_CRITERIA, par_sum_fields=prep_conf.BG_PAR_SUM_FIELDS)
        # Save enriched data
        dfToTable(bg_df, out_tbl, overwrite=True)
        # Extend BG output with ACS/LODES data
        in_tables = [race_tbl, commute_tbl, lodes_tbl]
        in_tbl_ids = [prep_conf.ACS_COMMON_KEY, prep_conf.ACS_COMMON_KEY, prep_conf.LODES_COMMON_KEY]
        in_tbl_flds = [prep_conf.ACS_RACE_FIELDS, prep_conf.ACS_COMMUTE_FIELDS, prep_conf.LODES_FIELDS]
        for table, tbl_id, fields in zip(in_tables, in_tbl_ids, in_tbl_flds):
            if arcpy.Exists(table):
                print(f"--- enriching parcels with {table} data")
                enrich_bg_with_econ_demog(tbl_path=out_tbl, tbl_id_field=prep_conf.BG_COMMON_KEY,
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
        par_fields = [prep_conf.PARCEL_COMMON_KEY, "LND_SQFOOT"]
        tbl_lu_field = "DOR_UC"
        dtype = {"DOR_UC": int}
        default_vals = {
            prep_conf.PARCEL_COMMON_KEY: "-1",
            "LND_SQFOOT": 0,
            par_lu_field: 999
        }
        par_df = prep_parcel_land_use_tbl(parcels_fc=parcels_fc, parcel_lu_field=par_lu_field, parcel_fields=par_fields,
                                          lu_tbl=lu_table, tbl_lu_field=tbl_lu_field,
                                          dtype_map=dtype, null_value=default_vals)
        # Calculate area columns
        for par_lu_col in prep_conf.PARCEL_LU_AREAS.keys():
            ref_col, crit = prep_conf.PARCEL_LU_AREAS[par_lu_col]
            par_df[par_lu_col] = np.select(
                [par_df[ref_col] == crit], [par_df[prep_conf.PARCEL_AREA_COL]], 0.0
            )
        # Export result
        checkOverwriteOutput(out_table, overwrite=True)
        dfToTable(par_df, out_table)


def process_imperviousness():
    impervious_download = makePath(RAW, "Imperviousness.zip")
    county_boundary = makePath(DATA, "PMT_BasicFeatures.gdb", "BasicFeatures", "MiamiDadeCountyBoundary")
    out_dir = PMT.validate_directory(makePath(CLEANED, "IMPERVIOUS"))
    impv_raster = prep_imperviousness(zip_path=impervious_download, clip_path=county_boundary, out_dir=out_dir,
                                      transform_crs=EPSG_FLSPF)
    for year in YEARS:
        gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        if "{year}" in prep_conf.ZONE_GEOM_FORMAT:
            zone_fc = prep_conf.ZONE_GEOM_FORMAT.replace("{year}", str(year))
        else:
            zone_fc = prep_conf.ZONE_GEOM_FORMAT
        impv = analyze_imperviousness(impervious_path=impv_raster,
                                      zone_geometries_path=zone_fc,
                                      zone_geometries_id_field=prep_conf.ZONE_GEOM_ID)
        zone_name = os.path.split(zone_fc)[1].lower()
        write_to = makePath(gdb, ''.join(["Imperviousness_", zone_name]))
        dfToTable(impv, write_to)


def process_osm_networks():
    net_versions = sorted({v[0] for v in prep_conf.NET_BY_YEAR.values()})
    for net_version in net_versions:
        # Import edges
        osm_raw = PMT.makePath(RAW, "OpenStreetMap")
        for net_type in ["bike", "walk"]:
            net_type_version = f"{net_type}_{net_version}"
            # Make output geodatabase
            clean_gdb = PMT.validate_geodatabase(
                makePath(prep_conf.NETS_DIR, f"{net_type_version}.gdb"),
                overwrite=True
            )
            # make output feature dataset
            net_type_fd = PMT.validate_feature_dataset(
                makePath(clean_gdb, "osm"),
                sr=prep_conf.OUT_CRS,
                overwrite=True
            )

            # import edges
            net_raw = PMT.makePath(osm_raw, net_type_version, "edges.shp")
            # transfer to gdb
            edges = importOSMShape(net_raw, net_type_fd, overwrite=True)

            if net_type == "bike":
                # Enrich features
                classifyBikability(edges)

                # Copy bike edges to year geodatabases
                for year, nv in prep_conf.NET_BY_YEAR.items():
                    if nv == net_version:
                        out_path = makePath(CLEANED, "PMT_{year}.gdb", "Networks")
                        out_name = "edges_bike"
                        arcpy.FeatureClassToFeatureClass_conversion(
                            in_features=edges, out_path=out_path, out_name=out_name)
                        out_fc = makePath(out_path, out_name)
                        arcpy.CalculateField_management(
                            in_table=out_fc, field="Year", expression=str(year), field_type="LONG")

            # Build network datasets
            template = makePath(REF, f"osm_{net_type}_template.xml")
            makeNetworkDataset(template, net_type_fd, "osm_ND")


def process_bg_estimate_activity_models():
    bg_enrich = makePath(YEAR_GDB_FORMAT, "Enrichment_blockgroups")
    save_path = analyze_blockgroup_model(bg_enrich_path=bg_enrich, bg_key="GEOID", fields="*",
                                         acs_years=prep_conf.ACS_YEARS, lodes_years=prep_conf.LODES_YEARS,
                                         save_directory=REF)
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
        if year not in prep_conf.LODES_YEARS:
            wl = np.argmin([abs(x - year) for x in prep_conf.LODES_YEARS])
            shares["LODES"] = bg_enrich.replace(str(year), str(prep_conf.LODES_YEARS[wl]))
        if year not in prep_conf.ACS_YEARS:
            wa = np.argmin([abs(x - year) for x in prep_conf.ACS_YEARS])
            shares["ACS"] = bg_enrich.replace(str(year), str(prep_conf.ACS_YEARS[wa]))
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
                                    bg_modeled=bg_modeled, bg_geom=bg_geom, bg_id_field=prep_conf.BG_COMMON_KEY,
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
                nd = makePath(
                    prep_conf.NETS_DIR, f"{mode}{net_suffix}.gdb", "osm", "osm_ND")
                skim = PMT.makePath(
                    prep_conf.NETS_DIR, f"{mode}_Skim{net_suffix}.csv")
                if mode == "bike":
                    restrictions = prep_conf.BIKE_RESTRICTIONS
                else:
                    restrictions = None
                # - Create and load problem
                # Confirm "Year" column is included in output table
                genODTable(origin_pts=layer, origin_name_field=prep_conf.MAZ_COMMON_KEY,
                           dest_pts=maz_pts, dest_name_field=prep_conf.MAZ_COMMON_KEY,
                           in_nd=nd, imped_attr=prep_conf.OSM_IMPED, cutoff=prep_conf.BIKE_PED_CUTOFF, net_loader=prep_conf.NET_LOADER,
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
        out_gdb = PMT.validate_geodatabase(makePath(CLEANED, f"PMT_{year}.gdb"))
        out_fds = PMT.validate_feature_dataset(makePath(out_gdb, "Polygons"), sr=SR_FL_SPF)
        out_fc = makePath(out_fds, "MAZ")
        maz_se_data = makePath(RAW, "SERPM", "V7", "maz_data.csv")  # TODO: standardize SERPM pathing
        # Summarize parcels to MAZ
        print("--- summarizing MAZ activities from parcels")
        par_fc = makePath(out_gdb, "Polygons", "Parcels")
        se_data = makePath(out_gdb, "EconDemog_parcels")
        # TODO: confirm we can use common keys here?
        par_data = estimate_maz_from_parcels(par_fc=par_fc, par_id_field=prep_conf.PARCEL_COMMON_KEY,
                                             maz_fc=out_fc, maz_id_field=prep_conf.MAZ_COMMON_KEY,
                                             taz_id_field=prep_conf.TAZ_COMMON_KEY, se_data=se_data,
                                             se_id_field=prep_conf.PARCEL_COMMON_KEY, agg_cols=prep_conf.MAZ_AGG_COLS,
                                             consolidations=prep_conf.MAZ_PAR_CONS)
        # Fetch MAZ data (enrollments, etc.)
        print("--- fetching other base-year MAZ data")
        maz_data = pd.read_csv(maz_se_data)
        maz_data.rename(columns=prep_conf.SERPM_RENAMES, inplace=True)
        # Consolidate
        maz_data = consolidate_cols(maz_data, [prep_conf.MAZ_COMMON_KEY, prep_conf.TAZ_COMMON_KEY],
                                    prep_conf.MAZ_SE_CONS)
        # Patch for full regional MAZ data
        print("--- combining parcel-based and non-parcel-based MAZ data")
        maz_data = patch_local_regional_maz(par_data, maz_data)
        # Export MAZ table
        print("--- exporting MAZ socioeconomic/demographic data")
        maz_table = makePath(out_gdb, "EconDemog_MAZ")
        dfToTable(maz_data, maz_table)
        # Summarize to TAZ scale
        print("--- summarizing MAZ data to TAZ scale")
        maz_data.drop(columns=[prep_conf.MAZ_COMMON_KEY], inplace=True)
        taz_data = maz_data.groupby(prep_conf.TAZ_COMMON_KEY).sum().reset_index()
        # Export TAZ table
        print("--- exporting TAZ socioeconomic/demographic data")
        taz_table = makePath(out_gdb, "EconDemog_TAZ")
        dfToTable(taz_data, taz_table)


def process_model_skims():
    """
    Assumes transit and auto skims have same fields
    """
    # Get field definitions
    o_field = [k for k in prep_conf.SKIM_RENAMES.keys() if prep_conf.SKIM_RENAMES[k] == "OName"][0]
    d_field = [k for k in prep_conf.SKIM_RENAMES.keys() if prep_conf.SKIM_RENAMES[k] == "DName"][0]

    # Clean each input/output for each model year
    for year in prep_conf.MODEL_YEARS:
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
            clean_skim(in_csv=i, o_field=o_field, d_field=d_field, imp_fields=prep_conf.SKIM_IMP_FIELD, out_csv=o,
                       rename=prep_conf.SKIM_RENAMES, chunksize=100000, thousands=",", dtype=prep_conf.SKIM_DTYPES)


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
    modes = ["walk"]  # ["walk", "bike"]
    dest_grp = ["stn", "parks"]
    runs = ["MERGE", "NO_MERGE", "OVERLAP", "NON_OVERLAP"]
    expected_fcs = [f"{mode}_to_{dg}_{run}"
                    for mode in modes
                    for dg in dest_grp
                    for run in runs
                    ]
    for year in YEARS:
        out_fds_path = makePath(CLEANED, f"PMT_{year}.gdb", "Networks")
        out_fds = PMT.validate_feature_dataset(out_fds_path, SR_FL_SPF, overwrite=False)
        # Network setup
        net_suffix = prep_conf.NET_BY_YEAR[year][0]
        if net_suffix in solved:
            # Copy from other year if already solved
            # Set a source to copy network analysis results from based on net_by_year
            # TODO: functionalize source year setting
            target_net = prep_conf.NET_BY_YEAR[year][0]
            source_year = None
            for solved_year in solved_years:
                solved_net = prep_conf.NET_BY_YEAR[solved_year][0]
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
                nd = makePath(prep_conf.NETS_DIR, f"{mode}{net_suffix}.gdb", "osm", "osm_ND")
                stns = ServiceAreaAnalysis(name=f"{mode}_to_stn", network_dataset=nd, facilities=stations,
                                           name_field=station_name, net_loader=prep_conf.NET_LOADER)
                parks = ServiceAreaAnalysis(name=f"{mode}_to_parks", network_dataset=nd, facilities=parks,
                                            name_field=parks_name, net_loader=prep_conf.NET_LOADER)
                # Solve service area problems
                for sa_prob in [stns, parks]:
                    print(f"\n - {sa_prob.name}")
                    # Set restrictions if needed
                    if "bike" in sa_prob.name:
                        restrictions = prep_conf.BIKE_RESTRICTIONS
                    else:
                        restrictions = ""
                    # Solve (exports output to the out_fds)
                    sa_prob.solve(imped_attr=prep_conf.OSM_IMPED, cutoff=prep_conf.OSM_CUTOFF, out_ws=out_fds,
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
        net_suffix = prep_conf.NET_BY_YEAR[year][0]
        out_fds_path = makePath(CLEANED, f"PMT_{year}.gdb", "Networks")
        out_fds = PMT.validate_feature_dataset(out_fds_path, SR_FL_SPF, overwrite=False)
        out_fc_name = "nodes_bike"
        out_fc = makePath(out_fds, out_fc_name)
        checkOverwriteOutput(out_fc, overwrite=True)
        if net_suffix in solved:
            # Copy from other year if already solved
            # TODO: functionalize source year setting
            target_net = prep_conf.NET_BY_YEAR[year][0]
            source_year = None
            for solved_year in solved_years:
                solved_net = prep_conf.NET_BY_YEAR[solved_year][0]
                if solved_net == target_net:
                    source_year = solved_year
                    break
            source_fds = makePath(CLEANED, f"PMT_{source_year}.gdb", "Networks")
            target_fds = makePath(CLEANED, f"PMT_{year}.gdb", "Networks")
            copy_net_result(source_fds, target_fds, fc_names=out_fc_name)
        else:
            # Get node and edge features as layers
            print(f"\n{net_suffix}")
            in_fds = makePath(prep_conf.NETS_DIR, f"bike{net_suffix}.gdb", "osm")
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
            centrality_df = network_centrality(in_nd=in_nd, in_features=out_fc, net_loader=prep_conf.CENTRALITY_NET_LOADER,
                                               name_field=node_id, impedance_attribute=prep_conf.CENTRALITY_IMPED,
                                               cutoff=prep_conf.CENTRALITY_CUTOFF, restrictions=prep_conf.BIKE_RESTRICTIONS, chunksize=1000)
            # Extend out_fc
            PMT.extendTableDf(in_table=out_fc, table_match_field=node_id,
                              df=centrality_df, df_match_field="Node")
            # Delete layers to avoid name collisions
            arcpy.Delete_management(edges)
            arcpy.Delete_management(nodes)
            # Keep track of solved networks
            solved.append(net_suffix)
        arcpy.CalculateField_management(out_fc, "Year", str(year), field_type="LONG")
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
    ref_time_field = f"ToCumul_{prep_conf.OSM_IMPED}"

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
            walk_time_df = parcel_walk_times(parcel_fc=parcels, parcel_id_field=prep_conf.PARCEL_COMMON_KEY, ref_fc=ref_fc,
                                             ref_name_field=ref_name_field, ref_time_field=ref_time_field,
                                             target_name=tgt_name)
            # Dump df to output table
            if _append_:
                extendTableDf(out_table, prep_conf.PARCEL_COMMON_KEY, walk_time_df, prep_conf.PARCEL_COMMON_KEY)
            else:
                dfToTable(walk_time_df, out_table, overwrite=True)
                _append_ = True
            # Add time bin field
            print("--- classifying time bins")
            bin_field = f"bin_{tgt_name}"
            min_time_field = f"min_time_{tgt_name}"
            parcel_walk_time_bin(in_table=out_table, bin_field=bin_field,
                                 time_field=min_time_field, code_block=prep_conf.TIME_BIN_CODE_BLOCK)


def process_ideal_walk_times():
    targets = ["stn", "park"]
    for year in YEARS:
        print(year)
        # Key paths
        year_gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        parcels_fc = makePath(year_gdb, "Polygons", "Parcels")
        stations_fc = makePath(BASIC_FEATURES, "SMART_Plan_Stations")
        parks_fc = makePath(CLEANED, "Park_Points.shp")
        out_table = makePath(year_gdb, "WalkTimeIdeal_parcels")
        target_fcs = [stations_fc, parks_fc]
        # Analyze ideal walk times
        dfs = []
        for target, fc in zip(targets, target_fcs):
            print(f" - {target}")
            # field_suffix = f"{target}_ideal"
            df = parcel_ideal_walk_time(parcels_fc=parcels_fc, parcel_id_field=prep_conf.PARCEL_COMMON_KEY, target_fc=fc,
                                        target_name_field="Name", radius=prep_conf.IDEAL_WALK_RADIUS, target_name=target,
                                        overlap_type="HAVE_THEIR_CENTER_IN", sr=None, assumed_mph=prep_conf.IDEAL_WALK_MPH)
            dfs.append(df)
        # Combine dfs, dfToTable
        # TODO: This assumes only 2 data frames, but could be generalized to merge multiple frames
        combo_df = dfs[0].merge(right=dfs[1], how="outer", on=prep_conf.PARCEL_COMMON_KEY)
        dfToTable(combo_df, out_table)
        # Add bin fields
        for target in targets:
            min_time_field = f"min_time_{target}"
            bin_field = f"bin_{target}"
            parcel_walk_time_bin(in_table=out_table, bin_field=bin_field,
                                 time_field=min_time_field, code_block=prep_conf.TIME_BIN_CODE_BLOCK)


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
            else:
                skim_year = model_year
            # Look up zone and skim data for each mode
            zone_data = makePath(gdb, f"EconDemog_{scale}")
            skim_data = makePath(CLEANED, source, f"{mode}_Skim_{skim_year}.csv")
            # Analyze access
            atd_df = summarizeAccess(skim_table=skim_data, o_field=prep_conf.SKIM_O_FIELD, d_field=prep_conf.SKIM_D_FIELD,
                                     imped_field=prep_conf.SKIM_IMP_FIELD, se_data=zone_data, id_field=id_field,
                                     act_fields=prep_conf.D_ACT_FIELDS, imped_breaks=prep_conf.ACCESS_TIME_BREAKS,
                                     units=prep_conf.ACCESS_UNITS, join_by="D",
                                     dtype=prep_conf.SKIM_DTYPES, chunk_size=100000
                                     )
            afo_df = summarizeAccess(skim_table=skim_data, o_field=prep_conf.SKIM_O_FIELD, d_field=prep_conf.SKIM_D_FIELD,
                                     imped_field=prep_conf.SKIM_IMP_FIELD, se_data=zone_data, id_field=id_field,
                                     act_fields=prep_conf.O_ACT_FIELDS, imped_breaks=prep_conf.ACCESS_TIME_BREAKS,
                                     units=prep_conf.ACCESS_UNITS, join_by="O",
                                     dtype=prep_conf.SKIM_DTYPES, chunk_size=100000
                                     )
            # Merge tables
            atd_df.rename(columns={prep_conf.SKIM_O_FIELD: id_field}, inplace=True)
            afo_df.rename(columns={prep_conf.SKIM_D_FIELD: id_field}, inplace=True)
            full_table = atd_df.merge(right=afo_df, on=id_field)

            # Export output
            out_table = makePath(gdb, f"Access_{scale}_{mode}")
            dfToTable(full_table, out_table, overwrite=True)


def process_contiguity(overwrite=True):
    county_fc = makePath(BASIC_FEATURES, "MiamiDadeCountyBoundary")
    chunk_fishnet = generate_chunking_fishnet(template_fc=county_fc, out_fishnet_name="quadrats", chunks=prep_conf.CTGY_CHUNKS)
    for year in YEARS:
        print(f"Processing Contiguity for {year}")
        gdb = YEAR_GDB_FORMAT.replace("YEAR", str(year))
        parcel_fc = makePath(gdb, "Polygons", "Parcels")
        buildings = makePath(RAW, "OpenStreetMap", "buildings_q1_2021", "OSM_Buildings_20210201074346.shp")

        ctgy_full = contiguity_index(quadrats_fc=chunk_fishnet, parcels_fc=parcel_fc, buildings_fc=buildings,
                                     parcels_id_field=prep_conf.PARCEL_COMMON_KEY,
                                     cell_size=prep_conf.CTGY_CELL_SIZE, weights=prep_conf.CTGY_WEIGHTS)
        if prep_conf.CTGY_SAVE_FULL:
            full_path = makePath(gdb, "Contiguity_full_singlepart")
            dfToTable(df=ctgy_full, out_table=full_path, overwrite=True)
        ctgy_summarized = contiguity_summary(full_results_df=ctgy_full, parcels_id_field=prep_conf.PARCEL_COMMON_KEY,
                                             summary_funcs=prep_conf.CTGY_SUMMARY_FUNCTIONS, area_scaling=prep_conf.CTGY_SCALE_AREA)
        summarized_path = makePath(gdb, "Contiguity_parcels")
        dfToTable(df=ctgy_summarized, out_table=summarized_path, overwrite=overwrite)


def process_lu_diversity():
    summary_areas_fc = makePath(BASIC_FEATURES, "SummaryAreas")
    lu_recode_table = makePath(REF, "Land_Use_Recode.csv")
    usecols = [prep_conf.LAND_USE_COMMON_KEY, prep_conf.LU_RECODE_FIELD]
    recode_df = pd.read_csv(lu_recode_table, usecols=usecols)

    # Filter recode table
    fltr = np.in1d(recode_df[prep_conf.LU_RECODE_FIELD], prep_conf.DIV_RELEVANT_LAND_USES)
    recode_df = recode_df[fltr].copy()
    # Iterate over analysis years
    for year in YEARS:
        print(year)
        gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        parcel_fc = makePath(gdb, "Polygons", "Parcels")
        out_fc = makePath(gdb, "Diversity_summaryareas")

        # Intersect parcels and summary areas
        print(" - intersecting parcels with summary areas")
        par_fields = [prep_conf.PARCEL_COMMON_KEY, prep_conf.LAND_USE_COMMON_KEY, prep_conf.PARCEL_BLD_AREA_COL]
        par_sa_int = assign_features_to_agg_area(
            parcel_fc, agg_features=summary_areas_fc, in_fields=par_fields, buffer=None, as_df=True)

        # Intersect can alter field name
        col_rename = {f"{prep_conf.SUMMARY_AREAS_COMMON_KEY}_": prep_conf.SUMMARY_AREAS_COMMON_KEY}
        par_sa_int.rename(columns=col_rename, inplace=True)

        # Merge generalized land uses
        in_df = par_sa_int.merge(recode_df, how="inner", on=prep_conf.LAND_USE_COMMON_KEY)
        # Adjust floor area since sqft are large numbers
        in_df[prep_conf.PARCEL_BLD_AREA_COL] /= 1000

        # Calculate div indices
        print(" - calculating diversity indices")
        div_funcs = [
            simpson_diversity,
            shannon_diversity,
            berger_parker_diversity,
            enp_diversity,
            # chi_squared_diversity
        ]
        count_lu = len(prep_conf.DIV_RELEVANT_LAND_USES)
        div_df = lu_diversity(in_df, prep_conf.SUMMARY_AREAS_COMMON_KEY, prep_conf.LU_RECODE_FIELD,
                              div_funcs, weight_field=prep_conf.PARCEL_BLD_AREA_COL,
                              count_lu=count_lu, regional_comp=True)

        # Export results
        print(" - exporting results")
        dfToTable(div_df, out_fc, overwrite=True)


def process_short_term_parcels():
    parcels = makePath(YEAR_GDB_FORMAT.replace("YEAR", str(max(YEARS))), "Polygons", "Parcels")
    permits = makePath(YEAR_GDB_FORMAT.replace("YEAR", str(max(YEARS))), "Points", "BuildingPermits")
    # TODO: needs a permenant solution for this temporary dataset
    save_gdb = PMT.validate_geodatabase(makePath(ROOT, CLEANED, "near_term_parcels.gdb"))
    permits_ref_df = prep_permits_units_reference(parcels=parcels, permits=permits, lu_key=prep_conf.LAND_USE_COMMON_KEY,
                                                  parcels_living_area_key=prep_conf.PARCEL_BLD_AREA_COL,
                                                  permit_value_key=prep_conf.PERMITS_UNITS_FIELD,
                                                  permits_units_name=prep_conf.PERMITS_BLD_AREA_NAME,
                                                  units_match_dict=prep_conf.PARCEL_REF_TABLE_UNITS_MATCH)
    build_short_term_parcels(parcels_path=parcels, permits_path=permits,
                             permits_ref_df=permits_ref_df, parcels_id_field=prep_conf.PARCEL_COMMON_KEY,
                             parcels_lu_field=prep_conf.LAND_USE_COMMON_KEY, parcels_living_area_field=prep_conf.PARCEL_BLD_AREA_COL,
                             parcels_land_value_field=prep_conf.PARCEL_LAND_VALUE, parcels_total_value_field=prep_conf.PARCEL_JUST_VALUE,
                             parcels_buildings_field=prep_conf.PARCEL_BUILDINGS, permits_id_field=prep_conf.PERMITS_ID_FIELD,
                             permits_lu_field=prep_conf.PERMITS_LU_FIELD, permits_units_field=prep_conf.PERMITS_UNITS_FIELD,
                             permits_values_field=prep_conf.PERMITS_VALUES_FIELD, permits_cost_field=prep_conf.PERMITS_COST_FIELD,
                             save_gdb_location=save_gdb, units_field_match_dict=prep_conf.SHORT_TERM_PARCELS_UNITS_MATCH)


if __name__ == "__main__":
    ''' setup basic features '''
    # SETUP CLEAN DATA
    # -----------------------------------------------
    # UDB might be ignored as this isnt likely to change and can be updated ad-hoc
    # process_udb() # TODO: udbToPolygon failing to create a feature class to store the output (likley an arcpy overrun)

    # process_basic_features() # TESTED # TODO: include the status field to drive selector widget

    # MERGES PARK DATA INTO A SINGLE POINT FEATURESET AND POLYGON FEARTURESET
    ##process_parks()  # TESTED UPDATES 03/10/21 CR
    #   YEAR over YEARS
    #   - sets up Points FDS and year GDB(unless they exist already
    #   - copies Park_Points in to each year gdb under the Points FDS
    #   - treat NEAR_TERM like any other year

    # CLEANS AND GEOCODES TRANSIT INTO INCLUDED LAT/LON
    process_transit()  # TESTED 02/26/21 CR
    #   YEAR over YEARS
    #     - cleans and consolidates transit data into Year POINTS FDS
    #     - if YEAR == NearTerm:
    #     -     most recent year is copied over

    # SETUP ANY BASIC NORMALIZED GEOMETRIES
    # process_normalized_geometries()  # TESTED # TODO: standardize column names
    #    YEAR BY YEAR
    #   - Sets up Year GDB, and Polygons FDS
    #   - Adds MAZ, TAZ, Census_Blocks, Census_BlockGroups, SummaryAreas
    #   - for each geometry type, the year is added as an attribute
    #   - for NearTerm, year is set to 9998 (allows for LongTerm to be 9999)

    # CLEANS AND GEOCODES PERMITS TO ASSOCIATED PARCELS
    # process_permits() # TESTED CR 03/01/21 # TODO: add prep_permits_units_reference to this procedure

    # COPIES DOWNLOADED PARCEL DATA AND ONLY MINIMALLY NECESSARY ATTRIBUTES INTO YEARLY GDB
    # process_parcels() # TESTED
    #   YEAR over YEARS
    #   - procedure joins parcels from DOR to NAL table keeping appropriate columns
    #   - if year == NearTerm:
    #       previous year parcels are copied in

    # updates parcels based on permits for near term analysis
    # process_short_term_parcels()  # TESTED 3/1/21 #TODO: needs to be broken down into smaller functions
    #   - record parcel land use groupings and multipliers/overwrites
    #   - replace relevant attributes for parcels with current permits
    #   update this procedure to pull from NearTerm and update parcel layer

    # -----------------ENRICH DATA------------------------------
    # ADD VARIOUS BLOCK GROUP LEVEL DEMOGRAPHIC, EMPLOYMENT AND COMMUTE DATA AS TABLE
    # enrich_block_groups() # TESTED CR 03/01/21
    #   YEAR over YEARS
    #   - enrich block group with parcel data and race/commute/jobs data as table
    #   - if Year == NearTerm:
    #       process as normal (parcel data have been updated to include permit udates)

    # MODELS MISSING DATA WHERE APPROPRIATE AND DISAGGREGATES BLOCK LEVEL DATA DOWN TO PARCEL LEVEL
    # process_bg_estimate_activity_models() # TESTED CR 03/02/21
    #   - creates linear model at block group-level for total employment, population, and commutes
    #       TODO: pull out of this function and insert to bg_apply and process once
    # process_bg_apply_activity_models()      # TESTED CR 03/02/21
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
    # process_parcel_land_use() # Tested by AB 3/3/21
    #   YEAR over YEARS
    #   - creates table of parcel records with land use and areas

    # prepare maz and taz socioeconomic/demographic data
    # process_model_se_data() # TODO: AB to test

    # ------------------NETWORK ANALYSES-----------------------------
    # BUILD OSM NETWORKS FROM TEMPLATES
    # process_osm_networks() #Tested by AB 2/26/21

    # ASSESS NETWORK CENTRALITY FOR EACH BIKE NETWORK
    # process_centrality() # Tested by AB 3/2/21

    # ANALYZE OSM NETWORK SERVICE AREAS
    # process_osm_service_areas() # Tested by AB 2/28/21

    # ANALYZE WALK/BIKE TIMES AMONG MAZS
    # process_osm_skims() #Tested by AB 3/2/21

    # RECORD PARCEL WALK TIMES
    # process_walk_times() # Tested by AB 3/2/21

    # RECORD PARCEL IDEAL WALK TIMES
    # process_ideal_walk_times() # Tested by AB 3/2/21

    # prepare serpm taz-level travel skims
    # process_model_skims() #TODO: AB to test

    # -----------------DEPENDENT ANALYSIS------------------------------
    # ANALYZE ACCESS BY MAZ, TAZ
    # process_access() TODO: AB to test

    # PREPARE TAZ TRIP LENGTH AND VMT RATES
    # process_travel_stats() #TODO: AB to write and test
    # TODO: script to calculate rates so year-over-year diffs can be estimated

    # ONLY UPDATED WHEN NEW IMPERVIOUS DATA ARE MADE AVAILABLE
    # process_imperviousness() # AW
    # TODO: ISGM for year-over-year changes? (low priority)

    # process_lu_diversity() # Tested by AB 3/4/21

    # generate contiguity index for all years
    # process_contiguity()

# TODO: !!! incorporate a project setup script or at minimum a yearly build geodatabase function/logic !!!
# TODO: handle multi-year data as own function
# TODO: add logging/print statements for procedure tracking (low priority)

''' deprecated '''
# cleans and geocodes crashes to included Lat/Lon
# process_crashes()
