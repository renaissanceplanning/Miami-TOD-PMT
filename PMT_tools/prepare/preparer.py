"""
preparation scripts used set up cleaned geodatabases
"""
# TODO: move these functions to a general helper file as they apply more broadly
from PMT_tools.download.download_helper import (validate_directory, validate_geodatabase, validate_feature_dataset)
# config global variables
from PMT_tools.config.prepare_config import IN_CRS, OUT_CRS
from PMT_tools.config.prepare_config import BASIC_STATIONS, STN_NAME_FIELD, STN_BUFF_DIST, STN_BUFF_METERS, STN_DISS_FIELDS, STN_CORRIDOR_FIELDS
from PMT_tools.config.prepare_config import BASIC_ALIGNMENTS, ALIGN_BUFF_DIST, ALIGN_DISS_FIELDS, CORRIDOR_NAME_FIELD
from PMT_tools.config.prepare_config import BASIC_STN_AREAS, BASIC_CORRIDORS, BASIC_LONG_STN, BASIC_SUM_AREAS, BASIC_RENAME_DICT, STN_LONG_CORRIDOR
from PMT_tools.config.prepare_config import (CRASH_FIELDS_DICT, USE_CRASH)
from PMT_tools.config.prepare_config import TRANSIT_RIDERSHIP_TABLES, TRANSIT_FIELDS_DICT, TRANSIT_LONG, TRANSIT_LAT
from PMT_tools.config.prepare_config import PARCEL_COLS, PARCEL_USE_COLS, PARCEL_AREA_COL, PARCEL_LU_AREAS, PARCEL_BLD_AREA
from PMT_tools.config.prepare_config import BG_COMMON_KEY, ACS_COMMON_KEY, LODES_COMMON_KEY
from PMT_tools.config.prepare_config import BG_PAR_SUM_FIELDS, ACS_RACE_FIELDS, ACS_COMMUTE_FIELDS, LODES_FIELDS, LODES_CRITERIA
from PMT_tools.config.prepare_config import SKIM_O_FIELD, SKIM_D_FIELD, SKIM_IMP_FIELD, SKIM_DTYPES, SKIM_RENAMES
from PMT_tools.config.prepare_config import OSM_IMPED, OSM_CUTOFF, BIKE_PED_CUTOFF
from PMT_tools.config.prepare_config import NETS_DIR, SEARCH_CRITERIA, SEARCH_QUERY, NET_LOADER, NET_BY_YEAR, BIKE_RESTRICTIONS
from PMT_tools.config.prepare_config import CENTRALITY_IMPED, CENTRALITY_CUTOFF, CENTRALITY_NET_LOADER
from PMT_tools.config.prepare_config import TIME_BIN_CODE_BLOCK, IDEAL_WALK_MPH, IDEAL_WALK_RADIUS
from PMT_tools.config.prepare_config import ACCESS_MODES, MODE_SCALE_REF, ACCESS_TIME_BREAKS, ACCESS_UNITS, O_ACT_FIELDS, D_ACT_FIELDS

OSM_IMPED = "Minutes"
OSM_CUTOFF = "15 30"
# prep/clean helper functions
from PMT_tools.prepare.prepare_helpers import *
from PMT_tools.prepare.prepare_osm_networks import *
# PMT functions
from PMT_tools.PMT import makePath, SR_FL_SPF, EPSG_FLSPF, checkOverwriteOutput, dfToTable
# PMT classes
from PMT_tools.PMT import ServiceAreaAnalysis
# PMT globals
from PMT_tools.PMT import (RAW, CLEANED, BASIC_FEAURES, REF, YEARS)
from PMT_tools.PMT import arcpy

import PMT_tools.logger as log

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
    BASE_FEATURES_GDB = makePath(CLEANED, "PMT_BasicFeatures.gdb")
    REF = makePath(ROOT, "Reference")
    RIF_CAT_CODE_TBL = makePath(REF, "road_impact_fee_cat_codes.csv")
    DOR_LU_CODE_TBL = makePath(REF, "Land_Use_Recode.csv")


def process_normalized_geometries():
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
    county = makePath(BASE_FEATURES_GDB, "BasicFeatures", "MiamiDadeCountyBoundary")
    for year in YEARS:
        for raw, cleaned, cols in zip([raw_block, raw_block_groups, raw_TAZ, raw_MAZ],
                                      [blocks, block_groups, TAZ, MAZ],
                                      ["GEOID10", "GEOID", TAZ, MAZ]):

            temp_file = r"in_memory\\temp_features"
            out_path = validate_feature_dataset(makePath(CLEANED, f"PMT_{year}.gdb", "Polygons"), sr=SR_FL_SPF)
            logger.log_msg(f"...building normalized {cleaned} in {out_path}")
            out_data = makePath(out_path, cleaned)
            prep_feature_class(in_fc=raw, geom="POLYGON", out_fc=temp_file, use_cols=cols, rename_dict=None)
            lyr = arcpy.MakeFeatureLayer_management(in_features=temp_file, out_layer="lyr")
            arcpy.SelectLayerByLocation_management(in_layer=lyr, overlap_type="HAVE_THEIR_CENTER_IN",
                                                   select_features=county)
            PMT.checkOverwriteOutput(output=out_data, overwrite=True)
            logger.log_msg(f"... outputing geometries and {cols} only")
            arcpy.CopyFeatures_management(in_features=lyr, out_feature_class=out_data)
            arcpy.CalculateField_management(in_table=out_data, field="Year", expression=year,
                                            expression_type="PYTHON3", field_type="LONG")

def process_basic_features():
    print("Making basic features")
    makeBasicFeatures(
        BASIC_FEATURES,
        BASIC_STATIONS,
        STN_DISS_FIELDS,
        STN_CORRIDOR_FIELDS,
        BASIC_ALIGNMENTS,
        ALIGN_DISS_FIELDS,
        stn_buff_dist=STN_BUFF_DIST,
        align_buff_dist=ALIGN_BUFF_DIST,
        stn_areas_fc=BASIC_STN_AREAS,
        corridors_fc=BASIC_CORRIDORS,
        long_stn_fc=BASIC_LONG_STN,
        rename_dict=BASIC_RENAME_DICT,
        overwrite=True)

    print("Making summarization features")
    makeSummaryFeatures(
        BASIC_FEATURES,
        BASIC_LONG_STN,
        BASIC_CORRIDORS,
        CORRIDOR_NAME_FIELD,
        BASIC_SUM_AREAS,
        stn_buffer_meters=STN_BUFF_METERS,
        stn_name_field=STN_NAME_FIELD,
        stn_cor_field=STN_LONG_CORRIDOR,
        overwrite=True)


def process_parks():
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
    prep_park_polys(in_fcs=park_polys, geom="POLYGON", out_fc=out_park_polys,
                    use_cols=poly_use_cols, rename_dicts=poly_rename_cols)
    prep_feature_class(in_fc=park_points, geom="POINT", out_fc=out_park_points)


def process_crashes():
    ''' crashes '''
    crash_json = makePath(RAW, "Safety_Security", "bike_ped.geojson")
    all_features = geojson_to_feature_class(geojson_path=crash_json, geom_type='POINT')
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


def process_permits():
    ''' permits '''
    permit_csv = makePath(RAW, "BUILDING_PERMITS", "Road Impact Fee Collection Report -- 2019.csv")

    out_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_2019.gdb"))
    in_fds = validate_feature_dataset(fds_path=makePath(out_gdb, "Polygons"), sr=SR_FL_SPF)
    parcels = makePath(in_fds, "Parcels")
    out_fds = validate_feature_dataset(makePath(out_gdb, "Points"), sr=SR_FL_SPF)
    permits_out = makePath(out_fds, "BuildingPermits")
    clean_permit_data(permit_csv=permit_csv, poly_features=parcels,
                      permit_key="FOLIO", poly_key="FOLIO",
                      out_file=permits_out, out_crs=EPSG_FLSPF)


def process_udb():
    ''' UDB '''
    udb_fc = makePath(RAW, "MD_Urban_Growth_Boundary.geojson")
    county_fc = makePath(RAW, "CensusGeo", "Miami-Dade_Boundary.geojson")
    out_fc = makePath(CLEANED, "UrbanDevelopmentBoundary.shp")

    temp_fc = geojson_to_feature_class(geojson_path=udb_fc, geom_type="POLYLINE")
    udbLineToPolygon(udb_fc=temp_fc, county_fc=county_fc, out_fc=out_fc)


def process_transit():
    ''' Transit Ridership
        - converts a list of ridership files to points with attributes cleaned
        - NOTE: transit folder reflects current location, needs update to reflect cleaner structure
    '''
    transit_folder = validate_directory(RAW, "TRANSIT", "TransitRidership_byStop")
    transit_shape_fields = [TRANSIT_LONG, TRANSIT_LAT]
    for year in YEARS:
        out_gdb = validate_geodatabase(os.path.join(CLEANED, f"PMT_{year}.gdb"))
        FDS = validate_feature_dataset(makePath(out_gdb, "Points"))
        out_name = 'TransitRidership'
        transit_xls_file = makePath(transit_folder, TRANSIT_RIDERSHIP_TABLES[year])
        transit_out_path = makePath(FDS, out_name)
        prep_transit_ridership(in_table=transit_xls_file, rename_dict=TRANSIT_FIELDS_DICT,
                               shape_fields=transit_shape_fields, from_sr=IN_CRS,
                               to_sr=OUT_CRS, out_fc=transit_out_path)


def process_parcels():
    parcel_folder = makePath(RAW, "Parcels")
    for year in YEARS[:1]:
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
        prep_parcels(in_fc=in_fc, in_tbl=in_csv, out_fc=out_fc, fc_key_field="PARCELNO",
                     tbl_key_field="PARCEL_ID", tbl_renames=renames, **csv_kwargs)
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
        # Make temp feature class # TODO: I don't think this is necessary but should confirm (AB)
        # unique_name = uuid.uuid4().hex
        # temp_bg = makePath("in_memory", f"__blockgroups_{str(unique_name)}__")
        # t_path, t_name = os.path.split(temp_bg)
        # arcpy.FeatureClassToFeatureClass_conversion(
        #     in_features=bg_fc, out_path=t_path, out_name=t_name)
        # Enrich BGs with parcel data
        bg_df = enrich_bg_with_parcels(
            bg_fc=bg_fc, bg_id_field=BG_COMMON_KEY,
            parcels_fc=parcels_fc, par_id_field=PARCEL_COMMON_KEY,
            par_lu_field=PARCEL_LU_COL, par_bld_area=PARCEL_BLD_AREA,
            sum_crit=LODES_CRITERIA, par_sum_fields=BG_PAR_SUM_FIELDS
            )
        # Save enriched data
        dfToTable(bg_df, out_table, overwrite=True)
        # Extend BG output with ACS/LODES data
        in_tables = [race_tbl, commute_tbl, lodes_tbl]
        in_tbl_ids = [ACS_COMMON_KEY, ACS_COMMON_KEY, LODES_COMMON_KEY]
        in_tbl_flds = [ACS_RACE_FIELDS, ACS_COMMUTE_FIELDS, LODES_FIELDS]
        for table, tbl_id, fields in zip(in_tables, in_tbl_ids, in_tbl_flds):
            enrich_bg_with_econ_demog(
                tbl_path=out_tbl,
                tbl_id_field=BG_COMMON_KEY,
                join_tbl=table,
                join_id_field=tbl_id,
                join_fields=fields
                )


def process_parcel_land_use():
    for year in YEARS:
        print(year)
        year_gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        # Parcels
        parcels_fc = makePath(year_gdb, "Polygons", "Parcels")
        # Reference table
        lu_table = makePath(REF, "Land_Use_Recode.csv") # TODO: put in config?
        # Output
        out_table = makePath(year_gdb, "LandUseCodes_parcels")

        # Create combo df
        par_lu_field = "DOR_UC"
        par_fields = [PARCEL_COMMON_KEY, "LND_SQFOOT"]
        tbl_lu_field = "DOR_UC"
        dtype={"DOR_UC": int}
        par_df = prep_parcel_land_use_tbl(parcels_fc, par_lu_field, par_fields,
                                          lu_table, tbl_lu_field, dtype=dtype)
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
    prep_imperviousness(zip_path=impervious_download, clip_path=county_boundary, out_dir=out_dir, out_sr=EPSG_FLSPF)


def process_osm_networks():
    net_version = "q1_2021"
    # Import edges
    osm_raw = PMT.makePath(RAW, "OpenStreetMap")
    for net_type in ["bike", "walk"]:
        net_type_version = f"{net_type}_{net_version}"
        clean_gdb = makeCleanNetworkGDB(CLEANED, gdb_name=net_type_version)
        net_type_fd = makeNetFeatureDataSet(clean_gdb, "osm", sr)

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


def process_osm_skims():
    if arcpy.CheckExtension("network") == "Available":
        arcpy.CheckOutExtension("network")
    else:
        raise arcpy.ExecuteError("Network Analyst Extension license is not available.")
    # Create and solve OD Matrix at MAZ scale
    osm_dir = makePath(CLEANED, "OSM_Networks")
    solved = []
    for year in PMT.YEARS:
        # Get MAZ features, create temp centroids for network loading
        maz_path = PMT.makePath(CLEANED, f"PMT_{year}.gdb", "Polygons", "MAZ")
        maz_pts = PMT.polygon_to_points_arc(maz_path, MAZ_COMMON_KEY)
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
                # - Skim input/output
                nd = makePath(
                    osm_dir, f"{mode}{net_suffix}.gdb", "osm", "osm_ND")
                skim = PMT.makePath(
                    osm_dir, f"{mode}_Skim{net_suffix}.csv")
                if mode == "bike":
                    restrictions = BIKE_RESTRICTIONS
                else:
                    restrictions = None
                # - Create and load problem
                genODTable(
                    origin_pts=layer,
                    origin_name_field=MAZ_COMMON_KEY,
                    dest_pts=maz_pts,
                    dest_name_field=MAZ_COMMON_KEY,
                    in_nd=nd,
                    imped_attr=OSM_IMPED,
                    cutoff=BIKE_PED_CUTOFF,
                    net_loader=NET_LOADER,
                    out_table=skim,
                    restrictions=restrictions,
                    use_hierarchy=False,
                    uturns="ALLOW_UTURNS",
                    o_location_fields=None,
                    d_location_fields=None,
                    o_chunk_size=5000
                    )
                # Clean up workspace
                arcpy.Delete_management(layer)
            # Mark as solved
            solved.append(net_suffix)


def process_model_se_data():
    # Summarize parcel data to MAZ
    for year in YEARS:
        # Set output
        out_gdb = validate_geodatabase(
            makePath(CLEANED, f"PMT_{year}.gdb"))
        out_fds = validate_feature_dataset(
            makePath(out_gdb, "Polygons"), sr=SR_FL_SPF)
        maz_se_data = makePath(RAW, "SERPM", "V7", "maz_data.csv") # TODO: standardize SERPM pathing
        # Summarize parcels to MAZ
        print("... summarizing MAZ activities from parcels")
        par_fc = makePath(out_gdb, "Polygons", "Parcels")
        se_data = makePath(out_gdb, "EconDemog_parcels")
        par_data = estimate_maz_from_parcels(
            par_fc=par_fc, par_id_field=PARCEL_COMMON_KEY, #TODO: confirm we can use common keys here?
            maz_fc=out_fc, maz_id_field=MAZ_COMMON_KEY,
            taz_id_field=TAZ_COMMON_KEY, se_data=se_data,
            se_id_field=PARCEL_COMMON_KEY, agg_cols=MAZ_AGG_COLS,
            consolidations=MAZ_PAR_CONS)
        # Fetch MAZ data (enrollments, etc.)
        print("... fetching other base-year MAZ data")
        maz_data = pd.read_csv(maz_se_data)
        maz_data.rename(columns=SERPM_RENAMES, inplace=True)
        # Consolidate
        maz_data = consolidate_cols(
            maz_data, [MAZ_COMMON_KEY, TAZ_COMMON_KEY], MAZ_SE_CONS)
        # Patch for full regional MAZ data
        print("... combining parcel-based and non-parcel-based MAZ data")
        maz_data = patch_local_regional_maz(par_data, maz_data)
        # Export MAZ table
        print("... exporting MAZ socioeconomic/demographic data")
        maz_table = makePath(out_gdb, "EconDemog_MAZ")
        dfToTable(maz_data, maz_table)
        # Summarize to TAZ scale
        print("... summarizing MAZ data to TAZ scale")
        maz_data.drop(columns=[MAZ_COMMON_KEY], inplace=True)
        taz_data = all_data.groupby(TAZ_COMMON_KEY).sum().reset_index()
        # Export TAZ table
        print("... exporting TAZ socioeconomic/demographic data")
        taz_table = makePath(out_gdb, "EconDemog_TAZ")
        dfToTable(taz_data, taz_table)


def process_model_skims():
    """
    Assumes transit and auto skims have same fields
    """
    # Get field definitions
    o_field = [k for k in SKIM_RENAMES.keys() if SKIM_RENAMES[k] == "OName"][0]
    d_field = [k for k in SKIM_RENAMES.keys() if SKIM_RENAMES[k] == "DName"][0]
    # Setup input/output tables
    auto_csv = makePath(RAW, "SERPM", f"GP_Skims_AM_{year}.csv")
    auto_out = makePath(CLEANED, "SERPM", f"Auto_Skim_{year}.csv")
    transit_csv = makePath(RAW, "SERPM", f"Tran_Skims_AM_{year}.csv")
    transit_out = makePath(CLEANED, "SERPM", f"Transit_Skim_{year}.csv")
    inputs = [auto_csv, transit_csv]
    outptus = [auto_out, transit_out]
    # Clean each input/output for each model year
    for year in MODEL_YEARS:
        print(year)
        for i, o in zip(inputs, outputs):
            print(f"... cleaning skim {i}")
            clean_skim(i, o_field, d_field, SKIM_IMP_FIELD, o,
                       rename=SKIM_RENAMES, chunksize=100000, thousands=",",
                       dtype=SKIM_DTYPES)


def process_osm_service_areas():
    # Facilities
    #  - Stations
    stations = makePath(BASIC_FEATURES, "SMART_Plan_Stations")
    station_name = "Name"
    # - Parks
    parks = PMT.makePath(CLEANED, "Parks", "Facility.shp")
    parks_name = "NAME"

    # For each analysis year, analyze networks (avoid redundant solves)
    solved = []
    solved_years = []
    modes = ["walk", "bike"]
    dest_grp =["stn", "parks"]
    runs = ["MERGE", "NO_MERGE", "OVERLAP", "NON_OVERLAP"]
    expected_fcs = [f"{mode}_to_{dg}_{run}"
                        for mode in modes
                        for dg in dest_grp
                        for run in runs
                        ]
    for year in YEARS:
        out_fds = makePath(CLEANED, f"PMT_{year}.gdb", "Networks")
        # Network setup
        net_suffix = NET_BY_YEAR[year][0]
        if net_suffix in solved:
            # Copy from other year if already solved
            copy_net_result(net_by_year, year, solved_years, expected_fcs)
        else:
            # Solve this network
            print(f"\n{net_suffix}")
            for mode in modes:
                # Create separate service area problems for stations and parks
                nd = makePath(
                    NETS_DIR, f"{mode}{net_suffix}.gdb", "osm", "osm_ND")
                stns = ServiceAreaAnalysis(f"{mode}_to_stn", nd, stations,
                                           station_name, NET_LOADER)
                parks = ServiceAreaAnalysis(f"{mode}_to_parks", nd, parks,
                                            parks_name, NET_LOADER)
                # Solve service area problems
                for sa_prob in [stns, parks]:
                    print(f"\n - {sa_prob.name}")
                    # Set restrictions if needed
                    if "bike" in sa_prob.name:
                        restrictions = BIKE_RESTRICTIONS
                    else:
                        restrictions = ""
                    # Solve (exports output to the out_fds)
                    sa_prob.solve(OSM_IMPED, OSM_CUTOFF, out_fds,
                                  restrictions=restrictions,
                                  use_hierarchy=False,
                                  net_location_fields="")
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
        out_fds = validate_feature_dataset(out_fds_path, SR_FL_SPF)
        out_fc_name = "nodes_bike"
        out_fc = makePath(out_fds, out_fc_name)
        checkOverwriteOutput(out_fc, overwrite=True)
        if net_suffix in solved:
            # Copy from other year if already solved
            copy_net_result(net_by_year, year, solved_years, out_fc_name)
        else:
            # Get node and edge features as layers
            print(f"\n{net_suffix}")
            in_fds = makePath(
                CLEANED, "osm_networks", f"bike{net_suffix}.gdb", "osm")
            in_nd = makePath(in_fds, "osm_ND")
            in_edges = makePath(in_fds, "edges")
            in_nodes = makePath(in_fds, "osm_ND_Junctions")
            edges = arcpy.MakeFeatureLayer_management(in_edges, "EDGES")
            nodes = arcpy.MakeFeatureLayer_management(in_nodes, "NODES")
            # Select edges by attribute - service roads
            where = arcpy.AddFieldDelimiters(edges, "highway")
            where = where + " LIKE '%service%'"
            arcpy.SelectLayerByAttribute_management(
                edges, "NEW_SELECTION", where)
            # Select nodes by location - nodes not touching services roads
            arcpy.SelectLayerByLocation_management(
                nodes, "INTERSECT", edges,
                selection_type="NEW_SELECTION",
                invert_spatial_relationship="INVERT"
                )
            # Export selected nodes to output fc
            arcpy.FeatureClassToFeatureClass_conversion(
                nodes, out_fds, out_fc_name)
            oid_field = arcpy.Describe(out_fc).OIDFieldName
            arcpy.CalculateField_management(out_fc, node_id, f"!{oid_field}!",
                                            expression_type="PYTHON",
                                            field_type="LONG"
                                            )
            # Calculate centrality (iterative OD solves)
            centrality_df = network_centrality(
                in_nd=in_nd,
                in_features=out_fc,
                net_loader=CENTRALITY_NET_LOADER,
                name_field=node_id,
                impedance_attribute=CENTRALITY_IMPED,
                cutoff=CENTRALITY_CUTOFF,
                restrictions=BIKE_RESTRICTIONS,
                chunksize=1000
                )
            # Extend out_fc
            extendTableDf(out_fc, "NODE_ID", centrality_df, "NODE_ID")
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
        out_table = makePath(year_gdb, "walk_time")
        # Iterate over targets and references
        net_fds = makePath(year_gdb, "Networks")
        for tgt_name, ref_fc in zip(target_names, ref_fcs):
            print(f"- {tgt_name}")
            ref_fc = makePath(net_fds, ref_fc)
            walk_time_df = parcel_walk_times(
                parcels, PARCEL_COMMON_KEY, ref_fc,
                ref_name_field, ref_time_field, tgt_name
                )
            # Add time bin field
            print("... classifying time bins")
            bin_field = f"bin_{tgt_name}"
            min_time_field = f"min_time_{tgt_name}"
            parcel_walk_time_bin(
                out_table, bin_field, min_time_field, TIME_BIN_CODE_BLOCK)


def process_ideal_walk_times():
    targets = ["stn", "parks"]
    for year in YEARS:
        print(year)
        # Key paths
        year_gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        parcels_fc = makePath(year_gdb, "Polygons", "Parcels")
        stations_fc = makePath(BASIC_FEATURES, "SMART_Plan_Stations")
        parks_fc = makePath(CLEANED, "Park_Points.shp")
        out_table = makePath(year_gdb, "ideal_walk_time")
        target_fcs = [stations_fc, parks_fc]
        # Analyze ideal walk times
        dfs = []
        for target, fc in zip(targets, target_fcs):
            print(f" - {target}")
            #field_suffix = f"{target}_ideal"
            df = parcel_ideal_walk_time(
                    parcels_fc, PARCEL_COMMON_KEY, fc,"Name",
                    IDEAL_WALK_RADIUS, target, #field_suffix
                    overlap_type="HAVE_THEIR_CENTER_IN",
                    sr=None, assumed_mph=IDEAL_WALK_MPH
                    )
        # Combine dfs, dfToTable
        combo_df = dfs[0].merge(dfs[1], how="outer", on=PARCEL_COMMON_KEY)
        dfToTable(combo_df, out_table)
        # Add bin fields
        for target in targets:
            min_time_field = f"min_time_{target}"
            bin_field = f"bin_{target}"
            parcel_walk_time_bin(
                out_table, bin_field, min_time_field, TIME_BIN_CODE_BLOCK)


def process_access():
    for year in YEARS:
        print(f"Analysis year: {year}")
        gdb = makePath(CLEANED, f"PMT_{year}.gdb")
        for mode in ACCESS_MODES:
            print(f"... {mode}")
            # Get reference info from globals
            source, scale, id_field = MODE_SCALE_REF[mode]
            osm_year, model_year = NET_BY_YEAR[year]
            if source == "OSM_Networks":
                skim_year = osm_year
            else:
                skim_yer = model_year
            # Look up zone and skim data for each mode
            zone_data = makePath(gdb, f"EconDemog_{scale}")
            skim_data = makePath(
                CLEANED, source, f"{mode}_Skim_{skim_year}.csv")
            # Analyze access
            atd_df = summarizeAccess(skim_data, SKIM_O_FIELD, SKIM_D_FIELD,
                                     SKIM_IMP_FIELD, zone_data, id_field,
                                     D_ACT_FIELDS, ACCESS_TIME_BREAKS
                                     units=ACCESS_UNITS, join_by="D",
                                     dtype=SKIM_DTYPES, chunk_size=100000
                                     )
            afo_df = summarizeAccess(skim_data, SKIM_O_FIELD, SKIM_D_FIELD,
                                     SKIM_IMP_FIELD, zone_data, id_field,
                                     O_ACT_FIELDS, ACCESS_TIME_BREAKS
                                     units=UNITS, join_by="O",
                                     dtype=SKIM_DTYPES, chunk_size=100000
                                     )
            # Merge tables
            atd_df.rename(columns={SKIM_O_FIELD: id_field}, inplace=True)
            afo_df.rename(columns={SKIM_D_FIELD: id_field}, inplace=True)
            full_table = atd_df.merge(afo_df, on=id_field)

            # Export output
            out_table = makePath(gdb, f"Access_{scale}_{mode}")
            PMT.dfToTable(full_table, out_table, overwrite=True)


if __name__ == "__main__":
    # setup basic features
    # process_basic_features()

    # setup any basic normalized geometries
    process_normalized_geometries() # TODO: standardize column names

    # copies downloaded parcel data and only minimally necessary attributes into yearly gdb
    # process_parcels()

    # merges park data into a single point featureset and polygon feartureset
    # process_parks()

    # cleans and geocodes crashes to included Lat/Lon
    # process_crashes()

    # cleans and geocodes transit into included Lat/Lon
    # process_transit() # TODO: reduce geo precision and consolidate points to reduce size

    # cleans and geocodes permits to associated parcels
    # process_permits() # TODO: gdfToFeatureclass is not working properly

    # Updates parcels based on permits for near term analysis
    # apply_permits_to_parcels() TODO: AW, integrate, rename, etc.

    # prepare near term parcels
    # enrich_block_groups()

    # estimate_bg_activity_model() # AW
    # apply_bg_activity_model() # AW
    # allocate_parcel_activity() # AW

    # UDB might be ignored as this isnt likely to change and can be updated ad-hoc
    # process_udb() # TODO: udbToPolygon failing to create a feature class to store the output (likley an arcpy overrun)

    # only updated when new impervious data are made available
    # process_imperviousness() # AW

    # TODO: test/debug methods below (from Alex, not yet tested)
    # TODO: check use validate_fds method instead of makePath in these and helper funcs

    # record parcel land use groupings and multipliers
    # process_parcel_land_use()

    # prepare MAZ and TAZ socioeconomic/demographic data
    # process_model_se_data()

    # build osm networks from templates
    # process_osm_networks()

    # analyze walk/bike times among MAZs
    # process_osm_skims()

    # assess network centrality for each bike network
    # process_centrality()

    # analyze osm network service areas
    # process_osm_service_areas()

    # record parcel walk times
    # process_walk_times()

    # record parcel ideal walk times
    # process_ideal_walk_times()

    # prepare serpm TAZ-level travel skims
    # process_model_skims()

    # analyze access by MAZ, TAZ
    # process_access()

    # Travel stats
    # TODO: vmt methodology (AB)

# TODO: incorporate a project setup script or at minimum a yearly build geodatabase function/logic
# TODO: handle multi-year data as own function
# TODO: handle parcel tabular data generation year over year
#       - parcel land use,
#       - parcel energy consumption
#       - parcel contiguity
#       - parcel Economic and Demographic
#       - parcel Walk Times,
# TODO: add logging/print statements for procedure tracking

