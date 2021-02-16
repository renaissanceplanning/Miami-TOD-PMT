"""
preparation scripts used set up cleaned geodatabases
"""
# TODO: move these functions to a general helper file as they apply more broadly
from PMT_tools.download.download_helper import (validate_directory, validate_geodatabase, validate_feature_dataset)
# config global variables
from PMT_tools.config.prepare_config import IN_CRS, OUT_CRS
from PMT_tools.config.prepare_config import (CRASH_FIELDS_DICT, USE_CRASH)
from PMT_tools.config.prepare_config import TRANSIT_RIDERSHIP_TABLES, TRANSIT_FIELDS_DICT, TRANSIT_LONG, TRANSIT_LAT
from PMT_tools.config.prepare_config import PARCEL_COLS, PARCEL_USE_COLS
from PMT_tools.config.prepare_config import YEAR_GDB_FORMAT, REFERENCE_DIRECTORY, LODES_YEARS, ACS_YEARS
# prep/clean helper functions
from PMT_tools.prepare.prepare_helpers import *
from PMT_tools.prepare.prepare_osm_networks import *
# PMT functions
from PMT_tools.PMT import makePath, SR_FL_SPF, EPSG_FLSPF
# PMT globals
from PMT_tools.PMT import (RAW, CLEANED, YEARS)
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
        
        
def process_bg_estimate_activity_models():
    bg_enrich = makePath(YEAR_GDB_FORMAT, 
                         "Enrichment_blockgroups")
    save_path = analyze_blockgroup_model(bg_enrich_path = bg_enrich,
                                         acs_years = ACS_YEARS,
                                         lodes_years = LODES_YEARS,
                                         save_directory = REFERENCE_DIRECTORY)
    return save_path


def process_bg_apply_activity_models():
    for year in YEARS:
        # Set the inputs based on the year
        bg_enrich = makePath(YEAR_GDB_FORMAT, 
                             "Enrichment_blockgroups").replace("{year}", str(year))
        bg_geometry = makePath(YEAR_GDB_FORMAT,
                               "Polygons",
                               "BlockGroups").replace("{year}", str(year))
        model_coefficients = makePath(REFERENCE_DIRECTORY,
                                      "block_group_model_coefficients.csv"),
        save_gdb = YEAR_GDB_FORMAT.replace("{year}", str(year))
        
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
        analyze_blockgroup_apply(year = year,
                                 bg_enrich_path = bg_enrich,
                                 bg_geometry_path = bg_geometry,
                                 model_coefficients_path = model_coefficients,
                                 save_gdb_location = save_gdb,
                                 shares_from = shares)
        
        
def process_allocate_bg_to_parcels():
    for year in YEARS:
        # Set the inputs based on the year
        parcel_fc = makePath(YEAR_GDB_FORMAT,
                             "Polygons",
                             "Parcels").replace("{year}", str(year))
        bg_modeled = makePath(YEAR_GDB_FORMAT, 
                              "Enrichment_blockgroups").replace("{year}", str(year))
        bg_geom = makePath(YEAR_GDB_FORMAT,
                           "Polygons",
                           "BlockGroups").replace("{year}", str(year))
        out_gdb = YEAR_GDB_FORMAT.replace("{year}", str(year))
        
        # Allocate
        analyze_blockgroup_allocate(parcel_fc = parcel_fc, 
                                    bg_modeled = bg_modeled, 
                                    bg_geom = bg_geom, 
                                    out_gdb = out_gdb,
                                    parcels_id="FOLIO", 
                                    parcel_lu="DOR_UC", 
                                    parcel_liv_area="TOT_LVG_AREA")
        
    
    


if __name__ == "__main__":
    # setup any basic normalized geometries
    process_normalized_geometries()

    # copies downloaded parcel data and only minimally necessary attributes into yearly gdb
    # process_parcels()

    # merges park data into a single point featureset and polygon feartureset
    # process_parks()

    # cleans and geocodes crashes to included Lat/Lon
    # process_crashes()

    # cleans and geocodes transit into included Lat/Lon
    # process_transit()

    # cleans and geocodes permits to associated parcels
    # process_permits() # TODO: gdfToFeatureclass is not working properly

    # UDB might be ignored as this isnt likely to change and can be updated ad-hoc
    # process_udb() # TODO: udbToPolygon failing to create a feature class to store the output (likley an arcpy overrun)

    # only updated when new impervious data are made available
    # process_imperviousness()

    # sets up the base network features
    # process_osm_networks()


# TODO: incorporate a project setup script or at minimum a yearly build geodatabase function/logic
# TODO: handle multi-year data as own function
# TODO: handle parcel tabular data generation year over year
#       - parcel land use,
#       - parcel energy consumption
#       - parcel contiguity
#       - parcel Economic and Demographic
#       - parcel Walk Times,
# TODO: add logging/print statements for procedure tracking

