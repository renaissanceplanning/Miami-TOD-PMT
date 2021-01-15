from collections import OrderedDict

'''Crash Configuration'''
# download configuration
CRASHES_SERVICE = r'https://gis.fdot.gov/arcgis/rest/services/Crashes_All/FeatureServer/0/'
PED_BIKE_QUERY = {"where": "COUNTY_TXT = 'MIAMI-DADE' AND PEDESTRIAN_BICYCLIST_IND = 'Y'"}

# cleaning configuration
CRASH_FIELDS_DICT = dict(
    [
        ("CALENDAR_YEAR", "YEAR"),
        ("DAYOWEEK", "WEEK_DAY"),
        ("LATITUDE", "LAT"),
        ("LONGITUDE", "LONG"),
        ("COUNTY_TXT", "COUNTY"),
        ("DHSCNTYCTY", "CITY"),
        ("PEDESTRIAN_RELATED_IND", "PED_TYPE"),
        ("BICYCLIST_RELATED_IND", "BIKE_TYPE"),
        ("INJSEVER", "INJSEVER"),
        ("SPEED_LIMIT", "SPEED_LIM"),
        ("MOST_HARM_EVNT_CD", "HARM_EVNT"),
        ("IN_TOWN_FLAG", "IN_TOWN"),
    ]
)

USE_CRASH = CRASH_FIELDS_DICT.keys()
DROP_CRASH = ["DATE", "BIKE_TYPE", "PED_TYPE", ]
# CRS
IN_CRS = 4326  # WGS84 latitude/longitude
OUT_CRS = 6437  # NAD83(2011) / Florida East meters
# Data subset
COUNTY = "MIAMI-DADE"

# incident type dict
CRASH_INCIDENT_TYPES = {
    "BIKE_TYPE": "BIKE",
    "PED_TYPE": "PEDESTRIAN",
}

# CODED VALUE LOOKUP
CRASH_HARMFUL_CODES = OrderedDict(
    [
        (1, "Overturn/Rollover"),
        (2, "Fire/Explosion"),
        (3, "Immersion"),
        (4, "Jackknife"),
        (5, "Cargo/Equipment Loss or Shift"),
        (6, "Fell/Jumped From Motor Vehicle"),
        (7, "Thrown or Falling Object"),
        (8, "Ran into Water/Canal"),
        (9, "Other Non-Collision"),
        (10, "Pedestrian"),
        (11, "Pedalcycle"),
        (12, "Railway Vehicle (train, engine)"),
        (13, "Animal"),
        (14, "Motor Vehicle in Transport"),
        (15, "Parked Motor Vehicle"),
        (16, "Work Zone/Maintenance"),
        (17, "Struck By Falling, Shifting Cargo"),
        (18, "Other Non-Fixed Object"),
        (19, "Impact Attenuator/Crash Cushion"),
        (20, "Bridge Overhead Structure"),
        (21, "Bridge Pier or Support"),
        (22, "Bridge Rail"),
        (23, "Culvert"),
        (24, "Curb"),
        (25, "Ditch"),
        (26, "Embankment"),
        (27, "Guardrail Face"),
        (28, "Guardrail End"),
        (29, "Cable Barrier"),
        (30, "Concrete Traffic Barrier"),
        (31, "Other Traffic Barrier"),
        (32, "Tree (standing)"),
        (33, "Utility Pole/Light Support"),
        (34, "Traffic Sign Support"),
        (35, "Traffice Signal Support"),
        (36, "Other Post, Pole or Support"),
        (37, "Fence"),
        (38, "Mailbox"),
        (39, "Other Fixed Object (wall, building, tunnel, etc.)"),
    ]
)

# coded injury severity
CRASH_SEVERITY_CODES = dict(
    [
        (0, "No Data"),
        (1, "None"),
        (2, "Possible"),
        (3, "Non-Incapacitating"),
        (4, "Incapacitating"),
        (5, "Fatal (within 30 days)"),
        (6, "Non-traffic Fatality"),
    ]
)

# city codes
CRASH_CITY_CODES = dict(
    [
        (100, "None"),
        (129, "Aventura"),
        (130, "Bal Harbor"),
        (132, "Bay Harbor Islands"),
        (131, "Miami-Dade County Schools"),
        (133, "Biscayne Gardens"),
        (134, "Biscayne Park"),
        (135, "Pine Crest Village"),
        (136, "Cutler Bay Police"),
        (136, "Coconut Grove"),
        (137, "Carol City"),
        (138, "Coral Gables"),
        (140, "Coral Way Village"),
        (141, "Miami-Dade Police"),
        (142, "Cutler Ridge"),
        (144, "El Portal"),
        (145, "Bunche Park"),
        (146, "Florida City"),
        (147, "Browns Village"),
        (148, "Golden Beach"),
        (150, "Golden Glades"),
        (151, "Palmetto Bay"),
        (152, "Goulds"),
        (154, "Hialeah"),
        (155, "Doral"),
        (156, "Hialeah Gardens"),
        (157, "Miami Gardens"),
        (158, "Homestead"),
        (159, "Homestead AFB"),
        (160, "Indian Creek Village"),
        (161, "Islandia"),
        (162, "Key Biscayne"),
        (163, "Kendall"),
        (164, "Medley"),
        (165, "Leisure City"),
        (166, "Miami"),
        (167, "Miami TP"),
        (168, "Miami Beach"),
        (169, "Miami Lakes"),
        (170, "Miami Shores"),
        (171, "Norwood"),
        (172, "Miami Springs"),
        (173, "Miccosukee Indian Reserv."),
        (174, "Naranja"),
        (176, "North Bay"),
        (177, "Olympia Heights"),
        (178, "North Bay Village"),
        (179, "Palmetto Estates"),
        (180, "North Miami"),
        (181, "Pinewood"),
        (182, "North Miami Beach"),
        (183, "Ojus"),
        (184, "Opa-Locka"),
        (185, "Perrine"),
        (186, "Richmond Heights"),
        (187, "South Miami"),
        (188, "Sunny Isles Beach"),
        (188, "Sunny Isles"),
        (189, "Surfside"),
        (190, "Sweetwater"),
        (191, "Unincorporated Cnt"),
        (191, "Miami"),
        (192, "Virginia Gardens"),
        (193, "West Miami"),
        (194, "University of Miami"),
        (195, "S Miami Heights"),
        (196, "Uleta"),
        (197, "Westwood Lakes"),
        (198, "Westview"),
        (199, "FL International University"),
    ]
)
''' Census Configuration'''
CENSUS_FTP_HOME = 'ftp://ftp2.census.gov/geo/tiger/TIGER2012/'
CENSUS_SCALE = "block_group"
CENSUS_GEO_TYPES = ["tabblock", "bg"]
CENSUS_STATE = "12"
CENSUS_COUNTY = "086"
ACS_RACE_TABLE = "B03002"
ACS_RACE_COLUMNS = {
    "002E": "Total_Non_Hisp",
    "012E": "Total_Hispanic",
    "003E": "White_Non_Hisp",
    "004E": "Black_Non_Hisp",
    "006E": "Asian_Non_Hisp",
    "009E": "Multi_Non_Hisp",
    "013E": "White_Hispanic",
    "014E": "Black_Hispanic",
    "016E": "Asian_Hispanic",
    "019E": "Multi_Hispanic"
}
ACS_MODE_TABLE = "B08301"
ACS_MODE_COLUMNS = {
    "001E": "Total_Commutes",
    "003E": "Drove_alone",
    "004E": "Carpool",
    "010E": "Transit",
    "016E": "Taxi",
    "017E": "Motorcycle",
    "018E": "Bicycle",
    "019E": "Walk",
    "020E": "Other",
    "021E": "Work_From_Home"
}

''' Urban Growth Boundary Config '''
URBAN_GROWTH_OPENDATA_URL = r"https://opendata.arcgis.com/datasets/a468dc11c02f4467ade836947627554b_0.geojson"

''' County Boundary '''
MIAMI_DADE_COUNTY_URL = r"https://opendata.arcgis.com/datasets/cec575982ea64ef7a11e587e532c6b6a_0.geojson"

''' Impervious Surface data NLCD '''
IMPERVIOUS_URL = r"https://s3-us-west-2.amazonaws.com/mrlc/NLCD_2016_Impervious_L48_20190405.zip"

''' Parks Open Data URLS - from '''
PARKS_URL_DICT = {
    "Municipal_Parks": r"https://opendata.arcgis.com/datasets/16fe02a1defa45b28bf14a29fb5f0428_0.geojson",
    "County_Parks": r"https://opendata.arcgis.com/datasets/aca1e6ff0f634be282d50cc2d534a832_0.geojson",
    "Federal_State_Parks": r"https://opendata.arcgis.com/datasets/fa11a4c0a3554467b0fd5bc54edde4f9_0.geojson",
    "Park_Facilities": r"https://opendata.arcgis.com/datasets/8c9528d3e1824db3b14ed53188a46291_0.geojson"
}

''' Energy Consumption Table Download '''
RESIDENTIAL_ENERGY_CONSUMPTION_URL = r"https://www.eia.gov/consumption/residential/data/2015/c&e/ce4.9.xlsx"
COMMERCIAL_ENERGY_CONSUMPTION_URL = r"https://www.eia.gov/consumption/commercial/data/2012/c&e/xls/pba3.xlsx"

''' LODES Download '''
LODES_URL = "https://lehd.ces.census.gov/data/lodes/LODES7"
LODES_FILE_TYPES = ["od", "rac", "wac"]
LODES_STATES = ["al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl",
                "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la",
                "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne",
                "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
                "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt",
                "va", "wa", "wv", "wi", "wy", "dc"]
LODES_WORKFORCE_SEGMENTS = ["S000", "SA01", "SA02", "SA03", "SE01", "SE02", "SE03", "SI01", "SI02", "SI03", ""]
LODES_PART = ["main", "aux", ""]
LODES_JOB_TYPES = ["JT00", "JT01", "JT02", "JT03", "JT04", "JT05"]
LODES_AGG_GEOS = ["st", "cty", "trct", "bgrp", "cbsa", "zcta", ""]
