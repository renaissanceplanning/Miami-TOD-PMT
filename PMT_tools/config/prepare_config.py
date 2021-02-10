from collections import OrderedDict
""" Bike Ped Crash Configs """
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

USE_CRASH = list(CRASH_FIELDS_DICT.keys())
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

# code conversions
CRASH_CODE_TO_STRING = ["CITY", "INJSEVER", "HARM_EVNT"]
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
CRASH_SEVERITY_CODES = OrderedDict(
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
CRASH_CITY_CODES = OrderedDict(
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

"""
Configuration variables to be used with building permit data
"""
from PMT_tools.config_project import RIF_CAT_CODE_TBL, DOR_LU_CODE_TBL
PERMITS_CAT_CODE_PEDOR = "PD"


# status code dict
PERMITS_STATUS_DICT = dict(
    [
        ("C", "Collected"),
        ("A", "Assessed"),
        ("L", "Letter of Credit submitted"),
        ("B", "Bond submitted")
    ]
)


PERMITS_FIELDS_DICT = dict(
    [
        ("PROC_NUM", "PROC_NUM"),
        ("FOLIO_NUM", "FOLIO"),
        ("SITE_ADDR", "ADDRESS"),
        ("ASSE_DATE", "DATE"),
        ("STATUS_CODE", "STATUS"),
        ("COL_CON", "CONST_COST"),
        ("COL_ADM", "ADMIN_COST"),
        ("ASSD_CATTHRES_CATC_CODE", "CAT_CODE"),
        ("ASSD_BASIS_QTY", "UNITS_VAL"),
    ]
)

PERMITS_USE = PERMITS_FIELDS_DICT.keys()
PERMITS_DROPS = ['CONST_COST', "ADMIN_COST", "CAT_CODE"]

# CRS
IN_CRS = 4326  # WGS84 latitude/longitude
OUT_CRS = 6437  # NAD83(2011) / Florida East meters

# Transit Ridership Tables
TRANSIT_FIELDS_DICT = {
    "ADAY": "DAY_OF_WEEK",
    "ATIMEPER": "TIME_PERIOD",
    "AROUTE": "ROUTE",
    "ATRIP": "TRIP_TIME",
    "ABLOCK": "BLOCK_OPERATED",
    "ADIR": "DIRECTION",
    "ASTOP": "SEQUENTIAL_STOP_NO",
    "AQSTOP": "UNIQUE_STOP_NO",
    "ANAMSTP": "STOP_NAME",
    "ALAT": "LAT",
    "ALONG": "LONG",
    "ADWELL_TIME": "DWELL_TIME",
}
TRANSIT_RIDERSHIP_TABLES = {
    2014: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1411_2015_APR_standard_format.XLS",
    2015: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1512_2016_APR_standard_format.XLS",
    2016: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1611_2017_APR_standard_format.XLS",
    2017: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1803_2018_APR_standard_format.XLS",
    2018: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1811_2019_APR_standard_format.XLS",
    2019: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_2003_2020_APR_standard_format.XLS",
}
TRANSIT_LAT = "LAT"
TRANSIT_LONG = "LONG"


# parcel config
PARCEL_COMMON_KEY = "FOLIO"
PARCEL_USE_COLS = {
    2019: [
        "CO_NO", "PARCEL_ID", "DOR_UC", "JV", "TV_NSD", "LND_VAL",
        "NCONST_VAL", "LND_SQFOOT", "TOT_LVG_AR", "NO_BULDNG",
        "NO_RES_UNT", "ACT_YR_BLT"
    ],
    "DEFAULT": [
        "CO_NO", "PARCEL_ID", "DOR_UC", "JV", "TV_NSD", "LND_VAL",
        "NCONST_VAL", "LND_SQFOOT", "TOT_LVG_AREA", "NO_BULDNG",
        "NO_RES_UNTS", "ACT_YR_BLT"
    ]
}
PARCEL_COLS = {
    2019: {
        'CO_NO': 'CO_NO',
        'PARCEL_ID': 'PARCEL_ID',
        'FILE_T': 'FILE_T',
        'ASMNT_YR': 'ASMNT_YR',
        'BAS_STRT': 'BAS_STRT',
        'ATV_STRT': 'ATV_STRT',
        'GRP_NO': 'GRP_NO',
        'DOR_UC': 'DOR_UC',
        'PA_UC': 'PA_UC',
        'SPASS_CD': 'SPASS_CD',
        'JV': 'JV',
        'JV_CHNG': 'JV_CHNG',
        'JV_CHNG_CD': 'JV_CHNG_CD',
        'AV_SD': 'AV_SD',
        'AV_NSD': 'AV_NSD',
        'TV_SD': 'TV_SD',
        'TV_NSD': 'TV_NSD',
        'JV_HMSTD': 'JV_HMSTD',
        'AV_HMSTD': 'AV_HMSTD',
        'JV_NON_HMS': 'JV_NON_HMSTD_RESD',
        'AV_NON_HMS': 'AV_NON_HMSTD_RESD',
        'JV_RESD_NO': 'JV_RESD_NON_RESD',
        'AV_RESD_NO': 'AV_RESD_NON_RESD',
        'JV_CLASS_U': 'JV_CLASS_USE',
        'AV_CLASS_U': 'AV_CLASS_USE',
        'JV_H2O_REC': 'JV_H2O_RECHRGE',
        'AV_H2O_REC': 'AV_H2O_RECHRGE',
        'JV_CONSRV_': 'JV_CONSRV_LND',
        'AV_CONSRV_': 'AV_CONSRV_LND',
        'JV_HIST_CO': 'JV_HIST_COM_PROP',
        'AV_HIST_CO': 'AV_HIST_COM_PROP',
        'JV_HIST_SI': 'JV_HIST_SIGNF',
        'AV_HIST_SI': 'AV_HIST_SIGNF',
        'JV_WRKNG_W': 'JV_WRKNG_WTRFNT',
        'AV_WRKNG_W': 'AV_WRKNG_WTRFNT',
        'NCONST_VAL': 'NCONST_VAL',
        'DEL_VAL': 'DEL_VAL',
        'PAR_SPLT': 'PAR_SPLT',
        'DISTR_CD': 'DISTR_CD',
        'DISTR_YR': 'DISTR_YR',
        'LND_VAL': 'LND_VAL',
        'LND_UNTS_C': 'LND_UNTS_CD',
        'NO_LND_UNT': 'NO_LND_UNTS',
        'LND_SQFOOT': 'LND_SQFOOT',
        'DT_LAST_IN': 'DT_LAST_INSPT',
        'IMP_QUAL': 'IMP_QUAL',
        'CONST_CLAS': 'CONST_CLASS',
        'EFF_YR_BLT': 'EFF_YR_BLT',
        'ACT_YR_BLT': 'ACT_YR_BLT',
        'TOT_LVG_AR': 'TOT_LVG_AREA',
        'NO_BULDNG': 'NO_BULDNG',
        'NO_RES_UNT': 'NO_RES_UNTS',
        'SPEC_FEAT_': 'SPEC_FEAT_VAL',
        'M_PAR_SAL1': 'MULTI_PAR_SAL1',
        'QUAL_CD1': 'QUAL_CD1',
        'VI_CD1': 'VI_CD1',
        'SALE_PRC1': 'SALE_PRC1',
        'SALE_YR1': 'SALE_YR1',
        'SALE_MO1': 'SALE_MO1',
        'OR_BOOK1': 'OR_BOOK1',
        'OR_PAGE1': 'OR_PAGE1',
        'CLERK_NO1': 'CLERK_NO1',
        'S_CHNG_CD1': 'SAL_CHNG_CD1',
        'M_PAR_SAL2': 'MULTI_PAR_SAL2',
        'QUAL_CD2': 'QUAL_CD2',
        'VI_CD2': 'VI_CD2',
        'SALE_PRC2': 'SALE_PRC2',
        'SALE_YR2': 'SALE_YR2',
        'SALE_MO2': 'SALE_MO2',
        'OR_BOOK2': 'OR_BOOK2',
        'OR_PAGE2': 'OR_PAGE2',
        'CLERK_NO2': 'CLERK_NO2',
        'S_CHNG_CD2': 'SAL_CHNG_CD2',
        'OWN_NAME': 'OWN_NAME',
        'OWN_ADDR1': 'OWN_ADDR1',
        'OWN_ADDR2': 'OWN_ADDR2',
        'OWN_CITY': 'OWN_CITY',
        'OWN_STATE': 'OWN_STATE',
        'OWN_ZIPCD': 'OWN_ZIPCD',
        'OWN_STATE_': 'OWN_STATE_DOM',
        'FIDU_NAME': 'FIDU_NAME',
        'FIDU_ADDR1': 'FIDU_ADDR1',
        'FIDU_ADDR2': 'FIDU_ADDR2',
        'FIDU_CITY': 'FIDU_CITY',
        'FIDU_STATE': 'FIDU_STATE',
        'FIDU_ZIPCD': 'FIDU_ZIPPCD',
        'FIDU_CD': 'FIDU_CD',
        'S_LEGAL': 'S_LEGAL',
        'APP_STAT': 'APP_STAT',
        'CO_APP_STA': 'CO_APP_STAT',
        'MKT_AR': 'MKT_AR',
        'NBRHD_CD': 'NBRHD_CD',
        'PUBLIC_LND': 'PUBLIC_LND',
        'TAX_AUTH_C': 'TAX_AUTH_CD',
        'TWN': 'TWN',
        'RNG': 'RNG',
        'SEC': 'SEC',
        'CENSUS_BK': 'CENSUS_BK',
        'PHY_ADDR1': 'PHY_ADDR1',
        'PHY_ADDR2': 'PHY_ADDR2',
        'PHY_CITY': 'PHY_CITY',
        'PHY_ZIPCD': 'PHY_ZIPCD',
        'ASS_TRNSFR': 'ASS_TRNSFR_FG',
        'PREV_HMSTD': 'PREV_HMSTD_OWN',
        'ASS_DIF_TR': 'ASS_DIF_TRNS',
        'CONO_PRV_H': 'CONO_PRV_HM',
        'PARCEL_ID_': 'PARCEL_ID_PRV_HMSTD',
        'YR_VAL_TRN': 'YR_VAL_TRNSF',
        'SEQ_NO': 'SEQ_NO',
        'RS_ID': 'RS_ID',
        'MP_ID': 'MP_ID',
        'STATE_PAR_': 'STATE_PARCEL_ID'
    }
}

