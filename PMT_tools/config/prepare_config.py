from collections import OrderedDict

from ..PMT import (
    Comp,
    And,
    Column,
    AggColumn,
    Consolidation,
    NetLoader,
)

# CRS
IN_CRS = 4326  # WGS84 latitude/longitude
OUT_CRS = 6437  # NAD83(2011) / Florida East meters

""" Basic features configuration """
BASIC_UGB = "UrbanGrowthBoundary"
UDB_FLAG = "IN_UDB"
BASIC_STATIONS = "SMARTplanStations"
STN_NAME_FIELD = "Name"
STN_STATUS_FIELD = "Status"
STN_BUFF_DIST = "2640 Feet"
STN_BUFF_METERS = 804.672
STN_ID_FIELD = "Id"
STN_DISS_FIELDS = [STN_ID_FIELD, STN_NAME_FIELD, STN_STATUS_FIELD]
STN_CORRIDOR_FIELDS = [
    "Beach",
    "EastWest",
    "Green",
    "Kendall",
    "Metromover",
    "North",
    "Northeast",
    "Orange",
    "South",
    "AllCorridors",
]

BASIC_ALIGNMENTS = "SMARTplanAlignments"
ALIGN_DISS_FIELDS = "Corridor"
ALIGN_BUFF_DIST = "2640 Feet"
CORRIDOR_NAME_FIELD = "Corridor"

BASIC_STN_AREAS = "StationAreas"
BASIC_CORRIDORS = "Corridors"
BASIC_LONG_STN = "StationsLong"
BASIC_SUM_AREAS = "SummaryAreas"
STN_LONG_CORRIDOR = "Corridor"
SUMMARY_AREAS_COMMON_KEY = "SummID"  # Changed from RowID as ROWID appears to be a reserved word in MSSQL and ORACLE

SUMMARY_AREAS_BASIC_FIELDS = [
    SUMMARY_AREAS_COMMON_KEY,
    STN_NAME_FIELD,
    STN_STATUS_FIELD,
    CORRIDOR_NAME_FIELD,
]

BASIC_RENAME_DICT = {"EastWest": "East-West", "AllCorridors": "(All corridors)"}

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
DROP_CRASH = [
    "DATE",
    "BIKE_TYPE",
    "PED_TYPE",
]

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
Park Configurations
"""
PARK_POINT_COLS = [
    "FOLIO",
    "ID",
    "NAME",
    "ADDRESS",
    "CITY",
    "ZIPCODE",
    "PHONE",
    "CONTACT",
    "TOTACRE",
    "LAT",
    "LON",
    "POINT_X",
    "POINT_Y",
    "CLASS",
    "TYPE",
    "MNGTAGCY",
]
PARK_POINTS_COMMON_KEY = "PARK_PT_ID"
PARK_POLY_COMMON_KEY = "PARK_PY_ID"

"""
bike facilities configuration
"""
BIKE_FAC_COMMON_KEY = "FacID"
BIKE_FAC_COL = "Bike_Fac"
BIKE_MILES_COL = "Bike_Miles"

"""
Configuration variables to be used with building permit data
"""
PERMITS_CAT_CODE_PEDOR = "PD"

# status code dict
PERMITS_STATUS_DICT = dict(
    [
        ("C", "Collected"),
        ("A", "Assessed"),
        ("L", "Letter of Credit submitted"),
        ("B", "Bond submitted"),
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
PERMITS_DROPS = ["CONST_COST", "ADMIN_COST", "CAT_CODE"]

# Transit Ridership Tables
TRANSIT_COMMON_KEY = "TRANSIT_ID"
TRANSIT_FIELDS_DICT = {
    # "ADAY": "DAY_OF_WEEK",
    "ATIMEPER": "TIME_PERIOD",
    # "AROUTE": "ROUTE",
    # "ATRIP": "TRIP_TIME",
    # "ABLOCK": "BLOCK_OPERATED",
    # "ADIR": "DIRECTION",
    # "ASTOP": "SEQUENTIAL_STOP_NO",
    # "AQSTOP": "UNIQUE_STOP_NO",
    # "ANAMSTP": "STOP_NAME",
    "ALAT": "LAT",
    "ALONG": "LONG",
    # "ADWELL_TIME": "DWELL_TIME",
}
TRANSIT_RIDERSHIP_TABLES = {
    2014: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1411_2015_APR_standard_format.XLS",
    2015: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1512_2016_APR_standard_format.XLS",
    2016: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1611_2017_APR_standard_format.XLS",
    2017: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1803_2018_APR_standard_format.XLS",
    2018: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1811_2019_APR_standard_format.XLS",
    2019: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_2003_2020_APR_standard_format.XLS",
    "NearTerm": "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_2003_2020_APR_standard_format.XLS",
}
TRANSIT_LAT = "LAT"
TRANSIT_LONG = "LONG"

# parcel config
PARCEL_DOR_KEY = "PARCELNO"
PARCEL_NAL_KEY = "PARCEL_ID"
PARCEL_COMMON_KEY = "FOLIO"
PARCEL_USE_COLS = {
    2019: [
        "CO_NO",
        "PARCEL_ID",
        "DOR_UC",
        "JV",
        "TV_NSD",
        "LND_VAL",
        "NCONST_VAL",
        "LND_SQFOOT",
        "TOT_LVG_AR",
        "NO_BULDNG",
        "NO_RES_UNT",  # "ACT_YR_BLT"
    ],
    "DEFAULT": [
        "CO_NO",
        "PARCEL_ID",
        "DOR_UC",
        "JV",
        "TV_NSD",
        "LND_VAL",
        "NCONST_VAL",
        "LND_SQFOOT",
        "TOT_LVG_AREA",
        "NO_BULDNG",
        "NO_RES_UNTS",  # "ACT_YR_BLT"
    ],
}
PARCEL_COLS = {
    2019: {
        "CO_NO": "CO_NO",
        "PARCEL_ID": "PARCEL_ID",
        "FILE_T": "FILE_T",
        "ASMNT_YR": "ASMNT_YR",
        "BAS_STRT": "BAS_STRT",
        "ATV_STRT": "ATV_STRT",
        "GRP_NO": "GRP_NO",
        "DOR_UC": "DOR_UC",
        "PA_UC": "PA_UC",
        "SPASS_CD": "SPASS_CD",
        "JV": "JV",
        "JV_CHNG": "JV_CHNG",
        "JV_CHNG_CD": "JV_CHNG_CD",
        "AV_SD": "AV_SD",
        "AV_NSD": "AV_NSD",
        "TV_SD": "TV_SD",
        "TV_NSD": "TV_NSD",
        "JV_HMSTD": "JV_HMSTD",
        "AV_HMSTD": "AV_HMSTD",
        "JV_NON_HMS": "JV_NON_HMSTD_RESD",
        "AV_NON_HMS": "AV_NON_HMSTD_RESD",
        "JV_RESD_NO": "JV_RESD_NON_RESD",
        "AV_RESD_NO": "AV_RESD_NON_RESD",
        "JV_CLASS_U": "JV_CLASS_USE",
        "AV_CLASS_U": "AV_CLASS_USE",
        "JV_H2O_REC": "JV_H2O_RECHRGE",
        "AV_H2O_REC": "AV_H2O_RECHRGE",
        "JV_CONSRV_": "JV_CONSRV_LND",
        "AV_CONSRV_": "AV_CONSRV_LND",
        "JV_HIST_CO": "JV_HIST_COM_PROP",
        "AV_HIST_CO": "AV_HIST_COM_PROP",
        "JV_HIST_SI": "JV_HIST_SIGNF",
        "AV_HIST_SI": "AV_HIST_SIGNF",
        "JV_WRKNG_W": "JV_WRKNG_WTRFNT",
        "AV_WRKNG_W": "AV_WRKNG_WTRFNT",
        "NCONST_VAL": "NCONST_VAL",
        "DEL_VAL": "DEL_VAL",
        "PAR_SPLT": "PAR_SPLT",
        "DISTR_CD": "DISTR_CD",
        "DISTR_YR": "DISTR_YR",
        "LND_VAL": "LND_VAL",
        "LND_UNTS_C": "LND_UNTS_CD",
        "NO_LND_UNT": "NO_LND_UNTS",
        "LND_SQFOOT": "LND_SQFOOT",
        "DT_LAST_IN": "DT_LAST_INSPT",
        "IMP_QUAL": "IMP_QUAL",
        "CONST_CLAS": "CONST_CLASS",
        "EFF_YR_BLT": "EFF_YR_BLT",
        "ACT_YR_BLT": "ACT_YR_BLT",
        "TOT_LVG_AR": "TOT_LVG_AREA",
        "NO_BULDNG": "NO_BULDNG",
        "NO_RES_UNT": "NO_RES_UNTS",
        "SPEC_FEAT_": "SPEC_FEAT_VAL",
        "M_PAR_SAL1": "MULTI_PAR_SAL1",
        "QUAL_CD1": "QUAL_CD1",
        "VI_CD1": "VI_CD1",
        "SALE_PRC1": "SALE_PRC1",
        "SALE_YR1": "SALE_YR1",
        "SALE_MO1": "SALE_MO1",
        "OR_BOOK1": "OR_BOOK1",
        "OR_PAGE1": "OR_PAGE1",
        "CLERK_NO1": "CLERK_NO1",
        "S_CHNG_CD1": "SAL_CHNG_CD1",
        "M_PAR_SAL2": "MULTI_PAR_SAL2",
        "QUAL_CD2": "QUAL_CD2",
        "VI_CD2": "VI_CD2",
        "SALE_PRC2": "SALE_PRC2",
        "SALE_YR2": "SALE_YR2",
        "SALE_MO2": "SALE_MO2",
        "OR_BOOK2": "OR_BOOK2",
        "OR_PAGE2": "OR_PAGE2",
        "CLERK_NO2": "CLERK_NO2",
        "S_CHNG_CD2": "SAL_CHNG_CD2",
        "OWN_NAME": "OWN_NAME",
        "OWN_ADDR1": "OWN_ADDR1",
        "OWN_ADDR2": "OWN_ADDR2",
        "OWN_CITY": "OWN_CITY",
        "OWN_STATE": "OWN_STATE",
        "OWN_ZIPCD": "OWN_ZIPCD",
        "OWN_STATE_": "OWN_STATE_DOM",
        "FIDU_NAME": "FIDU_NAME",
        "FIDU_ADDR1": "FIDU_ADDR1",
        "FIDU_ADDR2": "FIDU_ADDR2",
        "FIDU_CITY": "FIDU_CITY",
        "FIDU_STATE": "FIDU_STATE",
        "FIDU_ZIPCD": "FIDU_ZIPPCD",
        "FIDU_CD": "FIDU_CD",
        "S_LEGAL": "S_LEGAL",
        "APP_STAT": "APP_STAT",
        "CO_APP_STA": "CO_APP_STAT",
        "MKT_AR": "MKT_AR",
        "NBRHD_CD": "NBRHD_CD",
        "PUBLIC_LND": "PUBLIC_LND",
        "TAX_AUTH_C": "TAX_AUTH_CD",
        "TWN": "TWN",
        "RNG": "RNG",
        "SEC": "SEC",
        "CENSUS_BK": "CENSUS_BK",
        "PHY_ADDR1": "PHY_ADDR1",
        "PHY_ADDR2": "PHY_ADDR2",
        "PHY_CITY": "PHY_CITY",
        "PHY_ZIPCD": "PHY_ZIPCD",
        "ASS_TRNSFR": "ASS_TRNSFR_FG",
        "PREV_HMSTD": "PREV_HMSTD_OWN",
        "ASS_DIF_TR": "ASS_DIF_TRNS",
        "CONO_PRV_H": "CONO_PRV_HM",
        "PARCEL_ID_": "PARCEL_ID_PRV_HMSTD",
        "YR_VAL_TRN": "YR_VAL_TRNSF",
        "SEQ_NO": "SEQ_NO",
        "RS_ID": "RS_ID",
        "MP_ID": "MP_ID",
        "STATE_PAR_": "STATE_PARCEL_ID",
    }
}
LAND_USE_COMMON_KEY = "DOR_UC"
PARCEL_AREA_COL = "LND_SQFOOT"
PARCEL_BLD_AREA_COL = "TOT_LVG_AREA"
PARCEL_LU_AREAS = {
    # COL_NAME: (which_field, criteria)
    # "VAC_AREA": ["GN_VA_LU", "Vacant/Undeveloped"],
    # "RES_AREA": ["RES_NRES", "RES"],
    # "NRES_AREA": ["RES_NRES", "NRES"]
    "VAC_AREA": ["GN_VA_LU", Comp(comp_method="==", v="Vacant/Undeveloped")],
    "RES_AREA": ["NO_RES_UNTS", Comp(comp_method=">", v=0)],
    "NRES_AREA": ["Total_Employment", Comp(comp_method=">", v=0)],
}

# Block groups config
BG_COMMON_KEY = "GEOID"
BG_PAR_SUM_FIELDS = [
    "LND_VAL",
    "LND_SQFOOT",
    "JV",
    "NO_BULDNG",
    "NO_RES_UNTS",
    "TOT_LVG_AREA",
]
BLOCK_COMMON_KEY = "GEOID10"

# LODES/ACS config
ACS_COMMON_KEY = "GEOID10"
ACS_YEARS = [2014, 2015, 2016, 2017, 2018, 2019]
LODES_COMMON_KEY = "bgrp"
LODES_YEARS = [2014, 2015, 2016, 2017, 2018]

ACS_RACE_FIELDS = [ACS_COMMON_KEY] + [
    "Total_Non_Hisp",
    "Total_Hispanic",
    "White_Non_Hisp",
    "Black_Non_Hisp",
    "Asian_Non_Hisp",
    "Multi_Non_Hisp",
    "White_Hispanic",
    "Black_Hispanic",
    "Asian_Hispanic",
    "Multi_Hispanic",
    "Other_Non_Hisp",
    "Other_Hispanic",
]
ACS_COMMUTE_FIELDS = [ACS_COMMON_KEY] + [
    "Total_Commutes",
    "Drove_alone",
    "Carpool",
    "Transit",
    "Taxi",
    "Motorcycle",
    "Bicycle",
    "Walk",
    "Other",
    "Work_From_Home",
    "Drove",
    "NonMotor",
    "AllOther",
    "SOV_Share",
    "HOV_Share",
    "PT_Share",
    "NM_Share",
    "Oth_Share",
    "WFH_Share",
]
LODES_FIELDS = [LODES_COMMON_KEY] + [
    "C000",
    "CA01",
    "CA02",
    "CA03",
    "CE01",
    "CE02",
    "CE03",
    "CNS01",
    "CNS02",
    "CNS03",
    "CNS04",
    "CNS05",
    "CNS06",
    "CNS07",
    "CNS08",
    "CNS09",
    "CNS10",
    "CNS11",
    "CNS12",
    "CNS13",
    "CNS14",
    "CNS15",
    "CNS16",
    "CNS17",
    "CNS18",
    "CNS19",
    "CNS20",
    "CR01",
    "CR02",
    "CR03",
    "CR04",
    "CR05",
    "CR07",
    "CT01",
    "CT02",
    "CD01",
    "CD02",
    "CD03",
    "CD04",
    "CS01",
    "CS02",
    "CFA01",
    "CFA02",
    "CFA03",
    "CFA04",
    "CFA05",
    "CFS01",
    "CFS02",
    "CFS03",
    "CFS04",
    "CFS05",
]

LODES_CRITERIA = {
    "CNS_01_par": And([Comp(">=", 50), Comp("<=", 69)]),
    "CNS_02_par": Comp("==", 92),
    "CNS_03_par": Comp("==", 91),
    "CNS_04_par": [Comp("==", 17), Comp("==", 19)],
    "CNS_05_par": [Comp("==", 41), Comp("==", 42)],
    "CNS_06_par": Comp("==", 29),
    "CNS_07_par": And([Comp(">=", 11), Comp("<=", 16)]),
    "CNS_08_par": [Comp("==", 20), Comp("==", 48), Comp("==", 49)],
    "CNS_09_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_10_par": [Comp("==", 23), Comp("==", 24)],
    "CNS_11_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_12_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_13_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_14_par": Comp("==", 89),
    "CNS_15_par": [Comp("==", 72), Comp("==", 83), Comp("==", 84)],
    "CNS_16_par": [Comp("==", 73), Comp("==", 85)],
    "CNS_17_par": [And([Comp(">=", 30), Comp("<=", 38)]), Comp("==", 82)],
    "CNS_18_par": [Comp("==", 21), Comp("==", 22), Comp("==", 33), Comp("==", 39)],
    "CNS_19_par": [Comp("==", 27), Comp("==", 28)],
    "CNS_20_par": And([Comp(">=", 86), Comp("<=", 89)]),
    "RES_par": [
        And([Comp(">=", 1), Comp("<=", 9)]),
        And([Comp(">=", 100), Comp("<=", 102)]),
    ],
}

# SERPM config
#             base,  future
MODEL_YEARS = [2015]  # 2045
MAZ_COMMON_KEY = "MAZ"
TAZ_COMMON_KEY = "TAZ"
SERPM_RENAMES = {
    "mgra": MAZ_COMMON_KEY,
    "MAZ2010": MAZ_COMMON_KEY,
    "REG_TAZ": TAZ_COMMON_KEY,
}
SKIM_IMP_FIELD = "TIME"
SKIM_O_FIELD = "OName"
SKIM_D_FIELD = "DName"
SKIM_RENAMES = {"F_TAZ": SKIM_O_FIELD, "T_TAZ": SKIM_D_FIELD, "TOTTIME": SKIM_IMP_FIELD}
SKIM_DTYPES = {"F_TAZ": int, "T_TAZ": int, SKIM_IMP_FIELD: float, "TOTTIME": float}

# - MAZ aggregation specs
MAZ_AGG_COLS = [
    AggColumn("NO_RES_UNTS", rename="HH"),
    AggColumn("Total_Employment", rename="TotalJobs"),
    AggColumn("CNS16_PAR", rename="HCJobs"),
    AggColumn("CNS15_PAR", rename="EdJobs"),
]
# - MAZ consolidation specs (from parcels)
MAZ_PAR_CONS = [
    Consolidation(name="RsrcJobs", input_cols=["CNS01_PAR", "CNS02_PAR"]),
    Consolidation(name="IndJobs", input_cols=["CNS05_PAR", "CNS06_PAR", "CNS08_PAR"]),
    Consolidation(name="ConsJobs", input_cols=["CNS07_PAR", "CNS17_PAR", "CNS18_PAR"]),
    Consolidation(
        name="OffJobs",
        input_cols=[
            "CNS09_PAR",
            "CNS10_PAR",
            "CNS11_PAR",
            "CNS12_PAR",
            "CNS13_PAR",
            "CNS20_PAR",
        ],
    ),
    Consolidation(
        name="OthJobs", input_cols=["CNS03_PAR", "CNS04_PAR", "CNS14_PAR", "CNS19_PAR"]
    ),
]
# - MAZ consolidation specs (from MAZ se data)
MAZ_SE_CONS = [
    Column(name="HH", rename="HH"),
    Column(name="emp_total", rename="TotalJobs"),
    Consolidation(
        name="ConsJobs",
        input_cols=[
            "emp_retail",
            "emp_amusement",
            "emp_hotel",
            "emp_restaurant_bar",
            "emp_personal_svcs_retail",
            "emp_state_local_gov_ent",
        ],
    ),
    Consolidation(
        name="EdJobs",
        input_cols=["emp_pvt_ed_k12", "emp_pvt_ed_post_k12_oth", "emp_public_ed"],
    ),
    Column(name="emp_health", rename="HCJobs"),
    Consolidation(
        name="IndJobs",
        input_cols=["emp_mfg_prod", "emp_mfg_office", "emp_whsle_whs", "emp_trans"],
    ),
    Consolidation(
        name="OffJobs",
        input_cols=[
            "emp_prof_bus_svcs",
            "emp_personal_svcs_office",
            "emp_state_local_gov_white",
            "emp_own_occ_dwell_mgmt",
            "emp_fed_gov_accts",
            "emp_st_lcl_gov_accts",
            "emp_cap_accts",
        ],
    ),
    Consolidation(
        name="OthJobs",
        input_cols=[
            "emp_const_non_bldg_prod",
            "emp_const_non_bldg_office",
            "emp_utilities_prod",
            "emp_utilities_office",
            "emp_const_bldg_prod",
            "emp_const_bldg_office",
            "emp_prof_bus_svcs_bldg_maint",
            "emp_religious",
            "emp_pvt_hh",
            "emp_scrap_other",
            "emp_fed_non_mil",
            "emp_fed_mil",
            "emp_state_local_gov_blue",
        ],
    ),
    Column(name="emp_ag", rename="RsrcJobs"),
    Consolidation(
        name="EnrollAdlt",
        input_cols=["collegeEnroll", "otherCollegeEnroll", "AdultSchEnrl"],
    ),
    Consolidation(
        name="EnrollK12",
        input_cols=["EnrollGradeKto8", "EnrollGrade9to12", "PrivateEnrollGradeKto8"],
    ),
]

# osm config
NET_BY_YEAR = {
    # Year: [osm_data_pull, bas
    2014: ["_q3_2020", MODEL_YEARS[0]],
    2015: ["_q3_2020", MODEL_YEARS[0]],
    2016: ["_q3_2020", MODEL_YEARS[0]],
    2017: ["_q3_2020", MODEL_YEARS[0]],
    2018: ["_q3_2020", MODEL_YEARS[0]],
    2019: ["_q3_2020", MODEL_YEARS[0]],
    "NearTerm": ["_q3_2020", MODEL_YEARS[0]],
    # "LongTerm": ["_q3_2020", MODEL_YEARS[1]]
}
#NETS_DIR = makePath(CLEANED, "osm_networks")
SEARCH_CRITERIA = "edges SHAPE;osm_ND_Junctions NONE"
SEARCH_QUERY = "edges #;osm_ND_Junctions #"
NET_LOADER = NetLoader(
    "1500 meters",
    search_criteria=SEARCH_CRITERIA,
    match_type="MATCH_TO_CLOSEST",
    append="CLEAR",
    exclude_restricted="EXCLUDE",
    search_query=SEARCH_QUERY,
)
OSM_IMPED = "Minutes"
OSM_CUTOFF = "15 30"
BIKE_RESTRICTIONS = "Oneway;IsCycleway;LTS1;LTS2;LTS3;LTS4"
BIKE_PED_CUTOFF = 60

# centrality config
CENTRALITY_IMPED = "Length"
CENTRALITY_CUTOFF = "1609"
CENTRALITY_NET_LOADER = NetLoader(
    search_tolerance="5 meters",
    search_criteria="edges NONE;osm_ND_Junctions END",
    match_type="MATCH_TO_CLOSEST",
    append="CLEAR",
    snap="NO_SNAP",
    offset="5 meters",
    exclude_restricted="INCLUDE",
    search_query="edges #;osm_ND_Junctions #",
)

# walk times config
TIME_BIN_CODE_BLOCK = """
def assignBin(value):
    if value is None:
        return ""
    elif value <= 5:
        return "0 to 5 minutes"
    elif value <= 10:
        return "5 to 10 minutes"
    elif value <= 15:
        return "10 to 15 minutes"
    elif value <= 20:
        return "15 to 20 minutes"
    elif value <= 25:
        return "20 to 25 minutes"
    elif value <= 30:
        return "25 to 30 minutes"
    else:
        return "over 30 minutes"
"""
IDEAL_WALK_MPH = 3.0
IDEAL_WALK_RADIUS = "7920 Feet"

# accessibility scores
ACCESS_MODES = ["Auto", "Transit", "Walk", "Bike"]
MODE_SCALE_REF = {
    # Mode: [source (cleaned folder), scale, id_field]
    "Auto": ["SERPM", "taz", TAZ_COMMON_KEY],
    "Transit": ["SERPM", "taz", TAZ_COMMON_KEY],
    "Walk": ["OSM_Networks", "maz", MAZ_COMMON_KEY],
    "Bike": ["OSM_Networks", "maz", MAZ_COMMON_KEY],
}
ACCESS_TIME_BREAKS = [15, 30, 45, 60]
ACCESS_UNITS = "Min"
D_ACT_FIELDS = ["TotalJobs", "ConsJobs", "HCJobs", "EnrollAdlt", "EnrollK12"]
O_ACT_FIELDS = ["HH"]

"""
Configuration variables to be used in contiguity + developable area
"""

# will use YEAR_GDB_FORMAT for parcels_path
# will use PARCEL_COMMON_KEY for parcels_id_field
CTGY_CHUNKS = 20
CTGY_CELL_SIZE = 40
CTGY_WEIGHTS = "nn"
CTGY_SAVE_FULL = False  # should the table of sub-polygon results be saved to?
CTGY_SUMMARY_FUNCTIONS = ["min", "max", "median", "mean"]
CTGY_SCALE_AREA = True
# TODO: fill in the PROPER buildings path
BUILDINGS_PATH = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\OSM_Buildings\OSM_Buildings_20201001111703.shp"

"""
Configuration variables to be used in land use diversity
"""
LU_RECODE_FIELD = "DIV_CLASS"
DIV_RELEVANT_LAND_USES = [
    "auto",
    "civic",
    "education",
    "entertainment",
    "grocery",
    "healthcare",
    "industrial",
    "lodging",
    "mf",
    "office",
    "restaurant",
    "sf",
    "shopping",
]
# DIV_CHISQ_PROPS = None
# DIV_REGIONAL_ADJ = True
# DIV_REGIONAL_CONSTS = None

"""
Configuration variables to be used in permits/short term parcels prep
"""
# Use PARCEL_LU_COL for lu_match_field
# Use PARCEL_BLD_AREA for parcels_living_area_field
PERMITS_COMMON_KEY = "FOLIO"
PERMITS_UNITS_FIELD = "UNITS"
PERMITS_BLD_AREA_NAME = "sq. ft."
# format -->
PARCEL_REF_TABLE_UNITS_MATCH = {
    "bed": "NO_RES_UNTS",
    "room": "NO_RES_UNTS",
    "unit": "NO_RES_UNTS",
    "acre": "LND_SQFOOT / 43560",
}
PERMITS_REF_TABLE_UNITS_MATCH = {
    "student_20": "NO_RES_UNITS",
    "student_25": "NO_RES_UNITS",
    "student_30": "NO_RES_UNITS",
}

# Use PARCEL_COMMON_KEY for parcels_id_field
# Use PARCEL_LU_COL for parcels_lu_field
# USE PARCEL_BLD_AREA parcels_living_area_field
PARCEL_LAND_VALUE = "LND_VAL"
PARCEL_JUST_VALUE = "JV"
PARCEL_BUILDINGS = "NO_BULDNG"
PERMITS_ID_FIELD = PARCEL_COMMON_KEY
PERMITS_LU_FIELD = "DOR_UC"
# Use PERMITS_UNITS_FIELD for permits units field
PERMITS_VALUES_FIELD = "UNITS_VAL"
PERMITS_COST_FIELD = "COST"
# permit unit: parcel_field
# SHORT_TERM_PARCELS_UNITS_MATCH = {
#     "bed": "NO_RES_UNTS",
#     "room": "NO_RES_UNTS",
#     "unit": "NO_RES_UNTS",
# }
SHORT_TERM_PARCELS_UNITS_MATCH = {
    "NO_RES_UNTS": ["bed", "room", "unit"]
}
