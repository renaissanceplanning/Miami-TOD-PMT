from collections import OrderedDict

FIELDS_DICT = dict(
    [
        ("geometry", "geometry"),
        ("CRASH_DATE", "DATE"),
        ("DAYOWEEK", "WEEK_DAY"),
        ("LATITUDE", "LAT"),
        ("LONGITUDE", "LONG"),
        ("COUNTY_TXT", "COUNTY"),
        ("DHSCNTYCTY", "CITY"),
        ("PEDESTRIAN_RELATED_IND", "PED_TYPE"),
        ("BICYCLIST_RELATED_IND", "BIKE_TYPE"),
        ("GEO_URBAN_RURAL_IND", "LANDUSE"),
        ("INJSEVER", "INJSEVER"),
        ("SPEED_LIMIT", "SPEED_LIM"),
        ("MOST_HARM_EVNT_CD", "HARM_EVNT"),
        ("IN_TOWN_FLAG", "IN_TOWN"),
    ]
)

USE = FIELDS_DICT.keys()
DROPS = ["LAT", "LONG", "DATE", "BIKE_TYPE", "PED_TYPE",]
# CRS
IN_CRS = 4326  # WGS84 latitude/longitude
OUT_CRS = 6437  # NAD83(2011) / Florida East meters
# Data subset
COUNTY = "MIAMI-DADE"

# incident type dict
INCIDENT_TYPES = {
    "BIKE_TYPE": "BIKE",
    "PED_TYPE": "PEDESTRIAN",
}

# CODED VALUE LOOKUP
HARMFUL_CODES = OrderedDict(
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
SEVERITY_CODES = dict(
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
CITY_CODES = dict(
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