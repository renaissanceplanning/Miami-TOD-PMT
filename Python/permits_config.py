"""
Configuration variables to be used with building permit data
"""

FIELDS_DICT = dict(
    [
        ("ASSE_DATE", "DATE"),
        ("FOLIO_NUM", "PARCEL_NO"),
        ("SITE_ADDR", "ADDRESS"),
        ("PROC_NUM", "PROC_NUM"),
        ("ASSD_CATTHRES_CATC_CODE", "CAT_CODE"),
        ("ASSD_BASIS_QTY", "UNITS"),
        ("COL_CON", "CONST_COST"),
        ("COL_ADM", "ADMIN_COST"),
    ]
)

USE = FIELDS_DICT.keys()
DROPS = ["LAT", "LONG", "DATE", "BIKE_TYPE", "PED_TYPE"]
# CRS
IN_CRS = 4326  # WGS84 latitude/longitude
OUT_CRS = 6437  # NAD83(2011) / Florida East meters
# Data subset
COUNTY = "MIAMI-DADE"
