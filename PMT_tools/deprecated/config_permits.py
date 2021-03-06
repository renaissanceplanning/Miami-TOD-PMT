"""
Configuration variables to be used with building permit data
"""
from pathlib import Path

config_path = Path(__file__).parent

# category code table
PERMITS_CAT_CODE_PEDOR = "PD"
CAT_CODE_TBL = Path(config_path, "road_impact_fee_cat_codes.csv")
DOR_CODE_TBL = Path(config_path, "Land_Use_Recode.csv")

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
        ("FOLIO_NUM", "PARCELNO"),
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
