from pathlib import Path

this_file = Path(__file__).parent

SCRIPTS = this_file.parent
ROOT = Path(SCRIPTS).parents[0]
DATA = Path(ROOT, "Data")
RAW = Path(DATA, "raw")
CLEANED = Path(DATA, "cleaned")
REF = Path(DATA, "../ref")
BASIC_FEATURES = Path(ROOT, "Basic_features.gdb", "Basic_features_SPFLE")
YEARS = [2014, 2015, 2016, 2017, 2018, 2019]
SNAPSHOT_YEAR = 2019


RIF_CAT_CODE_TBL = Path(REF, "road_impact_fee_cat_codes.csv")
DOR_LU_CODE_TBL = Path(REF, "Land_Use_Recode.csv")