import os
from pathlib import Path


def makePath(in_folder, *subnames):
    """Dynamically set a path (e.g., for iteratively referencing
        year-specific geodatabases)
    Args:
        in_folder (str): String or Path
        subnames (list/tuple): A list of arguments to join in making the full path
            `{in_folder}/{subname_1}/.../{subname_n}
    Returns:
        Path
    """
    return os.path.join(in_folder, *subnames)


def validate_directory(directory):
    if os.path.isdir(directory):
        return directory
    else:
        try:
            os.makedirs(directory)
            return directory
        except:
            raise


def check_overwrite_path(output, overwrite=True):
    if Path.exists(output):
        if overwrite:
            print(
                f"--- --- deleting existing file {output}"
            )
            Path(output).unlink()
        else:
            raise RuntimeError(
                f"Output file {output} already exists"
            )


SCRIPTS = Path(r"K:\Projects\MiamiDade\PMT\code")
ROOT = Path(SCRIPTS).parents[0]
DATA = makePath(ROOT, "Data")
RAW = makePath(DATA, "Raw")
CLEANED = makePath(DATA, "Cleaned")
REF = makePath(DATA, "Reference")
BUILD = makePath(DATA, "Build")
BASIC_FEATURES = makePath(DATA, "PMT_BasicFeatures.gdb", "BasicFeatures")
YEAR_GDB_FORMAT = makePath(DATA, "IDEAL_PMT_{year}.gdb")
RIF_CAT_CODE_TBL = makePath(REF, "road_impact_fee_cat_codes.csv")
DOR_LU_CODE_TBL = makePath(REF, "Land_Use_Recode.csv")

YEARS = [2014, 2015, 2016, 2017, 2018, 2019]
SNAPSHOT_YEAR = 2019