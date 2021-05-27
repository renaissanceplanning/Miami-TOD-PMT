import os
from pathlib import Path
import shutil
import time


class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""


class Timer:
    def __init__(self):
        self._start_time = None

    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()
        print("Timer has started...")

    def stop(self):
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        elapsed_time = (time.perf_counter() - self._start_time)
        if elapsed_time > 60:
            elapsed_time = elapsed_time/60
            print(f"Elapsed time: {elapsed_time:0.4f} minutes")
        if elapsed_time > 3600:
            elapsed_time /= 3600
            print(f"Elapsed time: {elapsed_time:0.4f} hours")
        self._start_time = None


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
    output = Path(output)
    if output.exists():
        if overwrite:
            if output.is_file():
                print(f"--- --- deleting existing file {output.name}")
                output.unlink()
            if output.is_dir():
                print(f"--- --- deleting existing folder {output.name}")
                shutil.rmtree(output)
        else:
            print(
                f"Output file/folder {output} already exists"
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