import os
from pathlib import Path
import shutil
import time

import arcpy
import numpy as np
import pandas as pd
from six import string_types

from PMT_tools import PMT


def make_path(in_folder, *subnames):
    """Dynamically set a path (e.g., for iteratively referencing
        year-specific geodatabases)

    Args:
        in_folder (str): String or Path
        subnames (list/tuple): A list of arguments to join in making the full path
            `{in_folder}/{subname_1}/.../{subname_n}
    Returns:
        str: String path
    """
    return os.path.join(in_folder, *subnames)


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


def validate_directory(directory):
    if os.path.isdir(directory):
        return directory
    else:
        try:
            os.makedirs(directory)
            return directory
        except:
            raise





