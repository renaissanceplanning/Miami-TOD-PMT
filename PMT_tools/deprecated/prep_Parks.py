"""
Created: October 2020
@Author: Brian Froeb

Prepare park features
----------------------

This script defines a basic function for cleaning parks features
for use in the PMT. If run as the "main" script, default references
to raw parks features are used to push content to the cleaned data 
folder.
"""

# %% Imports
import geopandas as gpd
from PMT_tools.PMT import (make_path, mergeFeatures, copyFeatures, RAW, CLEANED)


# %% FUNCTION
def cleanParks(raw_dir, poly_fcs, points_fc, clean_dir, out_poly,
               out_points, drop_columns=[], rename_columns=[]):
    """
    Consolidates park polygons, tidying column names and saving features
    in the "cleaned" data directory. Park point features are copied from
    the "raw" data directory to the "cleaned" directory.

    Parameters
    -------------
    raw_dir: Path
        The directory where raw parks features are stored.
    poly_fcs: [String,...]
        The names of parks polygon feature clasess in `raw_dir`
    points_fc: String
        The name of the parks points feature class in `raw_dir`
    clean_dir: Path
        The directory whre cleaned parks features will be stored.
    out_poly: String
        The name of the output parks polygons feature class
    out_points: String
        The name of the outpu parks points feature class
    drop_columns: [[String,...]]
        A list of lists. Each list corresponds to feature classes listed in
        `poly_fcs`. Each includes column names to drop when combining
        features.
    rename_columns: [{String: String,...},...]
        A list of dictionaries. Each dictionary corresponds to feature classes
        listed in `poly_fcs`. Each has keys that reflect raw column names and
        values that assign new names to these columns.

    Returns
    --------
    None
        Writes two output files: `{clean_dir}/{out_poly}`,
        `{clean_dir}/{out_points}`
    """
    # Merge polygons
    mergeFeatures(raw_dir, poly_fcs, clean_dir, out_poly,
                  drop_columns=drop_columns, rename_columns=rename_columns)
    # Copy points
    in_points = make_path(raw_dir, points_fc)
    out_points = make_path(clean_dir, out_points)
    copyFeatures(in_points, out_points)


# %% Apply
if __name__ == "__main__":
    raw_dir = make_path(RAW, "Parks")
    clean_dir = make_path(CLEANED, "Parks")

    # Polygon specs
    poly_fcs = [
        "Municipal_Park.shp",
        "County_Park.shp",
        "Nat_St_Park.shp"
    ]
    drop_cols = [
        ["CONTACT", "MNGTAGCY"],
        ["NATAREA", "PKSCHOOL", "WIFI"],
        []  # Empty list if no drops are needed
    ]
    rename_cols = [
        {},  # Empty dict if no renames are needed
        {},
        {"FID": "ID",
         "ACRES": "TOTACRE",
         "SHAPE_Length": "Shape__Length",
         "SHAPE_Area": "Shape__Area"
         }
    ]
    out_poly = "Parks.shp"

    # Points spects
    points_fc = "Park_Facility.shp"
    out_points = "Park_Facility.shp"

    cleanParks(raw_dir, poly_fcs, points_fc, clean_dir, out_poly,
               out_points, drop_columns=drop_cols, rename_columns=rename_cols)
