"""
Created: October 2020
@Author: Alex Bell

Provides functions to estimate hypothetical walking times from parcels
to key features (transit stations, parks, e.g.). Walking times are estimated
based on straight-line distances between points and assumed travel speeds,
reflecting the "ideal" time (i.e, shortest possible path).

If run as "main", embellishes a parcel feature class in each PMT analysis
year geodatabase called "walk_times" to report hypotethical ideal walk time
information to transit stations and parks.
"""

# %% IMPORTS
import PMT
import arcpy
import pandas as pd
import numpy as np
from analyze_walk_time import CODE_BLOCK, _addField



# %% GLOBALS
WALK_MPH = 3.0
RADIUS = "7920 Feet"

# %% FUNCTIONS
def idealWalkTime(parcels_fc, parcel_id_field, target_fc, target_name_field,
                  radius, target_name, overlap_type="HAVE_THEIR_CENTER_IN",
                  sr=None, assumed_mph=3):
    """
    Estimate walk time between parcels and target features (stations, parks,
    e.g.) based on a straight-line distance estimate and an assumed walking
    speed.

    Parameters
    ------------
    parcels_fc: Path
    parcel_id_field: String
        A field that uniquely identifies features in `parcels_fc`
    target_fc: Path
    target_name_field: String
        A field that uniquely identifies features in `target_fc`
    radius: String
        A "linear unit" string for spatial selection ('5280 Feet', e.g.)
    target_name: String
        A string suffix included in output field names.
    overlap_type: String, default="HAVE_THEIR_CENTER_IN"
        A string specifying selection type (see ArcGIS )
    sr: SpatialReference, default=None
        A spatial reference code, string, or object to ensure parcel and
        target features are projected consistently. If `None`, the spatial
        reference from `parcels_fc` is used.
    assumed_mph: numeric, default=3
        The assumed average walk speed expressed in miles per hour.

    Returns
    --------
    None
        `parcel_fc` is modified in place to add new fields:
        `nearest_{target_name}`, `min_time_{target_name}`,
        `n_{target_name}`, `bin_{target_name}`
    """
    # output_fields
    nearest_field = f"nearest_{target_name}"
    min_time_field = f"min_time_{target_name}"
    n_field = f"n_{target_name}"
    bin_field = f"bin_{target_name}"

    # Set spatial reference
    if sr is None:
        sr = arcpy.Describe(parcels_fc).spatialReference
    else:
        sr = arcpy.SpatialReference(sr)
    mpu = float(sr.metersPerUnit)
    
    # Make feature layers
    par_lyr = arcpy.MakeFeatureLayer_management(parcels_fc, "parcels")
    tgt_lyr = arcpy.MakeFeatureLayer_management(target_fc, "target")
    
    try:
        print("... estimating ideal times")
        tgt_results = []
        # Iterate over targets
        tgt_fields = [target_name_field, "SHAPE@"]
        par_fields = [parcel_id_field, "SHAPE@X", "SHAPE@Y"]
        out_fields = [parcel_id_field, target_name_field, "minutes"]
        with arcpy.da.SearchCursor(
            tgt_lyr, tgt_fields, spatial_reference=sr) as tgt_c:
            for tgt_r in tgt_c:
                tgt_name, tgt_feature = tgt_r
                tgt_x = tgt_feature.centroid.X
                tgt_y = tgt_feature.centroid.Y
                # select parcels in target buffer
                arcpy.SelectLayerByLocation_management(
                    par_lyr, overlap_type, tgt_feature, search_distance=radius)
                # dump to df
                par_df = pd.DataFrame(
                    arcpy.da.FeatureClassToNumPyArray(
                        par_lyr, par_fields, spatial_reference=sr)
                )
                par_df[target_name_field] = tgt_name
                # estimate distances
                par_df["dx"] = par_df["SHAPE@X"] - tgt_x
                par_df["dy"] = par_df["SHAPE@Y"] - tgt_y
                par_df["meters"] = np.sqrt(par_df.dx ** 2 + par_df.dy ** 2) * mpu
                par_df["minutes"] = (par_df.meters * 60)/(assumed_mph * 1609.344)
                # store in mini df output
                tgt_results.append(par_df[out_fields].copy())

        # Bind up results
        print("... binding results")
        bind_df = pd.concat(tgt_results).set_index(target_name_field)
        
        # Group by/summarize
        print("... summarizing times")
        gb = bind_df.groupby(parcel_id_field)
        par_min = gb.min()
        par_count = gb.size()
        par_nearest = gb["minutes"].idxmin()
        sum_df = pd.concat([par_nearest, par_min, par_count], axis=1)
        sum_df.columns = [nearest_field, min_time_field, n_field]
        sum_df.reset_index(inplace=True)

        # Clean output fc columns
        del_fields = [nearest_field, min_time_field, n_field]
        for del_field in del_fields:
            check = arcpy.ListFields(parcels_fc, del_field)
            if check:
                print(f"... ... deleting field {del_field}")
                arcpy.DeleteField_management(parcels_fc, del_field)

        # Join to parcels
        print("... extending output table")
        PMT.extend_table_df(
            parcels_fc, parcel_id_field, sum_df, parcel_id_field)
        
        # Add bin field
        print("... classifying")
        bin_field = f"bin_{target_name}"
        _addField(parcels_fc, bin_field, "TEXT", field_length=20)
        arcpy.CalculateField_management(
            parcels_fc, bin_field, f"assignBin(!{min_time_field}!)",
            expression_type="PYTHON3", code_block=CODE_BLOCK
        )
    except:
        raise
    finally:
        arcpy.Delete_management(par_lyr)
        arcpy.Delete_management(tgt_lyr)


# %% MAIN
if __name__ == "__main__":
    parcel_id_field = "PARCELNO"
    for year in PMT.YEARS:
        print(year)
        year_gdb = PMT.makePath(PMT.DATA, f"PMT_{year}.gdb")
        # Layer references
        parcels_fc = PMT.makePath(year_gdb, "Parcels", "walk_time")
        stations_fc = PMT.makePath(PMT.BASIC_FEATURES, "SMART_Plan_Stations")
        parks_fc = PMT.makePath(PMT.CLEANED, "Parks", "Facility.shp")
        # Analyze stations
        print(" - Stations")
        idealWalkTime(parcels_fc, parcel_id_field, stations_fc,
                      "Name", RADIUS, "stn_ideal",
                      overlap_type="HAVE_THEIR_CENTER_IN",
                      sr=None, assumed_mph=WALK_MPH)
        # # Analyze parks
        print(" - Parks")
        idealWalkTime(parcels_fc, parcel_id_field, parks_fc,
                      "Name", RADIUS, "parks_ideal",
                      overlap_type="HAVE_THEIR_CENTER_IN",
                      sr=None, assumed_mph=WALK_MPH)

