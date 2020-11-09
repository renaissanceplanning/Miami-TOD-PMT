"""
Created: October 2020
@Author: Alex Bell

...
"""

# %% IMPORTS
import PMT
import arcpy
import pandas as pd


# %% GLOBALS
WALK_MPH = 3.0
RADIUS = "7920 Feet"

# %% FUNCTIONS
def idealWalkTime(parcels_fc, parcel_id_field, target_fc, target_name_field,
                  radius, out_field, overlap_type="HAVE_THEIR_CENTER_IN",
                  sr=None, assumed_mph=3):
    """

    Returns
    --------
    out_field: String
        A new field is added to `parcels_fc` recording each parcel's ideal
        walk time to the nearest feature in `target_fc`.
    """
    # Set spatial reference
    if sr is None:
        sr = arcpy.Describe(parcels_fc).spatialReference
    else:
        sr = arcpy.SpatialReference(sr)
    mpu = float(sr.metersPerUnit)
    # Add field to parcels
    arcpy.AddField_management(parcels_fc, out_field, "DOUBLE")

    # Intersect parcels and targets
    print("... intersecting parcels and target features")
    int_fc = arcpy.Intersect_analysis(
        [parcels_fc, target_fc], "in_memory\\int_fc", cluster_tolerance=radius)
    
    # Dump intersect to data frame
    print("... converting to data frame")
    dump_fields = [parcel_id_field, ref_name_field, ref_time_field]
    int_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(int_fc, dump_fields)
    )

    # # Make feature layers
    # par_lyr = arcpy.MakeFeatureLayer_management(parcels_fc, "parcels")
    # tgt_lyr = arcpy.MakeFeatureLayer_management(target_fc, "target")

    # try:
    #     # Select parcels in radius
    #     arcpy.SelectLayerByLocation_management(par_lyr, overlap_type,
    #                                            tgt_layer, radius,
    #                                            "NEW_SELECTION")
    #     par_fields = ["SHAPE@", out_field]
    #     with arcpy.da.UpdateCursor(
    #         par_lyr, par_fields, spatial_reference=sr) as par_c:
    #         # Iterate over parcels
    #         for par_row in par_c:
    #             par_shape = par_row[0]
    #             # Select targets
    #             arcpy.SelectLayerByLocation_management(tgt_lyr, overlap_type,
    #                                                    par_shape, radius,
    #                                                    "NEW_SELECTION")
    #             # Get closest
    #             min_dist = float('inf')
    #             with arcpy.da.SearchCursor(
    #                 tgt_lyr, "SHAPE@", spatial_reference=sr) as tgt_c:
    #                 # Iterate over targets, keeping the minimum distance
    #                 for tgt_row in tgt_c:
    #                     tgt_shape = tgt_row[0]
    #                     dist = tgt_shape.distanceTo(par_shape)
    #                     min_dist = min(min_dist, dist)
    #             # Convert distance to time
    #             min_meters = min_dist/mpu
    #             spd = (assumed_mph * 1609.344)/60.0
    #             time = min_meters/spd
    #             # Record min time
    #             par_row[1] = time
    #             par_c.updateRow(par_row)
        
    # except:
    #     arcpy.DeleteField_management(parcels_fc, out_field)
    #     raise
    # finally:
    #     arcpy.Delete_management(par_lyr)
    #     arcpy.Delete_management(tgt_lyr)


# %% MAIN
if __name__ == "__main__":
    for year in PMT.YEARS:
        print(year)
        # Layer references
        parcels_fc = PMT.makePath(year_gdb, "Parcels", "WalkTime")
        stations_fc = PMT.makePath(PMT.BASIC_FEATURES, "SMART_Plan_Stations")
        parks_fc = PMT.makePath(PMT.CLEANED, "Parks", "Facility.shp")
        # Analyze stations
        print(" - Stations")
        stn_field = "min_to_stn_ideal"
        stn_field = idealWalkTime(parcels_fc, stations_fc, RADIUS, stn_field,
                                  overlap_type="HAVE_THEIR_CENTER_IN",
                                  sr=None, assumed_mph=WALK_MPH)
        # Analyze parks
        print(" - Parks")
        park_field = "min_to_park_ideal"
        park_field = idealWalkTime(parcels_fc, parks_fc, RADIUS, park_field,
                                   overlap_type="HAVE_THEIR_CENTER_IN",
                                   sr=None, assumed_mph=WALK_MPH)

