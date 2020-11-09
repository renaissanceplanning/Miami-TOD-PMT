"""
Created: October 2020
@Author: Alex Bell

Defines a simple function to tag features in all feature classes in a given
workspace as being within (1) or beyond (0) a polygon feature class.

If run as "main", all PMT years' parcel datasets are tagged as being within
or beyond the Miami-Dade urban development boundary based on parcel centroid
location.
"""

# %% IMPORTS
import PMT
import arcpy

# %% FUNCTION
def tagInBoundary(boundary_fc, workspace, in_string, not_in_string,
                  boundary_field="INBOUNDARY", 
                  overlap_type="HAVE_THEIR_CENTER_IN"):
   """
   Iterates over feature classes in a worksapce, tagging all features that
   have a specified spatial relationship to the urban development boundary.
   Each feature class will have a field (`boundary_field`) added to its
   attribute table denoting each feature's presence in the boundary.
   
   WARNING!If the field `boundary_field` already exists, it is deleted and
   redefined by this script.

   Parameters
   ------------
   boundary_fc: Path
   workspace: Path
   in_string: String
      The value to record for features that are inside `boundary_fc`
   not_in_string: String
      The value to record for features that are not inside `boundary_fc`
   boundary_field: String, default="INBOUNDARY"
   overlap_type: String, default="HAVE_THEIR_CENTER_IN"

   Returns
   --------
   workspace: Path
   """
   # Set environment
   arcpy.env.workspace = workspace
   all_fcs = arcpy.ListFeatureClasses()
   
   # Make boundary layer
   boundary_layer = arcpy.MakeFeatureLayer_management(
         boundary_fc, "__boundary__")
   # Iterate over features
   try:
      for fc in all_fcs:
         print(f"...tagging features in {fc}")
         # Check for boundary_field
         f = arcpy.ListFields(fc, boundary_field)
         if f:
            print(f"... ...deleting existing field {boundary_field}")
            arcpy.DeleteField_management(fc, boundary_field)
         # Add boundary field
         print(f"... ...adding field {boundary_field}")
         arcpy.AddField_management(fc, boundary_field, "TEXT", field_length=20)
         arcpy.CalculateField_management(fc, boundary_field, f'"{not_in_string}"')
         # Apply selection
         print(f"... ...selecting features that {overlap_type} boundary")
         feature_layer = arcpy.MakeFeatureLayer_management(fc, "__fc__")
         # - Select parcels in boundary
         arcpy.SelectLayerByLocation_management(
            feature_layer, overlap_type, boundary_layer)
         # - Calculate field
         print(f"... ...tagging selected features")
         arcpy.CalculateField_management(feature_layer, boundary_field,
                                         f'"{in_string}"')
         # Tidy up
         arcpy.Delete_management(feature_layer)
   except:
      raise
   finally:
      arcpy.Delete_management(boundary_layer)
   
   return workspace


# %% MAIN
if __name__ == "__main__":
      boundary_fc = PMT.makePath(PMT.CLEANED, "UrbanDevelopmentBoundary.shp")
      station_areas = PMT.makePath(PMT.BASIC_FEATURES, "SMART_Plan_Station_Areas")
      corridors = PMT.makePath(PMT.BASIC_FEATURES, "SMART_Plan_Corridors")
      for year in PMT.YEARS:
         fds = PMT.makePath(PMT.ROOT, f"PMT_{year}.gdb", "parcels")
         print(fds)
         tagInBoundary(boundary_fc, fds,
                       in_string="UDB", not_in_string="NUDB",
                       boundary_field="INBOUNDARY", overlap_type="HAVE_THEIR_CENTER_IN")
         tagInBoundary(station_areas, fds,
                       in_string="SA", not_in_string="NSA",
                       boundary_field="INSTATION", overlap_type="HAVE_THEIR_CENTER_IN")
         tagInBoundary(corridors, fds,
                       in_string="C", not_in_string="NC",
                       boundary_field="INCORRIDOR", overlap_type="HAVE_THEIR_CENTER_IN")
