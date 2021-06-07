"""
Created: October 2020
@Author: Alex Bell

Uses the raw urban development boundary feature - a line defining the western
and southern edges for urban development in Miami-Dade County - to bisect the
county boundary, generating two polygons tagged as either within or outside
the UDB.

If run as "main" the raw UDB has its endpoints snapped to the county boundary
(nearest point on the line). The resulting feature is then used to cut the 
county into two polygons, which are stored in the "cleaned" directory.
"""

# %% IMPORTS
import arcpy
import numpy as np
from PMT_tools.PMT import make_path, RAW, CLEANED


# %% FUNCTION
# TODO: add function or call to convert geojson file to feature class
def udbLineToPolygon(udb_fc, county_fc, out_fc):
    """
    Uses the urban development boundary line to bisect the county boundary
    and generate two polygon output features.

    During processing the UDB line features are dissolved into a single
    feature - this assumes all polylines in the shape file touch one another
    such that a single cohesive polyline feature results.

    This function also assumes that the UDB will only define a simple
    bi-section of the county boundary. If the UDB geometry becomes more
    complex over time, modifications to this function may be needed.

    Parameters
    -----------
    udb_fc: Path
        The udb line features.
    county_fc: Path
        The county bondary polygon. This is expected to only include a
        single polygon encompassing the entire county.
    out_fc: Path
        The location to store the output feature class.

    Returns
    --------
    out_fc: Path
    """
    sr = arcpy.Describe(udb_fc).spatialReference
    # Prepare ouptut feature class
    out_path, out_name = out_fc.rsplit("\\", 1)
    arcpy.CreateFeatureclass_management(out_path, out_name, "POLYGON",
                                        spatial_reference=sr)
    arcpy.AddField_management(out_fc, "IN_UDB", "LONG")

    # Get geometry objects
    diss_line = arcpy.Dissolve_management(udb_fc, r"in_memory\UDB_dissolve")
    with arcpy.da.SearchCursor(diss_line, "SHAPE@", spatial_reference=sr) as c:
        for r in c:
            udb_line = r[0]
    with arcpy.da.SearchCursor(county_fc, "SHAPE@", spatial_reference=sr) as c:
        for r in c:
            county_poly = r[0]
    county_line = arcpy.Polyline(county_poly.getPart(0))

    # Get closest point on county boundary to each udb end point
    udb_start = arcpy.PointGeometry(udb_line.firstPoint)
    udb_end = arcpy.PointGeometry(udb_line.lastPoint)
    start_connector = county_line.snapToLine(udb_start)
    end_connector = county_line.snapToLine(udb_end)

    # Cut the county boundary based on the extended UDB line
    cutter_points = [p for p in udb_line.getPart(0)]
    cutter_points.insert(0, start_connector.centroid)
    cutter_points.append(end_connector.centroid)
    cutter = arcpy.Polyline(arcpy.Array(cutter_points))
    cuts = county_poly.cut(cutter.projectAs(sr))

    # Tag the westernmost feature as outside the UDB
    x_mins = []
    for cut in cuts:
        x_min = min([pt.X for pt in cut.getPart(0)])
        x_mins.append(x_min)
    in_udb = [1 for _ in x_mins]
    min_idx = np.argmin(x_mins)
    in_udb[min_idx] = 0

    # Write cut polygons to output feature class
    with arcpy.da.InsertCursor(out_fc, ["SHAPE@", "IN_UDB"]) as c:
        for cut, b in zip(cuts, in_udb):
            c.insertRow([cut, b])

    return out_fc


# %% MAIN
if __name__ == "__main__":
    udb_fc = make_path(RAW, "UrbanDevelopmentBoundary.shp")
    county_fc = make_path(RAW, "CensusGeo", "MiamiDadeBoundary.shp")
    out_fc = make_path(CLEANED, "UrbanDevelopmentBoundary.shp")
