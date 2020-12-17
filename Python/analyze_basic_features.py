"""
Created: October 2020
@Author: Alex Bell


"""

# %% IMPORTS
import PMT
import arcpy
import pandas as pd
import os
from six import string_types


# %% GLOBALS
# Station params
STATIONS_FC = "SMART_Plan_Stations"
STN_BUFF_DIST = "2640 Feet"
STN_BUFF_METERS = 804.672
STN_DISS_FIELDS = ["Id", "Name"]
STN_CORRIDOR_FIELDS = ["Beach",
                       "EastWest",
                       "Green",
                       "Kendall",
                       "Metromover",
                       "North",
                       "Northeast",
                       "Orange",
                       "South"]

# Alignment params
ALIGNMENTS_FC = "SMART_Plan_Alignments"
ALIGN_DISS_FIELDS = "Corridor"
ALIGN_BUFF_DIST = "2640 Feet"
CORRIDOR_NAME_FIELD = "Corridor"

# Outputs
STN_AREAS_FC="Station_Areas"
CORRIDORS_FC="Corridors"
LONG_STN_FC="Stations_Long"
SUM_AREAS_FC = "Summary_Areas"

# Other
RENAME_DICT = {
    "EastWest": "East-West"
}

# %% FUNCTIONS

def _listifyInput(input):
    if isinstance(input, string_types):
        return input.split(";")
    else:
        return list(input)

def _stringifyList(input):
    return ";".join(input)


def makeBasicFeatures(bf_gdb, stations_fc, stn_diss_fields, stn_corridor_fields,
                      alignments_fc, align_diss_fields, stn_buff_dist="2640 Feet",
                      align_buff_dist="2640 Feet", stn_areas_fc="Station_Areas",
                      corridors_fc="Corridors", long_stn_fc="Stations_Long",
                      rename_dict={}, overwrite=False):
    """

    Parameters
    -----------
    bf_gdb: Path
        A geodatabase with key basic features, including stations and
        alignments
    stations_fc: String
        A point feature class in`bf_gdb` with station locations and columns
        indicating belonging in individual corridors (i.e., the colum names
        reflect corridor names and flag whether the station is served by that
        corridor).
    stn_diss_fields: [String,...]
        Field(s) on which to dissovle stations when buffering station areas.
        Stations that reflect the same location by different facilities may
        be dissolved by name or ID, e.g. This may occur at intermodal locations.
        For example, where metro rail meets commuter rail - the station points
        may represent the same basic station but have slightly different
        geolocations.
    stn_corridor_fields: [String,...]
        The columns in `stations_fc` that flag each stations belonging in
        various corridors.
    alignments_fc: String
        A line feature class in `bf_gdb` reflecting corridor alignments
    align_diss_fields: [String,...]
        Field(s) on which to dissovle alignments when buffering corridor
        areas.
    stn_buff_dist: Linear Unit, default="2640 Feet"
        A linear unit by which to buffer station points to create station
        area polygons.
    align_buff_dist: Linear Unit, default="2640 Feet"
        A linear unit by which to buffer alignments to create corridor
        polygons
    stn_areas_fc: String, default="Station_Areas"
        The name of the output feature class to hold station area polygons
    corridors_fc: String, default="Corridors"
        The name of the output feature class to hold corridor polygons
    long_stn_fc: String, default="Stations_Long"
        The name of the output feature class to hold station features,
        elongated based on corridor belonging (to support dashboard menus)
    rename_dict: dict, default={}
        If given, `stn_corridor_fields` can be relabeled before pivoting
        to create `long_stn_fc`, so that the values reported in the output
        "Corridor" column are not the column names, but values mapped on
        to the column names (chaging "EastWest" column to "East-West", e.g.)
    overwrite: Boolean, default=False
    
    """
    stn_diss_fields = _listifyInput(stn_diss_fields)
    stn_corridor_fields = _listifyInput(stn_corridor_fields)
    align_diss_fields = _listifyInput(align_diss_fields)

    old_ws = arcpy.env.workspace
    arcpy.env.workspace = bf_gdb

    # Buffer features
    #  - stations (station areas, unique)
    print("... buffering station areas")
    PMT.checkOverwriteOutput(stn_areas_fc, overwrite)
    _diss_flds_ = _stringifyList(stn_diss_fields)
    arcpy.Buffer_analysis(stations_fc, stn_areas_fc, stn_buff_dist,
                          dissolve_option="LIST", dissolve_field=_diss_flds_)
    #  - alignments (corridors, unique)
    print("... buffering corridor areas")
    PMT.checkOverwriteOutput(corridors_fc, overwrite)
    _diss_flds_ = _stringifyList(align_diss_fields)
    arcpy.Buffer_analysis(alignments_fc, corridors_fc, align_buff_dist,
                          dissolve_option="LIST", dissolve_field=_diss_flds_)
    
    # Elongate stations by corridor (for dashboard displays, selectors)
    print("... elongating station features")
    # - dump to data frame
    fields = stn_diss_fields + stn_corridor_fields
    sr = arcpy.Describe(stations_fc).spatialReference
    fc_path = PMT.makePath(bf_gdb, stations_fc)
    stn_df = pd.DataFrame(
        arcpy.da.FeatureClassToNumPyArray(fc_path, fields + ["SHAPE@X", "SHAPE@Y"])
    )
    # rename columns if needed
    if rename_dict:
        stn_df.rename(columns=rename_dict, inplace=True)
        _cor_cols_ = [rename_dict.get(c, c) for c in stn_corridor_fields]
    else:
        _cor_cols_ = stn_corridor_fields
    # Melt to gather cols
    id_vars = stn_diss_fields + ["SHAPE@X", "SHAPE@Y"]
    long_df = stn_df.melt(id_vars=id_vars, value_vars=_cor_cols_,
                          var_name="Corridor", value_name="InCor")
    sel_df = long_df[long_df.InCor != 0].copy()
    long_out_fc = PMT.makePath(bf_gdb, long_stn_fc)
    PMT.checkOverwriteOutput(long_out_fc, overwrite)
    PMT.dfToPoints(sel_df, long_out_fc, ["SHAPE@X", "SHAPE@Y"],
                   from_sr=sr, to_sr=sr, overwrite=True)

    arcpy.env.workspace = old_ws


def makeSummaryFeatures(bf_gdb, long_stn_fc, corridors_fc, cor_name_field,
                        out_fc, stn_buffer_meters=804.672,
                        stn_name_field="Name", stn_cor_field="Corridor",
                        overwrite=False):
    """
    Creates a single feature class for data summarization based on station
    area and corridor geographies. The output feature class includes each
    station area, all combine station areas, the entire corridor area,
    and the portion of the corridor that is outside station areas.

    Parameters
    --------------
    bf_gdb: Path
    long_stn_fc: String
    corridors_fc: String
    cor_name_field: String
    out_fc: String
    stn_buffer_meters: Numeric, default=804.672 (1/2 mile)
    stn_name_field: String, default="Name"
    stn_cor_field: String, default="Corridor
    overwrite: Boolean, default=False
    """

    old_ws = arcpy.env.workspace
    arcpy.env.workspace = bf_gdb

    sr = arcpy.Describe(long_stn_fc).spatialReference
    mpu = float(sr.metersPerUnit)
    buff_dist = stn_buffer_meters/mpu

    # Make output container - polygon with fields for Name, Corridor
    print(f"... creating output feature class {out_fc}")
    PMT.checkOverwriteOutput(out_fc, overwrite)
    out_path, out_name = os.path.split(out_fc)
    arcpy.CreateFeatureclass_management(
        out_path, out_name, "POLYGON", spatial_reference=sr)
    # - Add fields    
    arcpy.AddField_management(out_fc, "Name", "TEXT", field_length=80)
    arcpy.AddField_management(out_fc, "Corridor", "TEXT", field_length=80)
    arcpy.AddField_management(out_fc, "RowID", "LONG")

    # Add all corridors with name="(Entire corridor)", corridor=cor_name_field
    print("... adding corridor polygons")
    out_fields = ["SHAPE@", "Name", "Corridor", "RowID"]
    cor_fields = ["SHAPE@", cor_name_field]
    cor_polys = {}
    i = 0
    with arcpy.da.InsertCursor(out_fc, out_fields) as ic:
        with arcpy.da.SearchCursor(corridors_fc, cor_fields) as sc:
            for sr in sc:
                i += 1
                # Add row for the whole corridor
                poly, corridor = sr
                out_row = [poly, "(Entire corridor)", corridor, i]
                ic.insertRow(out_row)
                # Keep the polygons in a dictionary for use later
                cor_polys[corridor] = poly
    
    # Add all station areas with name= stn_name_field, corridor=stn_cor_field
    print("... adding station polygons by corridor")
    stn_fields = ["SHAPE@", stn_name_field, stn_cor_field]
    cor_stn_polys = {}
    with arcpy.da.InsertCursor(out_fc, out_fields) as ic:
        with arcpy.da.SearchCursor(long_stn_fc, stn_fields) as sc:
            for sr in sc:
                i += 1
                # Add row for each station/corridor combo
                point, stn_name, corridor = sr
                poly = point.buffer(buff_dist)
                out_row = [poly, stn_name, corridor, i]
                ic.insertRow(out_row)
                # Merge station polygons by corridor in a dict for later use
                cor_poly = cor_stn_polys.get(corridor, None)
                if cor_poly is None:
                    cor_stn_polys[corridor] = poly
                else:
                    cor_stn_polys[corridor] = cor_poly.union(poly)

    # Add dissolved areas with name = (All stations), corridor=stn_cor_field
    # Add difference area with name = (Outside station areas), corridor=stn_cor_field
    print("... adding corridor in-station/non-station polygons")
    with arcpy.da.InsertCursor(out_fc, out_fields) as ic:
        for corridor in cor_stn_polys.keys():
            # Combined station areas
            i += 1
            all_stn_poly = cor_stn_polys[corridor]
            out_row = [all_stn_poly, "(All stations)", corridor, i]
            ic.insertRow(out_row)
            # Non-station areas
            i += 1
            cor_poly = cor_polys[corridor]
            non_stn_poly = cor_poly.difference(all_stn_poly)
            out_row = [non_stn_poly, "(Outside station areas)", corridor, i]
            ic.insertRow(out_row)

    arcpy.env.workspace = old_ws


# %% MAIN
if __name__ == "__main__":
    print("Making basic features")
    makeBasicFeatures(
        PMT.BASIC_FEATURES,
        STATIONS_FC,
        STN_DISS_FIELDS,
        STN_CORRIDOR_FIELDS,
        ALIGNMENTS_FC,
        ALIGN_DISS_FIELDS,
        stn_buff_dist = STN_BUFF_DIST,
        align_buff_dist=ALIGN_BUFF_DIST,
        stn_areas_fc=STN_AREAS_FC,
        corridors_fc=CORRIDORS_FC,
        long_stn_fc=LONG_STN_FC,
        rename_dict=RENAME_DICT,
        overwrite=True)

    print("Making summarization features")
    makeSummaryFeatures(
        PMT.BASIC_FEATURES,
        LONG_STN_FC,
        CORRIDORS_FC,
        CORRIDOR_NAME_FIELD,
        SUM_AREAS_FC,
        stn_buffer_meters=STN_BUFF_METERS,
        stn_name_field="Name",
        stn_cor_field="Corridor",
        overwrite=True)
