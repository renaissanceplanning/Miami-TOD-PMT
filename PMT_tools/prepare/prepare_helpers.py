import json
from json.decoder import JSONDecodeError
import os
import arcpy

from prepare_config import (CRASH_CODE_TO_STRING, CRASH_CITY_CODES, CRASH_SEVERITY_CODES, CRASH_HARMFUL_CODES )

def validate_json(json_file):
    with open(json_file) as file:
        try:
            return json.load(file)
        except JSONDecodeError:
            arcpy.AddMessage("Invalid JSON file passed")


def clean_and_drop(feature_class, use_cols=[], rename_dict=None):
    # reformat attributes and keep only useful
    fields = [f.name for f in arcpy.ListFields(feature_class) if not f.required]
    drop_fields = [f for f in fields if f not in list(use_cols) + ['Shape']]
    for drop in drop_fields:
        arcpy.DeleteField_management(in_table=feature_class, drop_field=drop)
    # rename attributes
    for name, rename in rename_dict.items():
        arcpy.AlterField_management(in_table=feature_class, field=name, new_field_name=rename, new_field_alias=rename)


def update_field_values(in_fc, fields, mappers):
    # ensure number of fields and dictionaries is the same
    try:
        if len(fields) == len(mappers):
            for attribute, mapper in zip(fields, mappers):
                # check that bother input types are as expected
                if isinstance(attribute, str) and isinstance(mapper, dict):
                    with arcpy.da.UpdateCursor(in_fc, field_names=attribute) as cur:
                        for row in cur:
                            if row[0] is not None:
                                row[0] = mapper[int(attribute)]
                            cur.updateRow(row)
    except ValueError:
        arcpy.AddMessage("either attributes (String) or mappers (dict) are of the incorrect type")


def update_crash_type(feature_class, data_fields, update_field):
    arcpy.AddField_management(in_table=feature_class, field_name=update_field, field_type="TEXT")
    with arcpy.da.UpdateCursor(feature_class, field_names=[update_field] + data_fields) as cur:
        for row in cur:
            both, ped, bike = row
            if ped == "Y":
                row[0] = "PEDESTRIAN"
            if bike == "Y":
                row[0] = "BIKE"
            cur.updateRow(row)
    for field in data_fields:
        arcpy.DeleteField_management(in_table=feature_class, drop_field=field)


def clean_bike_ped_crashes(in_fc, out_path, out_name, where_clause=None,
                           remap_list=[], remap_dict={},
                           use_cols=[], rename_dict={}):
    # dump subset to new FC
    out_fc = os.path.join(out_path, out_name)
    arcpy.FeatureClassToFeatureClass_conversion(in_features=in_fc, out_path=out_path,
                                                out_name=out_name, where_clause=where_clause)
    # reformat attributes and keep only useful
    clean_and_drop(feature_class=out_fc, use_cols=use_cols, rename_dict=rename_dict)
    # update city code/injury severity/Harmful event to text value
    update_field_values(in_fc=out_fc, fields=CRASH_CODE_TO_STRING,
                        mappers=[CRASH_CITY_CODES, CRASH_SEVERITY_CODES, CRASH_HARMFUL_CODES])

    # combine bike and ped type into single attribute and drop original
    update_crash_type(feature_class=out_fc, data_fields=["PED_TYPE", "BIKE_TYPE"], update_field="TRANS_TYPE")


def geojson_to_featureclass(geojson_path):
    if validate_json(json_file=geojson_path):
        try:
            # convert json to temp feature class
            temp_points = r"in_memory\\crash_points"
            arcpy.JSONToFeatures_conversion(in_json_file=geojson_path, out_features=temp_points,
                                            geometry_type="POINT")
            return temp_points
        except:
            print("something went wrong")
