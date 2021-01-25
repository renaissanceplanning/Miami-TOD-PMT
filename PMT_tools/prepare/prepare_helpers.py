import json
from json.decoder import JSONDecodeError
import os
import uuid
import arcpy

from prepare_config import (CRASH_CODE_TO_STRING, CRASH_CITY_CODES, CRASH_SEVERITY_CODES, CRASH_HARMFUL_CODES )


# general use functions
def validate_json(json_file):
    with open(json_file) as file:
        try:
            return json.load(file)
        except JSONDecodeError:
            arcpy.AddMessage("Invalid JSON file passed")


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


def geojson_to_featureclass(geojson_path):
    if validate_json(json_file=geojson_path):
        try:
            # convert json to temp feature class
            unique_name = str(uuid.uuid4().hex)
            temp_feature = fr"in_memory\\{unique_name}"
            arcpy.JSONToFeatures_conversion(in_json_file=geojson_path, out_features=temp_feature,
                                            geometry_type="POINT")
            return temp_feature
        except:
            print("something went wrong")


# crash functions
def clean_and_drop(feature_class, use_cols=[], rename_dict={}):
    # reformat attributes and keep only useful
    if use_cols:
        fields = [f.name for f in arcpy.ListFields(feature_class) if not f.required]
        drop_fields = [f for f in fields if f not in list(use_cols) + ['Shape']]
        for drop in drop_fields:
            arcpy.DeleteField_management(in_table=feature_class, drop_field=drop)
    # rename attributes
    if rename_dict:
        for name, rename in rename_dict.items():
            arcpy.AlterField_management(in_table=feature_class, field=name, new_field_name=rename, new_field_alias=rename)


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
    # TODO: verify the remap_list and remap_dict is needed
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


# parks functions
def field_mapper(in_fcs, use_cols, rename_dicts):
    """
    create a field mapping for one or more feature classes
    Parameters
    ----------
    in_fcs - list (string), list of feature classes
    use_cols - list (string), list or tuple of lists of column names to keep
    rename_dict - list of dict(s), dict or tuple of dicts to map field names

    Returns
    -------
    arcpy.FieldMapings object
    """
    _unmapped_types_ = ["Geometry", "OID", "GUID"]
    # check to see if we have only one use or rename and handle for zip
    if not any(isinstance(el, list) for el in use_cols):
        use_cols = [use_cols]
    if isinstance(rename_dicts, dict):
        rename_dicts = [rename_dicts]
    # create field mappings object and add/remap all necessary fields
    field_mappings = arcpy.FieldMappings()
    for in_fc, use, rename in zip(in_fcs, use_cols, rename_dicts):
        # only keep the fields that we want and that are mappable
        fields = [f.name for f in arcpy.ListFields()
                  if f.name in use
                  and f.type not in _unmapped_types_]
        for field in fields:
            fm = arcpy.FieldMap()
            fm.addInputField(in_fc, field)
            out_field = fm.outputField
            out_fname = rename.get(key=field, value=field)
            out_field.name = out_fname
            out_field.aliasName = out_fname
            fm.outputField = out_field
            field_mappings.addFieldMap(fm)
    return field_mappings


def clean_park_polys(in_fcs, out_fc, use_cols=None, rename_dicts=None):
    # Align inputs
    if rename_dicts is None:
        rename_dicts = [{} for _ in in_fcs]
    if use_cols is None:
        use_cols = [[] for _ in in_fcs]
    if isinstance(in_fcs, str):
        in_fcs = [in_fcs]

    # handle the chance of input raw data being geojson
    if any(fc.endswith("json") for fc in in_fcs):
        in_fcs = [geojson_to_featureclass(fc)
                  if fc.endswith("json")
                  else fc for fc in in_fcs]

    # merge into one feature class temporarily
    fm = field_mapper(in_fcs=in_fcs, use_cols=use_cols, rename_dicts=rename_dicts)
    arcpy.Merge_management(inputs=in_fcs, output=out_fc, field_mappings=fm)


def clean_park_points(in_fc, out_fc, use_cols=None, rename_dict=None):
    if rename_dict is None:
        rename_dict = {}
    if use_cols is None:
        use_cols = []
    if in_fc.endswith("json"):
        in_fc = geojson_to_featureclass(in_fc)
    out_dir, out_name = os.path.split(out_fc)
    fms = field_mapper(in_fcs=in_fc, use_cols=use_cols, rename_dicts=rename_dict)
    arcpy.FeatureClassToFeatureClass_conversion(in_features=in_fc, out_path=out_dir,
                                                out_name=out_name, field_mapping=fms)


