from PMT_tools.config.build_config import *

import uuid
import numpy as np
import pandas as pd
from six import string_types
from collections.abc import Iterable
import PMT_tools.PMT as PMT
from PMT_tools.PMT import Column, AggColumn, Consolidation, MeltColumn
import arcpy

def _list_table_paths(gdb, criteria="*"):
    old_ws = arcpy.env.workspace
    arcpy.env.workspace = gdb
    if isinstance(criteria, string_types):
        criteria = [criteria]
    # Get tables
    tables = []
    for c in criteria:
        tables += arcpy.ListTables(c)
    arcpy.env.workspace = old_ws
    return [PMT.makePath(gdb, table) for table in tables]

def _list_fc_paths(gdb, fds_criteria="*", fc_criteria="*"):
    old_ws = arcpy.env.workspace
    arcpy.env.workspace = gdb
    paths = []
    if isinstance(fds_criteria, string_types):
        fds_criteria = [fds_criteria]
    if isinstance(fc_criteria, string_types):
        fc_criteria = [fc_criteria]
    # Get feature datasets
    fds = []
    for fdc in fds_criteria:
        fds += arcpy.ListDatasets(fdc)
    # Get feature classes
    for fd in fds:
        for fc_crit in fc_criteria:
            fcs = arcpy.ListFeatureClasses(feature_dataset=fd, wild_card=fc_crit)
            paths += [PMT.makePath(gdb, fd, fc) for fc in fcs]
    arcpy.env.workspace = old_ws
    return paths

def add_year_columns(in_gdb, year):
    print("checking for/adding year columns")
    old_ws = arcpy.env.workspace
    arcpy.env.workspace = in_gdb
    fcs = _list_fc_paths(in_gdb)
    tables = _list_table_paths(in_gdb)
    for fc in fcs:
        arcpy.AddField_management(fc, "Year", "LONG")
        arcpy.CalculateField_management(fc, "Year", str(year))
    for table in tables:
        arcpy.AddField_management(table, "Year", "LONG")
        arcpy.CalculateField_management(table, "Year", str(year))
    arcpy.env.workspace = old_ws

def make_reporting_gdb(out_path, out_gdb_name=None, overwrite=False):
    if not out_gdb_name:
        out_gdb_name = f"_{uuid.uuid4().hex}.gdb"
        out_gdb = PMT.makePath(out_path, out_gdb_name)
    elif out_gdb_name and overwrite:
        out_gdb = PMT.makePath(out_path, out_gdb_name)
        PMT.checkOverwriteOutput(output=out_gdb, overwrite=overwrite)
    else:
        out_gdb = PMT.makePath(out_path, out_gdb_name)
    arcpy.CreateFileGDB_management(out_path, out_gdb_name)
    return out_gdb

def make_snapshot_template(in_gdb, out_path, out_gdb_name=None, overwrite=False):
    out_gdb = make_reporting_gdb(out_path, out_gdb_name, overwrite)
    # copy in the geometry data containing minimal tabular data
    for fds in ["Networks", "Points", "Polygons"]:
        print(f"... copying FDS {fds}")
        source_fd = PMT.makePath(in_gdb, fds)
        out_fd = PMT.makePath(out_gdb, fds)
        arcpy.Copy_management(source_fd, out_fd)
    return out_gdb

def make_trend_template(out_path, out_gdb_name=None, overwrite=False):
    out_gdb = make_reporting_gdb(out_path, out_gdb_name, overwrite)
    for fds in ["Networks", "Points", "Polygons"]:
        arcpy.CreateFeatureDataset_management(
            out_dataset_path=out_gdb, out_name=fds,
            spatial_reference=PMT.SR_FL_SPF)
    return out_gdb

def _validateAggSpecs(var, expected_type):
    e_type = expected_type.__name__
    # Simplest: var is the expected type
    if isinstance(var, expected_type):
        # Listify
        var = [var]
    # var could be an iterable of the expected type
    # - If not iterable, it's the wrong type, so raise error
    elif not isinstance(var, Iterable):
        bad_type = type(var)
        raise ValueError(
            f"Expected one or more {e_type} objects, got {bad_type}")
    # - If iterable, confirm items are the correct type
    else:
        for v in var:
            if not isinstance(v, expected_type):
                bad_type = type(v)
                raise ValueError(
                    f"Expected one or more {e_type} objects, got {bad_type}")
    # If no errors, return var (in original form or as list)
    return var


def joinAttributes(to_table, to_id_field, from_table, from_id_field,
                   join_fields, null_value=0.0, renames={}, drop_dup_cols=False):
    """
    """
    # If all columns, get their names
    if join_fields == "*":
        join_fields = [f.name for f in arcpy.ListFields(from_table)
                       if not f.required and f.name != from_id_field]
    # List expected columns based on renames dict
    expected_fields = [renames.get(jf, jf) for jf in join_fields]
    # Check if expected outcomes will collide with fields in the table
    if drop_dup_cols:
        # All relevant fields in table (excluding the field to join by)
        tbl_fields = [f.name for f in arcpy.ListFields(to_table)
                      if f.name != to_id_field]
        # List of which fields to drop
        drop_fields = [d for d in expected_fields #join_fields
                       if d in tbl_fields]
        # If all the fields to join will be dropped, exit
        if len(join_fields) == len(drop_fields):
            print('... no new fields')
            return #TODO: what if we want to update these fields?
        # else:
        #     join_fields = [jf for jf in join_fields if jf not in drop_fields]
    # Dump from_table to df
    dump_fields = [from_id_field] + join_fields
    df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(
            from_table, dump_fields, null_value=null_value
        )
    )
    # Rename columns and drop columns as needed
    if renames:
        df.rename(columns=renames, inplace=True)
    if drop_fields:
        df.drop(columns=drop_fields, inplace=True)
    # Join cols from df to to_table
    print(f"... {list(df.columns)} to {to_table}")
    PMT.extendTableDf(to_table, to_id_field, df, from_id_field, drop_dup_cols=True)


def summarizeAttributes(in_fc, group_fields, agg_cols,
                        consolidations=None, melt_col=None):
    """

    """
    # Validation (listify inputs, validate values)
    # - Group fields (domain possible)
    group_fields = _validateAggSpecs(group_fields, Column)
    gb_fields = [gf.name for gf in group_fields]
    dump_fields = [gf.name for gf in group_fields]
    keep_cols = []
    null_dict = dict([(gf.name, gf.default) for gf in group_fields])
    renames = [
        (gf.name, gf.rename) for gf in group_fields if gf.rename is not None]
    # - Agg columns (no domain expected)
    agg_cols = _validateAggSpecs(agg_cols, AggColumn)
    agg_methods = {}
    for ac in agg_cols:
        dump_fields.append(ac.name)
        keep_cols.append(ac.name)
        null_dict[ac.name] = ac.default
        agg_methods[ac.name] = ac.agg_method
        if ac.rename is not None:
            renames.append((ac.name, ac.rename))
    # - Consolidations (no domain expected)
    if consolidations:
        consolidations = _validateAggSpecs(consolidations, Consolidation)
        for c in consolidations:
            dump_fields += [ic for ic in c.input_cols]
            keep_cols.append(c.name)
            null_dict.update(c.defaultsDict())
            agg_methods[c.name] = c.agg_method
    else:
        consolidations = []
    # - Melt columns (domain possible)
    if melt_col:
        melt_col = _validateAggSpecs(melt_col, MeltColumn)[0]
        dump_fields += [ic for ic in melt_col.input_cols]
        gb_fields.append(melt_col.label_col)
        keep_cols.append(melt_col.val_col)
        null_dict.update(melt_col.defaultsDict())
        agg_methods[melt_col.val_col] = melt_col.agg_method

    # Dump the intersect table to df
    int_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(
            in_fc, dump_fields, null_value=null_dict)
    )

    # Consolidate columns
    for c in consolidations:
        int_df[c.name] = int_df[c.input_cols].agg(c.cons_method, axis=1)

    # Melt columns
    if melt_col:
        id_fields = [f for f in gb_fields if f != melt_col.label_col]
        id_fields += [f for f in keep_cols if f != melt_col.val_col]
        int_df = int_df.melt(
            id_vars=id_fields,
            value_vars=melt_col.input_cols,
            var_name=melt_col.label_col,
            value_name=melt_col.val_col
        ).reset_index()
    # Domains
    for group_field in group_fields:
        if group_field.domain is not None:
            group_field.applyDomain(int_df)
            gb_fields.append(group_field.domain.name)
    if melt_col:
        if melt_col.domain is not None:
            melt_col.applyDomain(int_df)
            gb_fields.append(melt_col.domain.name)


    # Group by - summarize
    all_fields = gb_fields + keep_cols
    sum_df = int_df[all_fields].groupby(gb_fields).agg(agg_methods).reset_index()

    # Apply renames
    if renames:
        sum_df.rename(columns=dict(renames), inplace=True)

    return sum_df


def _makeAccessColSpecs(activities, time_breaks, mode, include_average=True):
    cols = []
    new_names = []
    for a in activities:
        for tb in time_breaks:
            col = f"{a}{tb}Min"
            cols.append(col)
            new_names.append(f"{col}{mode[0]}")
        if include_average:
            col = f"AvgMin{a}"
            cols.append(col)
            new_names.append(f"{col}{mode[0]}")
    renames = dict(zip(cols, new_names))
    return cols, renames


def _createLongAccess(int_fc, id_field, activities, time_breaks, mode, domain=None):
    # result is long on id_field, activity, time_break
    # TODO: update to use Column objects? (null handling, e.g.)
    # --------------
    # Dump int fc to data frame
    acc_fields, renames = _makeAccessColSpecs(activities, time_breaks, mode, include_average=False)
    if isinstance(id_field, string_types):
        id_field = [id_field]  # elif isinstance(Column)?

    all_fields = id_field + list(renames.values())
    df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(int_fc, all_fields, null_value=0.0)
    )
    # Set id field(s) as index
    df.set_index(id_field, inplace=True)

    # Make tidy hierarchical columns
    levels = []
    order = []
    for tb in time_breaks:
        for a in activities:
            col = f"{a}{tb}Min{mode[0]}"
            idx = df.columns.tolist().index(col)
            levels.append((a, tb))
            order.append(idx)
    header = pd.DataFrame(np.array(levels)[np.argsort(order)],
                          columns=["Activity", "TimeBin"])
    mi = pd.MultiIndex.from_frame(header)
    df.columns = mi
    df.reset_index(inplace=True)
    # Melt
    return df.melt(id_vars=id_field)


def table_difference(this_table, base_table, idx_cols, fields="*", **kwargs):
    """
    this_table minus base_table
    """
    # Fetch data frames
    this_df = PMT.featureclass_to_df(this_table, keep_fields=fields, **kwargs)
    base_df = PMT.featureclass_to_df(base_table, keep_fields=fields, **kwargs)
    # Set index columns
    base_df.set_index(idx_cols, inplace=True)
    this_df.set_index(idx_cols, inplace=True)
    this_df = this_df.reindex(base_df.index, fill_value=0) # is this necessary?
    # Drop all remaining non-numeric columns
    base_df_n = base_df.select_dtypes(["number"])
    this_df_n = this_df.select_dtypes(["number"])
    # Take difference
    diff_df = this_df_n - base_df_n
    # Restore index columns
    diff_df.reset_index(inplace=True)

    return diff_df


def finalize_output(temp_gdb, final_gdb):
    if arcpy.Exists(final_gdb):
        arcpy.Delete_management(final_gdb)
    arcpy.Copy_management(in_data=temp_gdb, out_data=final_gdb)
    arcpy.Delete_management(temp_gdb)
