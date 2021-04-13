import os
import uuid
from collections.abc import Iterable
import itertools

import arcpy
import numpy as np
import pandas as pd
from six import string_types

import PMT_tools.PMT as PMT
from PMT_tools.PMT import Column, AggColumn, Consolidation, MeltColumn


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


def unique_values(table, field):
    with arcpy.da.SearchCursor(table, [field]) as cursor:
        return sorted({row[0] for row in cursor})


def add_year_columns(in_gdb, year):
    print("--- checking for/adding year columns")
    old_ws = arcpy.env.workspace
    arcpy.env.workspace = in_gdb
    fcs = _list_fc_paths(in_gdb)
    tables = _list_table_paths(in_gdb)
    for fc in fcs:
        if "Year" in [f.name for f in arcpy.ListFields(fc)]:
            values = unique_values(table=fc, field="Year")
            if len(values) == 1 and values[0] == year:
                print(f"--- 'Year' already in {fc}...skipping")
                continue
        arcpy.AddField_management(fc, "Year", "LONG")
        arcpy.CalculateField_management(fc, "Year", str(year))
    for table in tables:
        if "Year" in [f.name for f in arcpy.ListFields(table)]:
            values = unique_values(table=fc, field="Year")
            if len(values) == 1 and values[0] == year:
                print(f"--- 'Year' already in {fc}...skipping")
                continue
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
            out_dataset_path=out_gdb, out_name=fds, spatial_reference=PMT.SR_FL_SPF
        )
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
        raise ValueError(f"Expected one or more {e_type} objects, got {bad_type}")
    # - If iterable, confirm items are the correct type
    else:
        for v in var:
            if not isinstance(v, expected_type):
                bad_type = type(v)
                raise ValueError(
                    f"Expected one or more {e_type} objects, got {bad_type}"
                )
    # If no errors, return var (in original form or as list)
    return var


def joinAttributes(
    to_table,
    to_id_field,
    from_table,
    from_id_field,
    join_fields,
    null_value=0.0,
    renames=None,
    drop_dup_cols=False,
):
    """
    """
    # If all columns, get their names
    if renames is None:
        renames = {}
    if join_fields == "*":
        join_fields = [
            f.name
            for f in arcpy.ListFields(from_table)
            if not f.required and f.name != from_id_field
        ]
    # List expected columns based on renames dict
    expected_fields = [renames.get(jf, jf) for jf in join_fields]
    # Check if expected outcomes will collide with fields in the table
    if drop_dup_cols:
        # All relevant fields in table (excluding the field to join by)
        tbl_fields = [
            f.name for f in arcpy.ListFields(to_table) if f.name != to_id_field
        ]
        # List of which fields to drop
        drop_fields = [d for d in expected_fields if d in tbl_fields]  # join_fields
        # If all the fields to join will be dropped, exit
        if len(join_fields) == len(drop_fields):
            print("--- --- no new fields")
            return  # TODO: what if we want to update these fields?
    else:
        drop_fields = []

    # Dump from_table to df
    dump_fields = [from_id_field] + join_fields
    df = PMT.table_to_df(
        in_tbl=from_table, keep_fields=dump_fields, null_val=null_value
    )

    # Rename columns and drop columns as needed
    if renames:
        df.rename(columns=renames, inplace=True)
    if drop_fields:
        df.drop(columns=drop_fields, inplace=True)

    # Join cols from df to to_table
    print(f"--- --- {list(df.columns)} to {to_table}")
    PMT.extendTableDf(
        in_table=to_table,
        table_match_field=to_id_field,
        df=df,
        df_match_field=from_id_field,
    )


def summarizeAttributes(
    in_fc, group_fields, agg_cols, consolidations=None, melt_col=None
):
    """
    """
    # Validation (listify inputs, validate values)
    # - Group fields (domain possible)
    group_fields = _validateAggSpecs(group_fields, Column)
    gb_fields = [gf.name for gf in group_fields]
    dump_fields = [gf.name for gf in group_fields]
    keep_cols = []
    null_dict = dict([(gf.name, gf.default) for gf in group_fields])
    renames = [(gf.name, gf.rename) for gf in group_fields if gf.rename is not None]

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
            if hasattr(c, "input_cols"):
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
    dump_fields = list(
        set(dump_fields)
    )  # remove duplicated fields used in multiple consolidations/melts
    missing = PMT.which_missing(table=in_fc, field_list=dump_fields)
    if not missing:
        int_df = PMT.table_to_df(
            in_tbl=in_fc, keep_fields=dump_fields, null_val=null_dict
        )
    else:
        raise Exception(
            f"\t\tthese cols were missing from the intersected FC: {missing}"
        )
    # Consolidate columns
    for c in consolidations:
        if hasattr(c, "input_cols"):
            int_df[c.name] = int_df[c.input_cols].agg(c.cons_method, axis=1)

    # Melt columns
    if melt_col:
        id_fields = [f for f in gb_fields if f != melt_col.label_col]
        id_fields += [f for f in keep_cols if f != melt_col.val_col]
        int_df = int_df.melt(
            id_vars=id_fields,
            value_vars=melt_col.input_cols,
            var_name=melt_col.label_col,
            value_name=melt_col.val_col,
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


def apply_field_calcs(gdb, new_field_specs, recalculate=False):
    # Iterate over new fields
    for nf_spec in new_field_specs:
        # Get params
        tables = nf_spec["tables"]
        new_field = nf_spec["new_field"]
        field_type = nf_spec["field_type"]
        expr = nf_spec["expr"]
        code_block = nf_spec["code_block"]
        try:
            # Get params
            if isinstance(nf_spec["params"], Iterable):
                params = nf_spec["params"]
                all_combos = list(itertools.product(*params))
                for combo in all_combos:
                    combo_spec = nf_spec.copy()
                    del combo_spec["params"]
                    combo_spec["new_field"] = combo_spec["new_field"].format(*combo)
                    combo_spec["expr"] = combo_spec["expr"].format(*combo)
                    combo_spec["code_block"] = combo_spec["code_block"].format(*combo)
                    apply_field_calcs(gdb, [combo_spec])
            else:
                raise Exception("Spec Params must be an iterable if provided")
        except KeyError:
            add_args = {"field_name": new_field, "field_type": field_type}
            calc_args = {
                "field": new_field,
                "expression": expr,
                "expression_type": "PYTHON3",
                "code_block": code_block,
            }
            # iterate over tables
            if isinstance(tables, string_types):
                tables = [tables]
            print(f"--- Adding field {new_field} to {len(tables)} tables")
            for table in tables:
                t_name, t_id, t_fds = table
                in_table = PMT.makePath(gdb, t_fds, t_name)
                # update params
                add_args["in_table"] = in_table
                calc_args["in_table"] = in_table
                if field_type == "TEXT":
                    length = nf_spec["length"]
                    add_args["field_length"] = length

                # # check if new field already in dataset, if recalc True delete and recalculate
                # if PMT.which_missing(table=in_table, field_list=[new_field]):
                #     if recalculate:
                #         print(f"--- --- recalculating {new_field}")
                #         arcpy.DeleteField_management(in_table=in_table, drop_field=new_field)
                #     else:
                #         print(f"--- --- {new_field} already exists, skipping...")
                #         continue
                # add and calc field
                arcpy.AddField_management(**add_args)
                arcpy.CalculateField_management(**calc_args)


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
    acc_fields, renames = _makeAccessColSpecs(
        activities, time_breaks, mode, include_average=False
    )
    if isinstance(id_field, string_types):
        id_field = [id_field]  # elif isinstance(Column)?

    all_fields = id_field + list(renames.values())
    df = PMT.featureclass_to_df(in_fc=int_fc, keep_fields=all_fields, null_val=0.0)
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
    header = pd.DataFrame(
        np.array(levels)[np.argsort(order)], columns=["Activity", "TimeBin"]
    )
    mi = pd.MultiIndex.from_frame(header)
    df.columns = mi
    df.reset_index(inplace=True)
    # Melt
    melt_df = df.melt(id_vars=id_field)
    # TODO: add lower time bin value so ranges can be reported in dashboard lists
    melt_df["from_time"] = melt_df["TimeBin"].apply(
        lambda tb: _get_time_previous_time_break_(time_breaks, tb), axis=1)
    return melt_df

def _get_time_previous_time_break_(time_breaks, tb):
    idx = time_breaks.index(tb)
    if idx == 0:
        return 0
    else:
        return time_breaks[idx - 1]

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
    this_df = this_df.reindex(base_df.index, fill_value=0)  # is this necessary?
    # Drop all remaining non-numeric columns
    base_df_n = base_df.select_dtypes(["number"])
    this_df_n = this_df.select_dtypes(["number"])
    # Reindex columns
    # this_df_n.reindex(columns=base_df_n.columns, inplace=True)
    # Take difference
    diff_df = this_df_n - base_df_n
    # Restore index columns
    diff_df.reset_index(inplace=True)

    return diff_df


def finalize_output(intermediate_gdb, final_gdb):
    """Takes an intermediate GDB path and the final GDB path for that data and
        replaces the existing GDB if it exists, otherwise it makes a copy
        of the intermediate GDB and deletes the original
    Args:
        intermediate_gdb (str): path to file geodatabase
        final_gdb (str): poth to file geodatabase, cannot be the same as intermediate
    Returns:
        None
    """
    output_folder, _ = os.path.split(intermediate_gdb)
    temp_folder = PMT.validate_directory(PMT.makePath(output_folder, "TEMP"))
    _, copy_old_gdb = os.path.split(final_gdb)
    temp_gdb = PMT.makePath(temp_folder, copy_old_gdb)
    try:
        # make copy of existing data if it exists
        if arcpy.Exists(final_gdb):
            arcpy.Copy_management(in_data=final_gdb, out_data=temp_gdb)
            arcpy.Delete_management(in_data=final_gdb)
        arcpy.Copy_management(in_data=intermediate_gdb, out_data=final_gdb)
    except:
        # replace old data with copy made in previous step
        print("An error occured, rolling back changes")
        arcpy.Copy_management(in_data=temp_gdb, out_data=final_gdb)
    finally:
        arcpy.Delete_management(intermediate_gdb)


def list_fcs_in_gdb():
    """ set your arcpy.env.workspace to a gdb before calling """
    for fds in arcpy.ListDatasets("", "feature") + [""]:
        for fc in arcpy.ListFeatureClasses("", "", fds):
            yield os.path.join(arcpy.env.workspace, fds, fc)


def post_process_databases(basic_features_gdb, build_dir):
    print("Postprocessing build directory...")
    # copy BasicFeatures into Build
    path, basename = os.path.split(basic_features_gdb)
    out_basic_features = PMT.makePath(build_dir, basename)
    if not arcpy.Exists(out_basic_features):
        arcpy.Copy_management(in_data=basic_features_gdb, out_data=out_basic_features)
    # reset SummID to RowID
    arcpy.env.workspace = build_dir
    # delete TEMP folcer
    temp = PMT.makePath(build_dir, "TEMP")
    if arcpy.Exists(temp):
        print("--- deleting TEMP folder from previous build steps")
        arcpy.Delete_management(temp)
    for gdb in arcpy.ListWorkspaces(workspace_type="FileGDB"):
        print(f"Cleaning up {gdb}")
        arcpy.env.workspace = gdb
        summ_areas = [fc for fc in list_fcs_in_gdb() if "SummaryAreas" in fc]
        for sa_path in summ_areas:
            if "SummID" in [f.name for f in arcpy.ListFields(sa_path)]:
                print(f"--- Converting SummID to RowID for {sa_path}")
                arcpy.AlterField_management(
                    in_table=sa_path,
                    field="SummID",
                    new_field_name="RowID",
                    new_field_alias="RowID",
                )
        for tbl in arcpy.ListTables():
            tbl = os.path.join(gdb, tbl)
            if "SummID" in [f.name for f in arcpy.ListFields(tbl)]:
                print(f"--- Converting SummID to RowID for {tbl}")
                arcpy.AlterField_management(
                    in_table=tbl,
                    field="SummID",
                    new_field_name="RowID",
                    new_field_alias="RowID",
                )
    # TODO: incorporate a more broad AlterField protocol for Popup configuration


def tag_filename(filename, tag):
    name, ext = os.path.splitext(filename)
    return "{name}_{tag}_{ext}".format(name=name, tag=tag, ext=ext)
