import os
import uuid
from collections.abc import Iterable
import itertools

import arcpy
import numpy as np
from six import string_types

from PMT_tools import PMT
from PMT_tools.PMT import Column, AggColumn, Consolidation, MeltColumn
from PMT_tools.config import prepare_config as p_conf
from PMT_tools.config import build_config as b_conf
from PMT_tools.utils import _list_table_paths, _list_fc_paths, _createLongAccess


def build_access_by_mode(sum_area_fc, modes, id_field, out_gdb, year_val):
    """
    helper function to generate access tables by mode
    Args:
        sum_area_fc (str): path to summary area feature class
        modes (list): modes of travel
        id_fields (list): fields to be used as index
        out_gdb (str): path to output geodatabase
        year_val (int): value to insert for year

    Returns:
        None
    """
    for mode in modes:
        print(f"--- --- {mode}")
        df = _createLongAccess(
            int_fc=sum_area_fc,
            id_field=id_field,
            activities=b_conf.ACTIVITIES,
            time_breaks=b_conf.TIME_BREAKS,
            mode=mode,
        )
        df["Year"] = year_val
        out_table = PMT.make_path(out_gdb, f"ActivityByTime_{mode}")
        PMT.df_to_table(df, out_table)


def process_joins(in_gdb, out_gdb, fc_specs, table_specs):
    """Joins feature classes to associated tabular data from year set and appends to FC in output gdb
        in_gdb: String; path to g
    Returns:
        [String,...]; list of paths to joined feature classes ordered as
            Blocks, Parcels, MAZ, TAZ, SummaryAreas, NetworkNodes
    """

    #   tables need to be ordered the same as FCs
    _table_specs_ = []
    for fc in fc_specs:
        t_specs = [spec for spec in table_specs if fc[0].lower() in spec[0].lower()]
        _table_specs_.append(t_specs)
    table_specs = _table_specs_

    # join tables to feature classes, making them WIDE
    joined_fcs = []  # --> blocks, parcels, maz, taz, sa, net_nodes
    for fc_spec, table_spec in zip(fc_specs, table_specs):
        fc_name, fc_id, fds = fc_spec
        fc = PMT.make_path(out_gdb, fds, fc_name)

        for spec in table_spec:
            tbl_name, tbl_id, tbl_fields, tbl_renames = spec
            tbl = PMT.make_path(in_gdb, tbl_name)
            print(f"--- Joining fields from {tbl_name} to {fc_name}")
            joinAttributes(
                to_table=fc,
                to_id_field=fc_id,
                from_table=tbl,
                from_id_field=tbl_id,
                join_fields=tbl_fields,
                renames=tbl_renames,
                drop_dup_cols=True,
            )
            joined_fcs.append(fc)
    return joined_fcs


# TODO: add debug flag/debug_folder to allow intersections to be written to know location
def build_intersections(gdb, enrich_specs):
    """
    helper function that performs a batch intersection of polygon feature classes
    Args:
        enrich_specs (list): list of dictionaries specifying source data, groupings, aggregations,
            consolidations, melt/elongation, and boolean for full geometry or centroid use in intersection
        gdb (str): path to geodatabase that contains the source data
    Returns (dict):
        dictionary of the format {summ fc: {
                                    disag_fc: path/to/intersection
                                    }
                                }
        will return multiple results for each summ_fc if more than one intersection is made against it.
    """
    # Intersect features for long tables
    int_out = {}
    for intersect in enrich_specs:
        # Parse specs
        summ, disag = intersect["sources"]
        summ_name, summ_id, summ_fds = summ
        disag_name, disag_id, disag_fds = disag
        summ_in = PMT.make_path(gdb, summ_fds, summ_name)
        disag_in = PMT.make_path(gdb, disag_fds, disag_name)
        full_geometries = intersect["disag_full_geometries"]
        # Run intersect
        print(f"--- Intersecting {summ_name} with {disag_name}")
        int_fc = PMT.intersect_features(
            summary_fc=summ_in,
            disag_fc=disag_in,
            in_temp_dir=True,
            full_geometries=full_geometries,
        )
        # Record with specs
        sum_dict = int_out.get(summ, {})
        sum_dict[disag] = int_fc
        int_out[summ] = sum_dict

    return int_out


def build_enriched_tables(gdb, fc_dict, specs):
    """
    helper function used to enrich and/or elongate data for a summarization area
    Args:
        gdb (str): path to geodatabase where outputs are written
        fc_dict (dict): dictionary returned from build_intersections
        specs (list of dicts): list of dictionaries specifying sources, grouping, aggregations,
            consolidations, melts/elongations, and an output table (this is used by the try/except
            clause to make a new table (elongation) or append to an existing feature class (widening)

    Returns:
        None
    """
    # Enrich features through summarization
    for spec in specs:
        summ, disag = spec["sources"]
        fc_name, fc_id, fc_fds = summ
        d_name, d_id, d_fds = disag
        if summ == disag:
            # Simple pivot wide to long
            fc = PMT.make_path(gdb, fc_fds, fc_name)
        else:
            # Pivot from intersection
            fc = fc_dict[summ][disag]

        print(f"--- Summarizing data from {d_name} to {fc_name}")
        # summary vars
        group = spec["grouping"]
        agg = spec["agg_cols"]
        consolidate = spec["consolidate"]
        melts = spec["melt_cols"]
        summary_df = summarizeAttributes(
            in_fc=fc,
            group_fields=group,
            agg_cols=agg,
            consolidations=consolidate,
            melt_col=melts,
        )
        try:
            out_name = spec["out_table"]
            print(f"--- --- to long table {out_name}")
            out_table = PMT.make_path(gdb, out_name)
            PMT.df_to_table(df=summary_df, out_table=out_table, overwrite=True)
        except KeyError:
            # extend input table
            feature_class = PMT.make_path(gdb, fc_fds, fc_name)
            # if being run again, delete any previous data as da.ExtendTable will fail if a field exists
            summ_cols = [col for col in summary_df.columns.to_list() if col != fc_id]
            drop_fields = [
                f.name for f in arcpy.ListFields(feature_class) if f.name in summ_cols
            ]
            if drop_fields:
                print(
                    f"--- --- deleting previously generated data and replacing with current summarizations"
                )
                arcpy.DeleteField_management(
                    in_table=feature_class, drop_field=drop_fields
                )
            PMT.extend_table_df(
                in_table=feature_class,
                table_match_field=fc_id,
                df=summary_df,
                df_match_field=fc_id,
                append_only=False,
            )  # TODO: handle append/overwrite more explicitly


def sum_parcel_cols(gdb, par_spec, columns):
    """
    helper function to summarize a provided list of columns for the parcel layer, creating
    region wide statistics
    Args:
        gdb (str): path to geodatabase that parcel layer exists in
        par_spec (tuple): tuple of format (fc name, unique id column, feature dataset location)
        columns (list): string list of fields/columns needing summarization

    Returns:
        pandas.Dataframe
    """
    par_name, par_id, par_fds = par_spec
    par_fc = PMT.make_path(gdb, par_fds, par_name)
    df = PMT.featureclass_to_df(
        in_fc=par_fc, keep_fields=columns, skip_nulls=False, null_val=0
    )
    return df.sum()


def unique_values(table, field):
    """
    helper function to return all unique values for a provided field/column
    Args:
        table (str): path to table of interest
        field (str): field name of interest

    Returns (ndarray):
        sorted unique values from the field
    """
    data = arcpy.da.TableToNumPyArray(in_table=table, field_names=[field], null_value=0)
    return np.unique(data[field])


def add_year_columns(in_gdb, year):
    """
    helper function ensuring the year attribute is present in all layers/tables
    Args:
        in_gdb (str): path to geodatabase
        year (int): value to be calculated as year

    Returns:
        None
    """
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
        print(f"--- Adding and calculating 'Year' attribute for {fc}")
        arcpy.AddField_management(in_table=fc, field_name="Year", field_type="LONG")
        arcpy.CalculateField_management(in_table=fc, field="Year", expression=str(year))
    for table in tables:
        if "Year" in [f.name for f in arcpy.ListFields(table)]:
            values = unique_values(table=table, field="Year")
            if len(values) == 1 and values[0] == year:
                print(f"--- 'Year' already in {table}...skipping")
                continue
        print(f"--- Adding and calculating 'Year' attribute for {table}")
        arcpy.AddField_management(in_table=table, field_name="Year", field_type="LONG")
        arcpy.CalculateField_management(in_table=table, field="Year", expression=str(year))
    arcpy.env.workspace = old_ws


def make_reporting_gdb(out_path, out_gdb_name=None, overwrite=False):
    """
    helper function to create a temporary geodatabase to hold data as its procesed
    Args:
        out_path (str): path to folder
        out_gdb_name (str): name of geodatabase, Default is None, resulting in a unique name
        overwrite (bool): flag to delete an existing geodatabase

    Returns (str):
        path to output geodatabase
    """
    if not out_gdb_name:
        out_gdb_name = f"_{uuid.uuid4().hex}.gdb"
        out_gdb = PMT.make_path(out_path, out_gdb_name)
    elif out_gdb_name and overwrite:
        out_gdb = PMT.make_path(out_path, out_gdb_name)
        PMT.checkOverwriteOutput(output=out_gdb, overwrite=overwrite)
    else:
        out_gdb = PMT.make_path(out_path, out_gdb_name)
    arcpy.CreateFileGDB_management(out_path, out_gdb_name)
    return out_gdb


def make_snapshot_template(in_gdb, out_path, out_gdb_name=None, overwrite=False):
    """
    helper function to copy yearly feature classes into a reporting geodatabase;
    copies all feature datasets from corresponding clean data workspace
    Args:
        in_gdb (str): path to clean data workspace
        out_path (str): path where snapshot template gdb is written
        out_gdb_name (str): optional name of output gdb
        overwrite (bool): boolean flag to overwrite an existing copy of the out_gdb_name

    Returns (str):
        path to the newly created reporting geodatabase
    """
    out_gdb = make_reporting_gdb(out_path, out_gdb_name, overwrite)
    # copy in the geometry data containing minimal tabular data
    for fds in ["Networks", "Points", "Polygons"]:
        print(f"--- copying FDS {fds}")
        source_fd = PMT.make_path(in_gdb, fds)
        out_fd = PMT.make_path(out_gdb, fds)
        arcpy.Copy_management(source_fd, out_fd)
    return out_gdb


def make_trend_template(out_path, out_gdb_name=None, overwrite=False):
    """
    helper function to generate a blank output workspace with necessary feature
    dataset categories
    Args:
        out_path (str): path where trend template gdb is written
        out_gdb_name (str): optional name of output gdb
        overwrite (bool): boolean flag to overwrite an existing copy of the out_gdb_name

    Returns (str):
        path to the newly created reporting geodatabase
    """
    out_gdb = make_reporting_gdb(out_path, out_gdb_name, overwrite)
    for fds in ["Networks", "Points", "Polygons"]:
        arcpy.CreateFeatureDataset_management(
            out_dataset_path=out_gdb, out_name=fds, spatial_reference=PMT.SR_FL_SPF
        )
    return out_gdb


def _validateAggSpecs(var, expected_type):
    """
    helper function to validate a set of aggregation/grouping specs match the necessary object type
    Args:
        var (list/str): list of grouping/aggregation specs
        expected_type (str): object type

    Returns (list):
        if the var variable validates, the same list is returned
    """
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
    join_fields="*",
    null_value=0.0,
    renames=None,
    drop_dup_cols=False,
):
    """
    helper function to join attributes of one table to another
    Args:
        to_table (str): path to table being extended
        to_id_field (str): primary key
        from_table (str): path to table being joined
        from_id_field (str): foreign key
        join_fields (list/str): list of fields to be added to to_table;
            Default: "*", indicates all fields are to be joined
        null_value (int/str): value to insert for nulls
        renames (dict): key/value pairs of existing field names/ new field names
        drop_dup_cols (bool): flag to eliminate duplicated fields

    Returns:
        None
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
    PMT.extend_table_df(
        in_table=to_table,
        table_match_field=to_id_field,
        df=df,
        df_match_field=from_id_field,
    )


def summarizeAttributes(
    in_fc, group_fields, agg_cols, consolidations=None, melt_col=None
):
    """
    helper function to perform summarizations of input feature class defined by the
    group, agg, consolidate, and melt columns/objects provided
    Args:
        in_fc (str): path to feature class, typically this will be the result of an
            intersection of a summary fc and disaggregated fc
        group_fields (list): list of Column objects with optional rename attribute
        agg_cols (list): list of AggColumn objects with optional agg_method and rename attributes
        consolidations (list): list of Consolidation objects with optional consolidation method attribute
        melt_col (list): list of MeltColumn objects with optional agg_method, default value, and
            DomainColumn object

    Returns:
        pandas.Dataframe object with all data summarized according to specs
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
    """
    helper function that applies field calculations, adding a new field to a table
    Args:
        gdb (str): path to geodatabase containing table to have new calc added
        new_field_specs (list): list of dictionaries specifying table(s), new_field, field_type,
            expr, code_block
            Example:
                {"tables": [PAR_FC_SPECS], "new_field": "RES_AREA", "field_type": "FLOAT",
                "expr": "calc_area(!LND_SQFOOT!, !NO_RES_UNTS!)",
                "code_block": '''
                def calc_area(sq_ft, activity):
                if activity is None:
                    return 0
                elif activity <= 0:
                    return 0
                else:
                    return sq_ft
                ''',
            }

        recalculate (bool): flag to rerun a calculation if the field already exsits in the table;
            currently unused

    Returns:
        None
    """
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
                in_table = PMT.make_path(gdb, t_fds, t_name)
                # update params
                add_args["in_table"] = in_table
                calc_args["in_table"] = in_table
                if field_type == "TEXT":
                    length = nf_spec["length"]
                    add_args["field_length"] = length

                # TODO: fix the below to work, was failing previously
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


def finalize_output(intermediate_gdb, final_gdb):
    """
    Takes an intermediate GDB path and the final GDB path for that data and
        replaces the existing GDB if it exists, otherwise it makes a copy
        of the intermediate GDB and deletes the original
    Args:
        intermediate_gdb (str): path to file geodatabase
        final_gdb (str): path to file geodatabase, cannot be the same as intermediate
    Returns:
        None
    """
    output_folder, _ = os.path.split(intermediate_gdb)
    temp_folder = PMT.validate_directory(PMT.make_path(output_folder, "TEMP"))
    _, copy_old_gdb = os.path.split(final_gdb)
    temp_gdb = PMT.make_path(temp_folder, copy_old_gdb)
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
        print("")


def list_fcs_in_gdb():
    """
    helper function to grab all feature classes ina geodatabase, assumes you
    set your arcpy.env.workspace to a gdb before calling
    Yields:
        path to feature class
    """
    for fds in arcpy.ListDatasets("", "feature") + [""]:
        for fc in arcpy.ListFeatureClasses("", "", fds):
            yield os.path.join(arcpy.env.workspace, fds, fc)


def alter_fields(table_list, field, new_field_name):
    """
    helper function to rename a field found in multiple tables
        created to handle RowID used for summary features
    Args:
        table_list (list): list of paths to tables containing the field of interest
        field (str): current field name
        new_field_name (str): desired field name to replace existing

    Returns:
        None
    """
    for tbl in table_list:
        if field in [f.name for f in arcpy.ListFields(tbl)]:
            print(f"--- --- Converting SummID to {new_field_name} for {tbl}")
            arcpy.AlterField_management(
                in_table=tbl,
                field=field,
                new_field_name=new_field_name,
                new_field_alias=new_field_name,
            )


def tag_filename(filename, tag):
    """
    helper method to add a suffix to the end of a filename
    Args:
        filename (str): path to file or filename string
        tag (str): string suffix to append to end of filename
    Returns:
        str; updated filepath or filename string with suffix appended
    """
    name, ext = os.path.splitext(filename)
    return "{name}_{tag}_{ext}".format(name=name, tag=tag, ext=ext)


def post_process_databases(basic_features_gdb, build_dir):
    """
    copies in basic features gdb to build dir and cleans up FCs and Tables
        with SummID to RowID. Finally deletes the TEMP folder generated in the
        build process
    Args:
        basic_features_gdb (str): path to the basic features geodatabase
        build_dir (str): path to the build directory
    Returns:
        None
    """
    print("Postprocessing build directory...")

    # copy BasicFeatures into Build
    print("--- Overwriting basic features in BUILD dir with current version")
    path, basename = os.path.split(basic_features_gdb)
    out_basic_features = PMT.make_path(build_dir, basename)
    PMT.checkOverwriteOutput(output=out_basic_features, overwrite=True)
    arcpy.Copy_management(in_data=basic_features_gdb, out_data=out_basic_features)

    # reset SummID to RowID
    print("--- updating SummID to RowID project wide...")
    arcpy.env.workspace = build_dir
    for gdb in arcpy.ListWorkspaces(workspace_type="FileGDB"):
        print(f"--- Cleaning up {gdb}")
        arcpy.env.workspace = gdb
        # update feature classes
        fcs = [fc for fc in list_fcs_in_gdb()]
        tbls = arcpy.ListTables()
        all_tbls = fcs + tbls
        alter_fields(
            table_list=all_tbls,
            field=p_conf.SUMMARY_AREAS_COMMON_KEY,
            new_field_name=b_conf.SUMMARY_AREAS_FINAL_KEY)
    # TODO: incorporate a more broad AlterField protocol for Popup configuration

    # delete TEMP folder
    temp = PMT.make_path(build_dir, "TEMP")
    if arcpy.Exists(temp):
        print("--- deleting TEMP folder from previous build steps")
        arcpy.Delete_management(temp)


