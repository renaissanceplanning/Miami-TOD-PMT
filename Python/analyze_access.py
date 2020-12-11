"""
"""



# %% IMPORTS
import PMT
import arcpy
import pandas as pd
import numpy as np
from six import string_types


# %% GLOBALS
TIME_BREAKS = [10, 20, 30, 40, 50, 60]


# %% FUNCTIONS
def summarizeAccess(skim_table, o_field, d_field, imped_field,
                    se_data, id_field, act_fields, out_table,
                    imped_breaks, units="minutes",
                    join_by="D", chunk_size=100000, **kwargs):
    """
    Reads an origin-destination skim table, joins activity data,
    and summarizes activities by impedance bins.

    Parameters
    -----------
    skim_table: Path
    o_field: String
    d_field: String
    imped_field: String
    se_data: Path
    id_field: String
    act_fields: [String, ...]
    out_table: Path
    imped_breaks: [Numeric, ...]
    units: String, default="minutes"
    join_by: String, default="D"
    chunk_size: Int, default=100000
    kwargs:
        Keyword arguments for reading the skim table

    Returns
    --------
    out_table: Path
    """
    # Prep vars
    if isinstance(act_fields, string_types):
        act_fields = [act_fields]
    if join_by == "D":
        left_on = d_field
        gb_field = o_field
    elif join_by == "O":
        left_on = o_field
        gb_field = d_field
    else:
        raise ValueError(
            f"Expected 'D' or 'O' as `join_by` value - got {join_by}")
    # Read the activity data
    _a_fields_ = [id_field, act_fields]
    act_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(se_data, _a_fields_)
    )

    # Read the skim table
    out_dfs = []
    use_cols = [o_field, d_field, imped_field]
    for chunk in pd.read_csv(
        skim_table, usecols=use_cols, chunksize=chunk_size, **kwargs):
        # Define impedance bins
        low = -np.inf
        criteria = []
        labels = []
        for i_break in imped_breaks:
            crit = np.logical_and(
                chunk[imped_field] >= low,
                chunk[imped_field] < i_break
            )
            criteria.append(crit)
            labels.append(f"<{i_break} {units}")
            low = i_break
        # Apply categories
        chunk[f"BIN_{units}"] = np.select(
            criteria, labels, f"{i_break} {units}+"
        )
        # Join the activity data
        join_df = chunk.merge(
            act_df, how="inner", left_on=left_on, right_on=id_field)
        # Summarize
        sum_df = join_df.groupby(gb_field).sum().reset_index()
        out_dfs.append(sum_df)
    # Concatenate all
    out_df = pd.concat(out_dfs)
    PMT.dfToTable(out_df, out_table)




