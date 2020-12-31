import arcpy
import PMT
import panda as pd
from six import string_types

def calcTrend(snapshot_table, ref_table, update_table,
              index_cols, value_cols):
    """
    """
    # Validation and setup
    if isinstance(index_cols, string_types):
        index_cols = [index_cols]
    if isinstance(value_cols, string_types):
        value_vols = [value_cols]
    offset = len(index_cols)
    all_cols = index_cols + value_cols
 
    # Dump snapshot table to df
    snap_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(snapshot_table, all_cols, null_value=0)
    )
    snap_df.set_index(index_cols, inplace=True)
    # Dump ref table to df
    ref_df = pd.DataFrame(
        arcpy.da.TableToNumPyArray(ref_table, all_cols, null_value=0)
    )
    ref_df.set_index(index_cols, inplace=True)
    # Update rows in udpate table
    with arcpy.da.UpdateCursor(update_table, all_cols) as c:
        for r in c:
            id = tuple(r[:offset])
            snap_row = snap_df.loc[id]
            ref_row = ref_df.loc[id]
            for vi, vc in enumerate(value_cols):
                snap_val = snap_row[vc]
                ref_val = ref_row[vc]
                diff = ref_val - v
                r[vi+offset] = diff
            c.udpateRow(r)