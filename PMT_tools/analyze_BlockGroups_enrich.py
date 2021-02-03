"""
Created: October 2020
@Author: Brian Froeb & Alex Bell

...
...
"""

# %% IMPORTS
import PMT
import arcpy
import numpy as np
import pandas as pd
import geopandas as gpd
import censusdata as census
import re
from collections.abc import Iterable
from six import string_types
import os


# %% GLOBALS

# Comparison methods:
#   - __eq__() = equals [==]
#   - __ne__() = not equal to [!=]
#   - __lt__() = less than [<]
#   - __le__() = less than or equal to [<=]
#   - __gt__() = greater than [>]
#   - __ge__() = greater than or equal to [>=]

class Comp:
    """
    """

    def __init__(self, comp_method, v):
        _comp_methods = {
            "==": "__eq__",
            "!=": "__ne__",
            "<": "__lt__",
            "<=": "__le__",
            ">": "__gt__",
            ">=": "__ge__"
        }
        self.comp_method = _comp_methods[comp_method]
        self.v = v

    def eval(self, val):
        return getattr(val, self.comp_method)(self.v)


class And:
    """
    """

    def __init__(self, criteria):
        self.criteria = criteria

    def __setattr__(self, name, value):
        if name == "criteria":
            criteria = []
            if isinstance(value, Iterable):
                for v in value:
                    if not isinstance(v, Comp):
                        raise TypeError(f"Expected Comp, got {type(v)}")
                    criteria.append(v)
            else:
                if isinstance(value, Comp):
                    criteria.append(value)
                else:
                    raise TypeError(f"Expected Criterion, got {type(v)}")
            super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    def eval(self, *vals):
        """
        """
        # Check
        try:
            v = vals[1]
        except IndexError:
            vals = [vals[0] for _ in self.criteria]
        bools = [c.eval(v) for c, v in zip(self.criteria, vals)]

        return np.logical_and.reduce(bools)


class Or:
    """
    """

    def __init__(self, vector, criteria):
        self.vector = vector
        if isinstance(criteria, Iterable):
            self.criteria = criteria  # TODO: validate criteria
        else:
            self.criteria = [criteria]

    def eval(self):
        return (
            np.logical_or.reduce(
                [c.eval(self.vector) for c in self.criteria]
            )
        )


LODES_CRITERIA = {
    "CNS_01_par": And([Comp(">=", 50), Comp("<=", 69)]),
    "CNS_02_par": Comp("==", 92),
    "CNS_03_par": Comp("==", 91),
    "CNS_04_par": [Comp("==", 17), Comp("==", 19)],
    "CNS_05_par": [Comp("==", 41), Comp("==", 42)],
    "CNS_06_par": Comp("==", 29),
    "CNS_07_par": And([Comp(">=", 11), Comp("<=", 16)]),
    "CNS_08_par": [Comp("==", 20), Comp("==", 48), Comp("==", 49)],
    "CNS_09_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_10_par": [Comp("==", 23), Comp("==", 24)],
    "CNS_11_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_12_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_13_par": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_14_par": Comp("==", 89),
    "CNS_15_par": [Comp("==", 72), Comp("==", 83), Comp("==", 84)],
    "CNS_16_par": [Comp("==", 73), Comp("==", 85)],
    "CNS_17_par": [And([Comp(">=", 30), Comp("<=", 38)]), Comp("==", 82)],
    "CNS_18_par": [Comp("==", 21), Comp("==", 22), Comp("==", 33), Comp("==", 39)],
    "CNS_19_par": [Comp("==", 27), Comp("==", 28)],
    "CNS_20_par": And([Comp(">=", 86), Comp("<=", 89)]),
    "RES_par": [
        And([Comp(">=", 1), Comp("<=", 9)]),
        And([Comp(">=", 100), Comp("<=", 102)])
    ]
}


# %% FUNCTIONS

def enrichBlockGroups(bg_fc, parcels_fc, out_fc, bg_id_field="GEOID10",
                      par_id_field="PARCELNO", par_lu_field="DOR_UC",
                      par_bld_area="TOT_LVG_AREA", sum_crit={},
                      par_sum_fields=["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA"],
                      overwrite=False):
    """
    Relates parcels to block groups based on centroid location and summarizes
    key parcel fields to the block group level, including building floor area
    by potential activity type (residential, jobs by type, e.g.).

    Parameters
    ------------
    bg_fc: Path
    parcels_fc: Path
    out_fc: Path
    bg_id_field: String, default="GEOID10"
    par_id_field: String, default="PARCELNO"
    par_lu_field: String, default="DOR_UC"
    par_bld_area: String, default="TOT_LVG_AREA"
    par_sum_fields: List, [String,...], default=["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA]
        If provided, these parcel fields will also be summed to the block-group level.
    overwrite: Bool, set to True will delete and recreate an existing output
    """
    # Prep output
    if arcpy.Exists(out_fc):
        if overwrite:
            arcpy.Delete_management(out_fc)
        else:
            raise RuntimeError(f"Output feature class {out_fc} already exists")
    out_ws, out_name = os.path.split(out_fc)
    sr = arcpy.Describe(parcels_fc).spatialReference

    # Copy block groups to output location
    print("...copying block groups to output location")
    out_ws, out_name = os.path.split(out_fc)
    arcpy.FeatureClassToFeatureClass_conversion(bg_fc, out_ws, out_name)

    # Make parcel feature layer
    parcel_fl = arcpy.MakeFeatureLayer_management(parcels_fc, "__parcels__")
    par_fields = [par_id_field, par_lu_field, par_bld_area]
    par_fields += [psf for psf in par_sum_fields if psf not in par_fields]

    try:
        # Iterate over bg features
        print("...analyzing block group features")
        bg_fields = ["SHAPE@", bg_id_field]
        bg_stack = []
        with arcpy.da.SearchCursor(
                out_fc, bg_fields, spatial_reference=sr) as bgc:
            for bgr in bgc:
                bg_poly, bg_id = bgr
                # Select parcels in this BG
                arcpy.SelectLayerByLocation_management(
                    parcel_fl, "HAVE_THEIR_CENTER_IN", bg_poly)
                # Dump selected to data frame
                par_df = pd.DataFrame(
                    arcpy.da.TableToNumPyArray(
                        parcel_fl, par_fields, skip_nulls=False, null_value=0
                    )
                )
                if len(par_df) == 0:
                    print(f"... ...no parcels found for BG {bg_id}")
                # Get mean parcel values
                par_grp_fields = [par_id_field] + par_sum_fields
                par_sum = par_df[par_grp_fields].groupby(par_id_field).mean()
                # Summarize totals to BG level
                par_sum[bg_id_field] = bg_id
                bg_grp_fields = [bg_id_field] + par_sum_fields
                bg_sum = par_sum[bg_grp_fields].groupby(bg_id_field).sum()
                # Select and summarize new fields
                for grouping in sum_crit.keys():
                    # Mask based on land use criteria
                    crit = Or(par_df[par_lu_field], sum_crit[grouping])
                    mask = crit.eval()
                    # Summarize masked data
                    #  - Parcel means (to account for multi-poly's)
                    area = par_df[mask].groupby([par_id_field]).mean()[par_bld_area]
                    #  - BG Sums
                    if len(area) > 0:
                        area = area.sum()
                    else:
                        area = 0
                    bg_sum[grouping] = area
                bg_stack.append(bg_sum.reset_index())
        # Join bg sums to outfc
        print("...joining parcel summaries to block groups")
        bg_df = pd.concat(bg_stack)
        print(f"... ...{len(bg_df)} block group rows")
        PMT.extendTableDf(out_fc, bg_id_field, bg_df, bg_id_field)
    except:
        raise
    finally:
        arcpy.Delete_management(parcel_fl)


# %% MAIN
if __name__ == "__main__":
    # TODO:
    # Define analysis specs
    bg_id_field = "GEOID10"
    par_lu_field = "DOR_UC"
    par_bld_area = "TOT_LVG_AREA"
    sum_crit = LODES_CRITERIA
    par_sum_fields = ["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA"]
    # For all years, enrich block groups with parcels
    for year in PMT.YEARS:
        print(year)
        # Define inputs/outputs
        bg_fc = PMT.makePath(
            PMT.CLEANED, "BlockGroups.gdb", "BlockGroups_{}".format(year))
        parcels_fc = PMT.makePath(
            PMT.CLEANED, "Parcels.gdb", "Miami_{}".format(year))
        out_fc = PMT.makePath(PMT.ROOT, "PMT_{}.gdb".format(year),
                              "BlockGroups", "blockgroup_enrich")
        # Enrich BGs with parcel data
        enrichBlockGroups(bg_fc, parcels_fc, out_fc, bg_id_field=bg_id_field,
                          par_id_field="PARCELNO", par_lu_field=par_lu_field,
                          par_bld_area=par_bld_area, sum_crit=sum_crit,
                          par_sum_fields=par_sum_fields, overwrite=True)
