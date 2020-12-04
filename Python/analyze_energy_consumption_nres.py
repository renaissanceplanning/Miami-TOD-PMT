"""
Created: October 2020
@Author: Alex Bell

Provides a simple function to estimate energy consumption from non-residential
buildings based on building type (land use) and square footage.

If run as "main", estimates non-residential energy consumption for all
Miami-Dade County parcels for each year PMT analysis year. Results are appended
to the "parcels/land_use_and_value" features in each year's geodatabase.
"""

# %% IMPORTS
import PMT
import arcpy
import pandas as pd

# %% FUNCTIONS
def estimateParcelNresEnergy(energy_table, energy_lu_field, energy_sqft_field,
                             parcel_fc, parcel_lu_field, parcel_sqft_field):
    """
    Using a table of non-residential energy consumption rates and a parcel
    feature class, estimate non-residential energy consumption based on
    parcel land use and building area.

    Parameters
    -----------
    energy_table: Path
        A csv file containing energy consumption rates
        (btu per square foot) based on building type.
    energy_lu_field: String
        The field in `energy_table` that defines building types or land uses.
    energy_sqft_field: String
        The field in `energy_table` that records BTU per square foot rates
        for each building type.
    parcel_fc: Path
        A feature class of parcels
    parcel_lu_field: String
        The field in `parcel_fc` that defines each parcel's land use (values
        correspond to those in `energy_lu_field`).
    parcel_sqft_field: String
        The field in `parcel_fc` that records the total floor area of
        buildings on the parcel.

    Returns
    --------
    parcel_fc: Path
        `parcel_fc` is modified in place such that a new field `NRES_BTU` is
        added and populated based on building type and floor area.
    """
    # Read in csv table
    energy = pd.read_csv(energy_table)
    energy[energy_lu_field] = energy[energy_lu_field].str.strip()

    # Add NRES energy output fields
    arcpy.AddField_management(parcel_fc, "NRES_BTU", "DOUBLE")
    par_fields = [parcel_lu_field, parcel_sqft_field, "NRES_BTU"]

    # Update parcels
    with arcpy.da.UpdateCursor(parcel_fc, par_fields) as c:
        for r in c:
            lu, sqft, btu = r
            if lu is None or sqft is None:
                continue
            # Get btu multiplier for this lu
            fltr = energy[energy_lu_field] == lu]
            sel = energy[fltr][enerfy_sqft_field]
            factor = sel.iloc[0]
            r[-1] = sqft * factor
            c.updateRow(r)
    
    return parcel_fc


# %% MAIN
if __name__ == "__main__":
    # Read in csv table
    energy_table = PMT.makePath(
        PMT.CLEANED, "Energy_Consumption", "Nres_Energy_Consumption.csv")
    energy_lu_field = "BldgType"
    energy_sqft_field = "BTUperSqFt_Thous"
    for year in PMT.YEARS:
        par_fc = PMT.makePath(
            PMT.DATA, f"PMT_{year}.gdb", "Parcels", "land_use_and_value")
        par_lu_field = "NRES_ENERGY"
        par_sqft_field = "TOT_LVG_AREA"
    estimateParcelNresEnergy(energy_table, energy_lu_field, energy_sqft_field,
                             par_fc, par_lu_field, par_sqft_field)
