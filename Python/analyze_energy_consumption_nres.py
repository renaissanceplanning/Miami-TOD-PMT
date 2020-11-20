"""
Created: October 2020
@Author: Alex Bell


"""

# %% IMPORTS
import PMT
import arcpy
import pandas as pd

# %% FUNCTIONS
def estimateParcelNresEnergy(energy_table, energy_lu_field, energy_sqft_field,
                             parcel_fc, parcel_lu_field, parcel_sqft_field):
    """
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
