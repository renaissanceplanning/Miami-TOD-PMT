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
def estimateParcelNresEnergy(energy_df, energy_lu_field, energy_sqft_field,
                             parcel_fc, parcel_lu_field, parcel_sqft_field,
                             parcel_id_field, out_table, out_id_field):
    """
    Using a table of non-residential energy consumption rates and a parcel
    feature class, estimate non-residential energy consumption based on
    parcel land use and building area.

    Parameters
    -----------
    energy_df: DataFrame
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
    parcel_id_field: String
    out_table: Path
    out_id_field: String

    Returns
    --------
    parcel_fc: Path
        `parcel_fc` is modified in place such that a new field `NRES_BTU` is
        added and populated based on building type and floor area.
    """
    # # Read in csv table
    # energy = pd.read_csv(energy_table)
    # energy[energy_lu_field] = energy[energy_lu_field].str.strip()

    # # Add NRES energy output fields
    # arcpy.AddField_management(parcel_fc, "NRES_BTU", "DOUBLE")
    # par_fields = [parcel_lu_field, parcel_sqft_field, "NRES_BTU"]
    par_fields = [parcel_id_field, parcel_lu_field, parcel_sqft_field]
    df_cols = [parcel_id_field, "NRES_BTU"]

    # Update parcels
    out_rows = []
    with arcpy.da.SearchCursor(parcel_fc, par_fields) as c:
        for r in c:
            par_id, lu, sqft = r
            if lu is None or sqft is None:
                out_rows.append((par_id, 0))
                continue
            # Get btu multiplier for this lu
            fltr = energy_df[energy_lu_field] == lu
            sel = energy_df[fltr][energy_sqft_field]
            try:
                factor = sel.iloc[0]
            except IndexError:
                factor = 0
            BTU = sqft * factor
            out_rows.append((par_id, BTU))
    
    df = pd.DataFrame(out_rows, columns=df_cols)
    PMT.extend_table_df(out_table, out_id_field, df, parcel_id_field)
    return out_table


# %% MAIN
if __name__ == "__main__":
    # Read in csv table of btu rates
    energy_table = PMT.makePath(
        PMT.CLEANED, "Energy_Consumption", "Nres_Energy_Consumption.csv")
    energy_lu_field = "BldgType"
    energy_sqft_field = "BTUperSqFt_Thous"

    # Read in table of land use categories
    lu_table = PMT.makePath(PMT.REF, "Land_Use_Recode.csv")
    lu_dor_field = "DOR_UC"
    lu_nres_field = "NRES_ENERGY"

    # Make energy ref data frame
    keep_cols = [lu_dor_field, energy_lu_field, energy_sqft_field]
    rates_df = pd.read_csv(energy_table)
    rates_df[energy_lu_field] = rates_df[energy_lu_field].str.strip()
    lu_df = pd.read_csv(lu_table)
    lu_df[lu_nres_field] = lu_df[lu_nres_field].str.strip()
    energy_df = rates_df.merge(
        lu_df, how="inner", left_on=energy_lu_field, right_on=lu_nres_field)
    energy_df = energy_df[keep_cols].copy()

    print(energy_df)

    for year in PMT.YEARS:
        print(year)
        # In parcels
        par_fc = PMT.makePath(
            PMT.DATA, f"IDEAL_PMT_{year}.gdb", "Polygons", "Parcels")
        par_id_field = "FOLIO"
        par_lu_field = "DOR_UC"
        par_sqft_field = "TOT_LVG_AREA"

        # Out table
        econs_table = PMT.makePath(
            PMT.DATA, f"IDEAL_PMT_{year}.gdb", "EnergyCons_parcels")
        econs_id = "FOLIO"

        # Drop column if needed
        check = arcpy.ListFields(econs_table, "NRES_BTU")
        if check:
            print("... dropping existing field")
            arcpy.DeleteField_management(econs_table, "NRES_BTU")

        estimateParcelNresEnergy(energy_df, par_lu_field, energy_sqft_field,
                                 par_fc, par_lu_field, par_sqft_field, par_id_field,
                                 econs_table, econs_id)

