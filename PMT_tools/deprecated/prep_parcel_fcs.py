"""
Created: October 2020
@Author: Alex Bell

Functions that facilitate the cleaning of parcel shape files and joining
of parcel attributes in an output geodatabase. Column renaming dictionaries
(if specified) help ensure all parcel attribute tables use the same field
names.

If run as "main", parcel features and tables from the raw Miami-Dade PMT
folder are cleaned for the years 2014 to 2019.
Parcel columns are consistent from 2014 to 2018. In 2019, new column names
emerge. These are renamed to match values in the 14-18 time frame using
the `COLS19` global dictionary.
"""

# %% IMPORTS
import arcpy
import pandas as pd
import PMT

# %% GLOBALS
USE_COLS = {
    2019: [
        "CO_NO", "PARCEL_ID", "DOR_UC", "JV", "TV_NSD", "LND_VAL",
        "NCONST_VAL", "LND_SQFOOT", "TOT_LVG_AR", "NO_BULDNG",
        "NO_RES_UNT", "ACT_YR_BLT"
    ],
    "DEFAULT": [
        "CO_NO", "PARCEL_ID", "DOR_UC", "JV", "TV_NSD", "LND_VAL",
        "NCONST_VAL", "LND_SQFOOT", "TOT_LVG_AREA", "NO_BULDNG",
        "NO_RES_UNTS", "ACT_YR_BLT"
    ]
}

COLS = {
    2019: {
        'CO_NO': 'CO_NO',
        'PARCEL_ID': 'PARCEL_ID',
        'FILE_T': 'FILE_T',
        'ASMNT_YR': 'ASMNT_YR',
        'BAS_STRT': 'BAS_STRT',
        'ATV_STRT': 'ATV_STRT',
        'GRP_NO': 'GRP_NO',
        'DOR_UC': 'DOR_UC',
        'PA_UC': 'PA_UC',
        'SPASS_CD': 'SPASS_CD',
        'JV': 'JV',
        'JV_CHNG': 'JV_CHNG',
        'JV_CHNG_CD': 'JV_CHNG_CD',
        'AV_SD': 'AV_SD',
        'AV_NSD': 'AV_NSD',
        'TV_SD': 'TV_SD',
        'TV_NSD': 'TV_NSD',
        'JV_HMSTD': 'JV_HMSTD',
        'AV_HMSTD': 'AV_HMSTD',
        'JV_NON_HMS': 'JV_NON_HMSTD_RESD',
        'AV_NON_HMS': 'AV_NON_HMSTD_RESD',
        'JV_RESD_NO': 'JV_RESD_NON_RESD',
        'AV_RESD_NO': 'AV_RESD_NON_RESD',
        'JV_CLASS_U': 'JV_CLASS_USE',
        'AV_CLASS_U': 'AV_CLASS_USE',
        'JV_H2O_REC': 'JV_H2O_RECHRGE',
        'AV_H2O_REC': 'AV_H2O_RECHRGE',
        'JV_CONSRV_': 'JV_CONSRV_LND',
        'AV_CONSRV_': 'AV_CONSRV_LND',
        'JV_HIST_CO': 'JV_HIST_COM_PROP',
        'AV_HIST_CO': 'AV_HIST_COM_PROP',
        'JV_HIST_SI': 'JV_HIST_SIGNF',
        'AV_HIST_SI': 'AV_HIST_SIGNF',
        'JV_WRKNG_W': 'JV_WRKNG_WTRFNT',
        'AV_WRKNG_W': 'AV_WRKNG_WTRFNT',
        'NCONST_VAL': 'NCONST_VAL',
        'DEL_VAL': 'DEL_VAL',
        'PAR_SPLT': 'PAR_SPLT',
        'DISTR_CD': 'DISTR_CD',
        'DISTR_YR': 'DISTR_YR',
        'LND_VAL': 'LND_VAL',
        'LND_UNTS_C': 'LND_UNTS_CD',
        'NO_LND_UNT': 'NO_LND_UNTS',
        'LND_SQFOOT': 'LND_SQFOOT',
        'DT_LAST_IN': 'DT_LAST_INSPT',
        'IMP_QUAL': 'IMP_QUAL',
        'CONST_CLAS': 'CONST_CLASS',
        'EFF_YR_BLT': 'EFF_YR_BLT',
        'ACT_YR_BLT': 'ACT_YR_BLT',
        'TOT_LVG_AR': 'TOT_LVG_AREA',
        'NO_BULDNG': 'NO_BULDNG',
        'NO_RES_UNT': 'NO_RES_UNTS',
        'SPEC_FEAT_': 'SPEC_FEAT_VAL',
        'M_PAR_SAL1': 'MULTI_PAR_SAL1',
        'QUAL_CD1': 'QUAL_CD1',
        'VI_CD1': 'VI_CD1',
        'SALE_PRC1': 'SALE_PRC1',
        'SALE_YR1': 'SALE_YR1',
        'SALE_MO1': 'SALE_MO1',
        'OR_BOOK1': 'OR_BOOK1',
        'OR_PAGE1': 'OR_PAGE1',
        'CLERK_NO1': 'CLERK_NO1',
        'S_CHNG_CD1': 'SAL_CHNG_CD1',
        'M_PAR_SAL2': 'MULTI_PAR_SAL2',
        'QUAL_CD2': 'QUAL_CD2',
        'VI_CD2': 'VI_CD2',
        'SALE_PRC2': 'SALE_PRC2',
        'SALE_YR2': 'SALE_YR2',
        'SALE_MO2': 'SALE_MO2',
        'OR_BOOK2': 'OR_BOOK2',
        'OR_PAGE2': 'OR_PAGE2',
        'CLERK_NO2': 'CLERK_NO2',
        'S_CHNG_CD2': 'SAL_CHNG_CD2',
        'OWN_NAME': 'OWN_NAME',
        'OWN_ADDR1': 'OWN_ADDR1',
        'OWN_ADDR2': 'OWN_ADDR2',
        'OWN_CITY': 'OWN_CITY',
        'OWN_STATE': 'OWN_STATE',
        'OWN_ZIPCD': 'OWN_ZIPCD',
        'OWN_STATE_': 'OWN_STATE_DOM',
        'FIDU_NAME': 'FIDU_NAME',
        'FIDU_ADDR1': 'FIDU_ADDR1',
        'FIDU_ADDR2': 'FIDU_ADDR2',
        'FIDU_CITY': 'FIDU_CITY',
        'FIDU_STATE': 'FIDU_STATE',
        'FIDU_ZIPCD': 'FIDU_ZIPPCD',
        'FIDU_CD': 'FIDU_CD',
        'S_LEGAL': 'S_LEGAL',
        'APP_STAT': 'APP_STAT',
        'CO_APP_STA': 'CO_APP_STAT',
        'MKT_AR': 'MKT_AR',
        'NBRHD_CD': 'NBRHD_CD',
        'PUBLIC_LND': 'PUBLIC_LND',
        'TAX_AUTH_C': 'TAX_AUTH_CD',
        'TWN': 'TWN',
        'RNG': 'RNG',
        'SEC': 'SEC',
        'CENSUS_BK': 'CENSUS_BK',
        'PHY_ADDR1': 'PHY_ADDR1',
        'PHY_ADDR2': 'PHY_ADDR2',
        'PHY_CITY': 'PHY_CITY',
        'PHY_ZIPCD': 'PHY_ZIPCD',
        'ASS_TRNSFR': 'ASS_TRNSFR_FG',
        'PREV_HMSTD': 'PREV_HMSTD_OWN',
        'ASS_DIF_TR': 'ASS_DIF_TRNS',
        'CONO_PRV_H': 'CONO_PRV_HM',
        'PARCEL_ID_': 'PARCEL_ID_PRV_HMSTD',
        'YR_VAL_TRN': 'YR_VAL_TRNSF',
        'SEQ_NO': 'SEQ_NO',
        'RS_ID': 'RS_ID',
        'MP_ID': 'MP_ID',
        'STATE_PAR_': 'STATE_PARCEL_ID'
    }
}


# %% FUNCTIONS
def makeParcelGDB(folder, gdb_name="parcels", overwrite=False):
    """
    Makes a gdb (called `parcels` by default), deleting and replacing an
    existing gdb if `overwrite` is True.

    Parameters
    ------------
    folder: Path
    gdb_name: String, default="parcels"
    overwrite: Boolean

    Returns
    --------
    out_gdb: Path
        The path to the newly created output gdb
    """
    if gdb_name[-4:] != ".gdb":
        gdb_name = gdb_name + ".gdb"
    # check if it exists
    out_gdb = PMT.make_path(folder, gdb_name)
    if arcpy.Exists(out_gdb):
        if overwrite:
            print(f"Deleting existing data {out_gdb}")
            arcpy.Delete_management(out_gdb)
        else:
            raise RuntimeError("GDB already exists {}".format(out_gdb))
    # make gdb
    arcpy.CreateFileGDB_management(folder, gdb_name)

    return out_gdb


def cleanParcels(in_fc, in_csv, out_fc, fc_par_field="PARCELNO",
                 csv_par_field="PARCEL_ID", csv_renames={},
                 **kwargs):
    """
    Starting with raw parcel features and raw parcel attributes in a table,
    clean features by repairing invalid geometries, deleting null geometries,
    and dissolving common parcel ID's. Then join attribute data based on
    the parcel ID field, managing column names in the process.

    Parameters
    ------------
    in_fc: Path or feature layer
        A collection of raw parcel features (shapes)
    in_csv: Path
        A table of raw parcel attributes.
    out_fc: Path
        The path to the output feature class that will contain clean parcel 
        geometries with attribute columns joined.
    fc_par_fied: String, default="PARCELNO"
        The field in `in_fc` that identifies each parcel feature.
    csv_par_field: String, default="PARCEL_ID"
        The field in `in_csv` that identifies each parcel feature.
    csv_renames: dict, default={}
        Dictionary for renaming columns from `in_csv`. Keys are current column
        names; values are new column names.
    kwargs:
        Keyword arguments for reading csv data into pandas (dtypes, e.g.)
    """
    # Repair geom and remove null geoms
    print("...repair geometry")
    arcpy.RepairGeometry_management(in_fc, delete_null="DELETE_NULL")
    # Dissolve polygons
    print("...dissolve parcel polygons")
    arcpy.Dissolve_management(in_fc, out_fc, dissolve_field=fc_par_field,
                              statistics_fields="{} COUNT".format(fc_par_field),
                              multi_part="MULTI_PART")
    # Alter field
    arcpy.AlterField_management(out_fc, fc_par_field, "FOLIO", "FOLIO")
    fc_par_field = "FOLIO"
    # Read csv files
    print("...read csv tables")
    par_df = pd.read_csv(in_csv, **kwargs)
    # Rename columns if needed
    print("...renaming columns")
    csv_renames[csv_par_field] = "FOLIO"
    par_df.rename(csv_renames, axis=1, inplace=True)
    csv_par_field = "FOLIO"
    # Add columns to dissolved features
    print("...joining attributes to features")
    print(par_df.columns)
    PMT.extend_table_df(out_fc, fc_par_field, par_df, csv_par_field)


# %% MAIN
if __name__ == "__main__":
    # Create output gdb
    # print(f"making ouptut gdb at {PMT.CLEANED}\\parcels.gdb")

    # Clean parcels and join attributes
    for year in PMT.YEARS[:1]:
        print(year)
        # out_gdb = PMT.makePath(
        #     PMT.DATA, f"IDEAL_PMT_{year}.gdb", "Polygons"
        # )
        out_gdb = makeParcelGDB(PMT.DATA, gdb_name=f"parcels_{year}", overwrite=True)
        in_fc = PMT.make_path(
            PMT.RAW, "Parcels", "Miami_{}.shp".format(year))
        in_csv = PMT.make_path(
            PMT.RAW, "Parcels", "NAL_{}_23Dade_F.csv".format(year))
        out_fc = PMT.make_path(out_gdb, "Parcels")
        renames = COLS.get(year, {})
        usecols = USE_COLS.get(year, USE_COLS["DEFAULT"])
        csv_kwargs = {"dtype": {"PARCEL_ID": str, "CENSUS_BK": str},
                      "usecols": usecols}
        conversion = {"PARCEL_ID": '{:0>12}'.format}
        cleanParcels(in_fc, in_csv, out_fc, fc_par_field="PARCELNO",
                     csv_par_field="PARCEL_ID", csv_renames=renames,
                     **csv_kwargs)
