# -*- coding: utf-8 -*-
"""
Created: October 2020

@Author: Brian Froeb, Aaron Weinstock
"""

# %% IMPORTS
import arcpy
import pandas as pd
import numpy as np
import os

# %% FUNCTIONS
def analyze_blockgroup_allocation(parcels_path,
                                  bg_for_alloc_path,
                                  save_gdb_location,
                                  parcels_id_field="PARCELNO",
                                  parcels_land_use_field="DOR_UC",
                                  parcels_living_area_field="TOT_LVG_AREA"):
    
    """
    Allocated block group data to parcels using relative abundances of
    parcel building square footage
    
    Parameters 
    --------------
    parcels_path: Path 
        path to shape of parcel polygons, containing at a minimum a unique ID
        field, land use field, and total living area field (Florida DOR)
    bg_for_alloc_path: Path
        path to shape of block group polygons, attributed with job and
        population data for allocation
    save_gdb_location: Path
        path to location in which the allocated results will be saved. If 
        `bg_for_alloc_path` is in a .gdb, it makes sense to set `save_gdb` to
        that .gdb (or a feature class within that .gdb)
    parcels_id_field: str
        unique ID field in the parcels shape
        Default is "PARCELNO" for Florida parcels
    parcels_land_use_field: str
        land use code field in the parcels shape
        Default is "DOR_UC" for Florida parcels
    parcels_living_area_field: str
        building square footage field in the parcels shape
        Default is "TOT_LVG_AREA" for Florida parcels
        
    
    Returns
    ---------------
    path of location at which the allocation results are saved. 
    Saving will be completed as part of the function. The allocation estimates
    will be joined to the original parcels shape
    """
    
    # ------------------------------------------------------------------------
    
    print("")
    print("Spatial processing for allocation")
    
    # First, we have to set up the process with a few input variables
    print("-- setting up inputs for spatial processing...")
    # 1. write location for intermediates
    pcentroids_path = "in_memory\\centroids"
    pbgjoin_path = "in_memory\\join"
    # 2. field names we want to keep from parcels
    parcel_fields = ["ProcessID",
                     parcels_id_field,
                     parcels_land_use_field,
                     parcels_living_area_field,
                     "SHAPE@X",
                     "SHAPE@Y"]
    # 3. parcel spatial reference (for explicit definition of spatial
    # reference in arcpy operations
    sr = arcpy.Describe(parcels_path).spatialReference
    
    # Recognizing that the parcel ID field provided may not be unique (due to
    # singlepart vs multipart issues), we should add a ~definitively unique~ 
    # ID field of our own. We edit the parcels in place to do this, so we'll
    # delete it once it has served its purposed so we can maintain the
    # integrity of the original data.
    print("-- adding a definitively unique process ID to the parcel polygons...")
    # 1. create the field
    arcpy.AddField_management(in_table = parcels_path,
                              field_name = "ProcessID",
                              field_type = "LONG",
                              field_is_required = "NON_REQUIRED")
    # 2. define sequential numbers function
    # Thanks to: https://support.esri.com/en/technical-article/000011137
    # indent error when run as function?
    codeblock = """
    val = 0 
    def processID(): 
        global val 
        start = 1 
        if (val == 0):  
            val = start
        else:  
            val += 1  
        return val
     """
    # 3. calculate the field
    arcpy.CalculateField_management(in_table = parcels_path,
                                    field = "ProcessID",
                                    expression = "processID()",
                                    expression_type = "PYTHON3",
                                    code_block = codeblock)
       
    # Now, we extract parcel centroids to numpy array, then convert array
    # to feature class (saved to 'pcentroids')
    print("-- converting parcel polygons to parcel centroid points...")
    parcels_array = arcpy.da.FeatureClassToNumPyArray(in_table = parcels_path,
                                                      field_names = parcel_fields,
                                                      spatial_reference = sr,
                                                      null_value = -1)
    arcpy.da.NumPyArrayToFeatureClass(in_array = parcels_array,
                                      out_table = pcentroids_path,
                                      shape_fields = ["SHAPE@X", "SHAPE@Y"],
                                      spatial_reference = sr)
    
    # Then, we intersect parcels with block group for allocation data
    # to enrich the parcels with allocation info
    print("-- enriching parcels with block group for allocation data...")
    arcpy.Intersect_analysis(in_features = [pcentroids_path, bg_for_alloc_path],
                             out_feature_class = pbgjoin_path)
    
    # Finally, we load our enriched data
    print("-- loading enriched parcel data...")
    keep_vars = ["ProcessID",
                 parcels_id_field, 
                 parcels_land_use_field, 
                 parcels_living_area_field,
                 'GEOID10',
                 'CNS01', 'CNS02', 'CNS03', 'CNS04', 'CNS05', 
                 'CNS06', 'CNS07', 'CNS08', 'CNS09', 'CNS10', 
                 'CNS11', 'CNS12', 'CNS13', 'CNS14', 'CNS15', 
                 'CNS16', 'CNS17', 'CNS18', 'CNS19', 'CNS20',        
                 'Total_Hispanic', 'White_Hispanic', 'Black_Hispanic', 
                 'Asian_Hispanic', 'Multi_Hispanic', 'Other_Hispanic',
                 'Total_Non_Hisp', 'White_Non_Hisp', 'Black_Non_Hisp', 
                 'Asian_Non_Hisp', 'Multi_Non_Hisp', 'Other_Non_Hisp',       
                 'Drove', 'Carpool', 'Transit', 
                 'NonMotor', 'Work_From_Home', 'AllOther']
    df = arcpy.da.FeatureClassToNumPyArray(in_table = pbgjoin_path,
                                           field_names = keep_vars,
                                           spatial_reference = sr,
                                           null_value = -1)
    df = pd.DataFrame(df)
    
    # Now we have our data for allocation prepped -- great!
    # Ultimately, we'll want to merge our allocation results back to the
    # parcels, which we'll do on the process ID field. So, we'll initialize
    # the process by creating a new parcels feature class containig only
    # ProcessID and the provided parcel ID field.
    print("-- initializing a mew feature class for allocation results...")
    # Thanks to: https://gis.stackexchange.com/questions/229187/copying-only-certain-fields-columns-from-shapefile-into-new-shapefile-using-mode
    fkeep = ["ProcessID", parcels_id_field]
    fmap = arcpy.FieldMappings()
    fmap.addTable(parcels_path)
    fields = {f.name: f for f in arcpy.ListFields(parcels_path)}
    for fname, fld in fields.items():
        if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
            if fname not in fkeep:
                fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
    arcpy.conversion.FeatureClassToFeatureClass(in_features = parcels_path, 
                                                out_path = save_gdb_location,
                                                out_name = "socioeconomic_and_demographic2",
                                                field_mapping = fmap)
    
    # With spatial processing complete, we can delete our intermediates. This
    # includes two files, as well as the "ProcessID" field put in the
    # original parcels data    
    print("-- deleting intermediates...")
    arcpy.Delete_management(pcentroids_path)
    arcpy.Delete_management(pbgjoin_path)
    arcpy.DeleteField_management(in_table = parcels_path,
                                 drop_field = "ProcessID")
    
    # ------------------------------------------------------------------------
    
    print("")
    print("Allocation")
    
    # First, we have to format the block group for allocation data
    # for processing the allocation
    print("-- formatting block group for allocation data...")
    # 1. if a column to be allocated has a value < 0, push up to 0. We can
    # also apply this to Total Living Area, because we set all NAs to -1,
    # and NA values are parcels with 0 living area
    to_clip = ['CNS01', 'CNS02', 'CNS03', 'CNS04', 'CNS05', 
                'CNS06', 'CNS07', 'CNS08', 'CNS09', 'CNS10', 
                'CNS11', 'CNS12', 'CNS13', 'CNS14', 'CNS15', 
                'CNS16', 'CNS17', 'CNS18', 'CNS19', 'CNS20',        
                'Total_Hispanic', 'White_Hispanic', 'Black_Hispanic', 
                'Asian_Hispanic', 'Multi_Hispanic', 'Other_Hispanic',
                'Total_Non_Hisp', 'White_Non_Hisp', 'Black_Non_Hisp', 
                'Asian_Non_Hisp', 'Multi_Non_Hisp', 'Other_Non_Hisp',       
                'Drove', 'Carpool', 'Transit', 
                'NonMotor', 'Work_From_Home', 'AllOther',
                parcels_living_area_field]
    for var in to_clip:
        df[f'{var}'] = df[f'{var}'].clip(lower = 0)
    # 2. replace -1 in DOR_UC with NA
    pluf = parcels_land_use_field
    elu = df[pluf] == -1
    df.loc[elu, pluf] = None
    
    # Step 1 in allocation is totaling the living area by activity in
    # each block group. To do this, we define in advance which activities
    # can go to which land uses
    
    # First, we set up this process by matching activities to land uses
    print("-- setting up activity-land use matches...")
    lu_mask = {
        'CNS01': ((df[pluf] >= 50) & (df[pluf] <= 69)),
        'CNS02': (df[pluf] == 92),
        'CNS03': (df[pluf] == 91),
        'CNS04': ((df[pluf] == 17) | (df[pluf] == 19)),
        'CNS05': ((df[pluf] == 41) | (df[pluf] == 42)),
        'CNS06': (df[pluf] == 29),
        'CNS07': ((df[pluf] >= 11) & (df[pluf] <= 16)),
        'CNS08': ((df[pluf] == 48) | (df[pluf] == 49) | (df[pluf] == 20)),
        'CNS09': ((df[pluf] == 17) | (df[pluf] == 18) | (df[pluf] == 19)),
        'CNS10': ((df[pluf] == 23) | (df[pluf] == 24)),
        'CNS11': ((df[pluf] == 17) | (df[pluf] == 18) | (df[pluf] == 19)),
        'CNS12': ((df[pluf] == 17) | (df[pluf] == 18) | (df[pluf] == 19)),
        'CNS13': ((df[pluf] == 17) | (df[pluf] == 18) | (df[pluf] == 19)),
        'CNS14': (df[pluf] == 89),
        'CNS15': ((df[pluf] == 72) | (df[pluf] == 83) | (df[pluf] == 84)),
        'CNS16': ((df[pluf] == 73) | (df[pluf] == 85)),
        'CNS17': (((df[pluf] >= 30) & (df[pluf] <= 38)) | (df[pluf] == 82)),
        'CNS18': ((df[pluf] == 21) | (df[pluf] == 22) | (df[pluf] == 33) | (df[pluf] == 39)),
        'CNS19': ((df[pluf] == 27) | (df[pluf] == 28)),
        'CNS20': ((df[pluf] >= 86) & (df[pluf] <= 89)),
        'Population' : (((df[pluf] >= 1) & (df[pluf] <= 9)) | ((df[pluf] >= 100) & (df[pluf] <= 102)))
    }
    
    # Next, we'll total parcels by block group (this is just a simple operation
    # to give our living area totals something to join to)
    print("-- initializing living area sums...")
    tot_bg = df.groupby(['GEOID10'])['GEOID10'].agg(['count'])
    tot_bg.rename(columns = {'count' : 'NumParBG'}, 
                  inplace = True) 
    tot_bg = tot_bg.reset_index()
    
    # Now we can begin totaling living area. We'll start with jobs
    print("-- totaling living area by job type...")
    # 1. Define our jobs variables
    lodes_vars = ['CNS01', 'CNS02', 'CNS03', 'CNS04', 'CNS05', 'CNS06', 
                  'CNS07', 'CNS08', 'CNS09', 'CNS10', 'CNS11', 'CNS12', 
                  'CNS13', 'CNS14', 'CNS15', 'CNS16', 'CNS17', 'CNS18', 
                  'CNS19', 'CNS20']
    # 2. get count of total living area (w.r.t. land use mask) for each
    # job type
    plaf = parcels_living_area_field
    for var in lodes_vars:
        area = df[lu_mask[var]].groupby(['GEOID10'])[plaf].agg(['sum'])
        area.rename(columns = {'sum' : f'{var}_Area'}, 
                    inplace = True) 
        tot_bg = pd.merge(tot_bg, area,
                          how = 'left', 
                          on = 'GEOID10')
    
    # Repeat the above with population
    print("-- totaling living area for population...")
    area = df[lu_mask['Population']].groupby(['GEOID10'])[plaf].agg(['sum'])
    area.rename(columns = {'sum' : 'Population_Area'}, 
                inplace = True) 
    tot_bg = pd.merge(tot_bg, area, 
                      how = 'left', 
                      on = 'GEOID10')
    
    # Now, we format and re-merge with our original parcel data
    print("-- merging living area totals with parcel-level data...")
    # 1. fill table with NAs
    tot_bg = tot_bg.fillna(0)
    # 2. merge back to original data
    df = pd.merge(df, tot_bg,
                  how = 'left', 
                  on = 'GEOID10')
 
    # Step 2 in allocation is taking parcel-level proportions of living area
    # relative to the block group total, and calculating parcel-level
    # estimates of activities by multiplying the block group activity total
    # by the parcel-level proportions
    
    # First up, we'll allocate jobs
    print("-- allocating jobs...")
    # 1. for each job variable, calculate the proportion, then allocate
    for var in lodes_vars:
        df.loc[lu_mask[var], f'{var}_Par_Prop'] = (
            df['TOT_LVG_AREA'][lu_mask[var]] / df[f'{var}_Area'][lu_mask[var]]
        )  
        df[f'{var}_PAR'] = 0
        df.loc[lu_mask[var], f'{var}_PAR'] = (
            df[f'{var}_Par_Prop'][lu_mask[var]] * df[var][lu_mask[var]]
        ) 
    # 2. then total employment is the sum of all these
    df['Total_Employment'] = (
        df['CNS01_PAR'] + df['CNS02_PAR'] + df['CNS03_PAR'] + 
        df['CNS04_PAR'] + df['CNS05_PAR'] + df['CNS06_PAR'] + 
        df['CNS07_PAR'] + df['CNS08_PAR'] + df['CNS09_PAR'] +
        df['CNS10_PAR'] + df['CNS11_PAR'] + df['CNS12_PAR'] + 
        df['CNS13_PAR'] + df['CNS14_PAR'] + df['CNS15_PAR'] +
        df['CNS16_PAR'] + df['CNS17_PAR'] + df['CNS18_PAR'] + 
        df['CNS19_PAR'] + df['CNS20_PAR']
    )
    
    # Now, we'll allocate population
    print("-- allocating population...")
    # 1. calculate the proportion for population
    df.loc[lu_mask['Population'], 'Pop_Par_Prop'] = (
        df['TOT_LVG_AREA'][lu_mask['Population']] / df['Population_Area'][lu_mask['Population']]
    )
    # 2. we use the pop proportion for a lot of variables. define them
    race_vars = ['Total_Hispanic', 'White_Hispanic', 'Black_Hispanic', 
                 'Asian_Hispanic', 'Multi_Hispanic', 'Other_Hispanic', 
                 'Total_Non_Hisp', 'White_Non_Hisp', 'Black_Non_Hisp', 
                 'Asian_Non_Hisp', 'Multi_Non_Hisp', 'Other_Non_Hisp']
    # 3. allocate the variables for which we use the pop proportion
    for var in race_vars:
        df[f'{var}_PAR'] = 0
        df.loc[lu_mask['Population'], f'{var}_PAR'] = (
            df['Pop_Par_Prop'][lu_mask['Population']] * df[var][lu_mask['Population']]
        )
    # 4. manually calculate total pop
    df['Total_Population'] = (
        df['Total_Non_Hisp_PAR'] + df['Total_Hispanic_PAR']
    )
    
    # Finally, we'll allocate transportation usage
    print("-- allocating commutes...")
    # 1. transit is going to be based on total pop. define transit variables
    transit_vars = ['Drove', 'Carpool', 'Transit', 
                    'NonMotor', 'Work_From_Home', 'AllOther']
    # 2. allocate transit variables
    for var in transit_vars:
        df[f'{var}_PAR'] = 0
        df.loc[lu_mask['Population'], f'{var}_PAR'] = (
            df[var][lu_mask['Population']] * df['Total_Population'][lu_mask['Population']]
        )
      
    # ------------------------------------------------------------------------
    
    print("")
    print("Writing results")
    
    # We don't need all the columns we have, so first we define the columns
    # we want and select them from our data. Note that we don't need to
    # maintain the parcels_id_field here, because our save file has been
    # initialized with this already!
    print("-- selecting columns of interest...")
    to_keep = ["ProcessID",
               'GEOID10',   
               'Total_Employment', 
               'CNS01_PAR', 'CNS02_PAR', 'CNS03_PAR', 'CNS04_PAR',
               'CNS05_PAR', 'CNS06_PAR', 'CNS07_PAR', 'CNS08_PAR', 
               'CNS09_PAR', 'CNS10_PAR', 'CNS11_PAR', 'CNS12_PAR', 
               'CNS13_PAR', 'CNS14_PAR', 'CNS15_PAR', 'CNS16_PAR', 
               'CNS17_PAR', 'CNS18_PAR', 'CNS19_PAR', 'CNS20_PAR', 
               'Total_Population',
               'Total_Hispanic_PAR', 
               'White_Hispanic_PAR', 'Black_Hispanic_PAR', 'Asian_Hispanic_PAR', 
               'Multi_Hispanic_PAR', 'Other_Hispanic_PAR', 
               'Total_Non_Hisp_PAR', 
               'White_Non_Hisp_PAR', 'Black_Non_Hisp_PAR', 'Asian_Non_Hisp_PAR', 
               'Multi_Non_Hisp_PAR', 'Other_Non_Hisp_PAR', 
               'Drove_PAR', 'Carpool_PAR', 'Transit_PAR', 
               'NonMotor_PAR', 'Work_From_Home_PAR', 'AllOther_PAR']
    df = df[to_keep]
    
    # For saving, we join the allocation estimates back to the ID shape we
    # initialized during spatial processing
    print("-- merging allocations back to parcel shape (please be patient, this could take a while)...")
    # 1. convert pandas df to numpy array for use with arcpy ExtendTable
    df_et = np.rec.fromrecords(recList = df.values, 
                               names = df.dtypes.index.tolist())
    df_et = np.array(df_et)
    # 2. use ExtendTable to modify the parcels data
    allocation_path = os.path.join(save_gdb_location, 
                                   "socioeconomic_and_demographic2")
    arcpy.da.ExtendTable(in_table = allocation_path,
                         table_match_field = "ProcessID",
                         in_array = df_et,
                         array_match_field = "ProcessID")
    # 3. delete the ProcessID field from the allocation feature class
    arcpy.DeleteField_management(in_table = allocation_path,
                                 drop_field = "ProcessID")

    # Note that instead of using this "ProcessID" method, we could groupby-sum
    # our df on PARCELNO and join back to the parcels
    
    # ------------------------------------------------------------------------
    
    print("")
    print("Done!")
    print("Allocation saved to: " + allocation_path)
    print("")
    return(allocation_path)
    


# %% MAIN
if __name__ == "__main__":
    parcels_path = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Parcels.gdb/Miami_2019"
    bg_for_alloc_path = "K:/Projects/MiamiDade/PMT/PMT_2019.gdb/BlockGroups/blockgroup_for_alloc2"
    save_gdb_location = "K:/Projects/MiamiDade/PMT/PMT_2019.gdb/Parcels"
    parcels_id_field = "PARCELNO"
    parcels_land_use_field = "DOR_UC"
    parcels_living_area_field = "TOT_LVG_AREA"
    analyze_blockgroup_allocation(parcels_path = parcels_path,
                                  bg_for_alloc_path = bg_for_alloc_path,
                                  save_gdb_location = save_gdb_location,
                                  parcels_id_field = "PARCELNO",
                                  parcels_land_use_field = "DOR_UC",
                                  parcels_living_area_field = "TOT_LVG_AREA")   
    