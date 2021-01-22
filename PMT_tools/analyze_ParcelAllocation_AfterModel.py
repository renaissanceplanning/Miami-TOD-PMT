"""
Created: October 2020
@Author: Brian Froeb

....

"""

# %% Imports and Set Up
import geopandas as gpd
import pandas as pd
import gc



def allocateJobs(parcels_fc, lodes_csv):
    pass

def allocatePop(parcels_fc, lodes_csv):
    pass


# Read in the parcel data - parcel id as string
# Read in LODES data


# shape_path = r'K:/Projects/MiamiDade/PMT/Data/Reference'
# shape_file = r'CensusBG.shp'

# shape = gpd.read_file(f'{shape_path}/{shape_file}')

# shape = shape[shape['geometry'].isnull() == False]

# years = [2014, 2015, 2016, 2017, 2018, 2019]

# for year in years:
#    Parcels_Path =(r'K:/Projects/MiamiDade/PMT/Data/Cleaned/' 
#                    'Parcels/Parcel_Geometry')
#    Parcels_File = f'Miami_{year}.shp'
#    Parcels      = gpd.read_file(f'{Parcels_Path}/{Parcels_File}')
   
#    Attribute_Path =(r'K:/Projects/MiamiDade/PMT/Data/Cleaned/'
#                      'Parcels/Parcel_Attributes')  
#    Attribute_File = f'Miami_{year}_DOR.csv'
#    Attribute      = pd.read_csv(f'{Attribute_Path}/{Attribute_File}')
   
#    Parcels["PARCELNO"] = Parcels["PARCELNO"].astype('str')
#    Attribute["PARCELNO"] = Attribute["PARCELNO"].astype('str').str.zfill(13)
   
#    Parcels = pd.merge(Parcels, Attribute,  
#                       how = "inner", 
#                       on="PARCELNO",
#                       validate = 'one_to_one')
   
#    del Attribute, Attribute_Path, Attribute_File
   
   LODES_Path   = r'K:/Projects/MiamiDade/PMT/Data/Cleaned/Block_Groups'
   LODES_File   = f'Block_Groups_{year}.csv'
   LODES        =  pd.read_csv(f'{LODES_Path}/{LODES_File}')
   
   LODES = LODES.drop(
            columns = ['LND_VAL', 'LND_SQFOOT', 'JV', 'TOT_LVG_AREA',
                       'NO_BULDNG', 'NO_RES_UNTS',        
                       
                       'CNS01_LVG_AREA', 'CNS02_LVG_AREA', 'CNS03_LVG_AREA', 
                       'CNS04_LVG_AREA', 'CNS05_LVG_AREA', 'CNS06_LVG_AREA', 
                       'CNS07_LVG_AREA', 'CNS08_LVG_AREA', 'CNS09_LVG_AREA', 
                       'CNS10_LVG_AREA', 'CNS11_LVG_AREA', 'CNS12_LVG_AREA', 
                       'CNS13_LVG_AREA', 'CNS14_LVG_AREA', 'CNS15_LVG_AREA', 
                       'CNS16_LVG_AREA', 'CNS17_LVG_AREA', 'CNS18_LVG_AREA', 
                       'CNS19_LVG_AREA', 'CNS20_LVG_AREA'])
   
   Clip_Variables = ['CNS01', 'CNS02', 'CNS03', 'CNS04', 'CNS05', 
                     'CNS06', 'CNS07', 'CNS08', 'CNS09', 'CNS10', 
                     'CNS11', 'CNS12', 'CNS13', 'CNS14', 'CNS15', 
                     'CNS16', 'CNS17', 'CNS18', 'CNS19', 'CNS20',
                     
                     'RESIDENTIAL_LVG_AREA', 'Total_Non_Hisp', 
                     'Total_Hispanic',       'White_Hispanic', 
                     'Black_Hispanic',       'Asian_Hispanic', 
                     'Multi_Hispanic',       'Other_Hispanic', 
                     'White_Non_Hisp',       'Black_Non_Hisp', 
                     'Asian_Non_Hisp',       'Multi_Non_Hisp', 
                     'Other_Non_Hisp',       
                     
                     'Drove',    'Carpooled', 'Transit',
                     'NonMotor', 'WFH',       'Other']
   
   for var in Clip_Variables:
       LODES[f'{var}'] = LODES[f'{var}'].clip(lower = 0)
   
   LODES['GEOID10'] = LODES['GEOID10'].astype('str')
   
   LODES        = pd.merge(Shape, LODES,  
                           how = "inner", 
                           on="GEOID10",
                           validate = 'one_to_one')

#    # remove any null geometries
#    Parcels = Parcels[Parcels['geometry'].isnull() == False]
   
   Parcels  = Parcels.to_crs("ESRI:102733")
   LODES    = LODES.to_crs("ESRI:102733")
   
   Parcels['Parcel_Area'] = Parcels['geometry'].area
   LODES['LODES_Area']    = LODES['geometry'].area
   
   # because this is proportional where TOT_LVG is null replace with land area
   LVG_Mask = Parcels['TOT_LVG_AREA'].isnull()

   Parcels.loc[LVG_Mask, 'TOT_LVG_AREA'] = (Parcels['Parcel_Area'][LVG_Mask] * 
                                       0.1573389833137218)
   # %% Parcel Processing
   Enrich_Parcels = gpd.sjoin(Parcels, LODES, how="left", op="within")
   
   del Parcels, LODES
   gc.collect()
    
   Landuse_Mask = {
     'CNS01' : ((Enrich_Parcels['DOR_UC'] >= 50) & 
                  (Enrich_Parcels['DOR_UC'] <= 69)),
     
     'CNS02' :  (Enrich_Parcels['DOR_UC'] == 92),
     
     'CNS03' :  (Enrich_Parcels['DOR_UC'] == 91),
     
     'CNS04' : ((Enrich_Parcels['DOR_UC'] == 17) |
                  (Enrich_Parcels['DOR_UC'] == 19)),
     
     'CNS05' : ((Enrich_Parcels['DOR_UC'] == 41) | 
                  (Enrich_Parcels['DOR_UC'] == 42)),
     
     'CNS06' :  (Enrich_Parcels['DOR_UC'] == 29),
     
     'CNS07' : ((Enrich_Parcels['DOR_UC'] >= 11) & 
                  (Enrich_Parcels['DOR_UC'] <= 16)),
     
     'CNS08' : ((Enrich_Parcels['DOR_UC'] == 48) | 
                  (Enrich_Parcels['DOR_UC'] == 49) | 
                  (Enrich_Parcels['DOR_UC'] == 20)),
     
     'CNS09' : ((Enrich_Parcels['DOR_UC'] == 17) | 
                  (Enrich_Parcels['DOR_UC'] == 18) | 
                  (Enrich_Parcels['DOR_UC'] == 19)),
     
     'CNS10' : ((Enrich_Parcels['DOR_UC'] == 23) | 
                  (Enrich_Parcels['DOR_UC'] == 24)),
     
     'CNS11' : ((Enrich_Parcels['DOR_UC'] == 17) | 
                  (Enrich_Parcels['DOR_UC'] == 18) | 
                  (Enrich_Parcels['DOR_UC'] == 19)),
     
     'CNS12' : ((Enrich_Parcels['DOR_UC'] == 17) | 
                  (Enrich_Parcels['DOR_UC'] == 18) | 
                  (Enrich_Parcels['DOR_UC'] == 19)),
     
     'CNS13' : ((Enrich_Parcels['DOR_UC'] == 17) | 
                  (Enrich_Parcels['DOR_UC'] == 18) | 
                  (Enrich_Parcels['DOR_UC'] == 19)),
     
     'CNS14' : ((Enrich_Parcels['DOR_UC'] == 89)),
     
     'CNS15' : ((Enrich_Parcels['DOR_UC'] == 72) | 
                  (Enrich_Parcels['DOR_UC'] == 83) | 
                  (Enrich_Parcels['DOR_UC'] == 84)),
     
     'CNS16' : ((Enrich_Parcels['DOR_UC'] == 73)  |
                  (Enrich_Parcels['DOR_UC'] == 85)),
     
     'CNS17' : (((Enrich_Parcels['DOR_UC'] >= 30) & 
                   (Enrich_Parcels['DOR_UC'] <= 38)) | 
                  (Enrich_Parcels['DOR_UC'] == 82)),
     
     'CNS18' : ((Enrich_Parcels['DOR_UC'] == 21) | 
                  (Enrich_Parcels['DOR_UC'] == 22) | 
                  (Enrich_Parcels['DOR_UC'] == 33) | 
                  (Enrich_Parcels['DOR_UC'] == 39)),
     
     'CNS19' : ((Enrich_Parcels['DOR_UC'] == 27) | 
                  (Enrich_Parcels['DOR_UC'] == 28)),
     
     'CNS20' : ((Enrich_Parcels['DOR_UC'] >= 86) & 
                  (Enrich_Parcels['DOR_UC'] <= 89)),
     
     'Population' : (((Enrich_Parcels['DOR_UC'] >= 1) & 
                        (Enrich_Parcels['DOR_UC'] <= 9)) |
                       ((Enrich_Parcels['DOR_UC'] >= 100) & 
                          (Enrich_Parcels['DOR_UC'] <= 102)))
   }
   
   LODES_Vars = ['CNS01', 'CNS02', 'CNS03', 'CNS04', 'CNS05', 'CNS06', 'CNS07', 
                 'CNS08', 'CNS09', 'CNS10', 'CNS11', 'CNS12', 'CNS13', 'CNS14', 
                 'CNS15', 'CNS16', 'CNS17', 'CNS18', 'CNS19', 'CNS20']
   
   TOT_BG = Enrich_Parcels.groupby(['GEOID10'])['GEOID10'].agg(['count'])
   TOT_BG.rename(columns = {'count' : 'NumParBG'}, inplace = True) 
   TOT_BG = TOT_BG.reset_index()
   
   for var in LODES_Vars:
       Area = Enrich_Parcels[Landuse_Mask[var]].groupby(
                ['GEOID10'])['TOT_LVG_AREA'].agg(['sum'])
       Area.rename(columns = {'sum' : f'{var}_Area'}, inplace = True) 
       TOT_BG = pd.merge(TOT_BG, Area, how='left', on = 'GEOID10')
       
   Area = (Enrich_Parcels[Landuse_Mask['Population']]
                         .groupby(['GEOID10'])['TOT_LVG_AREA']
                         .agg(['sum']))
   
   Area.rename(columns = {'sum' : 'Population_Area'}, inplace = True) 
   
   TOT_BG = pd.merge(TOT_BG, Area, 
                     how='left', 
                     on = 'GEOID10')
   
   TOT_BG = TOT_BG.fillna(0)
  
   Enrich_Parcels = pd.merge(Enrich_Parcels, TOT_BG, 
                             how='left', 
                             on = 'GEOID10')

   for var in LODES_Vars:
     Enrich_Parcels.loc[Landuse_Mask[var], f'{var}_Par_Prop'] = (
         Enrich_Parcels['TOT_LVG_AREA'][Landuse_Mask[var]] / 
         Enrich_Parcels[f'{var}_Area'][Landuse_Mask[var]])  
     Enrich_Parcels[f'{var}_PAR'] = 0
     Enrich_Parcels.loc[Landuse_Mask[var], f'{var}_PAR'] = (
         Enrich_Parcels[f'{var}_Par_Prop'][Landuse_Mask[var]] *  
         Enrich_Parcels[var][Landuse_Mask[var]]) 
   
   Race_Vars = ['Total_Non_Hisp', 'Total_Hispanic', 'White_Hispanic',
                'Black_Hispanic', 'Asian_Hispanic', 'Multi_Hispanic', 
                'Other_Hispanic', 'White_Non_Hisp', 'Black_Non_Hisp', 
                'Asian_Non_Hisp', 'Multi_Non_Hisp', 'Other_Non_Hisp']
   
   Enrich_Parcels.loc[Landuse_Mask['Population'], 'Pop_Par_Prop'] = (
       Enrich_Parcels['TOT_LVG_AREA'][Landuse_Mask['Population']] / 
       Enrich_Parcels['Population_Area'][Landuse_Mask['Population']])
   
   for var in Race_Vars:
     Enrich_Parcels[f'{var}_PAR'] = 0
     Enrich_Parcels.loc[Landuse_Mask['Population'], f'{var}_PAR'] = (
        Enrich_Parcels['Pop_Par_Prop'][Landuse_Mask['Population']] *  
        Enrich_Parcels[var][Landuse_Mask['Population']]) 
   
   Enrich_Parcels['Total_Population'] = (Enrich_Parcels['Total_Non_Hisp_PAR'] + 
                                         Enrich_Parcels['Total_Hispanic_PAR'])
   
   transit_var = ['Drove', 'Carpooled', 'Transit',
                  'NonMotor', 'WFH', 'Other']
   
   for var in transit_var:
     Enrich_Parcels[f'{var}_PAR'] = 0
     Enrich_Parcels.loc[Landuse_Mask['Population'], f'{var}_PAR'] = (
         Enrich_Parcels[var][Landuse_Mask['Population']] *
         Enrich_Parcels['Total_Population'][Landuse_Mask['Population']])
     
   Enrich_Parcels['Total_Employment'] = (Enrich_Parcels['CNS01_PAR'] + 
                                           Enrich_Parcels['CNS02_PAR'] + 
                                           Enrich_Parcels['CNS03_PAR'] + 
                                           Enrich_Parcels['CNS04_PAR'] + 
                                           Enrich_Parcels['CNS05_PAR'] + 
                                           Enrich_Parcels['CNS06_PAR'] + 
                                           Enrich_Parcels['CNS07_PAR'] + 
                                           Enrich_Parcels['CNS08_PAR'] + 
                                           Enrich_Parcels['CNS09_PAR'] + 
                                           Enrich_Parcels['CNS10_PAR'] + 
                                           Enrich_Parcels['CNS11_PAR'] + 
                                           Enrich_Parcels['CNS12_PAR'] + 
                                           Enrich_Parcels['CNS13_PAR'] + 
                                           Enrich_Parcels['CNS14_PAR'] + 
                                           Enrich_Parcels['CNS15_PAR'] + 
                                           Enrich_Parcels['CNS16_PAR'] + 
                                           Enrich_Parcels['CNS17_PAR'] + 
                                           Enrich_Parcels['CNS18_PAR'] + 
                                           Enrich_Parcels['CNS19_PAR'] + 
                                           Enrich_Parcels['CNS20_PAR'])
   
   Cols_to_Keep = ['PARCELNO', 'PARCEL_ID', 'geometry', 'GEOID10',   
                   
                   'Drove', 'Carpooled', 'Transit', 'NonMotor', 'WFH', 'Other', 
                   
                   'CNS01_PAR', 'CNS02_PAR', 'CNS03_PAR', 'CNS04_PAR',
                   'CNS05_PAR', 'CNS06_PAR', 'CNS07_PAR', 'CNS08_PAR', 
                   'CNS09_PAR', 'CNS10_PAR', 'CNS11_PAR', 'CNS12_PAR', 
                   'CNS13_PAR', 'CNS14_PAR', 'CNS15_PAR', 'CNS16_PAR', 
                   'CNS17_PAR', 'CNS18_PAR', 'CNS19_PAR', 'CNS20_PAR', 
                   
                   'Total_Non_Hisp_PAR', 'Total_Hispanic_PAR', 
                   'White_Hispanic_PAR', 'Black_Hispanic_PAR', 
                   'Asian_Hispanic_PAR', 'Multi_Hispanic_PAR', 
                   'Other_Hispanic_PAR', 'White_Non_Hisp_PAR',
                   'Black_Non_Hisp_PAR', 'Asian_Non_Hisp_PAR', 
                   'Multi_Non_Hisp_PAR', 'Other_Non_Hisp_PAR', 
                   
                   'Total_Employment', 'Total_Population']

  
   Enrich_Parcels = Enrich_Parcels[Cols_to_Keep]
   
   Attributes_Enriched = pd.DataFrame(Enrich_Parcels.drop(columns='geometry'))
    
   Out_Path = (r'K:/Projects/MiamiDade/PMT/Data/Cleaned/'
                     'Parcels/Parcel_Attributes')  
   Out_File = f'Miami_{year}_ENR.csv'
   
   Attributes_Enriched.to_csv(f'{Out_Path}/{Out_File}',
                              index = False)
   del Enrich_Parcels, Attributes_Enriched, LVG_Mask, TOT_BG
   gc.collect() 
