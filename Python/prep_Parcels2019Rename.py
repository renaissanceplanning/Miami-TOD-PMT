import pandas as pd

year = 2019

Parcels_Path = r'K:/Projects/MiamiDade/PMT/Data/Cleaned/Parcels/Parcel_Attributes'
Parcels_File = f'Miami_{year}_DOR.csv'

Parcels_2019 = pd.read_csv(f'{Parcels_Path}/{Parcels_File}')

Parcels_2019.rename(columns = {
    'TOT_LVG_AR' : 'TOT_LVG_AREA', 
    'NO_RES_UNT' : 'NO_RES_UNTS',
    'STATE_PAR_' : 'STATE_PARCEL_ID', 
    }, inplace = True)
 
Parcels_2019.to_csv(f'{Parcels_Path}/{Parcels_File}', index = False) 
