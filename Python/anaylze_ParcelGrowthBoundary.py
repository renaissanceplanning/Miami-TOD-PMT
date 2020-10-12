"""

"""

import arcpy
#import geopandas as gpd
import pandas as pd

#TODO: use os, path to always pull from "cleaned" parcel shapes and growth boundary

def tagInBoundary(boundary_shape, parcel_shape, tag_field="IN_UGB",
                  overlap_type="INTERSECT"):
   """
   
   """
   try:
      # Clean parcels to output gdb

      # Make feature layer
      boundary_layer = arcpy.MakeFeatureLayer_management(
         boundary_shape, "__boundary__"
      )
      parcel_layer = arcpy.MakeFeatureLayer_management(
         parcel_shape, "__parcels__"
      )
      # Select parcels in boundary
      arcpy.SelectLayerByLocation_management(
         parcel_Layer, overlap_type, boundary_layer 
      )
      #



boundary = gpd.read_file(r'K:\Projects\MiamiDade\PMT\Data\Reference\Growth_Boundary.shp')
boundary['In_Boundary'] = True

years = [2014, 2015, 2016, 2017, 2018, 2019]

for year in years:
  
   parcels_path = r'K:/Projects/MiamiDade/PMT/Data/Cleaned/Parcels/Parcel_Geometry'
   farcels_file = f'Miami_{year}.shp'
   
   Attribute_Path = r'K:/Projects/MiamiDade/PMT/Data/Cleaned/Parcels/Parcel_Attributes'
   Attribute_File = f'Miami_{year}_DOR.csv'
   
   Parcels  = gpd.read_file(f'{Parcels_Path}/{Parcels_File}')
   Attributes = pd.read_csv(f'{Attribute_Path}/{Attribute_File}')
   
   Parcels["PARCELNO"] = Parcels["PARCELNO"].astype('str')
   Attributes["PARCEL_ID"] = Attributes["PARCEL_ID"].astype('str').str.zfill(13)
   
   Parcels = pd.merge(Parcels, Attributes,  
                      how = "left", 
                      left_on="PARCELNO",
                      right_on="PARCEL_ID",
                      validate = 'one_to_one')
   
   my_crs = Parcels.crs
   Boundary = Boundary.to_crs(my_crs)
   Parcels_Enriched = gpd.sjoin(Parcels, Boundary, how = 'left', op = 'within')
   Parcels_Enriched["In_Boundary"] = Parcels_Enriched["In_Boundary"].fillna(value = False)
   
   Attributes_Enriched = pd.DataFrame(Parcels_Enriched.drop(columns='geometry'))
   Attributes_Enriched = Attributes_Enriched.drop(columns = ['Id', 'index_right'])
   
   out_path = r'K:/Projects/MiamiDade/PMT/Data/Cleaned/Parcels/Parcel_Attributes'
   Attributes_Enriched.to_csv(f'{out_path}/{Attribute_File}', index = False)



