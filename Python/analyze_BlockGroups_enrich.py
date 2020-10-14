"""
Created: October 2020
@Author: Brian Froeb

...
...
"""

# %% IMPORTS
import numpy as np
import pandas as pd
import geopandas as gpd
import censusdata as census
import re
from collections.abc import Iterable
from six import string_types


#%% GLOBALS

# Comparison methods:
#   - __eq__() = equals [==]
#   - __ne__() = not equal to [!=]
#   - __lt__() = less than [<]
#   - __le__() = less than or equal to [<=]
#   - __gt__() = greater than [>]
#   - __ge__() = greater than or equal to [>=]

class Comp():
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


class And():
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


class Or():
    """
    """
    def __init__(self, vector, criteria):
        self.vector = vector
        if isinstance(criteria, Iterable):
            self.criteria = criteria #TODO: validate criteria
        else:
            self.criteria = [criteria]
    
    def eval(self):
        return (
            np.logical_or.reduce(
                [c.eval(self.vector) for c in self.criteria]
            )
        )

LODES_CRIT = {
    "CNS_01": And([Comp(">=", 50), Comp("<=", 69)]),
    "CNS_02": Comp("==", 92),
    "CNS_03": Comp("==", 91),
    "CNS_04": [Comp("==", 17),Comp("==", 19)],
    "CNS_05": [Comp("==", 41), Comp("==", 42)],
    "CNS_06": Comp("==", 29),
    "CNS_07": And([Comp(">=", 11), Comp("<=", 16)]),
    "CNS_08": [Comp("==", 20), Comp("==", 48), Comp("==", 49)],
    "CNS_09": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_10": [Comp("==", 23), Comp("==", 24)],
    "CNS_11": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_12": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_13": [Comp("==", 17), Comp("==", 18), Comp("==", 19)],
    "CNS_14": Comp("==", 89),
    "CNS_15": [Comp("==", 72), Comp("==", 83), Comp("==", 84)],
    "CNS_16": [Comp("==", 73), Comp("==", 85)],
    "CNS_17": [And([Comp(">=", 30), Comp("<=", 38)]), Comp("==", 82)],
    "CNS_18": [Comp("==", 21), Comp("==", 22), Comp("==", 33), Comp("==", 39)],
    "CNS_19": [Comp("==", 27), Comp("==", 28)],
    "CNS_20": And([Comp(">=", 86), Comp("<=", 89)]),
    "RES": [
        And([Comp(">=", 1), Comp("<=", 9)]),
        And([Comp(">=", 100), Comp("<=", 102)])
    ]
}

# Landuse_Mask = {
#         "CNS01" :   ((Parcels['DOR_UC'] >= 50) & 
#                     (Parcels['DOR_UC'] <= 69)),
         
#          'CNS02' :  (Parcels['DOR_UC'] == 92),
         
#          'CNS03' :  (Parcels['DOR_UC'] == 91),
         
#          'CNS04' : ((Parcels['DOR_UC'] == 17) |
#                       (Parcels['DOR_UC'] == 19)),
         
#          'CNS05' : ((Parcels['DOR_UC'] == 41) | 
#                       (Parcels['DOR_UC'] == 42)),
         
#          'CNS06' :  (Parcels['DOR_UC'] == 29),
         
#          'CNS07' : ((Parcels['DOR_UC'] >= 11) & 
#                       (Parcels['DOR_UC'] <= 16)),
         
#          'CNS08' : ((Parcels['DOR_UC'] == 48) | 
#                       (Parcels['DOR_UC'] == 49) | 
#                       (Parcels['DOR_UC'] == 20)),
         
#          'CNS09' : ((Parcels['DOR_UC'] == 17) | 
#                       (Parcels['DOR_UC'] == 18) | 
#                       (Parcels['DOR_UC'] == 19)),
         
#          'CNS10' : ((Parcels['DOR_UC'] == 23) | 
#                       (Parcels['DOR_UC'] == 24)),
         
#          'CNS11' : ((Parcels['DOR_UC'] == 17) | 
#                       (Parcels['DOR_UC'] == 18) | 
#                       (Parcels['DOR_UC'] == 19)),
         
#          'CNS12' : ((Parcels['DOR_UC'] == 17) | 
#                       (Parcels['DOR_UC'] == 18) | 
#                       (Parcels['DOR_UC'] == 19)),
         
#          'CNS13' : ((Parcels['DOR_UC'] == 17) | 
#                       (Parcels['DOR_UC'] == 18) | 
#                       (Parcels['DOR_UC'] == 19)),
         
#          'CNS14' : ((Parcels['DOR_UC'] == 89)),
         
#          'CNS15' : ((Parcels['DOR_UC'] == 72) | 
#                       (Parcels['DOR_UC'] == 83) | 
#                       (Parcels['DOR_UC'] == 84)),
         
#          'CNS16' : ((Parcels['DOR_UC'] == 73)  |
#                       (Parcels['DOR_UC'] == 85)),
         
#          'CNS17' : (((Parcels['DOR_UC'] >= 30) & 
#                        (Parcels['DOR_UC'] <= 38)) | 
#                       (Parcels['DOR_UC'] == 82)),
         
#          'CNS18' : ((Parcels['DOR_UC'] == 21) | 
#                       (Parcels['DOR_UC'] == 22) | 
#                       (Parcels['DOR_UC'] == 33) | 
#                       (Parcels['DOR_UC'] == 39)),
         
#          'CNS19' : ((Parcels['DOR_UC'] == 27) | 
#                       (Parcels['DOR_UC'] == 28)),
         
#          'CNS20' : ((Parcels['DOR_UC'] >= 86) & 
#                       (Parcels['DOR_UC'] <= 89)),
         
#          'Population' : (((Parcels['DOR_UC'] >= 1) & 
#                             (Parcels['DOR_UC'] <= 9)) |
#                            ((Parcels['DOR_UC'] >= 100) & 
#                               (Parcels['DOR_UC'] <= 102)))
#        }



#%% FUNCTIONS

def enrichBlockGroups(bg_fc, parcels_fc, out_fc, bg_id_field="GEOID10",
                      par_lu_field="DOR_UC", par_bld_area="TOT_LVG_AREA",
                      sum_crit={},
                      par_sum_fields=["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA"]):
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
    par_lu_field: String, default="DOR_UC"
    par_bld_area: String, default="TOT_LVG_AREA"
    par_sum_fields: [String,...], default=["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA]
        If provided, these parcel fields will also be summed to the
        block-group level.
    """
    # Read bg-level data
    bg = gpd.read_file(bg_fc)
    # Read parcel data
    parcels = gpd.read_file(parcels_fc)
    # Project bg features into parcel crs
    bg = bg.to_crs(parcels.crs)
    
    # Spatial join parcels to blocks
    #   where parcel centroids are within
    parcels["geometry"] = parcels.geometry.centroids
    parcels = gpd.sjoin(parcels, bg, how='inner', op='within')
    
    # Summarize requested fields
    if par_sum_fields:
        if isinstance(par_sum_fields, string_types):
            par_sum_fields = [par_sum_fields]
        bg_sum_fields = [bg_id_field] + par_sum_fields
        bg_sum = parcels[bg_sum_fields].groupby(["GEOID10"]).sum()
        bg = bg.merge(
            bg_sum, how="left", left_on=bg_id_field, right_index=True)

    # Summarize parcel building area by activity type
    for grouping in sum_crit.keys():
        # Mask based on land use criteria
        crit = Or(parcels[par_lu_field], sum_crit[grouping])
        mask = crit.eval()
        # Summarize masked data
        area = parcels[mask].groupby([bg_id_field]).sum()[par_bld_area]
        # Rename sum data and join to block groups
        area.rename(columns={par_bld_area: f"{grouping}_area"}, inplace=True)
        bg = bg.merge(area, how="left", left_on=bg_id_field, right_index=True)

    # Output
    bg.to_file(out_fc)
    return out_fc



# %% MAIN
if __name__ == "__main__":
    # Define analysis specs
    bg_id_field = "GEOID10"
    par_lu_field = "DOR_UC"
    par_bld_area = "TOT_LVG_AREA"
    sum_crit = LODES_CRIT
    par_sum_fields = ["LND_VAL", "LND_SQFOOT", "JV", "NO_BULDNG", "NO_RES_UNTS", "TOT_LVG_AREA"]
    # For all years, enrich block groups with parcels
    for year in PMT.YEARS:
        # Define inputs/outputs
        bg_fc = PMT.makePath(
            PMT.CLEANED, "BlockGroups", "BG_{}.shp".format(year))
        parcels_fc = PMT.makePath(
            PMT.CLEANED, "Parcels.gdb", "Parcels_{}".format(year))
        out_fc = PMT.makePath(PMT.ROOT, "PMT_{}.gdb".format(year))
        # Enrich BGs with parcel data
        enrichBlockGroups(bg_fc, parcels_fc, out_fc, bg_id_field=bg_id_field,
                          par_lu_field=par_lu_field, par_bld_area=par_bld_area,
                          sum_crit=sum_crit, par_sum_fields=par_sum_fields



#%% ORIGINAL
# shape_file = r'K:/Projects/MiamiDade/PMT/Data/Reference/CensusBG.shp'
# Shape = gpd.read_file(shape_file)

# years = [2014, 2015, 2016, 2017, 2018, 2019]

# for year in years:
#     Parcels_Path = r'K:/Projects/MiamiDade/PMT/Data/Cleaned/Parcels/Parcel_Geometry'
#     Parcels_File = f'Miami_{year}.shp'
    
#     Attribute_Path = r'K:/Projects/MiamiDade/PMT/Data/Cleaned/Parcels/Parcel_Attributes'   
#     Attribute_File = f'Miami_{year}_DOR.csv'
   
#     Parcels    = gpd.read_file(f'{Parcels_Path}/{Parcels_File}')
#     Attributes = pd.read_csv(f'{Attribute_Path}/{Attribute_File}')
    
# # =============================================================================
# #     Parcels    = Parcels.drop_duplicates(subset = "PARCELNO").reset_index(drop = True)
# #     Attributes = Attributes.drop_duplicates(subset = "PARCELNO").reset_index(drop = True)
# # =============================================================================
   
#     Parcels["PARCELNO"]     = Parcels["PARCELNO"].astype('str')   
#     Attributes["PARCELNO"]  = Attributes["PARCELNO"].astype('str').str.zfill(13)
   
#     Parcels = pd.merge(Parcels, Attributes,  
#                       how = "inner", 
#                       on="PARCELNO",
#                       validate = 'one_to_one')
    
#     my_crs = Parcels.crs
    
#     Shape = Shape.to_crs(my_crs)
    
#     Parcels = gpd.sjoin(Parcels, Shape,
#                         how='inner',
#                         op='within')

    # Block_Groups = Parcels.groupby(["GEOID10"]).agg({
    #     "LND_VAL" : ["sum"],
    #     "LND_SQFOOT" : ["sum"],
    #     "JV" : ["sum"],
    #     "TOT_LVG_AREA" : ["sum"],
    #     "NO_BULDNG" : ["sum"],
    #     "NO_RES_UNTS" : ["sum"]
    #     })
    
    # Landuse_Area = Parcels[Landuse_Mask['Population']].groupby(["GEOID10"]).agg({
    #     "TOT_LVG_AREA" : ["sum"],
    #     })
    
    # Landuse_Area.rename(columns = {"TOT_LVG_AREA" : "RESIDENTIAL_LVG_AREA"},
    #                     inplace=True)
    
    # Block_Groups = pd.merge(Block_Groups, Landuse_Area, 
    #                         how = "left", 
    #                         on = "GEOID10")
    
    # LODES_Vars = ['CNS01', 'CNS02', 'CNS03', 'CNS04', 'CNS05', 'CNS06', 'CNS07', 
    #               'CNS08', 'CNS09', 'CNS10', 'CNS11', 'CNS12', 'CNS13', 'CNS14', 
    #               'CNS15', 'CNS16', 'CNS17', 'CNS18', 'CNS19', 'CNS20']
    
    # for var in LODES_Vars:
    #     Landuse_Area = Parcels[Landuse_Mask[var]].groupby(["GEOID10"]).agg({
    #     "TOT_LVG_AREA" : ["sum"],
    #     })
    #     Landuse_Area.rename(columns = {"TOT_LVG_AREA" : f'{var}_LVG_AREA'},
    #                         inplace=True)
    
    #     Block_Groups = pd.merge(Block_Groups, Landuse_Area, 
    #                             how = "left", 
    #                             on = "GEOID10",
    #                             validate = 'one_to_one')
    
    # Block_Groups.reset_index(inplace=True) 
    # Block_Groups.columns = Block_Groups.columns.droplevel(1)
    # Block_Groups = Block_Groups.rename(columns={'CENSUS_BK' : 'GEOID10'})
    # Block_Groups['GEOID10'] = Block_Groups['GEOID10'].astype('str') 
    # Block_Groups['GEOID10'] = Block_Groups['GEOID10'].str.slice(0, 12)
    # Block_Groups = Block_Groups.fillna(value = 0)
    
    # if year < 2018:
    #      LODES_Path   = f'K:/Projects/MiamiDade/PMT/Data/Cleaned/LODES/{year}/'
    #      LODES_File   = f'MCD_LODES_{year}.shp'
    #      LODES        = gpd.read_file(f'{LODES_Path}/{LODES_File}')
    #      LODES        = LODES[LODES['geometry'].isnull() == False]
    #      LODES        = LODES.to_crs("ESRI:102733")
         
    #      LODES        = pd.merge(LODES, Block_Groups, 
    #                              how='inner',
    #                              on = 'GEOID10')

    #      #adding census variables to LODES Data
    #      race_variables = ['B03002_002E', 'B03002_012E', 'B03002_003E', 'B03002_004E', 
    #                        'B03002_006E', 'B03002_009E', 'B03002_013E', 'B03002_014E', 
    #                        'B03002_016E', 'B03002_019E', ]
       
    #      race_data = census.download('acs5', 2014, 
    #                                 census.censusgeo([('state', '12'), 
    #                                                   ('county', '086'), 
    #                                                   ('block group', '*')]), 
    #                                 var = race_variables)
       
    #      race_data['Total_Non_Hisp'] = race_data['B03002_002E']
    #      race_data['Total_Hispanic'] = race_data['B03002_012E']
       
    #      race_data['White_Hispanic'] = race_data['B03002_013E']
    #      race_data['Black_Hispanic'] = race_data['B03002_014E']
    #      race_data['Asian_Hispanic'] = race_data['B03002_016E']
    #      race_data['Multi_Hispanic'] = race_data['B03002_019E']
       
    #      race_data['Other_Hispanic'] = (race_data['Total_Hispanic'] - 
    #                                      (race_data['White_Hispanic'] +
    #                                         race_data['Black_Hispanic'] +
    #                                         race_data['Asian_Hispanic'] +
    #                                         race_data['Multi_Hispanic']))
       
    #      race_data['White_Non_Hisp'] = race_data['B03002_003E']
    #      race_data['Black_Non_Hisp'] = race_data['B03002_004E']
    #      race_data['Asian_Non_Hisp'] = race_data['B03002_006E']
    #      race_data['Multi_Non_Hisp'] = race_data['B03002_009E']
       
    #      race_data['Other_Non_Hisp'] = (race_data['Total_Non_Hisp'] - 
    #                                     (race_data['White_Non_Hisp'] +
    #                                        race_data['Black_Non_Hisp'] +
    #                                        race_data['Asian_Non_Hisp'] +
    #                                        race_data['Multi_Non_Hisp']))
       
    #      # preping for merge
    #      race_data = race_data.reset_index()
       
    #      race_data['index'] = race_data['index'].astype(str)
       
    #      race_data['State']  = '12' 
    #      race_data['County'] = '086'
       
    #      tract = re.compile('([0-9]{6})')
    #      bg_number = re.compile('(block group:[0-9]{1})')
       
    #      race_data['tract']       = race_data['index'].str.extract(tract)
    #      race_data['block_group'] = race_data['index'].str.extract(bg_number)
    #      race_data['block_group'] = race_data['index'].str.slice(-1)
       
    #      race_data['GEOID'] = (race_data['State'] + race_data['County'] + 
    #                             race_data['tract'] + race_data['block_group'])
    #      race_data = race_data[['GEOID', 'Total_Non_Hisp', 'Total_Hispanic',
    #                             'White_Hispanic', 'Black_Hispanic', 'Asian_Hispanic',
    #                             'Multi_Hispanic', 'Other_Hispanic', 'White_Non_Hisp',
    #                             'Black_Non_Hisp', 'Asian_Non_Hisp', 'Multi_Non_Hisp', 
    #                             'Other_Non_Hisp']].copy()
       
    #      race_data.rename(columns = {'GEOID' : 'GEOID10'},
    #                     inplace = True)
       
         LODES = pd.merge(LODES, race_data, how = 'inner', on = 'GEOID10')
     
         ## Transit calculate mode shares at block group level then assign those shares to parcels and multiply by population.
         ## Drove (alone + motocycle), Carpooled, Transit, Non Moto (bike + ped), Work From Home, other, Total Commutes
       
         transit_variables = ['B08301_001E', 'B08301_003E', 'B08301_004E', 'B08301_010E', 
                             'B08301_017E', 'B08301_021E', 'B08301_018E', 'B08301_019E', 
                             'B08301_020E', 'B08301_016E']
       
         transit_data = census.download('acs5', 2014, 
                                      census.censusgeo([('state', '12'), ('county', '086'), 
                                                        ('block group', '*')]), 
                                      var = transit_variables)
       
         transit_data['Drove']     = ((transit_data['B08301_003E'] + 
                                       transit_data['B08301_017E']) / 
                                      transit_data['B08301_001E'])
       
         transit_data['Carpooled'] = (transit_data['B08301_004E'] / 
                                      transit_data['B08301_001E'])
       
         transit_data['Transit']   = (transit_data['B08301_010E'] / 
                                      transit_data['B08301_001E'])
       
         transit_data['NonMotor']  = ((transit_data['B08301_018E'] +
                                       transit_data['B08301_019E']) / 
                                      transit_data['B08301_001E'])
       
         transit_data['WFH']       = (transit_data['B08301_021E'] / 
                                      transit_data['B08301_001E'])
       
         transit_data['Other']     = ((transit_data['B08301_020E'] + 
                                       transit_data['B08301_016E']) / 
                                      transit_data['B08301_001E'])
       
         # preping for merge
         transit_data = transit_data.reset_index()
       
         transit_data['index'] = transit_data['index'].astype(str)
       
         transit_data['State']  = '12' 
         transit_data['County'] = '086'
       
         tract = re.compile('([0-9]{6})')
         bg_number = re.compile('(block group:[0-9]{1})')
       
         transit_data['tract']       = transit_data['index'].str.extract(tract)
         transit_data['block_group'] = transit_data['index'].str.extract(bg_number)
         transit_data['block_group'] = transit_data['index'].str.slice(-1)
       
         transit_data['GEOID'] = (transit_data['State'] + transit_data['County'] + 
                                  transit_data['tract'] + transit_data['block_group'])
       
         transit_data = transit_data[['GEOID', 'Drove', 'Carpooled', 'Transit',
                                    'NonMotor', 'WFH', 'Other']]
       
         transit_data.rename(columns = {'GEOID' : 'GEOID10'},
                            inplace = True)
       
         LODES = pd.merge(LODES, transit_data, how = 'inner', on = 'GEOID10')
         
         LODES = pd.DataFrame(LODES.drop(columns = 'geometry'))
         
         For_Model_Path = r'K:/Projects/MiamiDade/PMT/Data/Modeling/For_Model'
         For_Out_File   = f'Block_Groups_{year}.csv'
         LODES.to_csv(f'{For_Model_Path}/{For_Out_File}', index=False)
         
    else:
        Block_Groups = pd.merge(Shape, Block_Groups,
                                how = "inner",
                                on = 'GEOID10')
        
        Block_Groups = pd.DataFrame(Block_Groups.drop(columns = 'geometry'))
        
        To_Model_Path = r'K:/Projects/MiamiDade/PMT/Data/Modeling/To_Model'
        To_Out_File   = f'Block_Groups_{year}.csv'
        
        Block_Groups.to_csv(f'{To_Model_Path}/{To_Out_File}', index=False)