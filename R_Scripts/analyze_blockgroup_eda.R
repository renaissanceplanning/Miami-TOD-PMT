
# Requirements ---------------------------------------------------------------- 

library(tidyverse)
library(sf)
library(Hmisc)
library(car)
Helper_Path = paste0('K:/Projects/MiamiDade/PMT/Scripts/',
                     'R_Scripts/prep_Modeling_HelperFunctions.R')
source(Helper_Path)

# Data read -------------------------------------------------------------------

Out_Dir = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Block_Groups/"

For_Model_Dir = "K:/Projects/MiamiDade/PMT/Data/Modeling/For_Model"
My_Files = list.files(path = For_Model_Dir, pattern = "*.csv") %>%
  str_sub(start = 1, end = -5)

for (i in My_Files) {
  shapefile = paste0(i, ".csv")
  Block_Group = read.csv(paste0(For_Model_Dir, "/", shapefile)) 
  Block_Group$Year = as.factor(str_sub(i, start = -4) %>% as.numeric())
  Block_Group$Since_2013 =  factor_to_num(Block_Group$Year) - 2013
  assign(i, Block_Group)
  rm(Block_Group)
}

# for (i in My_Files) {
#   write_csv(get(i), path = paste0(Out_Dir, i, ".csv"))
# }

Block_Groups = bind_rows(mget(My_Files))
rm(list = ls(pattern = "*_201[4|5|6|7]"))

Block_Groups = Block_Groups %>%
  mutate(Total_Population = Total_Non_Hisp + Total_Hispanic,
         
         Total_Employment = CNS01 + CNS02 + CNS03 + CNS04 + CNS05 +
           CNS06 + CNS07 + CNS08 + CNS09 + CNS10 +
           CNS11 + CNS12 + CNS13 + CNS14 + CNS15 +
           CNS16 + CNS17 + CNS18 + CNS19 + CNS20,
         
         Total_Emp_Area   = CNS01_LVG_AREA + CNS02_LVG_AREA + CNS03_LVG_AREA + 
           CNS04_LVG_AREA + CNS05_LVG_AREA + CNS06_LVG_AREA + 
           CNS07_LVG_AREA + CNS08_LVG_AREA + CNS09_LVG_AREA + 
           CNS10_LVG_AREA + CNS11_LVG_AREA + CNS12_LVG_AREA + 
           CNS13_LVG_AREA + CNS14_LVG_AREA + CNS15_LVG_AREA + 
           CNS16_LVG_AREA + CNS17_LVG_AREA + CNS18_LVG_AREA + 
           CNS19_LVG_AREA + CNS20_LVG_AREA
  )

# EDA -------------------------------------------------------------------------

# Population
Bin_Width_Pop = Calc_Bin_Width(Block_Groups$Total_Population)

ggplot(Block_Groups, aes(Total_Population)) +
  geom_histogram(binwidth = Bin_Width_Pop) + 
  facet_wrap(~ Year)

## Population Growth Over Years
ggplot(Block_Groups, 
       aes(x = Year, y = Total_Population)) + 
  geom_violin() +
  stat_summary(fun = mean, geom = "point", shape=23, size=4) +
  coord_flip()

## Total Population 
Block_Groups %>%
  group_by(Year) %>%
  summarise(Tot_Pop = sum(Total_Population, na.rm = TRUE)) %>%
  ggplot(aes(x = factor_to_num(Year), y = Tot_Pop)) + 
  geom_col()

# Employment
Bin_Width_Pop = Calc_Bin_Width(Block_Groups$Total_Employment)

ggplot(Block_Groups, aes(Total_Employment)) +
  geom_histogram(binwidth = Bin_Width_Pop) + 
  facet_wrap(~ Year)

## Employment Growth Over Years
ggplot(Block_Groups, 
       aes(x = Year, y = Total_Employment)) + 
  geom_violin() +
  stat_summary(fun = mean, geom = "point", shape=23, size=4) +
  coord_flip()

## Total Employment by sector
gather(Block_Groups,
       key = LODES_Var,
       value = Employment,
       CNS01:CNS20,
       factor_key = TRUE) %>%
  group_by(Year, LODES_Var) %>%
  summarise(Total_Emp = sum(Employment, na.rm = TRUE)) %>%
  ggplot(aes(x=Year, y = Total_Emp, 
             fill = LODES_Var)) + 
  geom_col()

