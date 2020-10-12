#### set up #### 
library(tidyverse)
library(sf)
library(Hmisc)
library(car)
Helper_Path = paste0('K:/Projects/MiamiDade/PMT/Scripts/',
                     'R_Scripts/prep_Modeling_HelperFunctions.R')
source(Helper_Path)

#### Data Imports ####
Out_Dir = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Block_Groups/"

For_Model_Dir = "K:/Projects/MiamiDade/PMT/Data/Modeling/For_Model"
My_Files      = list.files(path = For_Model_Dir, pattern = "*.csv") %>%
                    str_sub(start = 1, end = -5)

for (i in My_Files) {
  shapefile = paste0(i, ".csv")
  Block_Group = read.csv(paste0(For_Model_Dir, "/", shapefile)) 
  Block_Group$Year = as.factor(str_sub(i, start = -4) %>% as.numeric())
  Block_Group$Since_2013 =  factor_to_num(Block_Group$Year) - 2013
  assign(i, Block_Group)
  rm(Block_Group)
}

for (i in My_Files) {
  write_csv(get(i), path = paste0(Out_Dir, i, ".csv"))
}

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

#### EDA ####
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

#### Exploratory Modeling ####
###correlations
Independent_Variables = c("LND_VAL",   "LND_SQFOOT",  "JV", "TOT_LVG_AREA" ,
                          "NO_BULDNG", "NO_RES_UNTS", "RESIDENTIAL_LVG_AREA",
                          
                          "CNS01_LVG_AREA", "CNS02_LVG_AREA", "CNS03_LVG_AREA",      
                          "CNS04_LVG_AREA", "CNS05_LVG_AREA", "CNS06_LVG_AREA", 
                          "CNS07_LVG_AREA", "CNS08_LVG_AREA", "CNS09_LVG_AREA", 
                          "CNS10_LVG_AREA", "CNS11_LVG_AREA", "CNS12_LVG_AREA", 
                          "CNS13_LVG_AREA", "CNS14_LVG_AREA", "CNS15_LVG_AREA",      
                          "CNS16_LVG_AREA", "CNS17_LVG_AREA", "CNS18_LVG_AREA", 
                          "CNS19_LVG_AREA", "CNS20_LVG_AREA", "Total_Emp_Area",
                          
                          "Year", "Since_2013")

Dependent_Variables_Emp = c("CNS01", "CNS02",               
                            "CNS03", "CNS04",                
                            "CNS05", "CNS06",               
                            "CNS07", "CNS08",                
                            "CNS09", "CNS10",               
                            "CNS11", "CNS12",                
                            "CNS13", "CNS14",               
                            "CNS15", "CNS16",                
                            "CNS17", "CNS18",               
                            "CNS19", "CNS20")                
                            
Dependent_Variables_Pop = c("Total_Non_Hisp", "Total_Hispanic", "White_Hispanic",    
                            "Black_Hispanic", "Asian_Hispanic", "Multi_Hispanic",     
                            "Other_Hispanic", "White_Non_Hisp", "Black_Non_Hisp", 
                            "Asian_Non_Hisp", "Multi_Non_Hisp", "Other_Non_Hisp") 
                            
Dependent_Variables_TRN = c("Drove", "Carpooled", "Transit", 
                            "NonMotor", "WFH", "Other")     
                            
Total_Variables         = c("Total_Population", "Total_Employment")

## Correlation Matrix
cor_matrix = rcorr(
  as.matrix(Block_Groups[c(Independent_Variables, Dependent_Variables_Emp,
                           Dependent_Variables_Pop, Dependent_Variables_TRN,
                           Total_Variables)])
)

## determing significant variables Population, using totals
Total_Population_Modeling = sign_coef_to_formula(cor_matrix, "Total_Population")

pop_plots = Block_Groups[c(Total_Population_Modeling$Variables, 
                           "Total_Population")] %>% 
              pivot_longer(cols = c(all_of(Total_Population_Modeling$Variables)))

ggplot(pop_plots,
       aes(x = Total_Population, y = value)) + 
  geom_jitter() +
  geom_smooth(method = "lm") +
  facet_wrap(~name) +
  theme_bw()

## determining significant variables Employment
Total_Employment_Modeling = sign_coef_to_formula(cor_matrix, "Total_Employment")

emp_plots = Block_Groups[c(Total_Employment_Modeling$Variables, 
                           "Total_Employment")] %>% 
  pivot_longer(cols = c(all_of(Total_Employment_Modeling$Variables)))

ggplot(emp_plots,
       aes(x = Total_Employment, y = value)) + 
  geom_jitter() +
  geom_smooth(method = "lm") +
  facet_wrap(~name) +
  theme_bw()

rm(emp_plots, pop_plots)
#### Modeling ####
##population
Total_Population_Model = lm(Total_Population_Modeling$Formula, data = Block_Groups)
summary(Total_Population_Model)

testing_residuals(Total_Population_Model)

##employment
Total_Employment_Model = lm(Total_Employment_Modeling$Formula, data = Block_Groups)
summary(Total_Employment_Model)

testing_residuals(Total_Employment_Model)

### Demographic Models
for (i in Dependent_Variables_Pop) {
  Modeling = sign_coef_to_formula(cor_matrix, i)
  
  Block_Groups[c(Modeling$Variables, i)] %>% 
    pivot_longer(cols = c(all_of(Modeling$Variables))) %>%
    ggplot(aes(x = get(i), y = value)) + 
      geom_jitter() +
      geom_smooth(method = "lm") +
      facet_wrap(~name) +
      theme_bw() + 
      ggtitle(label = i, subtitle = "Linear Test") %>% 
    print()

  Model = lm(Modeling$Formula, data = Block_Groups)
  cat(i, "Model Summary")
  print(summary(Model))

  testing_residuals(Model) %>% print()
  
  assign(paste0(i, "_Modeling"), Modeling)
  assign(paste0(i, "_Model"), Model)

  rm(Modeling, Model)
}

for (i in Dependent_Variables_Emp) {
  Modeling = sign_coef_to_formula(cor_matrix, i)
  
  Block_Groups[c(Modeling$Variables, i)] %>% 
    pivot_longer(cols = c(all_of(Modeling$Variables))) %>%
    ggplot(aes(x = get(i), y = value)) + 
    geom_jitter() +
    geom_smooth(method = "lm") +
    facet_wrap(~name) +
    theme_bw() + 
    ggtitle(label = i, subtitle = "Linear Test") %>% 
    print()
  
  Model = lm(Modeling$Formula, data = Block_Groups)
  cat(i, "Model Summary")
  print(summary(Model))
  
  testing_residuals(Model) %>% print()
  
  assign(paste0(i, "_Modeling"), Modeling)
  assign(paste0(i, "_Model"), Model)
  
  rm(Modeling, Model)
}

for (i in Dependent_Variables_TRN) {
  Modeling = sign_coef_to_formula(cor_matrix, i)
  
  Block_Groups[c(Modeling$Variables, i)] %>% 
    pivot_longer(cols = c(all_of(Modeling$Variables))) %>%
    ggplot(aes(x = get(i), y = value)) + 
    geom_jitter() +
    geom_smooth(method = "lm") +
    facet_wrap(~name) +
    theme_bw() + 
    ggtitle(label = i, subtitle = "Linear Test") %>% 
    print()
  
  Model = lm(Modeling$Formula, data = Block_Groups)
  cat(i, "Model Summary")
  print(summary(Model))
  
  testing_residuals(Model) %>% print()
  
  assign(paste0(i, "_Modeling"), Modeling)
  assign(paste0(i, "_Model"), Model)
  
  rm(Modeling, Model)
}



























#### Adding Modeled Data ####
To_Model_Dir = "K:/Projects/MiamiDade/PMT/Data/Modeling/To_Model"
Model_Files      = list.files(path = To_Model_Dir, pattern = "*.csv") %>%
                     str_sub(start = 1, end = -5)

for (i in Model_Files) {
  shapefile = paste0(i, ".csv")
  Block_Group = read.csv(paste0(To_Model_Dir, "/", shapefile)) 
  Block_Group$Year = as.factor(str_sub(i, start = -4) %>% as.numeric())
  Block_Group$Since_2013 =  factor_to_num(Block_Group$Year) - 2013
  Block_Group = Block_Group %>%
    mutate(Total_Emp_Area   = CNS01_LVG_AREA + CNS02_LVG_AREA + CNS03_LVG_AREA + 
                              CNS04_LVG_AREA + CNS05_LVG_AREA + CNS06_LVG_AREA + 
                              CNS07_LVG_AREA + CNS08_LVG_AREA + CNS09_LVG_AREA + 
                              CNS10_LVG_AREA + CNS11_LVG_AREA + CNS12_LVG_AREA + 
                              CNS13_LVG_AREA + CNS14_LVG_AREA + CNS15_LVG_AREA + 
                              CNS16_LVG_AREA + CNS17_LVG_AREA + CNS18_LVG_AREA + 
                              CNS19_LVG_AREA + CNS20_LVG_AREA
    )
  assign(i, Block_Group)
  rm(Block_Group)
}

Dependent_Variables = c(Dependent_Variables_Pop, Dependent_Variables_Emp,
                        Dependent_Variables_TRN, Total_Variables)

for (file in Model_Files){
  Data = get(file)
  for (var in Dependent_Variables) {
    Model = get(paste0(var, "_Model"))
    Data[var] <- predict(Model, newdata=Data, type='response') 
  }
  write_csv(Data, path = paste0(Out_Dir, 
                                file,
                                ".csv"))
}

