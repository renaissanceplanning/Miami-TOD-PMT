library(tidyverse)
library(sf)
library(foreign)
source("K:/Projects/MiamiDade/Features/PMT/PMT_Data/R_Scripts/dl_Parcels_HelperFunctions.R")

# # -------downloading necessary data----
# 
# # Attributes (no longer on website done locally)

# Shape, since FTP URLs are not standardized must be done manually and not in a function
setwd("K:/Projects/MiamiDade/Features/PMT/PMT_Data/Parcels/RAW/Shapes")

URLs <- c(Miami_2014 = "ftp://sdrftp03.dor.state.fl.us/Map%20Data/00_2014/dade/Dade_pin.zip",
          Miami_2015 = "ftp://sdrftp03.dor.state.fl.us/Map%20Data/00_2015/dade_pin.zip",
          Miami_2016 = "ftp://sdrftp03.dor.state.fl.us/Map%20Data/00_2016/dade_pin.zip",
          Miami_2017 = "ftp://sdrftp03.dor.state.fl.us/Map%20Data/00_2017/dade_pin.zip",
          Miami_2018 = "ftp://sdrftp03.dor.state.fl.us/Map%20Data/00_2018/dade_2018pin.zip",
          Miami_2019 = "ftp://sdrftp03.dor.state.fl.us/Map%20Data/2019%20PIN%20Data%20Distribution/dade_2019pin.zip")#,
#          Miami_2020 = "ftp://sdrftp03.dor.state.fl.us/Map%20Data/2020%20PIN%20Data%20Distribution/dade_2020pin.zip")

for (i in 1:length(URLs)) {
  year <- names(URLs[i])
  assign(year, url_shp_to_sf(URLs[i])) 
  write_sf(get(year), paste0(year, ".shp"))
} 

