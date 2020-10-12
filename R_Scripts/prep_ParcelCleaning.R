library(tidyverse)
library(sf)
library(foreign)
library(data.table)
source("K:/Projects/MiamiDade/PMT/Scripts/R_Scripts/Parcel_Helpers.R")

# ----Loading Data
cols_to_keep = c("CO_NO",     "PARCEL_ID",   "ASMNT_YR",  "DOR_UC", 
                 "LND_VAL",   "LND_SQFOOT",  "JV",        "TOT_LVG_AREA", 
                 "NO_BULDNG", "NO_RES_UNTS", "CENSUS_BK", "PHY_ADDR1", 
                 "PHY_ADDR2", "PHY_CITY",    "PHY_ZIPCD", "STATE_PARCEL_ID") 

cols_to_keep19 = c("CO_NO",      "PARCEL_ID",  "ASMNT_YR",  "DOR_UC", 
                   "LND_VAL",    "LND_SQFOOT", "JV",        "TOT_LVG_AR", 
                   "NO_BULDNG",  "NO_RES_UNT", "CENSUS_BK", "PHY_ADDR1", 
                   "PHY_ADDR2",  "PHY_CITY",   "PHY_ZIPCD",  "STATE_PAR_") 

landuse <- read.csv("K:/Projects/MiamiDade/PMT/Data/Reference/Land_Use_Recode.csv")

years <- 2014:2019

for (i in years) {
  cat("reading shape", i, "\n")
  shape_path <- "K:/Projects/MiamiDade/PMT/Data/Raw/Parcel_Shapes/"
  shape <- read_sf(paste0(shape_path, "Miami_", i, ".shp"))
  
  cat("removing dups for", i, "\n")
  n_before = nrow(shape)
  shape    = subset(shape, !duplicated(shape$PARCELNO))
  n_after  = nrow(shape)

  cat("duplicate parcels removed", as.character(n_before - n_after), "\n")
  
  cat("making clean shape valid for", i, "\n")
  n_before = nrow(shape)
  shape = subset(shape, !is.na(shape$geometry))
  n_after = nrow(shape)
  cat("invalid parcels removed", as.character(n_before - n_after), "\n")
  
  cat("reading table", i, "\n")
  table_path <- "K:/Projects/MiamiDade/PMT/Data/Raw/Parcel_Tables/"
  table <- read.csv(paste0(table_path, "NAL_", i, "_23Dade_F.csv"))
  
  cat("cleaning table", i, "\n")
  if(i != 2019){
    table <- table[cols_to_keep]
  } else {
    table <- table[cols_to_keep19]
  }
  
  cat("recoding DOR_UC", i, "\n")
  landuse$DOR_UC = as.integer(landuse$DOR_UC)
  table = left_join(table, landuse)
  
  table$PARCEL_ID = str_pad(table$PARCEL_ID, 
                            width = 13,
                            side = "left",
                            pad   = "0")
  
  cat("removing dups for", i, "\n")
  n_before = nrow(table)
  table = subset(table, !duplicated(table$PARCEL_ID))
  n_after = nrow(table)
  cat("duplicate parcels removed", as.character(n_before - n_after), "\n")

  cat("removing non matching parcels \n")
  table$PARCEL_ID = str_pad(table$PARCEL_ID, 
                            width = 13,
                            side = "left",
                            pad   = "0")
  
  table_ids = unique(table$PARCEL_ID)
  shape_ids = unique(shape$PARCELNO)
  
  n_before = nrow(shape)
  shape    = subset(shape, shape$PARCELNO %in% table_ids)
  n_after  = nrow(shape)
  cat("parcels removed from shape", as.character(n_before - n_after), "\n")
  
  n_before = nrow(table)
  table    = subset(table, table$PARCEL_ID %in% shape_ids)
  n_after  = nrow(table)
  cat("parcels removed from table", as.character(n_before - n_after), "\n")
  
  cat("writing clean table", i, "\n")
  table_path_clean <- "K:/Projects/MiamiDade/PMT/Data/Cleaned/Parcels/Parcel_Attributes/"
  write_csv(table, paste0(table_path_clean, "Miami_", i, "_DOR.csv"))
  
  cat("writing clean shape", i, "\n")
  shape_clean_path <- "K:/Projects/MiamiDade/PMT/Data/Cleaned/Parcels/Parcel_Geometry/"
  write_sf(shape, paste0(shape_clean_path, "Miami_", i, ".shp"))

}
