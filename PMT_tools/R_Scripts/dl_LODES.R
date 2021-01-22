library(data.table)
library(dplyr)
library(sf)
library(tigris)
library(R.utils)
library(stringr)

# Helper functions for parameterizing LODES_Download
# -----------------------------------------------------------------------------

versions = function(){
  cat("Select one of the following LODES version numbers: \n",
      "7: enumerated by 2010 census blocks \n",
      "5: enumerated with 2000 census blocks")
}
files = function(){
  cat("Select one of the following file dataset abbrevations: \n",
      "OD: Origin-Destination data, jobs totals are associated with both a home Census Block and a work Census Block \n",
      "RAC: Residence Area Characteristic data, jobs are totaled by home Census Block \n",
      "WAC: Workplace Area Characteristic data, jobs are totaled by work Census Block")
}
parts = function(){
  cat("If your file is 'OD', select one of the following file part names: \n",
      "main: includes jobs with both workplace and residence in the state \n",
      "aux: includes jobs with the workplace in the state and the residence outside of the state \n \n",
      "If your file is 'RAC' or 'WAC', select a 'segment' instead of a 'part'") 
}
segments = function(){
  cat("If your file is 'RAC' or 'WAC', select one of following workforce segments: \n",
      "S000: Total number of jobs \n",
      "SA01: Number of jobs of workers age 29 or younger \n",
      "SA02: Number of jobs for workers age 30 to 54 \n",
      "SA03: Number of jobs for workers age 55 or older \n",
      "SE01: Number of jobs with earnings $1250/month or less \n",
      "SE02: Number of jobs with earnings $1251/month to $3333/month \n",
      "SE03: Number of jobs with earnings greater than $3333/month \n",
      "SI01: Number of jobs in goods producing industry sectors \n",
      "SI02: Number of jobs in trade, transportation, and utilities industry sectors \n",
      "SI03: Number of jobs in all other services industry sectors \n \n",
      "If your file is 'OD', select a 'part' instead of a 'segment'")
}
types = function(){
  cat("Select one of the following job types: \n",
      "JT00: All jobs \n",
      "JT01: Primary jobs \n",
      "JT02: All private josb \n",
      "JT03: Primary private jobs \n",
      "JT04: All federal jobs \n",
      "JT05: Federal primary jobs")
}
years = function(){
  cat("If you selected LODES version 5, valid years are 2002 to 2009 \n",
      "If you selected LODES version 7, valid years are 2002 to 2017")
}

# file: CHARACTER, 'od', 'rac','wac', or 'geography' see 'files()'
# reference_directory: CHARACTER of directory where variable name meaning
#                      reference files are kept
# view: LOGICAL, do we want to open a view window of the reference file?
#       (default is TRUE, we want a view window)
# load_variables = function(file, reference_directory, view=TRUE){
#   file = tolower(file)
#   toload = file.path(reference_directory, paste0(file,".rds"))
#   x = readRDS(toload)
#   if(view == TRUE){View(x, file)}
#   return(x)
# }

# Geography download (part of LODES_Download)
# -----------------------------------------------------------------------------

# state: CHARACTER of state abbreviation (only 1 state at a time!)
# variables: vector with variable names to keep from the selected file, 
#            see 'load_variables()'. Add names if you want to change the 
#            column names
#            (default is NULL, keep all variables)
#
# note, block ID will always be kept and renamed to "block" for easy merging purposes
# with LODES_Download data

downloadGeography = function(state, variables=NULL){
  # Verify inputs
  state = tolower(state)
  
  if(!is.null(variables)){
    if(!("tabblk2010" %in% variables)){
      variables = c("block"="tabblk2010", variables)
    }
  }
  
  # Setup/download
  gspecs = paste0(state, "_xwalk.csv.gz")
  gdld = paste("https://lehd.ces.census.gov/data/lodes/LODES7", state, gspecs, sep="/")
  
  # Read
  geog = fread(gdld)
  
  # Variables
  if(!is.null(variables)){
    geog = geog[,..variables]
    if(!is.null(names(variables))){
      names(variables)[names(variables)==""] = variables[names(variables)==""]
      names(geog) = names(variables)
    }
  } else{
    names(geog)[1] = "block"
  }
  
  # Return
  return(geog)
}

# Aggregating up from blocks (part of LODES_Download)
# -----------------------------------------------------------------------------

# Lodes comes in blocks. If you want to aggregate to tract or block group,
# use this function
#
# lodes_df: output of LODES_Download
# geography_df: output of downloadGeography
# level: CHARACTER, 'block group' or 'tract'
# table: CHARACTER, type of lodes_df, either 'rac', 'wac', or 'od'

aggregateLODES = function(lodes_df, geography_df, level, table){
  # Verify inputs
  table = tolower(table)
  if(level != "tract" & level != "block group"){
    stop("'level' must be 'tract' or 'block group'")
  }
  if(table != "od" & table != "rac" & table != "wac"){
    stop("'table' must be 'od', 'rac', or 'wac'")
  }
  
  if(level == "tract"){
    vbl = "trct"
  } else{
    vbl = "bgrp"
  }
  block="block"
  geography_df = geography_df[, c(block,vbl), with=FALSE]
  
  # Aggregate
  if(table == "rac" | table == "wac"){
    geoged = merge(lodes_df, geography_df, by="block")
    dt = geoged[, block:=NULL]
    agg = dt[, lapply(.SD, sum, na.rm=TRUE), by=vbl]
  } else{
    l1 = merge(lodes_df, geography_df, by.x="wblock", by.y="block")
    vn1 = paste0(vbl, 1)
    names(l1)[ncol(l1)] = vn1
    l2 = merge(l1, geography_df, by.x="rblock", by.y="block")
    vn2 = paste0(vbl, 2)
    names(l2)[ncol(l2)] = vn2
    dt = l2[, c("rblock","wblock"):=NULL]
    agg = dt[, lapply(.SD, sum, na.rm=TRUE), by=c(vn1,vn2)]
  }
  
  # Return
  return(agg)
}

# Download LODES
# -----------------------------------------------------------------------------

# version: NUMERIC, 5 or 7, see 'versions()'
#          (default is 7, the newer version)
# state: CHARACTER of state abbreviation (only 1 state at a time!)
# file: CHARACTER, 'od', 'rac', or 'wac', see 'files()'
#       (default is wac, it's what we use for this analysis)
# part: CHARACTER, if file='od' must pick a file part, see 'parts()'
#       (default is NULL, it's not used for WAC files)
# segment: CHARACTER, if file='rac' or 'wac' must pick a workforce segment,
#          see 'segments()'
#          (default is S000, total number of jobs)
# type: CHARACTER, job type, see 'types()'
#       (default is JT00, all jobs)
# year: NUMERIC year of desired data
#       (default is 2017, most recent LODES year)
# lodes_variables: CHARACTER VECTOR with variable names to keep from the 
#                  selected file, see 'load_variables()'. Add names if you
#                  want to change the column names
#                  (default is NULL, keep all variables)
# geography: LOGICAL, should geography be downloaded too?
#            (default is TRUE, we want geography)
# geography_variables: CHARACTER VECTOR with variable names to keep from
#                      the geography file, see 'load_variables()'. Add names
#                      if you want to change the column names
#                      (default is NULL, keep all variables)
# aggregate: CHARACTER, provide either 'tract' or 'block group' to aggregate
#            up to that level, leave NULL to not aggregate
# save_dirs: CHARACTER VECTOR of save directories for the files IN ORDER OF
#            raw lodes, geography, aggregated data. Will only save those
#            requested in the function
#            (default is NULL, no save)
#
# note, block ID will always be kept and renamed to "block" for easy merging
# purposes with downloadGeography data
#
# note also: you can't aggregate if you don't request geography!
#
# if a save is requested, the files will be saved as "raw_lodes", "geography",
# and "aggregated_[level]"
#
# Returns list of block-level LODES if geography not requested
# Returns list of block-level LODES and geography table if geography requested
# but aggregate is not
# Returns list of block-level LODES, geography table, and aggregated LODES
# at requested level if geography and aggregation requested

download_LODES = function(version = 7,
                          state = NULL, file = "wac", part = NULL, segment = "S000", type = "JT00",
                          year = 2017,
                          lodes_variables = NULL,
                          geography = TRUE,
                          geography_variables = NULL,
                          aggregate = NULL,
                          save_dirs = NULL){
  # Verify inputs
  if(!is.null(part) & !is.null(segment)){
    stop("Only one of 'part' and 'segment' should be specified.
         Use 'part' when file = 'od' and 'segment' when file = 'rac' or 'wac'")
  }
  
  cat("Setting up for download... \n")
  
  state = tolower(state)
  file = tolower(file)
  part = tolower(part)
  segment = toupper(segment)
  type = toupper(type)
  
  if(!is.null(lodes_variables)){
    if(file == "od"){
      if(!("w_geocode" %in% lodes_variables)){
        lodes_variables = c("wblock"="w_geocode", lodes_variables)
      }
      if(!("h_geocode" %in% lodes_variables)){
        lodes_variables = c("rblock"="h_geocode", lodes_variables)
      }
    } else if(file == "rac" & !("h_geocode" %in% lodes_variables)){
      lodes_variables = c("block"="h_geocode", lodes_variables)
    } else if(file == "wac" & !("w_geocode" %in% lodes_variables)){
      lodes_variables = c("block"="w_geocode", lodes_variables)
    }
  }
  
  # Set up path
  base_path = "https://lehd.ces.census.gov/data/lodes/LODES"
  vsf = paste(version, state, file, sep="/")
  specs = if(file == "od"){
    paste0(
      paste(state, file, part, type, year, sep="_"), 
      ".csv.gz"
    )
  } else{
    paste0(
      paste(state, file, segment, type, year, sep="_"), 
      ".csv.gz"
    )
  }
  dld = paste(
    paste0(base_path, vsf), 
    specs, 
    sep="/"
  )
  
  # Read
  cat("Downloading the requested LODES file... \n")
  dt = fread(dld)
  
  # Variables
  if(!is.null(lodes_variables)){
    dt = dt[,..lodes_variables]
    if(!is.null(names(lodes_variables))){
      names(lodes_variables)[names(lodes_variables) == ""] = variables[names(lodes_variables) == ""]
      names(dt) = names(lodes_variables)
    }
  } else{
    if(file == "od"){
      names(dt)[1:2] = c("wblock","rblock")
    } else{
      names(dt)[1] = "block"
    }
  }
  
  # If geography requested, get it
  if(geography){
    cat("Downloading the associated geography... \n")
    geog = downloadGeography(state = state, 
                             variables = geography_variables)
    
    # If aggregate requested, do it
    if(!is.null(aggregate)){
      cat("Aggregating to the", aggregate, "level... \n")
      agged = aggregateLODES(lodes_df = dt,
                             geography_df = geog,
                             level = aggregate,
                             table = file)
      tabs = list("LODES"=dt,
                  "GEOG"=geog,
                  "AGGED"=agged)
    } else{
      tabs = list("LODES"=dt,
                  "GEOG"=geog)
    }
  } else{
    tabs = list("LODES"=dt)
  }
  
  # If save is requested, save
  if(!is.null(save_dirs)){
    cat("Saving the files produced... \n")
    save_dirs = rep_len(save_dirs, length.out = length(tabs))
    save_names = c("raw_lodes","geography","aggregated")[1:length(tabs)]
    if(length(save_names) == 3){
      save_names[3] = paste0(save_names[3], "_", aggregate)
    }
    for(i in 1:length(tabs)){
      if(!dir.exists(save_dirs[i])){
        dir.create(save_dirs[i])
      }
      fp = file.path(save_dirs[i], paste0(save_names[i], ".csv"))
      write.csv(tabs[[i]], fp, row.names=FALSE)
    }
  }
  
  # Done
  cat("Done! \n")
  return(tabs)
}

# Running for Miami-Dade County
# -----------------------------------------------------------------------------

# bg = block_groups(state="FL", 
#                   year=2010,
#                   class="sf") %>%
#   filter(COUNTYFP10 == "086") %>%
#   select(GEOID10) %>%
#   st_transform(4326)
# 
# yrs = 2002:2017
# lg = length(yrs)
# s = lapply(yrs, function(x){
#   w = which(yrs == x)
#   cat(paste0("\n(", w, "/", lg, ")"), "Jobs for", x, "\n")
#   d = paste0("Data/Processed_Data/LODES/", x)
  # lodes = download_LODES(version = 7,
  #                        state = "FL",
  #                        file = "wac",
  #                        part = NULL,
  #                        segment = "S000",
  #                        type = "JT00",
  #                        year = x,
  #                        lodes_variables = NULL,
  #                        geography = TRUE,
  #                        geography_variables = NULL,
  #                        aggregate = "block group",
  #                        save_dirs = NULL)
#   
#   cat("Formatting LODES data...\n")
#   lodes_fm = lodes$AGGED %>%
#     data.frame %>%
#     setNames(c("GEOID10", names(lodes$AGGED)[2:ncol(lodes$AGGED)])) %>%
#     select(c(1, which(str_detect(names(lodes$AGGED), "C000|CNS")))) %>%
#     mutate(GEOID10 = as.character(GEOID10))
#   
#   cat("Spatializing LODES data...\n")
#   lodes_sf = left_join(bg, lodes_fm, by="GEOID10")
#   
#   if(w == lg){cat("\nDone!\n\n")}
#   return(lodes_sf)
# })
# 
# s2 = lapply(s, function(x){
#   x[is.na(x)]=0
#   return(x)
# })
# 
# fd = "K:/Projects/MiamiDade/PMT/Data/Cleaned/LODES
# lapply(yrs, function(x){
#   cat("\n",x)
#   w = which(yrs == x)
#   st = file.path(fd, x)
#   if(!dir.exists(st)){dir.create(st)}
#   st_write(s2[[w]], file.path(st, paste0("LODES_",x,".shp")))
#   return(TRUE)
# })
# 

lodes = lapply(2014:2017, function(x){
  cat(x)
  download_LODES(version = 7,
                         state = "FL",
                         file = "wac",
                         part = NULL,
                         segment = "S000",
                         type = "JT00",
                         year = x,
                         lodes_variables = NULL,
                         geography = TRUE,
                         geography_variables = NULL,
                         aggregate = "block group",
                         save_dirs = NULL)
})
agged = lapply(lodes, function(x){
  x$AGGED
})
agged = lapply(agged, function(x){
  names(x)[1] = "GEOID10"
  return(x)
})
write.csv(agged[[4]],
          "K:/Projects/MiamiDade/PMT/Data/Raw/BlockGroups/LODES_2017_jobs.csv",
          row.names = FALSE)
