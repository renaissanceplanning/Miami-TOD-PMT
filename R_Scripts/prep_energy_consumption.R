
# -----------------------------------------------------------------------------
# --------------------------------- Function ----------------------------------
# -----------------------------------------------------------------------------

prep_energy_consumption = function(raw_res_energy_consumption_path,
                                   save_directory){
  
  # Requirements --------------------------------------------------------------
  
  req = c("readxl", "data.table", "magrittr", "stringr")
  loaded = loadedNamespaces()
  already_loaded = req %in% loaded
  if(any(!already_loaded)){
    tl = req[!already_loaded]
    for(i in tl){
      if(!require(i, character.only = TRUE)){
        cat("[Have to install ", i, "]\n", sep="")
        install.packages(i)
        library(i, character_only = TRUE)
      }
    }
  }
  
  # Validation ----------------------------------------------------------------

  # raw_res_energy_consumption_path: must be to the data we're looking for,
  # but that doesn't need an explicit check. Will fail obviously if it's the
  # wrong file. Documentation will also reflect the specific file need
  
  # save_directory must be existing or creatable
  # And, if it doesn't exist, we need to create it!
  if(!dir.exists(save_directory)){
    tryCatch(dir.create(save_directory),
             error = function(e){
               message("'save_directory' is not creatable")
             })
  }
  
  # Read the data -------------------------------------------------------------
  
  cat("Reading...\n")
  
  # Use suppressMessages so we don't get printouts about column names...
  # we know, and we're fixing it. Don't want to confuse progress output
  
  # Read the estimates
  ec_means = suppressMessages(
    read_xlsx(raw_res_energy_consumption_path,
              sheet = "Btu")
  )
  # Read the RSEs
  ec_sds = suppressMessages(
    read_xlsx(raw_res_energy_consumption_path,
              sheet = "rse")
  )
  
  # Format the data -----------------------------------------------------------
  
  cat("Formatting...\n")
  
  # Each table has the same layout, so we can format them the same way
  # This will make our life nice and easy for analysis!
  # We'll set it up as a long table for easy filtering
  ref = lapply(list(ec_means, ec_sds), function(x){
    # Fuel (electricity, nat gas, etc)
    # These are the types of energy used in households
    # There will also be "Housing Units" and "Total Energy" columns here
    nc = ncol(x)
    w = which(!is.na(x[4,]))
    fuel = lapply(4:nc, function(y){
      x[4, max(w[w <= y])]
    }) %>% 
      unlist %>% 
      unname %>%
      append(., 
             values = c("Housing Units", "Total Energy"), 
             after = 0)
    
    # End use (total, space heating, water heating, etc)
    # These are the ways energy is consumed
    # "Housing Units" and "Total Energy" will get the use "Total"
    end_use = x[5,4:ncol(x)] %>%
      unlist %>%
      unname %>%
      append(., 
             values = c("Total","Total"), 
             after = 0) %>%
      str_replace_all(., "[0-9]|-", "")
    
    # Fuel/end use combos -- merge Fuel and End Use
    fuelend_combos = data.table(Fuel = fuel,
                                End_Use = end_use)
    
    # Category (division, rural/urban, etc)
    # These are the different groups in which energy consumption is summarized
    # There will also be an "All Homes" category
    category = c("All homes",
                 rep("Division", 3),
                 rep("Rural/urban", 4),
                 rep("Metro/micro", 3),
                 rep("Climate", 5),
                 rep("Housing unit", 5),
                 rep("Ownership", 8),
                 rep("Year", 8),
                 rep("SQFT", 6),
                 rep("Size", 6),
                 rep("Income", 8),
                 rep("Payment", 4),
                 rep("Main fuel", 4))
    
    # Levels within categories (with some manual formatting)
    # These are the different levels for each summary category
    # The level for "All homes" will just be "All homes"
    # We do some manual formatting here for ease of reading
    value_rows = which(!is.na(x[,2]))
    level = x[value_rows, 1] %>% 
      unlist %>% 
      unname
    wna = which(is.na(level))
    value_rows = value_rows[-wna]
    level = level[-wna]
    level[6] = paste(level[5], level[6], sep="-")
    level[7] = paste(level[5], level[7], sep="-")
    level[23] = paste(level[22], level[23], sep="-")
    level[24] = paste(level[22], level[24], sep="-")
    level[25] = paste(level[22], level[25], sep="-")
    level[26] = str_replace_all(level[26], "[0-9]", "")
    level[27] = paste(level[26], level[27], sep="-")
    level[28] = paste(level[26], level[28], sep="-")
    level[29] = paste(level[26], level[29], sep="-")
    
    # Category/level combos -- merge Category and Level
    catlev_combos = data.table(Category = category,
                               Level = level)
    
    # Long table initiation
    lv = nc-1
    clrep = catlev_combos[rep(1:nrow(catlev_combos), times = nc-1),]
    ferep = fuelend_combos[rep(1:nrow(fuelend_combos), each = nrow(catlev_combos)),]
    
    # Values
    # use suppressWarnings so it doesn't tell us we've created NAs: we know,
    # and this is intentional, so we don't want the warnings to confuse a user
    vals = suppressWarnings(
      x[value_rows, 2:nc] %>% 
        data.table %>%
        .[, lapply(.SD, as.numeric)] %>%
        unlist %>%
        data.table() %>%
        setNames("Value")
    )
    
    # Long table
    df = cbind(clrep, ferep, vals)
    return(df)
  })
  
  # Once we have the long tables, we can merge estimate and RSE together
  names(ref[[1]])[names(ref[[1]]) == "Value"] = "Estimate"
  names(ref[[2]])[names(ref[[2]]) == "Value"] = "RSE"
  df = merge(ref[[1]], ref[[2]], by=c("Category","Level","Fuel","End_Use"))
  
  # Finally, we'll go ahead and calculate standard errors (where applicable)
  # SE = RSE/100 * Estimate
  # If the estimate is NA, SE will be NA too, but that's okay (can't do
  # anything with an NA estimate to begin with)
  df[, SE := RSE / 100 * Estimate]
  
  # Save ----------------------------------------------------------------------
  
  cat("Saving...\n")
  save_path = file.path(save_directory,
                        "Residential_Energy_Consumption.csv")
  write.csv(df,
            save_path,
            row.names = FALSE)
  cat("-- saved to:", save_path, "\n")
  
  # Done ----------------------------------------------------------------------
  
  cat("Done!\n\n")
  return(NULL)
}

# -----------------------------------------------------------------------------
# --------------------------------- For PMT -----------------------------------
# -----------------------------------------------------------------------------

rrecp = "K:/Projects/MiamiDade/PMT/Data/Raw/EIA_ResEnergy2015.xlsx"
sd = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Energy_Consumption"

prep_energy_consumption(raw_res_energy_consumption_path = rrecp,
                        save_directory = sd)
                                            
                                            
                                            
                                            