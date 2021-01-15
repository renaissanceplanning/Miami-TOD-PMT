# SHOULD WE EXPLORE LOESS FOR MODELING INSTEAD OF LINEAR REGRESSION? LINEAR
# REGRESSION SEEMS PROBLEMATIC AROUND OBSERVED 0s

# -----------------------------------------------------------------------------
# --------------------------------- Function ----------------------------------
# -----------------------------------------------------------------------------

# Use roxygen2 style documentation, later...

analyze_blockgroup_estimation = function(gdb_directory,
                                         job_years,
                                         dem_years,
                                         year_range){
  
  # Requirements -------------------------------------------------------------- 
  
  req = c("stringr","dplyr", "sf", "Hmisc", "car", 
          "arcgisbinding","data.table", "stringr", "zoo")
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
  
  # Connect R and Arc
  arc.check_product()
  
  # Validation ----------------------------------------------------------------
  
  # gdb_directory must be valid file path
  # if(!dir.exists(gdb_directory)){
  #   stop("'gdb_directory' does not exists")
  # }
  
  # years must be all integers
  # if(typeof(x) != "double"){
  #   stop("'years' must be numeric)
  # }
  
  # Helper functions for modeling ---------------------------------------------
  
  helper_funs = paste0("K:/Projects/MiamiDade/PMT/code/R_Scripts/",
                       "prep_Modeling_HelperFunctions.R")
  source(helper_funs)
  
  # Data read -----------------------------------------------------------------
  cat("\n1. Reading input data (block group)\n")
  
  # Sort the years for loop processing
  years = sort(year_range)
  
  # Loop read and process
  mod_frames = lapply(years, function(x){
    num = paste0("1.", which(years == x))
    cat(" ", num, x, "\n")
    # 1. Read
    gdb = file.path(gdb_directory,
                    paste0("PMT_", x, ".gdb"))
    df = st_read("blockgroup_enrich",
                 dsn = gdb,
                 quiet = TRUE) %>%
      data.table()
    
    # 2. Process
    block_groups = df %>%
      .[, Year := x] %>%
      .[, Since_2013 :=  x - 2013] %>%
      .[, Total_Emp_Area := CNS_01_par + CNS_02_par + CNS_03_par + CNS_04_par + 
          CNS_05_par + CNS_06_par + CNS_07_par + CNS_08_par + CNS_09_par + 
          CNS_10_par + CNS_11_par + CNS_12_par + CNS_13_par + CNS_14_par + 
          CNS_15_par + CNS_16_par + CNS_17_par + CNS_18_par + CNS_19_par + 
          CNS_20_par]
    if(x %in% job_years){
      block_groups = block_groups %>%
        .[, Total_Employment := CNS01 + CNS02 + CNS03 + CNS04 + CNS05 + CNS06 + 
            CNS07 + CNS08 + CNS09 + CNS10 + CNS11 + CNS12 + CNS13 + CNS14 + 
            CNS15 + CNS16 + CNS17 + CNS18 + CNS19 + CNS20]
    }
    if(x %in% dem_years){
      block_groups = block_groups %>% 
        .[, Total_Population := Total_Non_Hisp + Total_Hispanic]
    }
    return(block_groups)
  }) %>% 
    rbindlist(., fill=TRUE)
  
  
  # Modeling -----------------------------------------------------------------
  cat("\n2. Modeling total employment, population, and commutes\n")
  
  # Variable setup: defines our variables of interest for modeling
  independent_variables = c("LND_VAL", "LND_SQFOOT", "JV", "TOT_LVG_AREA" ,
                            "NO_BULDNG", "NO_RES_UNTS", "RES_par",
                            "CNS_01_par", "CNS_02_par", "CNS_03_par",
                            "CNS_04_par", "CNS_05_par", "CNS_06_par",
                            "CNS_07_par", "CNS_08_par", "CNS_09_par",
                            "CNS_10_par", "CNS_11_par", "CNS_12_par",
                            "CNS_13_par", "CNS_14_par", "CNS_15_par",
                            "CNS_16_par", "CNS_17_par", "CNS_18_par",
                            "CNS_19_par", "CNS_20_par", "Total_Emp_Area",
                            "Since_2013")
  response = list("Total_Employment" = job_years,
                  "Total_Population" = dem_years,
                  "Total_Commutes" = dem_years)
  
  # Step 1: Overwrite NA values with 0 (where we should have data but don't)
  # -- parcel-based variables should be there every time: fill all with 0
  # -- job variables should be there for `job_years`: fill these with 0
  # -- dem variables should be there for `dem_years`: fill these with 0
  cat("  2.1 replacing missing values\n")
  for(iv in independent_variables){
    set(x = mod_frames, 
        i = which(is.na(mod_frames[[iv]])), 
        j = iv, 
        value = 0)
  }
  mod_frames[is.na(Total_Employment) & Year %in% job_years, Total_Employment := 0]
  mod_frames[is.na(Total_Population) & Year %in% dem_years, Total_Population := 0]
  mod_frames[is.na(Total_Commutes) & Year %in% dem_years, Total_Commutes := 0]
  
  # Now, subset our table for ease of interpretation
  mf_cor = mod_frames %>%
    .[, c("GEOID10",
          independent_variables, names(response), 
          "Year", "Shape"),
      with=FALSE]
  
  # Step 2: conduct modeling by extracting a correlation matrix between candidate
  # explanatories and our responses, identifying explanatories with significant
  # correlations to our response, and fitting a MLR using these explanatories
  cat("  2.2 fitting and applying models\n")
  fits = lapply(names(response), function(x){
    num = paste0("2.2.", which(names(response) == x))
    cat("    ", num, x, "\n")
    
    # Correlation Matrix
    # rcorr produces 3 matrices:
    # 1. correlation coefficient
    # 2. sample size
    # 3. p-value (presumably of test that R != 0)
    mdf = mf_cor %>%
      .[Year %in% response[[x]]]
    cor_matrix = mdf %>%
      .[, c(independent_variables, x), with=FALSE] %>%
      as.matrix() %>%
      rcorr()
    
    # Determining significant variables for predicting 'x'
    # Answers the question, "For each independent variable we could use for
    # prediction, what is the correlation to 'x'? Is it significant?"
    model_inputs = sign_coef_to_formula(cor_matrix = cor_matrix, 
                                        variable = x,
                                        independent_variables = independent_variables)
    model = lm(model_inputs$formula, 
               data = mdf)
    
    # Applying the model. If any predictions are < 0, we'll go ahead and push
    # them up to 0 (because we can't have negative estimates!)
    pred = predict(model,
                   newdata = mod_frames,
                   type = "response")
    pred[pred < 0] = 0
    
    # Format results for return
    # Note that we only keep shape once because we don't need repeat geometry!
    # cat("         ", paste0(num, ".4"), "formatting results\n")
    retdf = mod_frames %>%
      .[, .(GEOID10, Year, get(x), Shape)] %>%
      .[, Pred := pred] %>%
      setNames(c("GEOID10", "Year", paste0(x, "_Obs"), "Shape", paste0(x, "_Pred")))
    if(x != "Total_Employment"){
      retdf[, Shape := NULL]
    }
    return(retdf)
  })
  
  # Step 3: merge the employment and population predictions to write out
  cat("  2.3 merging and formatting predictions\n")
  pwrite = merge(fits[[1]], fits[[2]], by=c("GEOID10", "Year")) %>%
    merge(., fits[[3]], by=c("GEOID10", "Year")) %>%
    .[, .(GEOID10, Year, 
          Total_Employment_Obs, Total_Employment_Pred,
          Total_Population_Obs, Total_Population_Pred,
          Total_Commutes_Obs, Total_Commutes_Pred,
          Shape)]
  
  # Shares --------------------------------------------------------------------
  cat("\n3. Identifying observed year shares of variable levels\n")
  
  # Variable setup: defines our variables of interest for modeling
  dependent_variables_emp = c("CNS01", "CNS02", "CNS03", "CNS04", "CNS05", 
                              "CNS06", "CNS07", "CNS08", "CNS09", "CNS10",
                              "CNS11", "CNS12", "CNS13", "CNS14", "CNS15", 
                              "CNS16", "CNS17", "CNS18", "CNS19", "CNS20",
                              "Total_Employment")
  dependent_variables_pop = c("Total_Hispanic", "Total_Non_Hisp",
                              "White_Hispanic", "Black_Hispanic", 
                              "Asian_Hispanic", "Multi_Hispanic",
                              "Other_Hispanic", "White_Non_Hisp", 
                              "Black_Non_Hisp", "Asian_Non_Hisp", 
                              "Multi_Non_Hisp", "Other_Non_Hisp",
                              "Total_Population")
  dependent_variables_trn = c("Drove", "Carpool", "Transit",
                              "NonMotor", "Work_From_Home", "AllOther",
                              "Total_Commutes")
  
  # Step 1: Overwrite NA values with 0 (where we should have data but don't)
  # -- emp variables should be there for `job_years`: fill these with 0
  # -- pop and trn variables should be there for `dem_years`: fill these with 0
  cat("  3.1 replacing missing values\n")
  
  # Job variables
  for(dve in dependent_variables_emp){
    set(x = mod_frames, 
        i = which(is.na(mod_frames[[dve]]) & mod_frames[["Year"]] %in% job_years), 
        j = dve, 
        value = 0)
  }
  
  # Demographic varialbes
  for(dvd in c(dependent_variables_pop, dependent_variables_trn)){
    set(x = mod_frames, 
        i = which(is.na(mod_frames[[dvd]]) & mod_frames[["Year"]] %in% dem_years), 
        j = dve, 
        value = 0)
  }
  
  # Now, subset our table for ease of interpretation
  shares_df = mod_frames %>%
    .[, c("GEOID10", "Year",
          dependent_variables_emp, dependent_variables_pop, dependent_variables_trn),
      with=FALSE]
  
  # Step 2: Calculate shares relative to total
  # This is done relative to the "Total" variable for each group 
  cat("  3.2 calculating shares\n")
  
  # Employment shares
  employment_shares = melt(shares_df[Year %in% job_years],
                           id.vars = c("GEOID10", "Year"),
                           measure.vars = dependent_variables_emp) %>%
    merge(., shares_df[, .(GEOID10, Year, Total_Employment)], by=c("GEOID10","Year")) %>%
    .[, Share := value / Total_Employment] %>%
    dcast(GEOID10 + Year ~ variable, value.var = "Share") %>%
    .[, Total_Employment := NULL]
  
  # Population shares
  population_shares = melt(shares_df[Year %in% dem_years],
                           id.vars = c("GEOID10", "Year"),
                           measure.vars = dependent_variables_pop) %>%
    merge(., shares_df[, .(GEOID10, Year, Total_Population)], by=c("GEOID10","Year")) %>%
    .[, Share := value / Total_Population] %>%
    dcast(GEOID10 + Year ~ variable, value.var = "Share") %>%
    .[, Total_Population := NULL]
  # For filling NAs, we'll need the Total Hisp/Non-Hisp separated from the
  # subgroups within those, because normalization will get confused
  population_shares_total = population_shares %>%
    .[, names(.)[str_detect(names(.), "Total|GEOID10|Year")], with=FALSE]
  population_shares_sub = population_shares %>%
    .[, names(.)[!str_detect(names(.), "Total")], with=FALSE]
  
  # Commute shares
  commute_shares = melt(shares_df[Year %in% dem_years],
                        id.vars = c("GEOID10", "Year"),
                        measure.vars = dependent_variables_trn) %>%
    merge(., shares_df[, .(GEOID10, Year, Total_Commutes)], by=c("GEOID10","Year")) %>%
    .[, Share := value / Total_Commutes] %>%
    dcast(GEOID10 + Year ~ variable, value.var = "Share") %>%
    .[, Total_Commutes := NULL]
  
  # Step 3: some rows have NA shares because the total for that class of
  # variables was 0. For these block groups, take the average share of all
  # block groups that touch that one
  cat("  3.3 estimating missing shares\n")
  
  # First, we'll need spatialized block groups
  sf_bg = mod_frames %>%
    .[, .(GEOID10, Year, Shape)] %>%
    st_sf
  
  # Now, identify block groups we'll need to fill and replace with mean of
  # block groups that touch it
  sl = list("Employment" = employment_shares, 
            "Population -- Total Hispanic/Non-Hispanic" = population_shares_total,
            "Population -- Subgroups of Hispanic/Non-Hispanic" = population_shares_sub,
            "Commutes" = commute_shares)
  filled_shares = lapply(1:length(sl), function(y){
    x = sl[[y]]
    n = names(sl)[[y]]
    num = paste0("3.3.", y)
    cat(" ", num, n, "\n")
    
    # Identify block groups in 'x' that need to be filled, and spatialize
    bg_fill = x[is.na(get(names(x)[3]))] %>%
      .[, .(GEOID10, Year)] %>%
      merge(., 
            sf_bg %>% data.table, 
            by=c("GEOID10","Year"), 
            all.x=TRUE) %>%
      st_sf()
    
    # Extract valid block groups for identifying touching features
    bg_valid = merge(x,
                     sf_bg %>% data.table,
                     by=c("GEOID10","Year"),
                     all.x=TRUE) %>%
      na.omit() %>%
      .[, .(GEOID10, Year, Shape)] %>%
      st_sf()
    
    # Identify which block groups touch those that need to be filled
    # We also remove matches where years are not the same, as this 
    # will confuse our averaging
    stt = st_touches(bg_fill, bg_valid)
    touch_match = data.table(BG_Fill = rep(bg_fill$GEOID10, times=lengths(stt)),
                             Fill_Year = rep(bg_fill$Year, times=lengths(stt)),
                             GEOID10 = bg_valid$GEOID10[unlist(stt)],
                             Year = bg_valid$Year[unlist(stt)]) %>%
      .[Fill_Year == Year] %>%
      .[, Fill_Year := NULL]
    
    # Merge matches table with shares data
    tm = merge(touch_match, 
               x, 
               by=c("GEOID10","Year"), 
               all.x=TRUE) %>%
      na.omit() %>%
      .[, GEOID10 := NULL]
    names(tm)[names(tm) == "BG_Fill"] = "GEOID10"
    
    # Average shares by year and block group to be filled
    # We'll also normalize, just to ensure these shares sum to 1
    mean_shares = tm %>%
      .[, lapply(.SD, mean), by=c("GEOID10","Year")] %>%
      melt(.,
           id.vars = c("GEOID10", "Year"),
           measure.vars = names(.)[!names(.) %in% c("GEOID10","Year")]) %>%
      .[, fd := 1 / sum(value), by = c("GEOID10", "Year")] %>%
      .[, Share := value * fd] %>%
      dcast(GEOID10 + Year ~ variable, value.var = "Share")
      
    # Now, it's possible that some block group/year combos to be filled had
    # 0 block groups in that year touching them that had data. If this happened,
    # we're just going to fill the row with the shares from the nearest 
    # block group that had data in that year. First, identify if there are any
    # that still need to be filled
    still_to_fill = merge(bg_fill %>% data.table,
                          mean_shares,
                          by=c("GEOID10","Year"),
                          all.x=TRUE) %>%
      .[!na.omit(.)] %>%
      .[, .(GEOID10, Year, Shape)] %>%
      st_sf()
    
    # If there are some, fill them, and bind up un-transformed, mean, and take
    # shares to return. Otherwise, just bind up un-transformed and mean shares
    if(nrow(still_to_fill) > 0){
      # Fill the shares
      take_shares = lapply(1:nrow(still_to_fill), function(y){
        yr_ft = bg_valid[bg_valid$Year == still_to_fill$Year[y],]
        s = yr_ft[st_nearest_feature(still_to_fill[y,], yr_ft),] %>%
          st_drop_geometry %>%
          data.table
        return(s)
      }) %>% 
        rbindlist %>%
        merge(.,
              x,
              by=c("GEOID10","Year"),
              all.x=TRUE) %>%
        .[, GEOID10 := still_to_fill$GEOID10] %>%
        .[, Year := still_to_fill$Year]
      
      # Merge into a final shares table
      fst = rbind(x %>% na.omit(),
                  mean_shares,
                  take_shares) %>%
        .[order(GEOID10, Year)]
    } else{
      fst = rbind(x %>% na.omit(),
                  mean_shares) %>%
        .[order(GEOID10, Year)] 
    }
    # SORTING IS IMPORTANT HERE because for constant shares, we'd fill by 
    # the most recent NA value in the table
    
    # That's all! 
    # Return the completed shares
    return(fst)
  })
  
  # Step 4: merge and format the shares
  cat("  3.4 merging and formatting shares\n")
  est_shares = merge(filled_shares[[1]], 
                     filled_shares[[2]], 
                     by=c("GEOID10", "Year"),
                     all = TRUE) %>%
    merge(., filled_shares[[3]], by=c("GEOID10", "Year"), all=TRUE) %>%
    merge(., filled_shares[[4]], by=c("GEOID10", "Year"), all=TRUE)
  
  # Forecasting shares --------------------------------------------------------
  cat("\n4. Forecasting un-observed year shares of variable levels\n")
  
  # We could...
  # Use a LOESS smooth on each GEOID to predict its share in unobserved years
  # For LOESS, we need a span parameter. We'll pick span by minimizing SSE
  # of predictions. For SSE, here's a function
  # (See: http://r-statistics.co/Loess-Regression-With-R.html)
  
  # For now...
  # Use constant shares: for absent years, just assume the share from the
  # most recent year
  cat("  4.1 using constant shares to forecast shares in un-observed years\n")
  
  # First, we'll need empty rows for years that we want to predict, but were
  # not present in shares identification. SORTING IS IMPORTANT HERE because
  # we're doing constant shares by filling the most recent NA value in the table
  geoids = unique(est_shares$GEOID10)
  missing_years = setdiff(years, unique(c(job_years, dem_years)))
  lmy = length(missing_years)
  if(lmy > 0){
    myt = data.table(GEOID10 = rep(geoids, each = lmy),
                     Year = rep(missing_years, times = length(geoids)))
    cs_tab = rbindlist(list(est_shares, myt), fill=TRUE) %>%
      .[order(GEOID10, Year)]
  } else{
    cs_tab = est_shares
  }
  
  # Now we use constant shares to fill missing values
  cs_shares = cs_tab %>%
    .[, lapply(.SD, na.locf)]
  
  # Block group estimation ----------------------------------------------------
  cat("\n5. Estimating variable levels using model estimates and shares\n")
  
  # Now, our allocations are simple multiplication problems! Hooray!
  # So, all we have to do is multiply the shares by the appropriate column
  # First, we'll merge our estimates and shares
  cat("  5.1 performing allocations\n")
  alloc = merge(pwrite, cs_shares, by=c("GEOID10","Year"))
  
  # Then, we'll set up our allocation variables
  ev = dependent_variables_emp[dependent_variables_emp != "Total_Employment"]
  pv = dependent_variables_pop[dependent_variables_pop != "Total_Population"]
  tv = dependent_variables_trn[dependent_variables_trn != "Total_Commutes"]
  
  # We'll do employment first
  cat("     5.1.1 Total Employment\n")
  emp = alloc %>%
    .[, ev, with=FALSE] %>%
    .[, lapply(.SD, function(x){x * alloc$Total_Employment_Pred})]
  
  # Now population
  cat("     5.1.2 Total Population\n")
  pop = alloc %>%
    .[, pv, with=FALSE] %>%
    .[, lapply(.SD, function(x){x * alloc$Total_Population_Pred})]
  
  # Finally commutes
  cat("     5.1.3 Total Commutes\n")
  trn = alloc %>%
    .[, tv, with=FALSE] %>%
    .[, lapply(.SD, function(x){x * alloc$Total_Commutes_Pred})]
  
  # Now, we just format the results and spatialize
  # The allocations above maintained the integrity of row indices, so we
  # can use a simply column bind here
  cat("  5.2 formatting and spatializing results\n")
  allocation_final = cbind(alloc[, .(GEOID10, Year)],
                           alloc[, .(Total_Employment_Obs, Total_Employment_Pred,
                                     Total_Population_Obs, Total_Population_Pred,
                                     Total_Commutes_Obs, Total_Commutes_Pred)],
                           emp,
                           pop,
                           trn,
                           alloc[, .(Shape)])
  
  # Writing -------------------------------------------------------------------
  cat("\n6. Writing outputs\n")
  
  # Here we write block group for enrich.
  for(x in years){
    w = which(years == x)
    cat(" ", paste0("6.",w), x, "\n")
    
    # Grab the data
    to_write = allocation_final %>%
      .[Year == x] %>%
      st_sf %>%
      as(., "Spatial")
    
    # Write the data
    gdb_path = file.path(gdb_directory,
                         paste0("PMT_", x, ".gdb"),
                         "BlockGroups/blockgroup_for_alloc2")
    arc.write(path = gdb_path,
              data = to_write,
              overwrite = TRUE)
  }
  
  # That's all folks ----------------------------------------------------------
  cat("\nDone! Data is ready for allocation")
  return(allocation_final %>% st_sf)
}


analyze_blockgroup_estimation(gdb_directory = "K:/Projects/MiamiDade/PMT",
                              job_years = 2014:2017,
                              dem_years = 2014:2018,
                              year_range = 2014:2019)
