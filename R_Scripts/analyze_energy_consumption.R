
# -----------------------------------------------------------------------------
# ------------ Helper function for area of distributional overlap -------------
# -----------------------------------------------------------------------------

max_of_overlap = function(categories, levels, estimates, tolerance = 0.001){
  # Get the estimates we want for the given levels
  dat = estimates[Category %in% categories & Level %in% levels] %>%
    .[, .(Estimate, SE)]
  
  # Set lower bound and upper bound for search
  # Set at min/max of mean -/+ 3 * sd, because there should be basically
  # 0 overlap at these tails
  lb = min(dat$Estimate - dat$SE * 3)
  ub = max(dat$Estimate + dat$SE * 3)
  
  # For a given value "x", all distributions will share overlapping density
  # up to the minimum density observed at "x" for any distribution. We can
  # use this property to find the value of x at which the maximum of the
  # overlap is observed
  
  # First, get our range of x-values to test
  xvals = seq(lb, ub, tolerance)
  
  # Now, get density values for all x-values for each distribution
  # We assume a normal distribution bc EIA suggests a normal confidence
  # interval for uncertainty quantification: 
  # https://www.eia.gov/consumption/commercial/data/what-is-an-rse.php
  dens = apply(dat, 1, function(x){
    dnorm(x = xvals,
          mean = x["Estimate"],
          sd = x["SE"])
  })
  
  # In each row of the result, find the min -- this is the amount at each
  # x-value that overlaps
  mind = apply(dens, 1, min)
  
  # The max of the overlap values calculated above is our estimate -- pull the
  # x-value associate with this
  est = xvals[which.max(mind)]
  return(est)
}

# -----------------------------------------------------------------------------
# --------------------------------- Function ----------------------------------
# -----------------------------------------------------------------------------

analyze_energy_consumption = function(cleaned_res_energy_consumption_path,
                                      parcels_path,
                                      save_directory,
                                      id_field = "PARCELNO",
                                      land_use_field = "DOR_UC",
                                      living_area_field = "TOT_LVG_AREA",
                                      year_built_field = "ACT_YR_BLT"){
  
  # Validation ----------------------------------------------------------------
  
  # cleaned_res_energy_consumption_path: must be to the data we're looking for,
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
  
  # Read the energy data ------------------------------------------------------
  
  cat("Reading energy data...\n")
  df = fread(cleaned_res_energy_consumption_path)
  
  # Format/estimate the energy data -------------------------------------------
  
  cat("Estimating energy consumption by household characteristics...\n")
  # All homes in MDC are in the SOuth Atlantic division, and we're only
  # going to make our assessment base on housing unit, year of construction,
  # and total square footage. We filter accordingly
  iv = df[(Category == "Division" & Level == "South Atlantic") 
          | Category == "Housing unit"
          | Category == "Year"
          | Category == "SQFT"]
  
  # We're also primarily interested in total energy usage, so we want
  # Fuel = "Total energy" and End_Use = "Total"
  iv = iv[Fuel == "Total Energy" & End_Use == "Total"]
  
  # From this, we can create our unique combinations of levels
  categories = c("Division", "Housing unit", "Year", "SQFT")
  uc = lapply(categories, function(x){
    unique(iv[Category == x]$Level)
  }) %>% 
    expand.grid %>%
    setNames(categories) %>%
    data.table
  
  # With levels in place, we can loop through and perform our distribution
  # overlap estimates. We'll use our pre-written 'max_of_overlap' function,
  # which is designed to work with the structure of uc
  energy_ests = apply(uc, 1, function(y){
    max_of_overlap(categories = categories,
                   levels = y,
                   estimates = iv,
                   tolerance = 0.001)
  })
  
  # Once the estimates are calculated, we can bind them back to the table
  uc[, Energy := energy_ests]
  
  # Reading parcels -----------------------------------------------------------
  
  cat("Reading parcels...\n")
  parcels_path = str_replace_all(parcels_path, "\\\\", "\\/")
  if(str_detect(parcels_path, "\\.gdb\\/")){
    layer = str_extract(parcels_path, "[^\\/]+$")
    gdb = str_split(parcels_path, paste0("/",layer))[[1]][1]
    parcels = st_read(parcels_path,
                      dsn = gdb)
  } else{
    parcels = st_read(parcels_path)
  }
  
  
  # Formatting parcels to match levels of energy data -------------------------
  
  cat("Formatting parcel attributes to match energy data...\n")
  
  # Get the necessary attributes from the parcels data. Then, we can trim
  # the original parcels data to only include the ID field
  pdat = parcels %>%
    st_drop_geometry %>%
    select(ID = !!sym(id_field),
           LU = !!sym(land_use_field),
           TLA = !!sym(living_area_field),
           YR = !!sym(year_built_field)) %>%
    data.table()
  parcels = parcels %>% 
    select(ID = !!sym(id_field))
  
  # Now we can create the variables!
  pdat[, "Division" := "South Atlantic"]
  pdat[, "Housing unit" := case_when(LU %in% c() ~ "Apartments in buildings with 2-4 units",
                                     LU %in% c() ~ "Apartments in buildings with 5 or more units",
                                     LU %in% c() ~ "Mobile homes",
                                     LU %in% c() ~ "Single-family attached",
                                     LU %in% c() ~ "Single-family detached",
                                     TRUE ~ "FAILED")]
  pdat[, "SQFT" := case_when(TLA <= 999 ~ "Fewer than 1,000",
                             between(TLA, 1000, 1499) ~ "1,000 to 1,499",
                             between(TLA, 1500, 1999) ~ "1,500 to 1,999",
                             between(TLA, 2000, 2499) ~ "2,000 to 2,499",
                             between(TLA, 2500, 2999) ~ "2,500 to 2,999",
                             TLA >= 3000 ~ "3,000 or greater",
                             TRUE ~ "FAILED")]
  pdat[, "Year" := case_when(YR <= 1949 ~ "Before 1950",
                             between(YR, 1950, 1959) ~ "1950 to 1959",
                             between(YR, 1960, 1969) ~ "1960 to 1969",
                             between(YR, 1970, 1979) ~ "1970 to 1979",
                             between(YR, 1980, 1989) ~ "1980 to 1989",
                             between(YR, 1990, 1999) ~ "1990 to 1999",
                             between(YR, 2000, 2009) ~ "2000 to 2009",
                             YR >= 2010 ~ "2010 to 2015",
                             TRUE ~ "FAILED")]
  
  # Merging parcels and energy ------------------------------------------------
  
  cat("Merging parels and energy data...\n")
  
  # Now we can match back to the data we've gathered for energy
  pdat = pdat[, .(ID, Division, `Housing unit`, SQFT, Year)] %>%
    merge(pdat, uc, all.x=TRUE) %>%
    .[, .(ID, Energy)]
  
  parcels = left_join(parcels, pdat) %>%
    select(ID, Energy, geometry)
  names(parcels)[names(parcels) == "ID"] = id_field
  
  # Save ----------------------------------------------------------------------
  
  # Done ----------------------------------------------------------------------
  
  cat("Done!\n")
  return(parcels)
}

# -----------------------------------------------------------------------------
# ------------------- Visual diagnostic for overlap method --------------------
# -----------------------------------------------------------------------------

mo_plot = function(categories, levels, estimates, tolerance){
  # Get the estimates we want for the given levels
  dat = estimates[Category %in% categories & Level %in% levels] %>%
    .[, .(Category, Level, Estimate, SE)]
  
  # Set lower bound and upper bound for search
  # Set at min/max of mean -/+ 3 * sd, because there should be basically
  # 0 overlap at these tails
  lb = min(dat$Estimate - dat$SE * 3)
  ub = max(dat$Estimate + dat$SE * 3)
  
  # For a given value "x", all distributions will share overlapping density
  # up to the minimum density observed at "x" for any distribution. We can
  # use this property to find the value of x at which the maximum of the
  # overlap is observed
  
  # First, get our range of x-values to test
  xvals = seq(lb, ub, tolerance)
  
  # Now, get density values for all x-values for each distribution
  # We assume a normal distribution bc EIA suggests a normal confidence
  # interval for uncertainty quantification: 
  # https://www.eia.gov/consumption/commercial/data/what-is-an-rse.php
  dens = apply(dat, 1, function(x){
    dnorm(x = xvals,
          mean = x["Estimate"] %>% as.numeric,
          sd = x["SE"] %>% as.numeric)
  })
  
  # In each row of the result, find the min -- this is the amount at each
  # x-value that overlaps
  mind = apply(dens, 1, min)
  
  # The max of the overlap values calculated above is our estimate -- pull the
  # x-value associate with this
  est = xvals[which.max(mind)]
  
  # For ggplot: table of xvals and densities
  ovl = data.table(X = xvals, 
                   Density = mind, 
                   Level = "Overlap")
  dis = data.table(X = rep(xvals, 4), 
                   Density = as.vector(dens),
                   Level = rep(dat$Level, each = length(xvals)))
  doo = rbind(ovl, dis)
  doo$Level = factor(doo$Level, levels = c(dat$Level, "Overlap"))
  
  # ggplots: a full and a zoomed version
  full = ggplot(data = doo) +
    geom_line(aes(x = X, y = Density, color = Level)) +
    scale_color_manual(name = "Distribution",
                       values = c("blue","purple","forestgreen",
                                  "orange","black")) +
    labs(x = "Household energy",
         y = "Density",
         title = "Distributional overlap -- full")
  
  zoomed = full + 
    ylim(c(0, max(mind) * 5)) +
    labs(title = "Distributional overlap -- zoomed") +
    geom_point(data = data.frame(x = est, y = max(mind)),
               aes(x = x, y = y),
               color = "black", size = 3) +
    geom_area(data = doo[Level == "Overlap"],
              aes(x = X, y = Density, fill = "Overlap")) +
    scale_fill_manual(name = "Overlap",
                      values = "grey50")
  
  # Grid arrange
  p = grid.arrange(full, zoomed, ncol=1)
  
  # Done
  return(list(plot = p, data = dat, est = est))
}

n = sample(1:240, 1)
x = mo_plot(categories = categories,
            levels = uc[n,1:4] %>% unlist %>% unname %>% as.character(),
            estimates = iv,
            tolerance = 0.01)

# -----------------------------------------------------------------------------
# --------------------------------- For PMT -----------------------------------
# -----------------------------------------------------------------------------

cleaned_res_energy_consumption_path = file.path("K:/Projects/MiamiDade/PMT/",
                                                "Data/Cleaned/Energy_Consumption",
                                                "Residential_Energy_Consumption.csv")
parcels_path = "K:/Projects/MiamiDade/PMT/PMT_2019.gdb/Miami_2019"
save_directory = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Energy_Consumption"
id_field = "PARCELNO",
land_use_field = "DOR_UC",
living_area_field = "TOT_LVG_AREA",
year_built_field = "ACT_YR_BLT"
