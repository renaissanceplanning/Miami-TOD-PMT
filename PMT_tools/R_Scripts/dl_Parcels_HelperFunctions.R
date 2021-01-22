# ----Downloading Shapesfiles and loading----
url_shp_to_sf <- function(URL){
  require(sf)
  
  wd <- getwd()
  td <- tempdir()
  setwd(td)
  
  temp <- tempfile(fileext = ".zip")
  download.file(URL, temp)
  unzip(temp)
  
  shp <- dir(tempdir(), "*.shp$")
  y <- read_sf(shp)
  
  unlink(dir(td))
  setwd(wd)
  return(y)
}

# ----Downloading dbf and loading----

url_dbf_to_df <- function(URL){
  require(sf)
  require(foreign)
  
  wd <- getwd()
  td <- tempdir()
  setwd(td)
  
  temp <- tempfile(fileext = ".zip")
  download.file(URL, temp)
  unzip(temp)
  
  dbf <- dir(tempdir(), "*.dbf$")
  y <- read.dbf(dbf)
  
  unlink(dir(td))
  setwd(wd)
  return(y)
}

# ----Downloading csv and loading-----

url_csv_to_df <- function(URL){
  require(sf)
  require(readr)

  wd <- getwd()
  td <- tempdir()
  setwd(td)
  
  temp <- tempfile(fileext = ".zip")
  download.file(URL, temp)
  unzip(temp)
  
  csv <- dir(tempdir(), "*.csv$")
  y <- read_csv(csv)
  
  unlink(dir(td))
  setwd(wd)
  return(y)
}