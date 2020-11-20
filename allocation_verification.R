library(magrittr)
library(data.table)
library(sf)
library(stringr)

year = 2014
gdb = file.path("K:/Projects/MiamiDade/PMT",
                paste0("PMT_", year, ".gdb"))

bg = st_read("blockgroup_for_alloc2",
             dsn = gdb,
             quiet = TRUE) %>%
  st_drop_geometry() %>%
  data.table()
alloc = fread("D:/Users/AZ7/Downloads/2014_Test_Data.csv")

alloc_sum = alloc %>%
  .[, -c("V1","ProcessID","Total_Commutes"), with=FALSE] %>%
  .[, lapply(.SD, function(x){sum(x, na.rm=TRUE)}), by="GEOID10"] %>%
  .[, GEOID10 := as.character(GEOID10)]
bg = bg %>%
  .[, -c("Year", "Shape_Area", "Shape_Length",
         "Total_Employment_Obs","Total_Population_Obs","Total_Commutes_Obs",
         "Total_Commutes_Pred"),
    with = FALSE] %>%
  .[, GEOID10 := as.character(GEOID10)]

vars = names(alloc_sum)
alloc_new_names = lapply(vars, function(x){
  if(str_detect(x, "Total_Emp|Total_Pop|Total_Comm")){
    x = paste0(x, "_Pred")
  } 
  y = str_replace_all(x, "_PAR","")
  y
}) %>% unlist
names(alloc_sum) = alloc_new_names
sum(sort(names(alloc_sum)) == sort(names(bg))) == ncol(bg)

j = merge(bg, alloc_sum, by="GEOID10", how="inner")

bg_tots = bg[, -"GEOID10", with=FALSE][, lapply(.SD, sum)] %>%
  data.table(Var = names(.),
             Val_BG = unlist(.)) %>%
  .[, .(Var, Val_BG)]
alloc_tots = alloc_sum[, -"GEOID10", with=FALSE][, lapply(.SD, sum)] %>%
  data.table(Var = names(.),
             Val_Alloc = unlist(.)) %>%
  .[, .(Var, Val_Alloc)]
tots = merge(bg_tots, alloc_tots, by="Var")
tots[, Off := round(abs(Val_Alloc - Val_BG), 3)]
tots[order(Off)]

n = names(j) %>% str_replace("\\.x|\\.y", "") %>% unique
n = n[-which(n == "GEOID10")]
x =lapply(n, function(x){
  bg_n = paste0(x, ".x")
  alloc_n = paste0(x, ".y")
  d = abs(unlist(j[,bg_n,with=FALSE]) - unlist(j[,alloc_n,with=FALSE]))
  data.table(Name = x,
             Min = min(d),
             Max = max(d))
}) %>% rbindlist
