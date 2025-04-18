source("scripts/etl/fetch-data.R")

fitbit_sleeplogs <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "sleeplogs$"))
  )

fitbit_sleeplogdetails <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "sleeplogdetails"))
  )

vars <- 
  selected_vars %>% 
  filter(str_detect(Export, "sleeplogs$")) %>% 
  pull(Variable)

###########  Required functions
## RMHDR-270 [https://sagebionetworks.jira.com/browse/RMHDR-270]
getSleepEfficiency <- function(Type,
                               SleepLevelLight, SleepLevelDeep, SleepLevelRem, SleepLevelWake,
                               SleepLevelAwake, SleepLevelAsleep, SleepLevelRestless){
  # we expect all vars to be numeric except Type. So cast them into numeric
  SleepLevelLight <- as.numeric(SleepLevelLight)
  SleepLevelDeep <- as.numeric(SleepLevelDeep)
  SleepLevelRem <- as.numeric(SleepLevelRem)
  SleepLevelWake <- as.numeric(SleepLevelWake)
  SleepLevelAwake <- as.numeric(SleepLevelAwake)
  SleepLevelAsleep <- as.numeric(SleepLevelAsleep)
  SleepLevelRestless<- as.numeric(SleepLevelRestless)
  
  
  # Type == 'classic'
  if(Type == 'classic'){
    sleep_efficiency <- tryCatch(sum(SleepLevelAsleep,SleepLevelRestless,na.rm = T)/sum(SleepLevelAwake,SleepLevelAsleep,SleepLevelRestless,na.rm = T),
                                 error = function(e){NA})
  }
  
  if(Type == 'stages'){
    sleep_efficiency <- tryCatch(sum(SleepLevelLight,SleepLevelDeep,SleepLevelRem,na.rm = T)/sum(SleepLevelLight,SleepLevelDeep,SleepLevelRem,SleepLevelWake,na.rm = T),
                                 error = function(e){NA})
  }
  
  # convert sleep efficiency into percentage and round it to an integer
  sleep_efficiency <- ifelse(Type %in% c('classic','stages'), round(100*sleep_efficiency), NA)
  
  return(sleep_efficiency)
  
}

WeeklyMeans <- function(dat) {
  dat$Date <- lubridate::as_date(dat$Date)
  
  ## for each date we first find the start and end dates of each week 
  ## (each week starts at a Sunday and ends at a Saturday)
  
  ## for each date, find the date of the day of that week (always a Sunday) 
  dat$week_startdate <- lubridate::floor_date(dat$Date, unit = "week", week_start = 7)
  
  ## find the date of the weeks last day (always a Saturday) 
  dat$week_enddate <- dat$week_startdate + lubridate::days(6)
  
  ## get the unique combinations of participant ids and week start dates
  dat$aux <- paste(dat$ParticipantIdentifier, dat$week_startdate, sep = "_")
  ucomb <- unique(dat$aux)
  nrec <- length(ucomb)
  out <- data.frame(matrix(NA, nrec, 4))
  variable_names <- c(
    "Efficiency",
    "Efficiency_computed"
  )
  names(out) <- c("ParticipantIdentifier", 
                  "Date", 
                  variable_names)
  for (i in seq(nrec)) {
    cat(i, "\n")
    
    sdat <- dat[dat$aux == ucomb[i],]
    out[i, "ParticipantIdentifier"] <- sdat[1, "ParticipantIdentifier"]
    
    ## we set Date to the week's start date (always a Sunday)
    out[i, "Date"] <- as.character(sdat[1, "week_startdate"][[1]])
    
    out[i, variable_names] <- apply(sdat[, variable_names], 2, mean, na.rm = TRUE)
  }
  
  return(out)
}




# Load the desired subset of the dataset in memory and do some feature 
# engineering for derived variables
sleeplogs_df <- 
  fitbit_sleeplogs %>% 
  select(all_of(c(vars, "LogId", "Type"))) %>% # need Type for Sleep Efficiency Calculation 
  collect() %>% 
  distinct() %>% 
  mutate(
    Date = lubridate::as_date(StartDate),
    # Duration = as.numeric(Duration),
    Efficiency = as.numeric(Efficiency),
    IsMainSleep = as.logical(IsMainSleep),
    # SleepStartTime = lubridate::as_datetime(ifelse(IsMainSleep==TRUE, StartDate, NA)),
    # SleepEndTime = lubridate::as_datetime(ifelse(IsMainSleep==TRUE, EndDate, NA)),
    # MidSleep = format((lubridate::as_datetime(SleepStartTime) + ((Duration/1000)/2)), format = "%H:%M:%S"),
    # SleepStartTime = 
    #   ((format(SleepStartTime, format = "%H:%M:%S") %>% lubridate::hms()) / lubridate::hours(24))*24,
    # SleepEndTime = 
    #   ((format(SleepEndTime, format = "%H:%M:%S") %>% lubridate::hms()) / lubridate::hours(24))*24,
    # MidSleep = 24*lubridate::hms(MidSleep)/lubridate::hours(24)
  ) %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(
    Efficiency_computed = getSleepEfficiency(Type, 
                                             SleepLevelLight, SleepLevelDeep, SleepLevelRem, SleepLevelWake,
                                             SleepLevelAwake, SleepLevelAsleep, SleepLevelRestless)
  ) %>% 
  dplyr::ungroup() %>% 
  dplyr::select(ParticipantIdentifier,
                Date,
                IsMainSleep,
                Efficiency,
                Efficiency_computed,
                LogId,
                Type)


## write the per record recomputed sleep efficiencies to synapse
write_csv(
  x = sleeplogs_df, 
  file = file.path(outputDataDir, "per_record_sleep_efficiencies.csv")
)

f <- 
  synapser::synStore(
    synapser::File(
      path = str_subset(list.files(outputDataDir, full.names = T), 
                        "per_record_sleep_efficiencies.csv"),
      parent = dailyMeasuresSynDirId
    ), 
    used = parquetDirId
  )



#pdf("Rplot.pdf", width = 10, height = 5.5)
par(mfrow = c(1, 2))
plot(sleeplogs_df$Efficiency, sleeplogs_df$Efficiency_computed, xlab = "Efficiency from i2b2",
     ylab = "Efficiency from syn64574665")
abline(a = 0, b = 1, col = "red")
####
plot(mdat$Efficiency, mdat$Efficiency_computed, xlab = "mean weekly Efficiency",
     ylab = "mean weekly Efficiency_computed")
abline(a = 0, b = 1, col = "red")
#dev.off()


## compute the weekly means of the variables
dat <- WeeklyMeans(dat = sleeplogs_df)
dim(dat)
output <- dat


write_csv(
  x = output, 
  file = file.path(outputDataDir, "weekly_sleep_efficiencies.csv")
)

f <- 
  synapser::synStore(
    synapser::File(
      path = str_subset(list.files(outputDataDir, full.names = T), 
                        "weekly_sleep_efficiencies.csv"),
      parent = dailyMeasuresSynDirId
    ), 
    used = parquetDirId
  )
