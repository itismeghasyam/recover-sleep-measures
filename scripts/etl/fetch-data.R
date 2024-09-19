library(tidyverse)

list2env(
  x = config::get(config = "default"),
  envir = .GlobalEnv
)

login <- synapser::synLogin()

cat("Fetching data....")

# Get input files from synapse
selected_vars <- 
  recoverutils::syn_file_to_df(selectedVarsFileID) %>% 
  mutate(Lower_Bound = suppressWarnings(as.numeric(Lower_Bound)),
         Upper_Bound = suppressWarnings(as.numeric(Upper_Bound)))

# Get list of which datasets to use
dataset_name_filter <- 
  selected_vars %>% 
  dplyr::pull(Export) %>% 
  unique()

# Sync S3 bucket to local
token <- synapser::synGetStsStorageToken(
  entity = parquetDirId,
  permission = "read_only",
  output_format = "json")

bucket_path <- 
  paste0(
    token$bucket, '/', 
    token$baseKey, '/', 
    stringr::str_extract(archiveVersion, stringr::regex("[\\d]{4}-[\\d]{2}-[\\d]{2}")), '/'
  )
  
s3 <- 
  arrow::S3FileSystem$create(
    access_key = token$accessKeyId,
    secret_key = token$secretAccessKey,
    session_token = token$sessionToken,
    region="us-east-1"
  )

dataset_list <- 
  s3$GetFileInfo(
    arrow::FileSelector$create(
      base_dir = bucket_path, 
      recursive = FALSE
    )
  )

dataset_paths <- character()
for (dataset in dataset_list) {
  dataset_paths <- c(dataset_paths, dataset$path)
}

if (!dir.exists(outputDataDir)) {
  dir.create(outputDataDir)
}

if (!dir.exists(outputDataDirSleepSD)) {
  dir.create(outputDataDirSleepSD)
}

if (!dir.exists(outputDataDirSRI)) {
  dir.create(outputDataDirSRI)
}

if (!dir.exists(outputDataDirTimeInSleepStages)) {
  dir.create(outputDataDirTimeInSleepStages)
}

if (!dir.exists(outputDataDirPercentSleepStart)) {
  dir.create(outputDataDirPercentSleepStart)
}

cat("OK\n")
