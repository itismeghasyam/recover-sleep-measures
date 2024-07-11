library(tidyverse)

list2env(
  x = config::get(config = "default"),
  envir = .GlobalEnv
)

cat("----Fetching data----\n")

login <- synapser::synLogin()

# Get input files from synapse
concept_map <- 
  recoverutils::syn_file_to_df(ontologyFileID, "CONCEPT_CD") %>% 
  filter(CONCEPT_CD!="<null>")

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
  entity = parquetDirID,
  permission = "read_only",
  output_format = "json")

bucket_path <- 
  paste0(
    token$bucket, '/', 
    token$baseKey, '/', 
    stringr::str_extract(s3basekey, stringr::regex("[\\d]{4}-[\\d]{2}-[\\d]{2}")), '/'
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

# For use in process-data steps
concept_replacements_reversed <- recoverutils::vec_reverse(concept_replacements)

if (!dir.exists(outputConceptsDir)) {
  dir.create(outputConceptsDir)
}

cat("Finished fetching data\n\n")
