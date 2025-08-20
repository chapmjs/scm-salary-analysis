# ==========================================
# SIMPLIFIED SCM SALARY ANALYSIS
# Single Occupation: Logisticians (2024)
# ==========================================

# Load required libraries
library(blsAPI)
library(tidyverse)
library(jsonlite)

# Load API key from config file
source("config.R")

# Verify API key
if(Sys.getenv("BLS_KEY") == "") {
  stop("BLS API key not found. Please check your config.R file.")
}

# ==========================================
# CONFIGURATION
# ==========================================

# Single occupation and year
occupation_code <- "13-1081"  # Logisticians
occupation_name <- "Logisticians"
analysis_year <- 2024

cat("Analyzing:", occupation_name, "for year", analysis_year, "\n")

# ==========================================
# DATA COLLECTION
# ==========================================

# Function to construct BLS OEWS series IDs
construct_series_ids <- function(occupation_code) {
  # Remove hyphen and pad to 6 digits
  clean_code <- sprintf("%06s", gsub("-", "", occupation_code))
  
  # OEWS series format: OE + U + N + 0000000 + 000000 + occupation + datatype
  base_id <- paste0("OEUN0000000000000", clean_code)
  
  # Correct data types: 01=employment, 04=mean_wage, 13=annual_median_wage
  series_ids <- paste0(base_id, c("01", "04", "13"))
  names(series_ids) <- c("employment", "mean_wage", "median_wage_annual")
  
  cat("Constructed series IDs:\n")
  for(i in 1:length(series_ids)) {
    cat("  ", names(series_ids)[i], ":", series_ids[i], "\n")
  }
  
  return(series_ids)
}

# Get data from BLS API
get_salary_data <- function(occupation_code, year) {
  series_ids <- construct_series_ids(occupation_code)
  
  # API payload
  payload <- list(
    'seriesid' = as.vector(series_ids),
    'startyear' = as.character(year),
    'endyear' = as.character(year),
    'registrationKey' = Sys.getenv("BLS_KEY")
  )
  
  # Make API call
  response <- blsAPI(payload, api_version = 2)
  json_data <- fromJSON(response)
  
  # Check if successful
  if(json_data$status != "REQUEST_SUCCEEDED") {
    stop("API request failed: ", json_data$message)
  }
  
  return(json_data)
}

# Fetch the data
cat("Fetching data from BLS API...\n")
raw_data <- get_salary_data(occupation_code, analysis_year)

# ==========================================
# DATA PROCESSING
# ==========================================

# Process API response into clean format
process_data <- function(api_response) {
  cat("Processing BLS API response...\n")
  
  series_df <- api_response$Results$series
  results <- list()
  
  # Process each series (row in the data.frame)
  for(i in 1:nrow(series_df)) {
    series_id <- series_df$seriesID[i]
    
    # Let's be more careful about what each series ID represents
    # OEWS data type codes:
    # 01 = Total employment
    # 04 = Mean annual wage
    # 10 = Annual 10th percentile wage
    # 11 = Annual 25th percentile wage
    # 12 = Hourly median wage
    # 13 = Annual median wage
    # 14 = Annual 75th percentile wage
    # 15 = Annual 90th percentile wage
    
    if(grepl("01$", series_id)) {
      data_type <- "employment"
    } else if(grepl("04$", series_id)) {
      data_type <- "mean_wage_annual" 
    } else if(grepl("10$", series_id)) {
      data_type <- "median_wage_annual"  # This should be annual median
    } else if(grepl("12$", series_id)) {
      data_type <- "median_wage_hourly"
    } else if(grepl("13$", series_id)) {
      data_type <- "median_wage_annual"
    } else {
      cat("Unknown series type:", series_id, "\n")
      next
    }
    
    cat("Processing", data_type, "(Series:", series_id, ")...\n")
    
    # Extract the data for this series
    series_data <- series_df$data[[i]]
    
    if(nrow(series_data) > 0) {
      value <- as.numeric(series_data$value[1])
      period <- series_data$period[1]
      
      cat("  Period:", period, "Value:", value, "\n")
      
      results[[data_type]] <- value
    }
  }
  
  cat("\nAll extracted values:\n")
  for(name in names(results)) {
    if(grepl("employment", name)) {
      cat(name, ":", scales::comma(results[[name]]), "workers\n")
    } else {
      cat(name, ": $", scales::comma(results[[name]]), "\n")
    }
  }
  
  return(results)
}

# Process the data
cat("Processing data...\n")
salary_data <- process_data(raw_data)

# ==========================================
# RESULTS
# ==========================================

# Create summary
create_summary <- function(data, occupation, year) {
  cat("\n", paste(rep("=", 50), collapse=""), "\n")
  cat("SALARY ANALYSIS SUMMARY\n")
  cat(paste(rep("=", 50), collapse=""), "\n")
  cat("Occupation:", occupation, "\n")
  cat("Year:", year, "\n")
  cat("Source: Bureau of Labor Statistics (OEWS)\n\n")
  
  cat("EMPLOYMENT & WAGES:\n")
  cat(paste(rep("-", 30), collapse=""), "\n")
  
  if(!is.null(data$employment)) {
    cat("Total Employment:", scales::comma(data$employment), "workers\n")
  }
  
  if(!is.null(data$median_wage)) {
    cat("Median Annual Wage:", scales::dollar(data$median_wage), "\n")
    cat("Median Hourly Wage:", scales::dollar(data$median_wage / 2080), "\n")
  }
  
  if(!is.null(data$mean_wage)) {
    cat("Mean Annual Wage:", scales::dollar(data$mean_wage), "\n")
    cat("Mean Hourly Wage:", scales::dollar(data$mean_wage / 2080), "\n")
  }
  
  # Calculate wage comparison if both available
  if(!is.null(data$median_wage) && !is.null(data$mean_wage)) {
    wage_ratio <- data$mean_wage / data$median_wage
    cat("\nWage Distribution:\n")
    cat(paste(rep("-", 20), collapse=""), "\n")
    cat("Mean/Median Ratio:", sprintf("%.2f", wage_ratio), "\n")
    if(wage_ratio > 1.1) {
      cat("Distribution: Right-skewed (some high earners)\n")
    } else if(wage_ratio < 0.9) {
      cat("Distribution: Left-skewed (wage compression)\n") 
    } else {
      cat("Distribution: Relatively symmetric\n")
    }
  }
  
  cat("\n", paste(rep("=", 50), collapse=""), "\n")
}

# Display results
create_summary(salary_data, occupation_name, analysis_year)

# ==========================================
# OPTIONAL: SAVE RESULTS
# ==========================================

# Save to CSV for further analysis
results_df <- data.frame(
  occupation = occupation_name,
  year = analysis_year,
  employment = salary_data$employment %||% NA,
  median_wage = salary_data$median_wage %||% NA,
  mean_wage = salary_data$mean_wage %||% NA,
  analysis_date = Sys.Date()
)

# Create output directory if it doesn't exist
if(!dir.exists("output")) dir.create("output")

# Save results
write_csv(results_df, "output/logisticians_salary_2024.csv")
cat("Results saved to: output/logisticians_salary_2024.csv\n")

# ==========================================
# QUICK COMPARISON FUNCTION (OPTIONAL)
# ==========================================

# Function to quickly analyze different occupations
quick_analysis <- function(occ_code, occ_name, year = 2024) {
  cat("\nQuick analysis for:", occ_name, "\n")
  
  tryCatch({
    data <- get_salary_data(occ_code, year)
    processed <- process_data(data)
    
    cat("Employment:", scales::comma(processed$employment %||% "N/A"), "\n")
    cat("Median Wage:", scales::dollar(processed$median_wage %||% 0), "\n")
    cat("Mean Wage:", scales::dollar(processed$mean_wage %||% 0), "\n\n")
    
  }, error = function(e) {
    cat("Error:", e$message, "\n\n")
  })
}

# Example: Compare with other SCM occupations
cat("\nCOMPARISON WITH OTHER SCM OCCUPATIONS:\n")
cat(paste(rep("=", 45), collapse=""), "\n")

# Uncomment to compare with other occupations:
# quick_analysis("11-3071", "Transportation Managers")
# quick_analysis("13-1023", "Purchasing Agents")  
# quick_analysis("11-3061", "Purchasing Managers")

cat("Analysis complete!\n")