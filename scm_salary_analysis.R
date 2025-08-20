# ==========================================
# COMPREHENSIVE SCM SALARY ANALYSIS
# Multiple Supply Chain Management Occupations
# ==========================================

# Load required libraries
library(blsAPI)
library(tidyverse)
library(jsonlite)
library(scales)

# Load API key from config file
source("config.R")

# Verify API key
if(Sys.getenv("BLS_KEY") == "") {
  stop("BLS API key not found. Please check your config.R file.")
}

# ==========================================
# CONFIGURATION
# ==========================================

# Define Supply Chain Management occupations
scm_occupations <- list(
  # Management Level
  "11-3061" = "Purchasing Managers",
  "11-3071" = "Transportation, Storage, and Distribution Managers", 
  "11-9199" = "Other Management Occupations",
  
  # Professional/Analytical
  "13-1081" = "Logisticians",
  "13-1023" = "Purchasing Agents (except Wholesale, Retail, Farm)",
  "13-1022" = "Wholesale and Retail Buyers (except Farm Products)",
  "13-1199" = "Other Business Operations Specialists",
  
  # Specialized/Support
  "43-5061" = "Production, Planning, and Expediting Clerks",
  "43-5071" = "Shipping, Receiving, and Traffic Clerks",
  "53-1047" = "Traffic Technicians"
)

analysis_year <- 2024

cat("Analyzing", length(scm_occupations), "SCM occupations for year", analysis_year, "\n")
cat("Occupations to analyze:\n")
for(code in names(scm_occupations)) {
  cat("  ", code, "-", scm_occupations[[code]], "\n")
}
cat("\n")

# ==========================================
# CORE FUNCTIONS
# ==========================================

# Function to construct BLS OEWS series IDs
construct_series_ids <- function(occupation_code) {
  # Remove hyphen and pad to 6 digits
  clean_code <- sprintf("%06s", gsub("-", "", occupation_code))
  
  # OEWS series format: OE + U + N + 0000000 + 000000 + occupation + datatype
  base_id <- paste0("OEUN0000000000000", clean_code)
  
  # Data types: 01=employment, 04=mean_wage, 13=annual_median_wage
  series_ids <- paste0(base_id, c("01", "04", "13"))
  names(series_ids) <- c("employment", "mean_wage", "median_wage")
  
  return(series_ids)
}

# Get data from BLS API with error handling
get_occupation_data <- function(occupation_code, year, max_retries = 3) {
  series_ids <- construct_series_ids(occupation_code)
  
  # API payload
  payload <- list(
    'seriesid' = as.vector(series_ids),
    'startyear' = as.character(year),
    'endyear' = as.character(year),
    'registrationKey' = Sys.getenv("BLS_KEY")
  )
  
  # Retry logic for API calls
  for(attempt in 1:max_retries) {
    tryCatch({
      response <- blsAPI(payload, api_version = 2)
      json_data <- fromJSON(response)
      
      if(json_data$status == "REQUEST_SUCCEEDED") {
        return(json_data)
      } else {
        warning(paste("API request failed for", occupation_code, ":", json_data$message))
        if(attempt == max_retries) return(NULL)
      }
    }, error = function(e) {
      warning(paste("Error fetching data for", occupation_code, "on attempt", attempt, ":", e$message))
      if(attempt == max_retries) return(NULL)
      Sys.sleep(1) # Brief pause before retry
    })
  }
  return(NULL)
}

# Process API response into clean format
process_occupation_data <- function(api_response, occupation_code, occupation_name) {
  if(is.null(api_response)) {
    return(data.frame(
      occupation_code = occupation_code,
      occupation_name = occupation_name,
      employment = NA,
      median_wage = NA,
      mean_wage = NA,
      data_available = FALSE
    ))
  }
  
  series_df <- api_response$Results$series
  results <- list(employment = NA, median_wage = NA, mean_wage = NA)
  
  # Process each series
  for(i in 1:nrow(series_df)) {
    series_id <- series_df$seriesID[i]
    series_data <- series_df$data[[i]]
    
    if(nrow(series_data) > 0) {
      value <- as.numeric(series_data$value[1])
      
      if(grepl("01$", series_id)) {
        results$employment <- value
      } else if(grepl("04$", series_id)) {
        results$mean_wage <- value
      } else if(grepl("13$", series_id)) {
        results$median_wage <- value
      }
    }
  }
  
  return(data.frame(
    occupation_code = occupation_code,
    occupation_name = occupation_name,
    employment = results$employment,
    median_wage = results$median_wage,
    mean_wage = results$mean_wage,
    data_available = !all(is.na(c(results$employment, results$median_wage, results$mean_wage)))
  ))
}

# ==========================================
# BATCH DATA COLLECTION
# ==========================================

# Function to analyze all occupations
analyze_all_occupations <- function(occupations_list, year) {
  cat("Starting batch analysis of", length(occupations_list), "occupations...\n")
  
  all_results <- list()
  successful <- 0
  failed <- 0
  
  for(i in seq_along(occupations_list)) {
    code <- names(occupations_list)[i]
    name <- occupations_list[[code]]
    
    cat(sprintf("(%d/%d) Analyzing: %s - %s\n", i, length(occupations_list), code, name))
    
    # Add small delay to be respectful to API
    if(i > 1) Sys.sleep(0.5)
    
    # Get and process data
    raw_data <- get_occupation_data(code, year)
    processed_data <- process_occupation_data(raw_data, code, name)
    
    all_results[[i]] <- processed_data
    
    if(processed_data$data_available) {
      successful <- successful + 1
      cat("  ✓ Success: Employment =", comma(processed_data$employment %||% 0), 
          ", Median =", dollar(processed_data$median_wage %||% 0), "\n")
    } else {
      failed <- failed + 1
      cat("  ✗ Failed: No data available\n")
    }
  }
  
  cat(sprintf("\nBatch analysis complete: %d successful, %d failed\n", successful, failed))
  
  # Combine all results
  final_results <- do.call(rbind, all_results)
  return(final_results)
}

# ==========================================
# DATA COLLECTION
# ==========================================

cat("Fetching data from BLS API...\n")
scm_data <- analyze_all_occupations(scm_occupations, analysis_year)

# ==========================================
# ANALYSIS AND REPORTING
# ==========================================

# Add calculated fields
scm_data <- scm_data %>%
  mutate(
    median_hourly = median_wage / 2080,
    mean_hourly = mean_wage / 2080,
    wage_ratio = mean_wage / median_wage,
    wage_distribution = case_when(
      wage_ratio > 1.15 ~ "Right-skewed (high earners)",
      wage_ratio < 0.85 ~ "Left-skewed (compressed)",
      TRUE ~ "Relatively symmetric"
    ),
    occupation_level = case_when(
      str_detect(occupation_code, "^11-") ~ "Management",
      str_detect(occupation_code, "^13-") ~ "Professional/Analytical", 
      str_detect(occupation_code, "^43-|^53-") ~ "Support/Specialized",
      TRUE ~ "Other"
    )
  ) %>%
  arrange(desc(median_wage))

# ==========================================
# COMPREHENSIVE REPORTING
# ==========================================

# Summary statistics
create_comprehensive_report <- function(data) {
  cat("\n", paste(rep("=", 70), collapse=""), "\n")
  cat("COMPREHENSIVE SCM SALARY ANALYSIS REPORT\n")
  cat(paste(rep("=", 70), collapse=""), "\n")
  cat("Analysis Date:", format(Sys.Date(), "%B %d, %Y"), "\n")
  cat("Data Year:", analysis_year, "\n")
  cat("Source: Bureau of Labor Statistics (OEWS)\n")
  cat("Occupations Analyzed:", nrow(data), "\n")
  cat("Occupations with Data:", sum(data$data_available), "\n\n")
  
  # Filter to available data
  available_data <- data %>% filter(data_available == TRUE)
  
  if(nrow(available_data) == 0) {
    cat("No data available for analysis.\n")
    return()
  }
  
  # Overall SCM market summary
  total_employment <- sum(available_data$employment, na.rm = TRUE)
  weighted_median <- weighted.mean(available_data$median_wage, available_data$employment, na.rm = TRUE)
  
  cat("OVERALL SCM MARKET SUMMARY:\n")
  cat(paste(rep("-", 35), collapse=""), "\n")
  cat("Total SCM Employment:", comma(total_employment), "workers\n")
  cat("Employment-Weighted Median Wage:", dollar(weighted_median), "\n")
  cat("Wage Range:", dollar(min(available_data$median_wage, na.rm=TRUE)), 
      "to", dollar(max(available_data$median_wage, na.rm=TRUE)), "\n\n")
  
  # By occupation level
  cat("ANALYSIS BY OCCUPATION LEVEL:\n")
  cat(paste(rep("-", 35), collapse=""), "\n")
  
  level_summary <- available_data %>%
    group_by(occupation_level) %>%
    summarise(
      count = n(),
      total_employment = sum(employment, na.rm = TRUE),
      median_wage_avg = mean(median_wage, na.rm = TRUE),
      median_wage_range = paste(dollar(min(median_wage, na.rm=TRUE)), 
                                "to", dollar(max(median_wage, na.rm=TRUE))),
      .groups = 'drop'
    )
  
  for(i in 1:nrow(level_summary)) {
    level <- level_summary$occupation_level[i]
    cat(sprintf("%s:\n", level))
    cat(sprintf("  Occupations: %d\n", level_summary$count[i]))
    cat(sprintf("  Employment: %s\n", comma(level_summary$total_employment[i])))
    cat(sprintf("  Avg Median Wage: %s\n", dollar(level_summary$median_wage_avg[i])))
    cat(sprintf("  Wage Range: %s\n\n", level_summary$median_wage_range[i]))
  }
  
  # Top paying occupations
  cat("TOP 5 HIGHEST PAYING SCM OCCUPATIONS:\n")
  cat(paste(rep("-", 45), collapse=""), "\n")
  
  top_5 <- available_data %>% 
    arrange(desc(median_wage)) %>% 
    head(5)
  
  for(i in 1:nrow(top_5)) {
    cat(sprintf("%d. %s\n", i, top_5$occupation_name[i]))
    cat(sprintf("   Median: %s | Mean: %s | Employment: %s\n", 
                dollar(top_5$median_wage[i]),
                dollar(top_5$mean_wage[i]), 
                comma(top_5$employment[i])))
    cat(sprintf("   Distribution: %s\n\n", top_5$wage_distribution[i]))
  }
  
  cat(paste(rep("=", 70), collapse=""), "\n")
}

# Generate the report
create_comprehensive_report(scm_data)

# ==========================================
# DETAILED DATA TABLE
# ==========================================

# Print detailed table
cat("\nDETAILED OCCUPATION DATA:\n")
cat(paste(rep("=", 90), collapse=""), "\n")

detailed_table <- scm_data %>%
  filter(data_available == TRUE) %>%
  select(occupation_code, occupation_name, occupation_level, 
         employment, median_wage, mean_wage, wage_distribution) %>%
  arrange(desc(median_wage))

print(detailed_table)

# ==========================================
# DATA EXPORT
# ==========================================

# Create output directory
if(!dir.exists("output")) dir.create("output")

# Save comprehensive results
write_csv(scm_data, paste0("output/scm_salary_analysis_", analysis_year, ".csv"))

# Save summary by level
level_summary <- scm_data %>%
  filter(data_available == TRUE) %>%
  group_by(occupation_level) %>%
  summarise(
    occupations_count = n(),
    total_employment = sum(employment, na.rm = TRUE),
    median_wage_avg = mean(median_wage, na.rm = TRUE),
    median_wage_min = min(median_wage, na.rm = TRUE),
    median_wage_max = max(median_wage, na.rm = TRUE),
    mean_wage_avg = mean(mean_wage, na.rm = TRUE),
    .groups = 'drop'
  )

write_csv(level_summary, paste0("output/scm_level_summary_", analysis_year, ".csv"))

cat("\nFiles saved:\n")
cat("- output/scm_salary_analysis_", analysis_year, ".csv\n")
cat("- output/scm_level_summary_", analysis_year, ".csv\n")

# ==========================================
# OPTIONAL: COMPARISON FUNCTIONS
# ==========================================

# Function to compare specific occupations
compare_occupations <- function(codes_to_compare) {
  comparison_data <- scm_data %>%
    filter(occupation_code %in% codes_to_compare, data_available == TRUE) %>%
    arrange(desc(median_wage))
  
  cat("\nOCCUPATION COMPARISON:\n")
  cat(paste(rep("-", 40), collapse=""), "\n")
  
  for(i in 1:nrow(comparison_data)) {
    cat(sprintf("%s:\n", comparison_data$occupation_name[i]))
    cat(sprintf("  Median Wage: %s\n", dollar(comparison_data$median_wage[i])))
    cat(sprintf("  Employment: %s\n", comma(comparison_data$employment[i])))
    cat(sprintf("  Level: %s\n\n", comparison_data$occupation_level[i]))
  }
}

# Example comparison - uncomment to use
# compare_occupations(c("13-1081", "11-3061", "13-1023"))

# Function to add custom occupations
add_custom_occupation <- function(code, name) {
  cat("Analyzing custom occupation:", name, "\n")
  
  raw_data <- get_occupation_data(code, analysis_year)
  processed_data <- process_occupation_data(raw_data, code, name)
  
  if(processed_data$data_available) {
    cat("Results for", name, ":\n")
    cat("  Employment:", comma(processed_data$employment), "\n")
    cat("  Median Wage:", dollar(processed_data$median_wage), "\n")
    cat("  Mean Wage:", dollar(processed_data$mean_wage), "\n")
  } else {
    cat("No data available for", name, "\n")
  }
  
  return(processed_data)
}

cat("\n✓ Analysis complete! Check the output folder for detailed results.\n")