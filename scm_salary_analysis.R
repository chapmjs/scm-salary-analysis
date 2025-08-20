# ==========================================
# SUPPLY CHAIN MANAGEMENT SALARY ANALYSIS
# Single File Implementation for Initial Development
# ==========================================

# ==========================================
# PHASE 1: SETUP AND CONFIGURATION
# ==========================================

# Source configuration file (contains API key and package setup)
source("config.R")

# Load additional required library for JSON parsing
library(rjson)

# Check if toJSON is available, if not load it
if(!exists("toJSON")) {
  library(jsonlite)
}

# Verify API key is loaded
if(Sys.getenv("BLS_KEY") == "") {
  stop("BLS API key not found. Please check your config.R file.")
} else {
  cat("✓ BLS API key loaded successfully\n")
}

# Create output directories if they don't exist
if(!dir.exists("data")) dir.create("data")
if(!dir.exists("data/raw")) dir.create("data/raw")
if(!dir.exists("data/processed")) dir.create("data/processed")
if(!dir.exists("output")) dir.create("output")
if(!dir.exists("output/plots")) dir.create("output/plots")
if(!dir.exists("output/reports")) dir.create("output/reports")

# Define SCM occupation codes and names
scm_occupations <- c(
  "11-3071" = "Transportation, Storage, and Distribution Managers",
  "13-1081" = "Logisticians",
  "13-1199" = "Business Operations Specialists, All Other",
  "43-5011" = "Cargo and Freight Agents",
  "43-5061" = "Production, Planning, and Expediting Clerks",
  "11-3061" = "Purchasing Managers",
  "13-1023" = "Purchasing Agents",
  "13-1022" = "Wholesale and Retail Buyers",
  "15-2031" = "Operations Research Analysts",
  "17-2112" = "Industrial Engineers"
)

# Analysis parameters
years <- 2021:2024
base_year <- max(years)  # For inflation adjustment

cat("✓ Setup complete. Analyzing", length(scm_occupations), "occupations from", min(years), "to", max(years), "\n")

# ==========================================
# PHASE 2: DATA COLLECTION FUNCTIONS
# ==========================================

# Function to construct BLS OEWS series IDs
construct_oews_series <- function(occupation_code) {
  # Remove hyphen from occupation code and ensure it's 6 digits
  clean_code <- gsub("-", "", occupation_code)
  # Pad with zeros if needed to make 6 digits
  clean_code <- sprintf("%06s", clean_code)
  
  # OEWS series ID format: OEUN[area][industry][occupation][data_type]
  # From the Hashrocket example: OEUN000000000000[6-digit-code][2-digit-data-type]
  # But looking at our error, we need fewer zeros
  # The format should be: OEU + N + 000000 + 000000 + [6-digit-occupation] + [2-digit-data-type]
  # Total: OEU(3) + N(1) + 000000(6) + 000000(6) + occupation(6) + data(2) = 24 characters
  
  # Let's try the correct format: OEUN000000000000 + occupation + data_type
  # Our current format has: OEUN + 14 zeros + occupation + data = too many zeros
  # Correct format should be: OEUN + 12 zeros + occupation + data
  
  # Try the data types that are most commonly available
  # 01 = employment, 04 = mean wage, 10 = median wage
  data_types <- c("01", "04", "10")
  
  # Correct format based on BLS documentation
  series_ids <- paste0("OEUN000000000000", clean_code, data_types)
  names(series_ids) <- c("employment", "mean_wage", "median_wage")
  
  return(series_ids)
}

# Function to pull OEWS data for a single occupation
get_oews_data <- function(occupation_code, occupation_name, years) {
  cat("Fetching data for:", occupation_name, "\n")
  
  series_ids <- construct_oews_series(occupation_code)
  cat("  Series IDs:", paste(series_ids, collapse = ", "), "\n")
  
  # Make API call using payload format
  tryCatch({
    # Create payload for API v2 (requires registration key)
    payload <- list(
      'seriesid' = as.vector(series_ids),  # Ensure it's a vector, not named vector
      'startyear' = as.character(min(years)),  # Convert to string
      'endyear' = as.character(max(years)),    # Convert to string
      'registrationKey' = Sys.getenv("BLS_KEY")
    )
    
    cat("  Payload:", toJSON(payload), "\n")
    
    api_response <- blsAPI(payload, api_version = 2)
    
    # Parse JSON response
    json_response <- fromJSON(api_response)
    
    # Check if API call was successful
    if(is.null(json_response) || json_response$status != "REQUEST_SUCCEEDED") {
      warning("API request failed for ", occupation_name, ": ", 
              ifelse(is.null(json_response$message), "Unknown error", json_response$message))
      cat("  Full response:", api_response, "\n")
      return(NULL)
    }
    
    return(list(data = json_response, occupation = occupation_name, code = occupation_code))
    
  }, error = function(e) {
    warning("Error fetching data for ", occupation_name, ": ", e$message)
    return(NULL)
  })
}

# Function to get CPI data for inflation adjustment
get_cpi_data <- function(years) {
  cat("Fetching CPI data for inflation adjustment...\n")
  
  cpi_series <- "CUUR0000SA0"  # CPI-U All items, seasonally adjusted
  
  tryCatch({
    # Create payload for API v2
    payload <- list(
      'seriesid' = c(cpi_series),
      'startyear' = as.character(min(years)),  # Convert to string
      'endyear' = as.character(max(years)),    # Convert to string
      'registrationKey' = Sys.getenv("BLS_KEY")
    )
    
    api_response <- blsAPI(payload, api_version = 2)
    
    # Parse JSON response
    json_response <- fromJSON(api_response)
    
    if(is.null(json_response) || json_response$status != "REQUEST_SUCCEEDED") {
      warning("CPI API request failed: ", 
              ifelse(is.null(json_response$message), "Unknown error", json_response$message))
      return(NULL)
    }
    
    return(json_response)
    
  }, error = function(e) {
    warning("Error fetching CPI data: ", e$message)
    return(NULL)
  })
}

# ==========================================
# PHASE 2: DATA COLLECTION EXECUTION
# ==========================================

cat("\n=== STARTING DATA COLLECTION ===\n")

# For initial testing, let's start with just one occupation to debug
test_occupation_code <- "13-1081"  # Logisticians
test_occupation_name <- "Logisticians"

cat("Testing with single occupation first:", test_occupation_name, "\n")
cat("Original occupation code:", test_occupation_code, "\n")

# Debug the series ID construction step by step
clean_code <- gsub("-", "", test_occupation_code)
cat("After removing hyphen:", clean_code, "\n")

padded_code <- sprintf("%06s", clean_code)
cat("After padding to 6 digits:", padded_code, "\n")

# Test the series ID construction
test_series <- construct_oews_series(test_occupation_code)
cat("Generated series IDs:", paste(test_series, collapse = ", "), "\n")

# Let's also manually construct what we think it should be
expected_series <- paste0("OEUN000000000000", padded_code, c("01", "04", "10"))
cat("Expected series IDs:", paste(expected_series, collapse = ", "), "\n")

# Test single occupation first
test_data <- get_oews_data(test_occupation_code, test_occupation_name, years)

if(!is.null(test_data)) {
  cat("✓ Test successful! Proceeding with all occupations...\n")
  
  # Collect data for all SCM occupations
  scm_data_raw <- list()
  
  # Add the successful test data
  scm_data_raw[[test_occupation_code]] <- test_data
  
  # Continue with remaining occupations
  for(i in seq_along(scm_occupations)) {
    occupation_code <- names(scm_occupations)[i]
    occupation_name <- scm_occupations[i]
    
    # Skip the test occupation since we already have it
    if(occupation_code == test_occupation_code) next
    
    data <- get_oews_data(occupation_code, occupation_name, years)
    if(!is.null(data)) {
      scm_data_raw[[occupation_code]] <- data
    }
    
    # Be nice to the API - small delay between requests
    Sys.sleep(0.5)
  }
} else {
  cat("✗ Test failed. Please check the series ID format and API key.\n")
  cat("Debug: Manually check if series ID", test_series[1], "exists in BLS database\n")
  stop("Cannot proceed without successful API connection")
}

# Get CPI data
cpi_data_raw <- get_cpi_data(years)

# Check what we actually collected
cat("\n=== DATA COLLECTION SUMMARY ===\n")
cat("Occupations with data:", length(scm_data_raw), "\n")
cat("CPI data available:", !is.null(cpi_data_raw), "\n")

if(length(scm_data_raw) == 0) {
  cat("✗ No occupation data collected. Check API key and series IDs.\n")
  stop("Cannot proceed without occupation data")
}

# Save raw data
save(scm_data_raw, cpi_data_raw, years, file = "data/raw/bls_raw_data.RData")
cat("✓ Raw data saved to data/raw/bls_raw_data.RData\n")

cat("✓ Data collection complete\n")

# ==========================================
# PHASE 3: DATA PROCESSING FUNCTIONS
# ==========================================

# Function to process BLS API response into tidy format
process_oews_series <- function(api_data) {
  occupation_name <- api_data$occupation
  
  cat("Processing data for:", occupation_name, "\n")
  
  # Check if we have valid data structure
  if(is.null(api_data$data) || is.null(api_data$data$Results) || is.null(api_data$data$Results$series)) {
    warning("Invalid data structure for ", occupation_name)
    return(tibble())
  }
  
  # Extract all series data from the JSON response
  all_series <- api_data$data$Results$series
  
  cat("  Found", length(all_series), "data series\n")
  
  if(length(all_series) == 0) {
    warning("No data series found for ", occupation_name)
    return(tibble())
  }
  
  # Process each series
  processed_data <- map_dfr(all_series, function(series) {
    series_id <- series$seriesID
    
    cat("    Processing series:", series_id, "\n")
    
    # Extract data type from series ID
    data_type <- case_when(
      str_detect(series_id, "01$") ~ "employment",
      str_detect(series_id, "04$") ~ "mean_wage",
      str_detect(series_id, "10$") ~ "median_wage", 
      str_detect(series_id, "11$") ~ "pct25_wage",
      str_detect(series_id, "12$") ~ "pct75_wage",
      str_detect(series_id, "13$") ~ "pct90_wage",
      TRUE ~ "unknown"
    )
    
    cat("      Data type:", data_type, "\n")
    
    # Check if series has data
    if(is.null(series$data) || length(series$data) == 0) {
      warning("No data points in series ", series_id)
      return(tibble())
    }
    
    cat("      Data points:", length(series$data), "\n")
    
    # Extract time series data
    series_data <- tibble(
      series_id = series_id,
      data_type = data_type,
      year = as.numeric(map_chr(series$data, "year")),
      period = map_chr(series$data, "period"),
      value = as.numeric(map_chr(series$data, "value")),
      footnotes = map_chr(series$data, function(x) ifelse(is.null(x$footnotes), "", x$footnotes))
    )
    
    # Filter for annual data
    annual_data <- series_data %>% filter(period == "M13")
    cat("      Annual data points:", nrow(annual_data), "\n")
    
    return(annual_data)
  })
  
  if(nrow(processed_data) == 0) {
    warning("No processed data for ", occupation_name)
    return(tibble())
  }
  
  # Pivot and clean
  final_data <- processed_data %>%
    select(-series_id, -period, -footnotes) %>%
    pivot_wider(names_from = data_type, values_from = value) %>%
    mutate(occupation = occupation_name) %>%
    select(occupation, year, everything())
  
  cat("  Final data dimensions:", nrow(final_data), "x", ncol(final_data), "\n")
  cat("  Columns:", paste(colnames(final_data), collapse = ", "), "\n")
  
  return(final_data)
}

# Function to process CPI data
process_cpi_data <- function(cpi_raw) {
  if(is.null(cpi_raw)) return(NULL)
  
  cpi_series <- cpi_raw$Results$series[[1]]
  
  tibble(
    year = as.numeric(map_chr(cpi_series$data, "year")),
    period = map_chr(cpi_series$data, "period"),
    cpi = as.numeric(map_chr(cpi_series$data, "value"))
  ) %>%
    filter(period == "M13") %>%  # Annual average
    select(year, cpi)
}

# ==========================================
# PHASE 3: DATA PROCESSING EXECUTION
# ==========================================

cat("\n=== STARTING DATA PROCESSING ===\n")

# Check if we have data to process
if(length(scm_data_raw) == 0) {
  cat("✗ No raw data available for processing. Check data collection step.\n")
  stop("Cannot process data - no raw data available")
}

# Process all occupation data
cat("Processing", length(scm_data_raw), "occupations...\n")
scm_processed <- map_dfr(scm_data_raw, process_oews_series)

# Check processed data
cat("Processed data dimensions:", nrow(scm_processed), "rows,", ncol(scm_processed), "columns\n")
cat("Columns:", paste(colnames(scm_processed), collapse = ", "), "\n")

if(nrow(scm_processed) == 0) {
  cat("✗ No data processed successfully. Check API responses.\n")
  stop("Cannot proceed - no processed data available")
}

# Process CPI data
cpi_processed <- process_cpi_data(cpi_data_raw)

# Apply inflation adjustment
cat("Applying inflation adjustment to", base_year, "dollars...\n")

if(!is.null(cpi_processed)) {
  base_cpi <- cpi_processed$cpi[cpi_processed$year == base_year]
  
  scm_inflation_adjusted <- scm_processed %>%
    left_join(cpi_processed, by = "year") %>%
    mutate(
      inflation_factor = base_cpi / cpi,
      across(ends_with("_wage"), ~ .x * inflation_factor, .names = "real_{.col}")
    ) %>%
    select(-inflation_factor, -cpi)
} else {
  warning("CPI data not available - using nominal wages only")
  scm_inflation_adjusted <- scm_processed %>%
    mutate(across(ends_with("_wage"), ~ .x, .names = "real_{.col}"))
}

# Check final data structure
cat("Final data dimensions:", nrow(scm_inflation_adjusted), "rows,", ncol(scm_inflation_adjusted), "columns\n")
cat("Final columns:", paste(colnames(scm_inflation_adjusted), collapse = ", "), "\n")

# Save processed data
write_csv(scm_inflation_adjusted, "data/processed/scm_salary_data.csv")
save(scm_inflation_adjusted, cpi_processed, file = "data/processed/processed_data.RData")

cat("✓ Data processing complete\n")
cat("✓ Processed data saved to data/processed/\n")

# Data quality check
cat("\nData Quality Summary:\n")
cat("- Occupations processed:", length(unique(scm_inflation_adjusted$occupation)), "\n")
cat("- Years covered:", paste(range(scm_inflation_adjusted$year, na.rm = TRUE), collapse = "-"), "\n")
cat("- Total records:", nrow(scm_inflation_adjusted), "\n")

# Check for wage data availability
if("real_median_wage" %in% colnames(scm_inflation_adjusted)) {
  cat("- Missing median wage data:", sum(is.na(scm_inflation_adjusted$real_median_wage)), "\n")
} else {
  cat("- ✗ No median wage data available\n")
}

if("real_mean_wage" %in% colnames(scm_inflation_adjusted)) {
  cat("- Missing mean wage data:", sum(is.na(scm_inflation_adjusted$real_mean_wage)), "\n")
} else {
  cat("- ✗ No mean wage data available\n")
}

# ==========================================
# PHASE 4: ANALYSIS FUNCTIONS
# ==========================================

# Function to calculate CAGR
calculate_cagr <- function(start_value, end_value, periods) {
  if(is.na(start_value) || is.na(end_value) || periods == 0) return(NA)
  ((end_value / start_value)^(1/periods)) - 1
}

# Function to perform trend regression
perform_trend_regression <- function(data, wage_column) {
  if(sum(!is.na(data[[wage_column]])) < 2) return(tibble(annual_change = NA, se = NA, p_value = NA))
  
  tryCatch({
    model <- lm(reformulate("year", response = wage_column), data = data)
    result <- tidy(model) %>%
      filter(term == "year") %>%
      select(estimate, std.error, p.value) %>%
      rename(annual_change = estimate, se = std.error, p_value = p.value)
    
    if(nrow(result) == 0) {
      return(tibble(annual_change = NA, se = NA, p_value = NA))
    }
    return(result)
  }, error = function(e) {
    return(tibble(annual_change = NA, se = NA, p_value = NA))
  })
}

# ==========================================
# PHASE 4: ANALYSIS EXECUTION  
# ==========================================

cat("\n=== STARTING ANALYSIS ===\n")

# Check if we have processed data with required columns
if(!exists("scm_inflation_adjusted") || nrow(scm_inflation_adjusted) == 0) {
  cat("✗ No processed data available for analysis. Check data processing step.\n")
  stop("Cannot perform analysis - no processed data available")
}

# Check for required columns
required_columns <- c("occupation", "year", "real_median_wage")
missing_columns <- required_columns[!required_columns %in% colnames(scm_inflation_adjusted)]

if(length(missing_columns) > 0) {
  cat("✗ Missing required columns:", paste(missing_columns, collapse = ", "), "\n")
  cat("Available columns:", paste(colnames(scm_inflation_adjusted), collapse = ", "), "\n")
  stop("Cannot perform analysis - missing required data columns")
}

# 1. CAGR Analysis
cat("Calculating Compound Annual Growth Rates...\n")
cagr_analysis <- scm_inflation_adjusted %>%
  group_by(occupation) %>%
  arrange(year) %>%
  filter(!is.na(real_median_wage)) %>%
  summarise(
    years_span = max(year, na.rm = TRUE) - min(year, na.rm = TRUE),
    n_years = n(),
    start_year = min(year, na.rm = TRUE),
    end_year = max(year, na.rm = TRUE),
    start_salary = first(real_median_wage, na_rm = TRUE),
    end_salary = last(real_median_wage, na_rm = TRUE),
    median_cagr = calculate_cagr(start_salary, end_salary, years_span),
    .groups = "drop"
  )

# Add mean wage CAGR if available
if("real_mean_wage" %in% colnames(scm_inflation_adjusted)) {
  cagr_mean <- scm_inflation_adjusted %>%
    group_by(occupation) %>%
    arrange(year) %>%
    filter(!is.na(real_mean_wage)) %>%
    summarise(
      mean_cagr = calculate_cagr(first(real_mean_wage, na_rm = TRUE), 
                                 last(real_mean_wage, na_rm = TRUE), 
                                 max(year, na.rm = TRUE) - min(year, na.rm = TRUE)),
      .groups = "drop"
    )
  
  cagr_analysis <- cagr_analysis %>%
    left_join(cagr_mean, by = "occupation")
}

cagr_analysis <- cagr_analysis %>% arrange(desc(median_cagr))

# 2. Regression Analysis
cat("Performing trend regression analysis...\n")
regression_results <- scm_inflation_adjusted %>%
  group_by(occupation) %>%
  nest() %>%
  mutate(
    median_trend = map(data, ~perform_trend_regression(.x, "real_median_wage"))
  ) %>%
  select(-data) %>%
  unnest(median_trend, names_sep = "_")

# Add mean wage regression if available
if("real_mean_wage" %in% colnames(scm_inflation_adjusted)) {
  regression_mean <- scm_inflation_adjusted %>%
    group_by(occupation) %>%
    nest() %>%
    mutate(
      mean_trend = map(data, ~perform_trend_regression(.x, "real_mean_wage"))
    ) %>%
    select(-data) %>%
    unnest(mean_trend, names_sep = "_")
  
  regression_results <- regression_results %>%
    left_join(regression_mean, by = "occupation")
}

regression_results <- regression_results %>%
  arrange(desc(median_trend_annual_change))

# 3. Summary Statistics by Year
cat("Calculating summary statistics...\n")
summary_by_year <- scm_inflation_adjusted %>%
  group_by(year) %>%
  summarise(
    n_occupations = n(),
    total_employment = sum(employment, na.rm = TRUE),
    avg_median_salary = mean(real_median_wage, na.rm = TRUE),
    median_median_salary = median(real_median_wage, na.rm = TRUE),
    salary_sd = sd(real_median_wage, na.rm = TRUE),
    min_salary = min(real_median_wage, na.rm = TRUE),
    max_salary = max(real_median_wage, na.rm = TRUE),
    .groups = "drop"
  )

# 4. Current Rankings
cat("Creating current salary rankings...\n")
current_rankings <- scm_inflation_adjusted %>%
  filter(year == max(year, na.rm = TRUE)) %>%
  arrange(desc(real_median_wage)) %>%
  mutate(rank = row_number())

# Select available columns for rankings
ranking_columns <- c("rank", "occupation", "real_median_wage", "employment")
if("real_mean_wage" %in% colnames(current_rankings)) {
  ranking_columns <- c(ranking_columns, "real_mean_wage")
}
current_rankings <- current_rankings %>% select(all_of(ranking_columns))

# 5. Volatility Analysis
cat("Analyzing salary volatility...\n")
volatility_analysis <- scm_inflation_adjusted %>%
  group_by(occupation) %>%
  summarise(
    avg_salary = mean(real_median_wage, na.rm = TRUE),
    salary_sd = sd(real_median_wage, na.rm = TRUE),
    cv = salary_sd / avg_salary,  # Coefficient of variation
    salary_range = max(real_median_wage, na.rm = TRUE) - min(real_median_wage, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(cv))

# Save analysis results
save(cagr_analysis, regression_results, summary_by_year, current_rankings, volatility_analysis,
     file = "data/processed/analysis_results.RData")

cat("✓ Analysis complete\n")
cat("CAGR analysis:", nrow(cagr_analysis), "occupations\n")
cat("Regression analysis:", nrow(regression_results), "occupations\n")
cat("Current rankings:", nrow(current_rankings), "occupations\n")

# ==========================================
# PHASE 5: VISUALIZATION
# ==========================================

cat("\n=== CREATING VISUALIZATIONS ===\n")

# Set up theme for consistent plotting
theme_scm <- theme_minimal() +
  theme(
    plot.title = element_text(size = 14, face = "bold"),
    plot.subtitle = element_text(size = 12, color = "gray60"),
    axis.text = element_text(size = 10),
    axis.title = element_text(size = 11),
    legend.text = element_text(size = 9),
    legend.title = element_text(size = 10),
    strip.text = element_text(size = 9, face = "bold")
  )

# 1. Salary Trends Over Time
trend_plot <- scm_inflation_adjusted %>%
  ggplot(aes(x = year, y = real_median_wage, color = occupation)) +
  geom_line(size = 1.1, alpha = 0.8) +
  geom_point(size = 2, alpha = 0.9) +
  scale_y_continuous(labels = scales::dollar_format()) +
  scale_x_continuous(breaks = years) +
  labs(
    title = "Real Median Salary Trends by SCM Occupation",
    subtitle = paste("Inflation-adjusted to", base_year, "dollars"),
    x = "Year", 
    y = "Median Salary ($)",
    color = "Occupation"
  ) +
  theme_scm +
  theme(legend.position = "bottom") +
  guides(color = guide_legend(nrow = 5, byrow = TRUE))

ggsave("output/plots/salary_trends.png", trend_plot, width = 12, height = 8, dpi = 300)

# 2. CAGR Comparison
cagr_plot <- cagr_analysis %>%
  filter(!is.na(median_cagr)) %>%
  ggplot(aes(x = reorder(occupation, median_cagr), y = median_cagr)) +
  geom_col(fill = "steelblue", alpha = 0.7) +
  geom_text(aes(label = scales::percent(median_cagr, accuracy = 0.1)), 
            hjust = -0.1, size = 3) +
  coord_flip() +
  scale_y_continuous(labels = scales::percent_format(), 
                     limits = c(min(cagr_analysis$median_cagr, na.rm = TRUE) * 1.1,
                                max(cagr_analysis$median_cagr, na.rm = TRUE) * 1.1)) +
  labs(
    title = "Compound Annual Growth Rate by SCM Occupation",
    subtitle = paste("Real median wage growth,", min(years), "-", max(years)),
    x = "", 
    y = "CAGR (%)"
  ) +
  theme_scm

ggsave("output/plots/cagr_comparison.png", cagr_plot, width = 10, height = 8, dpi = 300)

# 3. Current Salary Rankings
ranking_plot <- current_rankings %>%
  slice(1:10) %>%  # Top 10 only for readability
  ggplot(aes(x = reorder(occupation, real_median_wage), y = real_median_wage)) +
  geom_col(fill = "darkgreen", alpha = 0.7) +
  geom_text(aes(label = scales::dollar(real_median_wage, accuracy = 1000)), 
            hjust = -0.1, size = 3) +
  coord_flip() +
  scale_y_continuous(labels = scales::dollar_format(),
                     limits = c(0, max(current_rankings$real_median_wage) * 1.1)) +
  labs(
    title = paste("Current SCM Salary Rankings -", max(years)),
    subtitle = paste("Real median salaries (", base_year, "dollars)"),
    x = "", 
    y = "Median Salary ($)"
  ) +
  theme_scm

ggsave("output/plots/current_rankings.png", ranking_plot, width = 10, height = 6, dpi = 300)

# 4. Salary Distribution Evolution
distribution_plot <- scm_inflation_adjusted %>%
  select(occupation, year, real_pct25_wage, real_median_wage, real_pct75_wage) %>%
  pivot_longer(cols = starts_with("real_"), 
               names_to = "percentile", 
               values_to = "wage") %>%
  mutate(percentile = case_when(
    percentile == "real_pct25_wage" ~ "25th Percentile",
    percentile == "real_median_wage" ~ "Median", 
    percentile == "real_pct75_wage" ~ "75th Percentile"
  )) %>%
  ggplot(aes(x = year, y = wage, color = percentile)) +
  geom_line(size = 1) +
  geom_point(size = 1.5) +
  facet_wrap(~str_wrap(occupation, 30), scales = "free_y", ncol = 3) +
  scale_y_continuous(labels = scales::dollar_format()) +
  scale_x_continuous(breaks = years) +
  labs(
    title = "Salary Distribution Trends by SCM Occupation",
    subtitle = "25th percentile, median, and 75th percentile wages",
    x = "Year", 
    y = "Salary ($)",
    color = "Percentile"
  ) +
  theme_scm +
  theme(strip.text = element_text(size = 8))

ggsave("output/plots/distribution_trends.png", distribution_plot, width = 15, height = 12, dpi = 300)

cat("✓ Visualizations saved to output/plots/\n")

# ==========================================
# PHASE 5: REPORTING
# ==========================================

cat("\n=== GENERATING SUMMARY REPORT ===\n")

# Create summary report
generate_summary_report <- function() {
  cat("\n", "="*60, "\n")
  cat("     SUPPLY CHAIN MANAGEMENT SALARY ANALYSIS REPORT\n")
  cat("="*60, "\n")
  
  cat("Analysis Period:", min(years), "-", max(years), "\n")
  cat("Base Year for Inflation Adjustment:", base_year, "\n")
  cat("Number of Occupations Analyzed:", length(unique(scm_inflation_adjusted$occupation)), "\n")
  cat("Total Employment (Latest Year):", scales::comma(sum(current_rankings$employment, na.rm = TRUE)), "\n\n")
  
  cat("TOP 5 HIGHEST PAYING SCM OCCUPATIONS (", max(years), "):\n")
  cat("-"*50, "\n")
  for(i in 1:min(5, nrow(current_rankings))) {
    cat(sprintf("%d. %s\n   Median: %s | Employment: %s\n\n", 
                i, 
                str_wrap(current_rankings$occupation[i], 40),
                scales::dollar(current_rankings$real_median_wage[i]),
                scales::comma(current_rankings$employment[i])))
  }
  
  cat("TOP 5 FASTEST GROWING SCM OCCUPATIONS (CAGR):\n")
  cat("-"*50, "\n")
  top_growth <- cagr_analysis %>% filter(!is.na(median_cagr)) %>% slice(1:5)
  for(i in 1:nrow(top_growth)) {
    cat(sprintf("%d. %s\n   CAGR: %s | Current Salary: %s\n\n", 
                i, 
                str_wrap(top_growth$occupation[i], 40),
                scales::percent(top_growth$median_cagr[i], accuracy = 0.1),
                scales::dollar(top_growth$end_salary[i])))
  }
  
  cat("MARKET SUMMARY:\n")
  cat("-"*50, "\n")
  latest_summary <- summary_by_year %>% filter(year == max(year))
  cat(sprintf("Average Median Salary: %s\n", scales::dollar(latest_summary$avg_median_salary)))
  cat(sprintf("Salary Range: %s - %s\n", 
              scales::dollar(latest_summary$min_salary),
              scales::dollar(latest_summary$max_salary)))
  cat(sprintf("Total SCM Employment: %s\n", scales::comma(latest_summary$total_employment)))
  
  cat("\nMOST VOLATILE OCCUPATIONS (Coefficient of Variation):\n")
  cat("-"*50, "\n")
  top_volatile <- volatility_analysis %>% slice(1:3)
  for(i in 1:nrow(top_volatile)) {
    cat(sprintf("%d. %s (CV: %.2f)\n", i, str_wrap(top_volatile$occupation[i], 40), top_volatile$cv[i]))
  }
  
  cat("\n" , "="*60, "\n")
  cat("Report generated on:", Sys.time(), "\n")
  cat("="*60, "\n")
}

# Generate and save report
sink("output/reports/scm_salary_summary.txt")
generate_summary_report()
sink()

# Also display in console
generate_summary_report()

# Create data export for further analysis
write_csv(current_rankings, "output/reports/current_salary_rankings.csv")
write_csv(cagr_analysis, "output/reports/cagr_analysis.csv")
write_csv(regression_results, "output/reports/regression_analysis.csv")
write_csv(volatility_analysis, "output/reports/volatility_analysis.csv")

cat("✓ Reports saved to output/reports/\n")
cat("✓ Analysis complete! Check output folders for results.\n")

# ==========================================
# FINAL STATUS SUMMARY
# ==========================================

cat("\n", "="*60, "\n")
cat("ANALYSIS COMPLETE - FILES GENERATED:\n")
cat("="*60, "\n")

# Only show file summary if we actually generated files
if(exists("scm_inflation_adjusted") && nrow(scm_inflation_adjusted) > 0) {
  cat("📊 Data Files:\n")
  cat("  - data/raw/bls_raw_data.RData\n")
  cat("  - data/processed/scm_salary_data.csv\n")
  cat("  - data/processed/processed_data.RData\n")
  cat("  - data/processed/analysis_results.RData\n\n")
  cat("📈 Visualizations:\n")
  cat("  - output/plots/salary_trends.png\n")
  cat("  - output/plots/cagr_comparison.png\n")
  cat("  - output/plots/current_rankings.png\n")
  cat("  - output/plots/distribution_trends.png\n\n")
  cat("📄 Reports:\n")
  cat("  - output/reports/scm_salary_summary.txt\n")
  cat("  - output/reports/current_salary_rankings.csv\n")
  cat("  - output/reports/cagr_analysis.csv\n")
  cat("  - output/reports/regression_analysis.csv\n")
  cat("  - output/reports/volatility_analysis.csv\n")
} else {
  cat("⚠️  Analysis incomplete - check data collection and processing steps\n")
  cat("Run the script section by section to debug issues.\n")
}
cat("="*60, "\n")

# ==========================================
# DEBUG SECTION - RUN THIS TO TEST API CONNECTION
# ==========================================

# Uncomment this section to test just the API connection and data processing
# without running the full analysis and visualization

# cat("\n=== DEBUG: TESTING API CONNECTION ===\n")
# 
# # Test single series ID manually
# test_series_id <- "OEUN000000000000131081101"  # Logisticians employment
# cat("Testing series ID:", test_series_id, "\n")
# 
# payload_test <- list(
#   'seriesid' = c(test_series_id),
#   'startyear' = "2021",
#   'endyear' = "2024",
#   'registrationKey' = Sys.getenv("BLS_KEY")
# )
# 
# cat("Test payload:", toJSON(payload_test), "\n")
# 
# response_test <- blsAPI(payload_test, api_version = 2)
# cat("Raw API response:\n", response_test, "\n")
# 
# json_test <- fromJSON(response_test)
# cat("Parsed response status:", json_test$status, "\n")
# if(!is.null(json_test$message)) {
#   cat("API message:", json_test$message, "\n")
# }