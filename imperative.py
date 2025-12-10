import statistics
from common import (
    read_csv,
    save_to_csv,
    parse_float,
    parse_int,
    parse_date,
    parse_string,
    DECIMAL_PLACES
)

# --- CONFIGURATION ---
# The recursion limit is no longer relevant in the imperative version
# as it avoids deep recursive calls (like the functional get_column).

INPUT_FILE = 'dirty_cafe_sales.csv'
OUTPUT_FILE = 'imperative_output.csv'

# --- CORE IMPERATIVE DATA PROCESSING ---

def compute_column_stats(data):
    """
    Computes statistical defaults (median, mean, mode) for cleaning in an imperative style.
    This replaces the recursive get_column and subsequent map/statistics calls in the main.
    """
    # 1. Collect all non-dirty values for required columns in lists
    quantities = []
    prices_per_unit = []
    items = []
    payment_methods = []
    locations = []
    transaction_dates = []

    for row in data:
        # Quantity (Need to parse to int first)
        if row['Quantity'] not in ["ERROR", "UNKNOWN", ""]:
            try:
                quantities.append(int(row['Quantity']))
            except ValueError:
                pass # Skip unparseable values

        # Price Per Unit (Need to parse to float first)
        if row['Price Per Unit'] not in ["ERROR", "UNKNOWN", ""]:
            try:
                prices_per_unit.append(float(row['Price Per Unit']))
            except ValueError:
                pass # Skip unparseable values

        # Categorical Columns (Simple non-empty check)
        if row['Item'] not in ["ERROR", "UNKNOWN", ""]:
            items.append(row['Item'])
        if row['Payment Method'] not in ["ERROR", "UNKNOWN", ""]:
            payment_methods.append(row['Payment Method'])
        if row['Location'] not in ["ERROR", "UNKNOWN", ""]:
            locations.append(row['Location'])
        if row['Transaction Date'] not in ["ERROR", "UNKNOWN", ""]:
            # Optionally check date format validity here, but for mode, raw string is okay
            transaction_dates.append(row['Transaction Date'])
    
    # 2. Compute the defaults (Imperative use of statistics module)
    defults = {
        'defult_quantity_median': statistics.median(quantities) if quantities else 0,
        'defult_price_per_unit_mean': statistics.mean(prices_per_unit) if prices_per_unit else 0.0,
        'defult_item_mode': statistics.mode(items) if items else 'UNKNOWN',
        'defult_payment_method_mode': statistics.mode(payment_methods) if payment_methods else 'UNKNOWN',
        'defult_location_mode': statistics.mode(locations) if locations else 'UNKNOWN',
        'defult_transaction_date_mode': statistics.mode(transaction_dates) if transaction_dates else '1970-01-01'
    }
    return defults

def clean_data_imperative(raw_data, defults):
    """
    Cleans and transforms data by modifying the list of dictionaries directly (in-place modification 
    or building a new list with explicit loops), which is a characteristic of imperative style.
    """
    cleaned_data = [] # We build a new list to avoid modifying the input list in-place
    
    # Explicit loop replacing the map() function
    for row in raw_data:
        # Apply cleaning and parsing for each field
        quantity = parse_int(row['Quantity'], defults['defult_quantity_median'])
        price_per_unit = parse_float(row['Price Per Unit'], defults['defult_price_per_unit_mean'])
        
        # Compute New Columns
        corrected_total = quantity * price_per_unit
        
        # Build the new, cleaned row dictionary
        cleaned_row = {
            **row,
            'Item': parse_string(row['Item'], defults['defult_item_mode']),
            'Quantity': quantity,
            'Price Per Unit': price_per_unit,
            'Total Spent': parse_float(row['Total Spent'], 0.0), # keep 0.0 as it will be recomputed (0.0 is safe default)
            'Payment Method': parse_string(row['Payment Method'], defults['defult_payment_method_mode']),
            'Location': parse_string(row['Location'], defults['defult_location_mode']),
            'Transaction Date': parse_date(row['Transaction Date'], defults['defult_transaction_date_mode']),
            'Corrected Total': round(float(corrected_total), DECIMAL_PLACES)
        }
        cleaned_data.append(cleaned_row)
        
    return cleaned_data

def get_aggregate_total_spent(data, item_name, column_name):
    """
    Aggregates the total spent for a specific item using an explicit loop and accumulator.
    This replaces the filter/get_column/reduce chain.
    """
    total = 0.0 # Accumulator variable
    # Explicit loop replacing filter and reduce
    for row in data:
        if row['Item'] == item_name:
            # We assume the data is already cleaned and the column value is a number
            total += row[column_name] 
    
    return total

# --- ANALYSIS FUNCTIONS (Imperative-specific implementations) ---

def print_numeric_analysis_imperative(data, column_name, label):
    """Calculates and prints numeric stats using an imperative loop for data extraction."""
    values = []
    # Explicit loop replacing the map() function
    for row in data:
        # We assume the data is cleaned and the column value is a number
        values.append(row[column_name])
        
    if not values:
        print(f"\n--- Analysis: {label} ---")
        print("No valid numeric data found.")
        return
        
    print(f"\n--- Analysis: {label} ---")
    print(f"Mean:       {statistics.mean(values):.2f}")
    print(f"Median:     {statistics.median(values):.2f}")
    print(f"Variance:   {statistics.variance(values):.2f}")

def print_categorical_analysis_imperative(data, column_name, label):
    """Finds and prints the mode for a categorical column using an imperative loop."""
    values = []
    # Explicit loop replacing the map() function
    for row in data:
        values.append(row[column_name])
        
    print(f"\n--- Trend: {label} ---")
    if not values:
        print("Most Common (Mode): No data found")
        return

    try:
        print(f"Most Common (Mode): {statistics.mode(values)}")
    except statistics.StatisticsError:
        print(f"Most Common (Mode): Multiple found")

# --- MAIN EXECUTION BLOCK ---

def main_imperative():
    print("--- Starting Imperative Pipeline ---")
    
    # 1. Load Data (IO is isolated)
    raw_data = read_csv(INPUT_FILE)
    if not raw_data:
        return
        
    # 2. Compute Defaults (Using imperative, loop-based function)
    defults = compute_column_stats(raw_data)
    print("\nDefaults Computed.")
    
    # 3. Clean Data (Using imperative, loop-based function)
    cleaned_data = clean_data_imperative(raw_data, defults)
    print("Data Cleaned and Transformed.")

    # 4. Example Total spent on Coffee (Data Transformation/Aggregation)
    # Using the new imperative aggregation function
    total_spent_coffee = get_aggregate_total_spent(cleaned_data, 'Coffee', 'Corrected Total')
    print("\n--- Total Spent on Coffee ---")
    print(f"total:      {total_spent_coffee:.2f}")
    
    # 5. Run Numeric Analysis
    print("\n--- Running Statistical Analysis ---")
    print_numeric_analysis_imperative(cleaned_data, 'Quantity', 'Quantity Sold')
    print_numeric_analysis_imperative(cleaned_data, 'Price Per Unit', 'Unit Price')
    print_numeric_analysis_imperative(cleaned_data, 'Total Spent', 'Original Total Spent (from CSV)')
    print_numeric_analysis_imperative(cleaned_data, 'Corrected Total', 'Corrected Total (Calculated)')

    # 6. Run Categorical/Trend Analysis
    print_categorical_analysis_imperative(cleaned_data, 'Item', 'Top Selling Items')
    print_categorical_analysis_imperative(cleaned_data, 'Location', 'Top Locations')
    print_categorical_analysis_imperative(cleaned_data, 'Payment Method', 'Preferred Payment Methods')
    print_categorical_analysis_imperative(cleaned_data, 'Transaction Date', 'Busiest Day')

    # 7. Save Cleaned Data
    save_to_csv(OUTPUT_FILE, cleaned_data)
    print(f"\nSaved {OUTPUT_FILE}")
    print("\n--- Imperative Pipeline Finished ---")


if __name__ == "__main__":
    main_imperative()