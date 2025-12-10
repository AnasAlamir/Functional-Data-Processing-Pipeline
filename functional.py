import csv
import sys
import statistics
from datetime import datetime
from functools import reduce
# --- CONFIGURATION ---
# CRITICAL: Increase recursion limit to handle 10,000 rows
sys.setrecursionlimit(20000)

INPUT_FILE = 'dirty_cafe_sales.csv'
OUTPUT_FILE = 'functional_pure_output.csv'
DECIMAL_PLACES = 2

def read_csv(file_path):
    """Reads a CSV file and returns its contents as a list of dictionaries."""
    with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        return list(csv.DictReader(csvfile))

def save_to_csv(file_path, data):
    """Saves a list of dictionaries to a CSV file."""
    if not data:
        return
    keys = data[0].keys()
    try:
        with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)
    except PermissionError:
        print(f"\n[ERROR] Could not save to '{file_path}'.")
        print("Is the file open in Excel? Please close it and try again.")
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred: {e}")

def parse_float(value: str, default: float) -> float:
    """
    Uses Pattern Matching to handle dirty data strings.
    """
    try:
        match value:
            case "ERROR" | "UNKNOWN" | "": 
                return round(float(default), DECIMAL_PLACES)
            case _: 
                return round(float(value), DECIMAL_PLACES)
    except ValueError:
        raise ValueError(f"Cannot convert {value} to float.")

def parse_int(value: str, default: int) -> int:
    """
    Uses Pattern Matching to handle dirty data strings.
    """
    try:
        match value:
            case "ERROR" | "UNKNOWN" | "": 
                return default
            case _: 
                return int(value)
    except ValueError:
        raise ValueError(f"Cannot convert {value} to int.")
    
def parse_date(value: str, default: str) -> str:
    try:
        match value:
            case "ERROR" | "UNKNOWN" | "":
                return default
            case _:
                datetime.strptime(value, '%Y-%m-%d')
                return value
    except ValueError:
        raise ValueError(f"Cannot parse date from {value}.")

def parse_string(value: str, default: str) -> str:
    match value:
        case "ERROR" | "UNKNOWN" | "":
            return default
        case _:
            return value

def get_column(data, column_name, accumulator):
    match data:
        case []:
            return accumulator
        case [head, *tail]:
            match head[column_name]:
                case "ERROR" | "UNKNOWN" | "":
                    return get_column(tail, column_name, accumulator)
                case _:
                    return get_column(tail, column_name, accumulator + [head[column_name]])
        
def clean_row(row, defults):
    quantity = parse_int(row['Quantity'], defults['defult_quantity_median']) 
    price_per_unit = parse_float(row['Price Per Unit'], defults['defult_price_per_unit_mean'])
    # Compute New Columns
    corrected_total = quantity * price_per_unit
    return {
        **row,
        'Item': parse_string(row['Item'], defults['defult_item_mode']), 
        'Quantity': quantity, 
        'Price Per Unit': price_per_unit, 
        'Total Spent': parse_float(row['Total Spent'], 0.0), # keep 0.0 as it will be recomputed
        'Payment Method': parse_string(row['Payment Method'], defults['defult_payment_method_mode']), 
        'Location': parse_string(row['Location'], defults['defult_location_mode']), 
        'Transaction Date': parse_date(row['Transaction Date'], defults['defult_transaction_date_mode']), 
        'Corrected Total': round(float(corrected_total), DECIMAL_PLACES)
    }

# Filter rows based on conditions (Data Transformation)
def filter_by_column_and_value(row, column_name, value):
    return row[column_name] == value

# Aggregate Data (Data Transformation)
def get_aggregate_by_coloumn_for_item(data, item_name, column_name, operation):
    item_data_iter = filter(lambda row: filter_by_column_and_value(row, 'Item', item_name), data)
    final_item_list = list(item_data_iter)
    corected_totals_item = get_column(final_item_list, column_name, [])
    return reduce(operation, corected_totals_item, 0.0)

# Data Analysis & Statistical Summaries
def print_numeric_analysis(final_data, column_name, label):
    # Use map() to extract column efficiently (Faster than recursive get_column)
    values = list(map(lambda row: row[column_name], final_data))
    print(f"\n--- Analysis: {label} ---")
    print(f"Mean:     {statistics.mean(values):.2f}")
    print(f"Median:   {statistics.median(values):.2f}")
    print(f"Variance: {statistics.variance(values):.2f}")

def print_categorical_analysis(final_data, column_name, label):
    values = list(map(lambda row: row[column_name], final_data))
    print(f"\n--- Trend: {label} ---")
    try:
        print(f"Most Common (Mode): {statistics.mode(values)}")
    except statistics.StatisticsError:
        print(f"Most Common (Mode): Multiple found")

def main():
    print("--- Starting Pure Functional Pipeline ---")
    
    # Load Data (IO is never pure, but we isolate it)
    raw_data = read_csv(INPUT_FILE)

    # Compute Defults
    list_quantity_defaults = get_column(raw_data, 'Quantity', [])
    quantity_values_defaults = list(map(lambda x: parse_int(x, 0), list_quantity_defaults))
    quantity_median_defaults = statistics.median(quantity_values_defaults)

    list_price_per_unit_defaults = get_column(raw_data, 'Price Per Unit', [])
    price_per_unit_values_defaults = list(map(lambda x: parse_float(x, 0.0), list_price_per_unit_defaults))
    price_per_unit_mean_defaults = statistics.mean(price_per_unit_values_defaults)
    
    list_item_defaults = get_column(raw_data, 'Item', [])
    item_mode_defaults = statistics.mode(list_item_defaults)

    list_payment_method_defaults = get_column(raw_data, 'Payment Method', [])
    payment_method_mode_defaults = statistics.mode(list_payment_method_defaults)

    list_location_defaults = get_column(raw_data, 'Location', [])
    location_mode_defaults = statistics.mode(list_location_defaults)

    list_transaction_date_defaults = get_column(raw_data, 'Transaction Date', [])
    transaction_date_mode_defaults = statistics.mode(list_transaction_date_defaults)
    
    defults = {
        'defult_quantity_median': quantity_median_defaults,
        'defult_price_per_unit_mean': price_per_unit_mean_defaults,
        'defult_item_mode': item_mode_defaults,
        'defult_payment_method_mode': payment_method_mode_defaults,
        'defult_location_mode': location_mode_defaults,
        'defult_transaction_date_mode': transaction_date_mode_defaults
    }
    # Clean Data
    cleaned_data = map(lambda row: clean_row(row, defults), raw_data)
    full_cleaned_data = list(cleaned_data)
    # Example Total spent on Coffee (Data Transformation)
    total_spent_coffee = get_aggregate_by_coloumn_for_item(full_cleaned_data, 'Coffee', 'Corrected Total', lambda x, y: x + y)
    print("\n--- Total Spent on Coffee ---")
    print(f"total:     {total_spent_coffee}")
    
    # 1. Run Numeric Analysis
    print_numeric_analysis(full_cleaned_data, 'Quantity', 'Quantity Sold')
    print_numeric_analysis(full_cleaned_data, 'Price Per Unit', 'Unit Price')
    print_numeric_analysis(full_cleaned_data, 'Total Spent', 'Original Total Spent (from CSV)')
    print_numeric_analysis(full_cleaned_data, 'Corrected Total', 'Corrected Total (Calculated)')

    # 2. Run Categorical/Trend Analysis
    print_categorical_analysis(full_cleaned_data, 'Item', 'Top Selling Items')
    print_categorical_analysis(full_cleaned_data, 'Location', 'Top Locations')
    print_categorical_analysis(full_cleaned_data, 'Payment Method', 'Preferred Payment Methods')
    print_categorical_analysis(full_cleaned_data, 'Transaction Date', 'Busiest Day')


    # Save Cleaned Data
    save_to_csv(OUTPUT_FILE, full_cleaned_data)

    print("\nSaved functional_cleaned.csv")


if __name__ == "__main__":
    main()











# def mean(values):
#     return sum(values) / len(values) if values else 0.0

# def mode(values, frequency = {}):
#     match values:
#         case []:
#             if frequency == {}:
#                 return None
#             return max(frequency, key=frequency.get)
#         case [head, *tail]:
#             frequency[head] = frequency.get(head, 0) + 1
#             return mode(tail, frequency)

# def median(values):
#     sorted_values = sorted(values)
#     n = len(sorted_values)
#     if n % 2 == 1:
#         return sorted_values[n // 2]
#     else:
#         mid1 = sorted_values[n // 2 - 1]
#         mid2 = sorted_values[n // 2]
#         return (mid1 + mid2) / 2