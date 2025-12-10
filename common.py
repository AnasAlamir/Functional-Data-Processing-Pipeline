import csv
import statistics
from datetime import datetime

# --- CONFIGURATION ---
DECIMAL_PLACES = 2

# --- I/O FUNCTIONS ---
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

# --- DATA PARSING FUNCTIONS ---
def parse_float(value: str, default: float) -> float:
    """Handles dirty data strings for floats."""
    try:
        if value in ["ERROR", "UNKNOWN", ""]:
            return round(float(default), DECIMAL_PLACES)
        else:
            return round(float(value), DECIMAL_PLACES)
    except ValueError:
        raise ValueError(f"Cannot convert {value} to float.")

def parse_int(value: str, default: int) -> int:
    """Handles dirty data strings for integers."""
    try:
        if value in ["ERROR", "UNKNOWN", ""]:
            return default
        else:
            return int(value)
    except ValueError:
        raise ValueError(f"Cannot convert {value} to int.")
    
def parse_date(value: str, default: str) -> str:
    """Handles dirty data strings for dates."""
    try:
        if value in ["ERROR", "UNKNOWN", ""]:
            return default
        else:
            datetime.strptime(value, '%Y-%m-%d')
            return value
    except ValueError:
        raise ValueError(f"Cannot parse date from {value}.")

def parse_string(value: str, default: str) -> str:
    """Handles dirty data strings for strings."""
    if value in ["ERROR", "UNKNOWN", ""]:
        return default
    else:
        return value

# --- ANALYSIS FUNCTIONS ---
def print_numeric_analysis(data, column_name, label):
    """Calculates and prints numeric stats."""
    values = list(map(lambda row: row[column_name], data))
    print(f"\n--- Analysis: {label} ---")
    print(f"Mean:     {statistics.mean(values):.2f}")
    print(f"Median:   {statistics.median(values):.2f}")
    print(f"Variance: {statistics.variance(values):.2f}")

def print_categorical_analysis(data, column_name, label):
    """Finds and prints the mode for a categorical column."""
    values = list(map(lambda row: row[column_name], data))
    print(f"\n--- Trend: {label} ---")
    try:
        print(f"Most Common (Mode): {statistics.mode(values)}")
    except statistics.StatisticsError:
        print(f"Most Common (Mode): Multiple found")
