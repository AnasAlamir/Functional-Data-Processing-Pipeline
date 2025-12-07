#  Imperative Data Processing Pipeline

This project implements a data cleaning and analysis pipeline for cafe sales data using the **Imperative Programming Paradigm** in Python.

[cite_start]This implementation fulfills one half of the course requirement to complete the "Functional Data Processing Pipeline" task twice: once using Functional Programming and once using Imperative Programming[cite: 5].

---

##  Paradigm: Imperative Programming

The core design philosophy of this project is to explicitly define a sequence of steps that mutate and transform the data.

* **State Management:** Data processing relies on **modifying variables and data structures in place** or building them using iterative mutation (e.g., list `.append()` within `for` loops).
* **Control Flow:** All iteration, filtering, and aggregation is handled using explicit **`for` loops** and standard conditional statements (`if`/`else`), replacing high-order functions like `map`, `filter`, and `reduce` typical of the functional style.

---

##  Project Features and Implementation

The script performs the following steps on the input `dirty_cafe_sales.csv`:

1.  **Data Loading:** Reads the raw data from the CSV file into a list of dictionaries.
2.  **Default Computation:** Calculates necessary statistical defaults (median quantity, mean price, mode for categorical fields) by iterating through the raw data.
3.  **Data Cleaning & Transformation:** Iterates through every row of the data and applies cleaning logic:
    * [cite_start]**Handling Missing Data[cite: 22]:** Replaces missing/error values (`"ERROR"`, `"UNKNOWN"`, `""`) in columns like `Quantity`, `Price Per Unit`, `Item`, `Location`, and `Payment Method` with the pre-computed statistical defaults.
    * [cite_start]**Data Transformation[cite: 22, 23]:** Computes a new column, **`Corrected Total`**, by multiplying the cleaned `Quantity` and `Price Per Unit`.
4.  [cite_start]**Data Aggregation[cite: 24]:** Calculates the total revenue generated specifically for the 'Coffee' item using a simple loop and an accumulator.
5.  [cite_start]**Data Analysis[cite: 25, 26]:** Runs statistical summaries on the cleaned data, including:
    * Mean, Median, and Variance for numerical columns (`Quantity`, `Price Per Unit`, `Corrected Total`).
    * Mode (Most Common) for categorical/trend analysis (`Item`, `Location`, `Payment Method`).
6.  [cite_start]**Output:** Prints the analysis summaries to the console and saves the fully cleaned and transformed dataset to a new CSV file (`imperative_output.csv`)[cite: 30].

---

##  How to Run the Project

### Prerequisites

* Python 3.10+
* The standard Python `csv` and `statistics` modules (no external libraries required).

### Setup

1.  Ensure the input data file, **`dirty_cafe_sales.csv`**, is placed in the same directory as the Python script.
2.  Run the script from your terminal:

```bash
python imperative_pipeline.py