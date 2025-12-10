# Comparison of Functional and Imperative Paradigms in Data Processing

## Introduction

This document compares the functional and imperative programming paradigms as implemented in the provided data processing pipeline. It highlights the differences, advantages, and usage of key functional programming concepts with code examples from `functional.py` and `imperative.py`.

---

## 1. Higher Order Functions

**Definition:** Functions that take other functions as arguments or return them as results.

- **Functional Example:**
  - `map`, `filter`, and `reduce` are used extensively.
  - Example from `functional.py`:
    ```python
    cleaned_data = map(lambda row: clean_row(row, defults), raw_data)
    item_data_iter = filter(lambda row: filter_by_column_and_value(row, 'Item', item_name), data)
    return reduce(operation, corected_totals_item, 0.0)
    ```
- **Imperative Example:**
  - Uses explicit loops instead of higher order functions.
  - Example from `imperative.py`:
    ```python
    cleaned_data = []
    for row in raw_data:
        # ...
        cleaned_data.append(cleaned_row)
    ```

**Comparison:**

- Functional code is more concise and expressive for data transformations.
- Imperative code is more explicit and easier to debug for beginners.

---

## 2. Tail Recursion

**Definition:** A recursive function where the recursive call is the last operation.

- **Functional Example:**
  - The `get_column` function is tail-recursive:
    ```python
    def get_column(data, column_name, accumulator):
        match data:
            case []:
                return accumulator
            case [head, *tail]:
                # ...
                return get_column(tail, column_name, accumulator + [head[column_name]])
    ```
- **Imperative Example:**
  - Uses loops instead of recursion:
    ```python
    for row in data:
        if row['Quantity'] not in ["ERROR", "UNKNOWN", ""]:
            quantities.append(int(row['Quantity']))
    ```

**Comparison:**

- Functional code can leverage recursion for list processing, but may hit recursion limits in Python.
- Imperative code avoids recursion, using loops for better performance in Python.

---

## 3. Single Assignment (Pure Functions)

**Definition:** Variables are assigned once; functions have no side effects.

- **Functional Example:**
  - Functions like `clean_row` do not modify input data, but return new data:
    ```python
    def clean_row(row, defults):
        # ...
        return { **row, ... }
    ```
- **Imperative Example:**
  - May modify or build new lists, but often uses in-place updates:
    ```python
    cleaned_row = { **row, ... }
    cleaned_data.append(cleaned_row)
    ```

**Comparison:**

- Functional code encourages immutability and pure functions.
- Imperative code may use mutable data structures and side effects.

---

## 4. Lists

**Definition:** Core data structure for storing sequences.

- **Functional Example:**
  - Uses list comprehensions, `map`, and recursion:
    ```python
    values = list(map(lambda row: row[column_name], final_data))
    ```
- **Imperative Example:**
  - Uses explicit loops to build lists:
    ```python
    values = []
    for row in data:
        values.append(row[column_name])
    ```

**Comparison:**

- Both paradigms use lists, but functional code prefers declarative transformations, while imperative code uses explicit iteration.

---

## 5. Mutability vs Immutability

**Definition:** Mutable objects can change in place; immutable objects cannot be changed after creation.

- **Functional Example:**
  - Favors immutability and returning new data structures.
  - `clean_row` returns a new dict instead of mutating the incoming row.
  - `map`/`filter`/`reduce` build new sequences rather than altering inputs.
- **Imperative Example:**
  - Relies on mutable accumulators and in-place construction.
  - `compute_column_stats` appends to lists like `quantities`, `prices_per_unit`.
  - `clean_data_imperative` builds `cleaned_data` by mutating a list accumulator.

**Comparison:**

- Immutability reduces accidental side effects and eases reasoning (helps testability and concurrency safety).
- Mutability can be more straightforward and sometimes faster in Python, but shared mutable state increases bug risk.
- Choose immutability when clarity and safety matter; use mutability when performance or simplicity of in-place updates is needed.

---

## Conclusion

- **Functional programming** offers concise, expressive code with a focus on immutability and pure functions, but may be less familiar to Python programmers and can hit recursion limits.
- **Imperative programming** is more explicit, easier to debug, and better suited for Python's performance characteristics, but can be more verbose and prone to side effects.

**Choosing a paradigm** depends on the problem, team familiarity, and language features. Both styles are valuable and can be mixed for practical software development.
