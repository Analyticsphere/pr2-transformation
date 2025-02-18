import re
import pandas as pd  # Optional, for pd.isna()

def is_false_array(x, valid_values=None):
    """
    Determine whether a sequence of values (x) qualifies as a "false array".
    GitHub Issue: https://github.com/Analyticsphere/bq2/issues/6

    A "false array" is defined here as having:
      - At most 3 unique non-missing values.
      - All unique values (including missing values) are among the allowed valid_values.
      - At most one unique value that matches a nine-digit concept ID in the form "[123456789]".

    Parameters:
        x (iterable): A list (or Pandas Series) of string values.
        valid_values (list, optional): A list of allowed values. Defaults to:
            [None, "[]", "[178420302]", "[958239616]"]

    Returns:
        bool: True if x qualifies as a false array, False otherwise.
    """
    if valid_values is None:
        valid_values = [None, "[]", "[178420302]", "[958239616]"]

    # Get unique values; convert to list so order doesn't matter.
    unique_values = list(set(x))
    
    # Filter out missing values using pd.isna() (works for both None and np.nan)
    non_missing = [v for v in unique_values if not pd.isna(v)]
    num_unique = len(non_missing)
    
    # Condition 1: At most 3 unique non-missing values.
    cond1 = num_unique <= 3
    
    # Condition 2: All values (including missing) must be in valid_values.
    # For missing values, pd.isna(v) is True.
    cond2 = all((v in valid_values) or pd.isna(v) for v in unique_values)
    
    # Condition 3: At most one unique value matches the nine-digit concept pattern.
    pattern = re.compile(r"\[\d{9}\]")
    cond3 = sum(1 for v in unique_values if (not pd.isna(v)) and pattern.search(v)) <= 1

    return cond1 and cond2 and cond3

# ----- Usage Example -----
if __name__ == "__main__":
    # Create a sample DataFrame similar to your R example.
    data = {
        "col1": [None, "[]", "[178420302]", "[178420302]", "[178420302]", None],
        "col2": ["[]", "[]", "invalid", "[987654321]", "[327986541]", "[]"],
        "col3": ["[123456789]", "[123456789]", "[123456789]", "[987654321]", "[327986541]", "[123456789]"],
        "col4": ["[]", None, "[958239616]", None, "[958239616]", "[958239616]"],
        "col5": ["[]", None, "[958239616]", None, "[178420302]", "[958239616]"]
    }
    df = pd.DataFrame(data)
    
    # Define allowed valid values (here, None represents missing values)
    valid_values = [None, "[]", "[178420302]", "[958239616]"]
    
    # Apply the function to each column.
    results = {col: is_false_array(df[col].tolist(), valid_values) for col in df.columns}
    
    print(results)
    # Expected output:
    # {'col1': True, 'col2': False, 'col3': False, 'col4': True, 'col5': False}
