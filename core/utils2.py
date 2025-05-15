
def is_false_array(x: str) -> bool:
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

def render_unwrap_singleton_expression(col_name: str, default_value: str) -> str:
    
    """
    Render a SQL expression to handle survey unwrapping singleton values from variables stored as "false arrays".

    This function returns a SQL snippet that:
      - Returns NULL when the column's value equals "[]"
      - Returns the unbracketed nine-digit concept_id when the value is in the form "[123456789]"
      - Returns the provided default value (cast as a string) when the value does not match the nine-digit pattern
      - Assigns an alias to the expression using the provided column name

    Parameters:
        col_name (str): The column name to be processed.
        default_value (str): The default SQL literal to return when the pattern is not matched.
                             (For example, a numeric default as 0, or a string default as "'ERROR!'").
                             The default value will be cast to STRING.

    Returns:
        str: A SQL snippet containing the CASE expression with an alias assignment.
    """
    
    sql = \
        rf"""CASE
            WHEN {col_name} = "[]" THEN NULL
            WHEN REGEXP_CONTAINS({col_name}, r'\[\d{{9}}\]') THEN REGEXP_REPLACE({col_name}, r'\[(\d{{9}})\]', r'\1') -- remove brackets around CID
            WHEN {col_name} IS NULL THEN NULL
            ELSE CAST({default_value} AS STRING)
        END AS {col_name}"""
        
    return sql
