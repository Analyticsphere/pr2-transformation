import logging
from google.cloud import bigquery
import re
import pandas as pd # TODO Try to avoid using pandas
from collections import defaultdict

def get_column_names(client: str, project: str, dataset: str, table_name: str) -> set:
    """
    Fetches column names of a given BigQuery table.

    Args:
        client (bigquery.Client): BigQuery client instance.
        project (str): Google Cloud project ID.
        dataset (str): BigQuery dataset name.
        table_name (str): Table name.

    Returns:
        set: A set of column names from the table.
    """
    query = f"""
    SELECT column_name 
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` 
    WHERE table_name = '{table_name}';
    """
    try:
        result = client.query(query).to_dataframe()
        return set(result["column_name"])
    except Exception as e:
        logging.error(f"Failed to fetch schema for {table_name}: {e}")
        return set()
    
def get_binary_yes_no_fields(project_id: str, dataset_id: str, table_id: str) -> tuple:
    """
    Identifies columns in a BigQuery table that contain only:
        - '0' and '1'
        - '0', '1', and NULL

    Args:
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.

    Returns:
        tuple: (Formatted SQL query string, list of column names that meet criteria).
    """
    client = bigquery.Client()

    try:
        # Get table schema
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        table = client.get_table(table_ref)

        # Extract column names for STRING and INTEGER types
        column_names = [field.name for field in table.schema if field.field_type in ("STRING", "INTEGER")]

        if not column_names:
            raise ValueError(f"No STRING or INTEGER columns found in `{table_ref}`.")

        # Construct query using COUNTIF for efficiency
        formatted_conditions = ",\n    ".join(
            f"""
            (COUNTIF({col} NOT IN ('0', '1') AND {col} IS NOT NULL) = 0) AS `{col}`
            """.strip()
            for col in column_names
        )

        # Format the query for better readability
        query = f"""
        SELECT 
            {formatted_conditions}
        FROM `{table_ref}`
        """.strip()

        # Execute the query
        query_job = client.query(query)
        result = query_job.result().to_dataframe()

        # Extract columns that meet the criteria
        binary_columns = [col for col in result.columns if result.iloc[0][col]]

        return query, binary_columns

    except Exception as e:
        logging.ERROR(f"Error getting binary fields: {e}")
        return None, []

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

def render_convert_0_1_to_yes_no_cids_expression(col_name:str) -> str:
    """
    Render a SQL expression to repace "0" and "1" values to concept IDs "353358909" and "104430631" for "Yes" and "No", respectively.
    
    Use Case: Many of the "select-all-that-apply" questions in our surveys are flattened into binary responses where each possible
    selection is given a "Yes/No" response. However, during flattening, Yes/No responses are encoded as "0"/"1" instead of the desired 
    concept ids of "353358909"/"104430631". 
    
    Parameters:
        col_name (str): The column name to be processed.
        
    Returns:
        str: A SQL snippet containing the CASE expression with an alias assignment.
    """
    
    sql = \
    rf"""CASE
        WHEN {col_name} = "0" THEN "353358909" -- CID for Yes
        WHEN {col_name} = "1" THEN "104430631" -- CID for No
        WHEN {col_name} IS NULL THEN NULL      -- NULL
    END AS {col_name}"""
    
    return sql
    
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

def extract_ordered_concept_ids(var: str) -> list:
    """
    Extracts concept IDs (9-digit numbers) from a variable name in the order of appearance.
    
    For example:
        >>> extract_ordered_concept_ids("D_812370563_1_1_D_812370563_1_1_D_665036297")
        ['812370563', '812370563', '665036297']
    """
    pattern = re.compile(r'[dD]_(\d{9})')
    return pattern.findall(var)

def is_pure_variable(var: str) -> bool:
    """
    Returns True if the variable name is "pure"—i.e. it only consists of allowed tokens.
    
    Allowed tokens:
      - The literal "D" (or "d")
      - Any token composed solely of digits (e.g. a 9-digit concept ID or a loop number of any length)
      - The tokens "Connect_ID" or "token" (if expected)
    
    For example:
        >>> is_pure_variable("D_869387390_11_11_D_478706011_11")
        True
        >>> is_pure_variable("Connect_ID")
        True
        >>> is_pure_variable("token")
        True
        >>> is_pure_variable("D_907590067_4_4_SIBCANC3O_D_650332509_4")
        False
    """
    allowed_var_names = ['Connect_ID', 'token', 'uid']
    allowed_extras = ['SIBCANC3D','CHOL','MOMCANC3D','SIBCANC3O','UF','DADCANC3K','BLOODCLOT','DEPRESS2',
                      'DADCANC3K','SIBCANC3D','HTN','APPEND','TUBLIG','TONSILS','BREASTDIS','DM2',
                      'num','provided','string','entity'] 
    # NOTE: 'num', 'string', 'integer' and 'provided' are key words that indicate data type inconsistencies upstream in Firestore.
    #       These inconsistencies must be addressed by DevOps or critical data will be dropped.
    
    if var in allowed_var_names:
        return True
    
    tokens = var.split('_')
    for token in tokens:
        token = token.strip()
        if not token:
            continue
        # Allow literal "d" (case-insensitive)
        if token.lower() == 'd':
            continue
        # Allow tokens that are entirely digits
        if token.isdigit():
            continue
        # Allow additional allowed tokens
        if token in allowed_extras:
            continue
        # Otherwise, token is not allowed
        return False
    return True

def extract_loop_number(var_name: str) -> int:
    """
    Extracts the loop number (N) from a variable name.

    The loop number appears as a repeated digit `_N_N` following a concept ID.

    Examples:

        >>> extract_loop_number("d_123456789_2_2_d_987654321_2_2")
        2
        >>> extract_loop_number("d_111111111_1_1_d_222222222_1_1")
        1
        >>> extract_loop_number("d_123456789_9_9_d_987654321_9_9_9_9_9_9")
        9
        >>> extract_loop_number("d_123456789_5_5")
        5

    Args:
        var_name (str): The variable name containing the loop number.

    Returns:
        int: The identified loop number, or None if no valid pattern is found.
    """
    match = re.search(r'_(\d)\_\1(?!\d)', var_name)
    return int(match.group(1)) if match else None

def group_vars_by_cid_and_loop_num(var_names: list) -> dict:
    """
    Groups variable names that share the same concept IDs and the same loop number.

    Each key in the output dictionary is a tuple (concept_ids, loop_number), where:
      - `concept_ids` is a **frozenset** of unique concept IDs extracted from the variable name.
      - `loop_number` is the repeated integer (N) following `_N_N` (if present).

    Only variables with a valid loop number (N) are included.

    Examples:
        >>> var_list = [
        ...     "d_123456789_1_1_d_987654321_1_1",
        ...     "d_123456789_2_2_d_987654321_2_2",
        ...     "d_111111111_1_1_d_222222222_1_1",
        ...     "d_123456789_9_9_d_987654321_9_9",
        ...     "d_123456789_9_9_d_987654321_9_9_9_9_9_9",
        ...     "d_123456789_5_5",
        ...     "d_123456789"  # No loop number, should be ignored
        ... ]

        >>> grouped_vars = group_vars_by_cid_and_loop_num(var_list)

        >>> for key, vars in grouped_vars.items():
        ...     concept_ids, loop_number = key
        ...     print(f"Concept IDs: {sorted(concept_ids)}, Loop Number: {loop_number}, Variables: {vars}")

        Output:
        Concept IDs: ['123456789', '987654321'], Loop Number: 1, Variables: ['d_123456789_1_1_d_987654321_1_1']
        Concept IDs: ['123456789', '987654321'], Loop Number: 2, Variables: ['d_123456789_2_2_d_987654321_2_2']
        Concept IDs: ['111111111', '222222222'], Loop Number: 1, Variables: ['d_111111111_1_1_d_222222222_1_1']
        Concept IDs: ['123456789', '987654321'], Loop Number: 9, Variables: ['d_123456789_9_9_d_987654321_9_9', 'd_123456789_9_9_d_987654321_9_9_9_9_9_9']
        Concept IDs: ['123456789'], Loop Number: 5, Variables: ['d_123456789_5_5']

    Args:
        var_names (list): A list of variable names.

    Returns:
        dict: A dictionary where keys are tuples (frozenset(concept_ids), loop_number),
              and values are lists of variable names that match the criteria.
    """
    grouped_vars = defaultdict(list)

    for var in var_names:
        concept_ids = frozenset(extract_ordered_concept_ids(var))  # Unique set of concept IDs
        loop_number = extract_loop_number(var)  # Extract loop number (can be None)

        if concept_ids and loop_number is not None:  # Only include loop variables
            grouped_vars[(concept_ids, loop_number)].append(var)

    return dict(grouped_vars)

