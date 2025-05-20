"""Module providing utility functions to support main pipeline functions."""

import os
import re
import sys
import logging
from collections import defaultdict

from google.cloud import bigquery, storage
import pandas as pd #TODO Try to avoid using pandas

if __name__ == "__main__":
    # Add parent directory to Python path when running as script
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.utils as utils
import core.constants as constants

# Set up a logging instance that will write to stdout (and therefor show up in Google Cloud logs)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
# Create the logger at module level so its settings are applied throughout code base
logger = logging.getLogger(__name__)

def parse_fq_table(fq_table: str) -> tuple[str, str, str]:
    """
    Parses a fully qualified BigQuery table name in the format 'project.dataset.table'
    and returns the project, dataset, and table name.
    """
    parts = fq_table.split('.')
    if len(parts) != 3:
        raise ValueError(f"Table name '{fq_table}' is not fully qualified as project.dataset.table")
    return parts[0], parts[1], parts[2]

def get_column_names(client: bigquery.Client, fq_table: str) -> list[str]:
    """
    Retrieves column names from a BigQuery table specified as a fully qualified name.
    
    Args:
        client (bigquery.Client): An initialized BigQuery client.
        fq_table (str): A fully qualified table name (e.g., "project.dataset.table").
        
    Returns:
        list[str]: A list of column names.
    """
    table = client.get_table(fq_table)
    return [schema_field.name for schema_field in table.schema]

def save_sql_string(sql: str, path: str, storage_client: storage.Client = None) -> None:
    """
    Saves the provided SQL string to a local file or a GCS bucket based on the given path.

    Args:
        sql (str): The SQL string to save.
        path (str): Either a local file path (e.g., "queries/submitted_query.sql") or a GCS path
                    in the format "gs://bucket_name/path/to/file.sql".
        storage_client (google.cloud.storage.Client, optional): An already initialized GCS client.
            If not provided, a new client will be created.
    """
    if path.startswith("gs://"):
        # Remove the gs:// scheme and split bucket name from the rest of the path.
        path_without_scheme = path.replace('gs://', '')
        # Split into two parts: (1) bucket_name and (2) Everything after the first '/' (i.e., the blob_path)
        parts = path_without_scheme.split(sep='/', maxsplit=1)
        if len(parts) != 2:
            raise ValueError("GCS path must be in the format gs://bucket_name/path/to/file")
        bucket_name, blob_path = parts[0], parts[1]
        
        # Use the provided client or create a new one.
        if storage_client is None:
            storage_client = storage.Client()
        
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(sql)
        print(f"SQL saved to {path}")
    else:
        # Ensure the local directory exists.
        local_dir = os.path.dirname(path)
        if local_dir and not os.path.exists(local_dir):
            os.makedirs(local_dir)
        with open(path, "w") as f:
            f.write(sql)
        print(f"SQL saved locally to {path}")

def extract_ordered_concept_ids(var: str) -> list:
    """
    Extracts concept IDs (9-digit numbers) from a variable name in the order of appearance.
    
    For example:
        >>> extract_ordered_concept_ids("D_812370563_1_1_D_812370563_1_1_D_665036297")
        ['812370563', '812370563', '665036297']
    """
    pattern = re.compile(r'[dD]_(\d{9})')
    return pattern.findall(var)

def find_non_standard_concept_ids(column_names: list) -> list:
    """
    Identifies column names with concept IDs that don't follow the 9-digit standard.
    
    Args:
        column_names: List of column names to check
        
    Returns:
        list: Column names with non-standard concept IDs
    """
    non_standard = []
    for col in column_names:
        # Check for the pattern d_ or D_ followed by digits that aren't exactly 9 characters
        matches = re.findall(r'[dD]_(\d+)(?=_|$)', col)
        for match in matches:
            if len(match) != 9:
                non_standard.append((col, match, len(match)))
    
    return non_standard

def validate_column_names(client: bigquery.Client, fq_table: str) -> None:
    """
    Validates column names in the table to ensure they follow naming standards.
    Raises a warning for non-standard columns.
    """
    columns = get_column_names(client, fq_table)
    non_standard = find_non_standard_concept_ids(columns)
    
    if non_standard:
        utils.logger.warning(f"Found {len(non_standard)} columns with non-standard concept IDs:")
        for col, concept_id, length in non_standard:
            utils.logger.warning(f"  - {col}: Concept ID '{concept_id}' has {length} digits (should be 9)")
        
        # Optionally, we could raise an exception to halt the pipeline
        # raise ValueError("Non-standard concept IDs found. Please fix the source data.")

def is_pure_variable(var: str) -> bool:
    """
    Returns True if the variable name is "pure"—i.e. it only consists of allowed tokens.
    
    Allowed tokens:
      - The literal "D" (or "d")
      - Any token composed solely of digits (e.g. a 9-digit concept ID or a loop number of any length)
      - The tokens "Connect_ID" or "token" (if expected)
      - Version indicators like "v1", "v2", "v3", etc.
    
    For example:
        >>> is_pure_variable("D_869387390_11_11_D_478706011_11")
        True
        >>> is_pure_variable("D_907590067_4_4_SIBCANC3O_D_650332509_4")
        False
        >>> is_pure_variable("D_299417266_v2")
        True
    """
    
    if var.lower() in constants.ALLOWED_NON_CID_VARIABLE_NAMES and var.lower():
        return True
    
    if var.lower() in constants.FORBIDDEN_NON_CID_VARIABLE_NAMES:
        return False
    
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
        # Allow version indicators like v1, v2, v3, etc.
        if token.lower().startswith('v') and token[1:].isdigit():
            continue
        # Allow additional allowed tokens
        if token.lower() in constants.ALLOWED_NON_CID_SUBSTRINGS:
            continue
        # Otherwise, token is not allowed
        return False
    return True

def extract_version_suffix(var_name: str) -> str:
    """
    Extracts the version suffix (like _v2, _v3) from a variable name, regardless of position
    
    Returns the version suffix if found, or an empty string if no version is present.
    
    Examples:
        >>> extract_version_suffix("d_123456789_v2_1_1")
        "_v2"
        >>> extract_version_suffix("d_123456789_V3_1_1")
        "_v3"
        >>> extract_version_suffix("d_123456789_1_1")
        ""
    """
    match = re.search(r'_[vV](\d+)(?=_|$)', var_name)
    if match:
        return f"_v{match.group(1)}"
    return ""

def excise_version_from_column_name(column_name: str) -> str:
    """
    Removes version suffixes (_v1, _v2, _v3, etc.) from column names while preserving 
    the rest. This works regardless of the position of _vN.
    
    Examples:
    - D_191057574_V2 → D_191057574
    - D_715581797_V3_1_1 → D_715581797_1_1
    - D_899251483_V2_D_452438775 → D_899251483_D_452438775
    
    Parameters:
        column_name (str): Original column name with version suffix
        
    Returns:
        str: Cleaned column name with version suffix removed
    """
    # Use regex to find and remove the _vN part where N is any digit
    return re.sub(r'_[vV]\d+(?=_|$)', '', column_name)

def extract_loop_number(var_name: str) -> int:
    """
    Extracts the loop number from a variable name, accounting for version suffixes and patterns.
    Returns the first matched loop number, or None if not found.
    """
    # Case 1: Version-style loop pattern like _v2_5_5
    match = re.search(r'_v\d+_(\d+)_\1(?!\d)', var_name, re.IGNORECASE)
    if match:
        return int(match.group(1))

    cleaned_var = excise_version_from_column_name(var_name)

    # Case 2: Match all _N_N loop patterns
    matches = re.findall(r'_(\d+)_\1(?!\d)', cleaned_var)
    if matches:
        return int(matches[0])

    # Case 3: Only match trailing _N if there's also _N_N pattern in the string
    if re.search(r'_(\d+)_\1', cleaned_var):  # pattern like _3_3
        match = re.search(r'_(\d+)$', cleaned_var)
        if match:
            return int(match.group(1))

    return None

def group_vars_by_cid_and_loop_num(var_names: list) -> dict:
    """
    Groups variable names that share the same concept IDs, loop number, and version.

    Each key in the output dictionary is a tuple (concept_ids, loop_number, version_suffix), where:
    - `concept_ids` is a **frozenset** of unique concept IDs extracted from the variable name.
    - `loop_number` is the repeated integer (N) following `_N_N` (if present).
    - `version_suffix` is the version suffix (e.g., "_v2", "_v3") or empty string for no version.
    
    Only variables with a valid loop number (N) are included.
    """
    grouped_vars = defaultdict(list)
    
    for var in var_names:
        # Extract the version suffix
        version_suffix = extract_version_suffix(var)
        
        # Clean the variable name to extract concept IDs
        cleaned_var = excise_version_from_column_name(var)
        
        # Extract concept IDs and loop number from the cleaned variable name
        concept_ids = frozenset(extract_ordered_concept_ids(cleaned_var))
        loop_number = extract_loop_number(var)
        
        if concept_ids and loop_number is not None:  # Only include loop variables
            # Include version_suffix in the grouping key
            grouped_vars[(concept_ids, loop_number, version_suffix)].append(var)
    
    return dict(grouped_vars)

def get_list_non_cid_str_patterns(column_names):
    """
    Pulls out column names that do not meet pre-defined structures and returns invalid strings with the column names they come from.

     Examples:
        D_907590067_4_4_SIBCANC3O_D_650332509_4 --> ['sibcanc3o', 'D_907590067_4_4_SIBCANC3O_D_650332509_4']
        hello --> ['hello', 'hello']
        d_123456789_1_1_d_987654321_1_1 --> []
    
    Args:
        column_names: a list of column names to run the code on
        
    Returns:
        list[zip(invalid_str_params, original_col_names)]: A list of tuples of invalid string parameters and the column names they are pulled from.
    """
    invalid_str_params = []
    original_col_names = []
    
    for colname in column_names:
        pattern = r'd_\d{9}(?:_\d{1,2})*'  # * allows zero or more occurrences of _n_n
        cleaned_colname = re.sub(pattern, '', colname, flags=re.IGNORECASE).strip("_").strip()
    
    if cleaned_colname and cleaned_colname != "_" and cleaned_colname != "connect_id" and cleaned_colname != "token":
        invalid_str_params.append(cleaned_colname)
        original_col_names.append(colname)
    
    return list(zip(invalid_str_params, original_col_names))

def get_column_exceptions_to_exclude(client: bigquery.Client, fq_table: str) -> list:
    """
    Retrieve a list of column names to exclude from the table based on forbidden names
    and excluded substrings.
    
    Parameters:
        client (bigquery.Client): A BigQuery client used to query the table schema.
        fq_table (str): Fully-qualified table name from which to retrieve column names.
        
    Returns:
        list: A list of column names that should be excluded from further processing.
    """
    # Retrieve all column names for the table
    columns = get_column_names(client, fq_table)
    
    # Retrieve forbidden column names and substrings to exclude from constants
    forbidden = constants.FORBIDDEN_NON_CID_VARIABLE_NAMES
    excluded_substrings = constants.EXCLUDED_NON_CID_SUBSTRINGS
    
    columns_to_exclude = []
    for col in columns:
        # If the column is explicitly forbidden, mark it for exclusion.
        if col.lower() in [f.lower() for f in forbidden]:
            columns_to_exclude.append(col)
        else:
            # Otherwise, check if any excluded substring is present in the column name (case-insensitive).
            if any(sub.lower() in col.lower() for sub in excluded_substrings):
                columns_to_exclude.append(col)
    
    return columns_to_exclude

def get_valid_column_names(client: bigquery.Client, fq_table: str) -> set:
    """
    Retrieves valid column names by removing excluded columns from all columns.
    
    Parameters:
        client: A database client used to query the table schema.
        fq_table (str): Fully-qualified table name from which to retrieve column names.
        
    Returns:
        set: A set of valid column names that can be used for further processing.
    """
    columns_all = get_column_names(client=client, fq_table=fq_table)
    columns_exclude = get_column_exceptions_to_exclude(client=client, fq_table=fq_table)
    valid_columns = set(columns_all) - set(columns_exclude)
    return list(valid_columns)

def excise_substrings(var_name: str, substrings_to_excise: list[str]) -> str:
    """
    Removes all substrings from a variable name that appear in the substrings_to_fix list.
    """
    for substring in substrings_to_excise:
        var_name = var_name.replace(substring, "")
    return var_name

def standardize_column_case(column_name: str) -> str:
    """
    Standardizes column names to lowercase for consistency.
    Preserves Connect_ID as-is since it's the special case.
    
    Args:
        column_name: Original column name
        
    Returns:
        Standardized column name
    """
    if column_name == "Connect_ID":
        return column_name  # Preserve Connect_ID case
    return column_name.lower()

def get_binary_columns(client: bigquery.Client, fq_table: str) -> list:
    if client is None:
        client = bigquery.Client()
    
    project_id, dataset_id, table_id = parse_fq_table(fq_table)
    utils.logger.info(f"Starting binary column detection for {fq_table}")
    
    # Step 1: Get STRING columns
    schema_query = f"""
        SELECT column_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_id}' AND data_type = 'STRING'
    """
    
    try:
        columns = [row.column_name for row in client.query(schema_query).result()]
        utils.logger.info(f"Found {len(columns)} STRING columns")
        
        if not columns:
            return []
        
        # Step 2: Process in smaller batches to avoid excessive query size
        batch_size = 500
        binary_columns = []
        
        for i in range(0, len(columns), batch_size):
            batch = columns[i:i+batch_size]
            utils.logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} columns")
            
            checks = []
            for col in batch:
                check_expr = f"""COUNTIF(
                    NOT (`{col}` = "0" OR `{col}` = "1" OR `{col}` IS NULL OR `{col}` = "")
                ) = 0 AS `{col}`"""
                checks.append(check_expr)
            
            joined_checks = ',\n'.join(checks)
            batch_query = f"""
                SELECT
                    {joined_checks}
                FROM `{fq_table}`
            """
            
            batch_result = client.query(batch_query).result()
            batch_df = batch_result.to_dataframe().transpose()
            batch_df.columns = ['is_binary']
            batch_df.reset_index(inplace=True)
            batch_df.rename(columns={'index': 'column_name'}, inplace=True)
            
            batch_binary = batch_df[batch_df['is_binary'] == True]['column_name'].tolist()
            binary_columns.extend(batch_binary)
            
            utils.logger.info(f"Found {len(batch_binary)} binary columns in this batch")
        
        utils.logger.info(f"Total binary columns detected: {len(binary_columns)}")
        return binary_columns
        
    except Exception as e:
        utils.logger.error(f"Error in binary column detection: {str(e)}")
        utils.logger.error(f"Exception type: {type(e).__name__}")
        return []

def render_convert_0_1_to_yes_no_cids_expression(col_name: str) -> str:
    """
    Generate a SQL CASE expression that replaces binary string values "0"/"1"
    with standardized concept IDs for "No" and "Yes", respectively.

    Concept ID mapping:
      - "1" → "353358909" (Yes)
      - "0" → "104430631" (No)

    This is used for select-all-that-apply survey questions that have
    been flattened to binary columns during ETL.

    Parameters:
        col_name (str): The name of the column to transform.

    Returns:
        str: A SQL CASE expression with aliasing, ready to be inserted into a SELECT clause.
    
    Example:
        render_convert_0_1_to_yes_no_cids_expression("D_12345")
        → CASE WHEN D_12345 = "1" THEN "353358909" ...
    """
    return f"""CASE
        WHEN {col_name} = "1" THEN "353358909" -- CID for Yes
        WHEN {col_name} = "0" THEN "104430631" -- CID for No
        WHEN {col_name} IS NULL THEN NULL
        WHEN {col_name} = "" THEN NULL
        ELSE NULL
    END AS {col_name}
    """.strip() 

def get_strict_false_array_columns(client: bigquery.Client, fq_table: str, batch_size=100) -> list:
    """
    Optimized version of get_strict_false_array_columns that reduces table scans
    while maintaining code readability.
    
    This function performs all three conditions of false array detection in a single query per column:
    1. ≤ 3 distinct values
    2. Only contains specific values or NULL as specified in constants module
    3. ≤ 1 unique value matching the bracketed 9-digit pattern
    
    Args:
        client (bigquery.Client): BigQuery client
        fq_table (str): Fully qualified table name
        batch_size (int): Number of columns to process in each batch
        
    Returns:
        list: Column names that satisfy all false array conditions
    """
    if client is None:
        client = bigquery.Client()

    project_id, dataset_id, table_id = utils.parse_fq_table(fq_table)
    utils.logger.info(f"Starting optimized strict false array detection for {fq_table}")

    try:
        # Get all column names
        columns = utils.get_column_names(client=client, fq_table=fq_table)
        if not columns:
            return []
        
        # Explicitly exclude "Connect_ID" from processing
        columns = [col for col in columns if col != "Connect_ID"]
        utils.logger.info(f"Processing {len(columns)} columns after excluding Connect_ID")
        
        strict_false_columns = []
        
        # Process columns in batches to avoid excessive query size
        for i in range(0, len(columns), batch_size):
            batch = columns[i:i+batch_size]
            utils.logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} columns")
            
            # Build a more readable version of the false array values condition
            false_values_list = ", ".join([f"'{val}'" for val in constants.FALSE_ARRAY_VALUES])
            
            # Create a single query per column that performs all checks at once
            column_checks = []
            for col in batch:
                # Create a clear, well-structured check for this column
                column_check = f"""
                SELECT
                '{col}' AS column_name,
                -- Check 1: Column has ≤3 distinct values AND at least one non-null value
                ((SELECT COUNT(DISTINCT `{col}`) FROM `{fq_table}`) <= 3 
                AND 
                (SELECT COUNT(DISTINCT `{col}`) FROM `{fq_table}` WHERE `{col}` IS NOT NULL) > 0) AS has_few_non_null_values,
                -- Check 2: Column only contains NULL or values from our false array list
                (SELECT COUNTIF(
                `{col}` IS NOT NULL
                AND `{col}` NOT IN ({false_values_list})
                ) FROM `{fq_table}`) = 0 AS only_has_false_array_values,
                -- Check 3: Column has at most 1 value matching our bracketed pattern
                (SELECT COUNT(DISTINCT `{col}`)
                FROM `{fq_table}`
                WHERE REGEXP_CONTAINS(`{col}`, r'{constants.BRACKETED_NINE_DIGIT_PATTERN}')
                ) <= 1 AS has_single_concept_id
                FROM
                -- This is just a dummy FROM clause that returns exactly one row
                (SELECT 1) AS dummy
                """
                column_checks.append(column_check)
            
            # Combine all column checks with UNION ALL
            combined_query = "\nUNION ALL\n".join(column_checks)
            
            # Add an outer query that filters for columns passing all checks
            final_query = f"""
            SELECT column_name
            FROM ({combined_query})
            WHERE 
                has_few_non_null_values = TRUE
                AND only_has_false_array_values = TRUE
                AND has_single_concept_id = TRUE
            """
            
            utils.logger.info(f"Executing combined query to check {len(batch)} columns")
            
            try:
                # Write out the query for debugging if needed
                with open('combined_query.sql', 'w') as file:
                    file.write(final_query)
                
                # Execute the query and collect results
                query_job = client.query(final_query)
                batch_results = [row.column_name for row in query_job.result()]
                
                strict_false_columns.extend(batch_results)
                utils.logger.info(f"Found {len(batch_results)} strict false array columns in this batch")
            except Exception as e:
                utils.logger.error(f"Error executing batch query: {str(e)}")
                utils.logger.error(f"Exception type: {type(e).__name__}")
                utils.logger.error(f"Exception args: {e.args}")
                # Continue with next batch instead of failing completely
        
        utils.logger.info(f"Total strict false array columns detected: {len(strict_false_columns)}")
        return strict_false_columns

    except Exception as e:
        utils.logger.error(f"Error in optimized strict false array detection: {str(e)}")
        utils.logger.error(f"Exception type: {type(e).__name__}")
        utils.logger.error(f"Exception args: {e.args}")
        return []
    
def get_false_array_columns_for_tables(tables: list[str], batch_size=50) -> dict:
    """
    Generate a dictionary with table names and their false array columns.
    
    Args:
        tables (list[str]): List of fully qualified table names
        batch_size (int): Number of columns to process in each batch
        
    Returns:
        dict: Dictionary with table names as keys and lists of false array columns as values
    """
    client = bigquery.Client()
    result = {}
    
    for table in tables:
        utils.logger.info(f"Processing table: {table}")
        try:
            false_array_columns = get_strict_false_array_columns(
                client=client, 
                fq_table=table, 
                batch_size=batch_size
            )
            
            # Store the result in the dictionary
            result[table] = false_array_columns
            
            # Log the results
            utils.logger.info(f"Found {len(false_array_columns)} false array columns in {table}")
            if false_array_columns:
                utils.logger.info(f"False array columns: {', '.join(false_array_columns)}")
        except Exception as e:
            utils.logger.error(f"Error processing table {table}: {str(e)}")
            result[table] = []  # Empty list for failed tables
    
    return result
   
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

if __name__ == '__main__':


    # List of tables to process

    tables_to_process = [
        "nih-nci-dceg-connect-prod-6d04.ForTestingOnly.module1_v1_with_cleaned_columns",
        "nih-nci-dceg-connect-prod-6d04.ForTestingOnly.module1_v2_with_cleaned_columns",
        "nih-nci-dceg-connect-prod-6d04.ForTestingOnly.module2_v1_with_cleaned_columns",
        "nih-nci-dceg-connect-prod-6d04.ForTestingOnly.module2_v2_with_cleaned_columns",
        "nih-nci-dceg-connect-prod-6d04.ForTestingOnly.module3_with_cleaned_columns",
        "nih-nci-dceg-connect-prod-6d04.ForTestingOnly.module4_with_cleaned_columns",
        # Add more tables as needed
    ]
    
    # Process all tables and get results
    results = get_false_array_columns_for_tables(tables_to_process, batch_size=50)
    
    # Print the results in a readable format
    print("\nSummary of False Array Columns by Table:")
    print("=======================================")
    for table, columns in results.items():
        table_short_name = table.split('.')[-1]  # Get the last part of the table name for cleaner output
        print(f"\n{table_short_name}:")
        if columns:
            for col in columns:
                print(f"  - {col}")
        else:
            print("  No false array columns found")
    
    # You could also save the results to a file
    import json
    with open('false_array_columns.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\nResults have been saved to false_array_columns.json")