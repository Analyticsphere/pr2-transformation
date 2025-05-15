"""Module providing utility functions to support main pipeline functions."""

import os
import re
import sys
import logging
from collections import defaultdict

from google.cloud import bigquery, storage
import pandas as pd #TODO Try to avoid using pandas

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
    valid_values = constants.VALID_VALUES_FOR_FALSE_ARRAYS

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
    
    # Condition 3: At most one unique value matches the nine-digit concept pattern within brackets, e.g., "[178420302]".
    pattern = re.compile(r"\[\d{9}\]")
    cond3 = sum(1 for v in unique_values if (not pd.isna(v)) and pattern.search(v)) <= 1

    return cond1 and cond2 and cond3
