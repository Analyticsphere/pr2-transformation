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
        >>> is_pure_variable("D_907590067_4_4_SIBCANC3O_D_650332509_4")
        False
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
        # Allow additional allowed tokens
        if token.lower() in constants.ALLOWED_NON_CID_SUBSTRINGS:
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
        >>> extract_loop_number("d_123456789_9_9_d_987654321_9_9_9_9_9_9")
        9
        >>> extract_loop_number("d_123456789_5_5")
        5

    Args:
        var_name (str): The variable name containing the loop number.

    Returns:
        int: The identified loop number, or None if no valid pattern is found.
    """
    match = re.search(r'_(\d+)\_\1(?!\d)', var_name)
    if match:
        return int(match.group(1))
    
    return None

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
