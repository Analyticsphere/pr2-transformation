'''Module to compose SQL to execute data transformations.'''

from google.cloud import bigquery
from google.cloud import storage
import core.constants as constants
import core.utils as utils

def merge_table_versions(source_tables: list[str], destination_table: str) -> dict:
    """
    Creates or replaces a BigQuery table by performing a full outer join on the source tables.
    
    Both destination_table and each table in source_tables must be fully qualified in the format:
    project.dataset.table
    
    The function composes a query using the following logic:
      - Retrieves column names from each source table.
      - Computes common columns across all tables and unique columns per table.
      - Constructs a SELECT clause that uses COALESCE for common columns and retains unique columns.
      - Builds the FROM and FULL OUTER JOIN clauses using the fully qualified source tables.
      - Wraps the inner SELECT query with a CREATE OR REPLACE TABLE statement for the destination table.
      - Saves the composed SQL to a constant output path defined in constants.OUTPUT_SQL_PATH.
      
    Args:
        destination_table (str): Fully qualified table name for the destination (e.g., "myproject.mydataset.mytable").
        source_tables (list[str]): List of fully qualified table names to join.
        
    Returns:
        dict: Contains:
            - "status": A success message.
            - "submitted_sql_path": The constant path where the SQL was saved.
        
    Raises:
        ValueError: If the inputs are invalid or if a source table's columns cannot be retrieved.
        Exception: Propagates any errors encountered during query execution.
    """
    if not destination_table or not source_tables or len(source_tables) < 2:
        raise ValueError("A destination table and at least two source tables must be provided.")
    
    client = bigquery.Client()

    table_columns = {}
    aliases = []
    
    # Retrieve column names for each source table.
    for idx, table in enumerate(source_tables, start=1):
        # cols = utils.get_column_names(client, table)
        cols = utils.get_valid_column_names(client=client, fq_table=table)
        if not cols:
            error_msg = f"No columns retrieved from table: {table}"
            utils.logger.error(error_msg)
            raise ValueError(error_msg)
        alias = f"v{idx}"
        aliases.append(alias)
        table_columns[alias] = set(cols)

    # Compute the common columns across all source tables.
    common_columns = set.intersection(*table_columns.values())
    
    # Compute unique columns per table.
    unique_columns = {
        alias: table_columns[alias] - common_columns
        for alias in table_columns
    }
    
    # Build the SELECT clause.
    select_clauses = []
    
    if common_columns:
        select_clauses.append("-- Coalesced common columns")
        alias_order = aliases[::-1]
        for col in sorted(common_columns):
            coalesce_expr = ", ".join(f"{alias}.{col}" for alias in alias_order)
            select_clauses.append(f"COALESCE({coalesce_expr}) AS {col}")

    for alias in aliases:
        cols = unique_columns.get(alias)
        if cols:
            select_clauses.append(f"-- Unique columns from {alias}")
            for col in sorted(cols):
                select_clauses.append(f"{alias}.{col}")
    
    base_alias = aliases[-1]
    base_table = source_tables[-1]
    from_clause = f"`{base_table}` {base_alias}"
    
    join_clauses = []
    for alias, table in zip(aliases[:-1][::-1], source_tables[:-1][::-1]):
        join_clause = (
            f"FULL OUTER JOIN `{table}` {alias}\n"
            f"ON {base_alias}.Connect_ID = {alias}.Connect_ID"
        )
        join_clauses.append(join_clause)

    joined_select_clauses = ",\n    ".join(select_clauses)
    joined_join_clauses = "\n    ".join(join_clauses)

    inner_query = f"""
    SELECT
        {joined_select_clauses}
    FROM
        {from_clause}
        {joined_join_clauses}
    """.strip()

    final_query = f"CREATE OR REPLACE TABLE `{destination_table}` AS ({inner_query})"

    # Save the SQL to GCS Bucket for audit purposes
    try:
        gcs_client = storage.Client()
        gcs_path = f"{constants.OUTPUT_SQL_PATH}{destination_table}.sql"
        utils.save_sql_string(sql=final_query, path=gcs_path, storage_client=gcs_client)
    except Exception as e:
        utils.logger.exception(f"Error executing saving query {gcs_path}.")
        raise e

    # Submit query job to BQ
    try:
        query_job = client.query(final_query)
        query_job.result()  # Wait for the query to finish.
        status = f"Table {destination_table} successfully created or replaced."
    except Exception as e:
        utils.logger.exception("Error executing the BigQuery job.")
        raise e

    return {
        "status": status,
        "submitted_sql_path": constants.OUTPUT_SQL_PATH
    }

def apply_one_off_column_renames(source_table: str, destination_table: str) -> tuple[str, bool]:
    """
    Applies one-off column renames from constants.ONE_OFF_COLUMN_RENAME_MAPPINGS to a table.
    
    If a target column already exists, it will coalesce the source and target columns.
    
    Args:
        source_table (str): A fully qualified BigQuery table (e.g., "project.dataset.table").
        destination_table (str): A fully qualified destination table.
        
    Returns:
        tuple[str, bool]: A tuple containing:
            - The table name to use for the next step (either the original source_table if no renames
              were applied, or the destination_table if renames were applied)
            - A boolean indicating whether a new table was created (True) or not (False)
    """
    project, dataset, table_name = utils.parse_fq_table(source_table)
    client = bigquery.Client(project=project)
    
    # Get full table identifier to check for mappings
    parts = source_table.split('.')
    if len(parts) >= 2:
        full_table_identifier = '.'.join(parts[1:])
    else:
        full_table_identifier = table_name
    
    # Check if there are any mappings for this table
    mappings = constants.ONE_OFF_COLUMN_RENAME_MAPPINGS.get(full_table_identifier, [])
    
    if not mappings:
        # No mappings, no need to create a new table
        utils.logger.info(f"No one-off column mappings for table {full_table_identifier}, skipping rename step")
        return source_table, False
    
    # Get all columns from the source table
    columns = utils.get_column_names(client, source_table)
    
    # Convert to lowercase for case-insensitive matching (except Connect_ID)
    columns_lower = [col.lower() if col != "Connect_ID" else col for col in columns]
    
    # Create a lookup dictionary for original column names
    col_case_map = {col.lower(): col for col in columns if col != "Connect_ID"}
    col_case_map["connect_id"] = "Connect_ID"  # Ensure Connect_ID is preserved
    
    # Track which target names have been seen to handle duplicates
    target_columns = set()
    coalesce_groups = {}
    
    # Process mappings and group columns that need to be coalesced
    for mapping in mappings:
        source_col_lower = mapping['source'].lower()
        target_col_lower = mapping['target'].lower()
        
        # Skip mappings where the source column doesn't exist
        if source_col_lower not in columns_lower and source_col_lower not in col_case_map:
            utils.logger.warning(f"Source column '{mapping['source']}' not found in table, skipping")
            continue
        
        # Get the correctly cased source column
        source_col = col_case_map.get(source_col_lower, mapping['source'])
        
        # Add this to our coalesce groups
        if target_col_lower in target_columns or target_col_lower in columns_lower:
            # Target already exists, add to coalesce group
            if target_col_lower not in coalesce_groups:
                # If target already exists in original columns, initialize the group with it
                if target_col_lower in columns_lower:
                    original_col = col_case_map.get(target_col_lower)
                    coalesce_groups[target_col_lower] = [original_col]
                else:
                    coalesce_groups[target_col_lower] = []
            
            # Add the source column to the coalesce group
            coalesce_groups[target_col_lower].append(source_col)
            utils.logger.info(f"Adding '{source_col}' to coalesce group for '{mapping['target']}'")
        else:
            # First time seeing this target, mark it as seen
            target_columns.add(target_col_lower)
            # Initialize a coalesce group
            coalesce_groups[target_col_lower] = [source_col]
    
    # Prepare the SELECT clause with renames and coalescing
    select_parts = []
    
    # First add Connect_ID
    if "Connect_ID" in columns:
        select_parts.append("Connect_ID")
    
    # Process columns that are not involved in any rename
    for col in columns:
        col_lower = col.lower()
        
        # Skip Connect_ID as we've already added it
        if col == "Connect_ID":
            continue
        
        # Skip columns that are used as sources in our mappings
        is_source = False
        for mapping in mappings:
            if mapping['source'].lower() == col_lower:
                is_source = True
                break
        
        # Skip columns that will be processed as part of a coalesce group
        is_in_coalesce_group = False
        for group_cols in coalesce_groups.values():
            if col in group_cols:
                is_in_coalesce_group = True
                break
        
        if not is_source and not is_in_coalesce_group:
            select_parts.append(col)
    
    # Add coalesce groups
    for target_col, source_cols in coalesce_groups.items():
        # Get the properly cased target column name
        target_col_cased = next((mapping['target'] for mapping in mappings 
                               if mapping['target'].lower() == target_col), target_col)
        
        if len(source_cols) == 1:
            # No need to coalesce if there's only one source
            select_parts.append(f"{source_cols[0]} AS {target_col_cased}")
            utils.logger.info(f"Renaming: {source_cols[0]} AS {target_col_cased}")
        else:
            # Coalesce multiple source columns
            coalesce_expr = f"COALESCE({', '.join(source_cols)}) AS {target_col_cased}"
            select_parts.append(coalesce_expr)
            utils.logger.info(f"Coalescing: {coalesce_expr}")
    
    # Create the SQL for the destination table
    sql = f"""
    CREATE OR REPLACE TABLE `{destination_table}` AS
    SELECT
        {',\\n        '.join(select_parts)}
    FROM `{source_table}`
    """
    
    # Save the SQL to GCS for audit purposes
    try:
        gcs_client = storage.Client()
        gcs_path = f"{constants.OUTPUT_SQL_PATH}{destination_table}_rename.sql"
        utils.save_sql_string(sql=sql, path=gcs_path, storage_client=gcs_client)
    except Exception as e:
        utils.logger.exception(f"Error saving SQL: {e}")
        raise e
    
    # Execute the SQL
    try:
        query_job = client.query(sql)
        query_job.result()  # Wait for the query to finish
        status = f"Table {destination_table} created with renamed columns"
        utils.logger.info(status)
        return destination_table, True
    except Exception as e:
        utils.logger.exception(f"Error executing SQL: {e}")
        raise e

def process_loop_and_versioned_variables(source_table: str, destination_table: str) -> dict:
    """
    Generates and executes a SQL statement to coalesce multiple versions of loop variables,
    creating or replacing a destination table in BigQuery, and saves the SQL to a constant path.

    This function also applies one-off column renames specified in ONE_OFF_COLUMN_RENAME_MAPPINGS
    if they exist for the source table.
    
    Examples:
        Input variables -> Output columns
        
        # Basic loop variable
        d_123456789_1_1 -> d_123456789_1
        
        # Multiple variables with same concept IDs and loop number (coalesced)
        d_123456789_2_2, D_123456789_2_2_2_2 -> 
            COALESCE(d_123456789_2_2, D_123456789_2_2_2_2) AS d_123456789_2
        
        # Version handling (separate columns)
        d_123456789_1_1, d_123456789_v2_1_1 -> 
            d_123456789_1, d_123456789_1_v2
        
        # Multiple concept IDs
        d_123456789_3_3_d_987654321_3_3 -> d_123456789_d_987654321_3
    
    Args:
        source_table (str): A fully qualified BigQuery table (e.g., "project.dataset.table").
        destination_table (str): A fully qualified BigQuery table to create or replace.
        
    Returns:
        dict: A dictionary containing:
            - "status": A success message.
            - "submitted_sql_path": The constant path where the SQL was saved.
    
    Raises:
        ValueError: If any variable name is not pure.
        Exception: Propagates any errors encountered during query execution.
    """
    project, _, _ = utils.parse_fq_table(source_table)
    client = bigquery.Client(project=project)
    
    # At the beginning of compose_coalesce_loop_variable_query
    utils.validate_column_names(client, source_table)

    #variables = utils.get_column_names(client, source_table)
    variables = utils.get_valid_column_names(client=client, fq_table=source_table)

    # Convert all variable names to lower case except for "Connect_ID"
    variables = [v.lower() if v != "Connect_ID" else v for v in variables]
    utils.logger.info(f"Processing {len(variables)} total variables")

    for var in variables:
        if not utils.is_pure_variable(var):
            raise ValueError(f"Variable {var} is not pure. Please pre-process exceptions before composing the query.")
    
    # Group loop variables
    grouped_loop_vars = utils.group_vars_by_cid_and_loop_num(variables)

    # Find non-loop variables (all variables except those in the grouped loop vars)
    all_loop_vars = []
    for var_list in grouped_loop_vars.values():
        all_loop_vars.extend(var_list)

    non_loop_vars = [var for var in variables if var not in all_loop_vars and var != "Connect_ID"]

    select_clauses = []

    # Process loop variables
    for key, var_list in grouped_loop_vars.items():
        concept_ids, loop_number, version_suffix = key
        
        # Get the first variable for reference - only needed for concept ID ordering
        first_var = var_list[0]
        
        # Use the cleaned version (without version suffix) to get ordered concept IDs
        cleaned_var = utils.excise_version_from_column_name(first_var)
        ordered_ids = utils.extract_ordered_concept_ids(cleaned_var)
        
        # Construct raw variable name using ordered concept IDs, loop number, and version
        # Then remove fixed substrings like 'num' and 'state_' to standardize the output name
        raw_name = "_".join(f"d_{cid}" for cid in ordered_ids) + f"_{loop_number}" + version_suffix
        new_var_name = utils.excise_substrings(raw_name, constants.SUBSTRINGS_TO_FIX)

        
        if len(var_list) == 1:
            clause = f"{var_list[0]} AS {new_var_name}"
        else:
            clause = f"COALESCE({', '.join(var_list)}) AS {new_var_name}"
        select_clauses.append((new_var_name, clause))
        
    # Include non-loop variables in the SELECT clause
    # Also remove fixed substrings from their names for consistency
    for var in non_loop_vars:
        new_var_name = utils.excise_substrings(var, constants.SUBSTRINGS_TO_FIX)
        select_clauses.append((new_var_name, var))
    
    sorted_clauses = sorted(select_clauses, key=lambda x: x[0])
    select_clause_strs = [clause for _, clause in sorted_clauses]
    
    joined_select_clauses = ",\n        ".join(select_clause_strs)
    inner_query = f"""
    SELECT
        Connect_ID,
        {joined_select_clauses}
    FROM `{source_table}`
    """.strip()
    
    final_query = f"CREATE OR REPLACE TABLE `{destination_table}` AS ({inner_query})"
    
    # Save the SQL to GCS for Audit purposes:
    try:
        gcs_client = storage.Client()
        gcs_path = f"{constants.OUTPUT_SQL_PATH}{destination_table}.sql"
        utils.save_sql_string(sql=final_query, path=gcs_path, storage_client=gcs_client)
    except Exception as e:
        utils.logger.exception(f"Error executing saving query {gcs_path}.")
        raise e

    # Submit query job to BQ
    try:
        query_job = client.query(final_query)
        query_job.result()  # Wait for the query to finish.
        status = f"Table {destination_table} successfully created or replaced."
    except Exception as e:
        utils.logger.exception("Error executing the BigQuery job.")
        raise e

    return {
        "status": status,
        "submitted_sql_path": constants.OUTPUT_SQL_PATH
    }

def process_columns(source_table: str, destination_table: str) -> dict:
    """
    Pipeline function that:
    1. Applies one-off column renames from constants (if applicable)
    2. Processes loop and versioned variables
    
    Args:
        source_table (str): A fully qualified BigQuery table (e.g., "project.dataset.table").
        destination_table (str): A fully qualified destination table.
        
    Returns:
        dict: A dictionary with status information.
    """
    # Create intermediate table name with timestamp to avoid collisions
    from datetime import datetime
    project, dataset, table = utils.parse_fq_table(source_table)
    intermediate_table = f"{project}.{dataset}.intermediate_{table}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # Step 1: Apply one-off renames (if applicable)
        utils.logger.info("Step 1: Checking for one-off column renames")
        input_table, created_intermediate = apply_one_off_column_renames(source_table, intermediate_table)
        
        # Step 2: Process loop and versioned variables
        utils.logger.info("Step 2: Processing loop and versioned variables")
        result = process_loop_and_versioned_variables(input_table, destination_table)
        
        # Clean up intermediate table if it was created
        if created_intermediate:
            utils.logger.info(f"Cleaning up intermediate table {intermediate_table}")
            client = bigquery.Client(project=project)
            client.query(f"DROP TABLE `{intermediate_table}`")
        
        return result
    except Exception as e:
        utils.logger.exception(f"Error in process_columns: {e}")
        # Try to clean up intermediate table in case of failure
        try:
            client = bigquery.Client(project=project)
            client.query(f"DROP TABLE IF EXISTS `{intermediate_table}`")
        except:
            pass
        raise e