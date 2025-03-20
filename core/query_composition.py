'''Module to compose SQL to execute data transformations.'''

from google.cloud import bigquery
from google.cloud import storage
import core.constants as constants
import core.utils as utils

def create_or_replace_table_with_outer_join(source_tables: list[str], destination_table: str) -> dict:
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
        utils.logger.exception("Error executing saving query {gcs_path}.")
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


def compose_coalesce_loop_variable_query(source_table: str, destination_table: str) -> dict:
    """
    Generates and executes a SQL statement to coalesce multiple versions of loop variables,
    creating or replacing a destination table in BigQuery, and saves the SQL to a constant path.
    
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
    
    #variables = utils.get_column_names(client, source_table)
    variables = utils.get_valid_column_names(client=client, fq_table=source_table)

    # Convert all variable names to lower case except for "Connect_ID"
    variables = [v.lower() if v != "Connect_ID" else v for v in variables]
    
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
        
        # Get the first variable for reference
        first_var = var_list[0]
        
        # Extract ordered concept IDs for naming
        cleaned_first_var = utils.excise_version_from_column_name(first_var)
        ordered_ids = utils.extract_ordered_concept_ids(cleaned_first_var)
        
        # Use the version suffix in the new variable name
        # Move version_suffix to the end and ensure only one loop number
        new_var_name = "_".join(f"d_{cid}" for cid in ordered_ids) + f"_{loop_number}{version_suffix}"
        
        if len(var_list) == 1:
            clause = f"{var_list[0]} AS {new_var_name}"
        else:
            clause = f"COALESCE({', '.join(var_list)}) AS {new_var_name}"
        select_clauses.append((new_var_name, clause))
        
    # Add non-loop variables
    for var in non_loop_vars:
        select_clauses.append((var, var))
    
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
        utils.logger.exception("Error executing saving query {gcs_path}.")
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