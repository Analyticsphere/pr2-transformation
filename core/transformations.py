'''Module to compose SQL to execute data transformations.'''

import re
from google.cloud import bigquery
from google.cloud import storage
import core.constants as constants
import core.utils as utils
import core.transform_renderer as transform_renderer

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
        cols = utils.get_valid_column_names(client=client, fq_table=table)
        if not cols:
            error_msg = f"No columns retrieved from table: {table}"
            utils.logger.error(error_msg)
            raise ValueError(error_msg)
        alias = f"v{idx}"
        aliases.append(alias)
        table_columns[alias] = cols  # Keep as list to preserve order
    
    # Create case-insensitive mappings for each table
    case_maps = {}
    for alias in aliases:
        # Maps lowercase column names to their original cases
        case_maps[alias] = {col.lower(): col for col in table_columns[alias]}
    
    # Compute common columns (case-insensitively)
    common_columns_lower = set()
    for alias in aliases:
        lower_cols = set(case_maps[alias].keys())
        if not common_columns_lower:
            # First table - all columns are potentially common
            common_columns_lower = lower_cols
        else:
            # Intersect with columns from this table
            common_columns_lower = common_columns_lower.intersection(lower_cols)
    
    # Track processed columns to avoid duplicates
    processed_columns_lower = set()
    
    # Build the SELECT clause.
    select_clauses = []
    
    # Handle common columns first
    if common_columns_lower:
        select_clauses.append("-- Coalesced common columns")
        for col_lower in sorted(common_columns_lower):
            # Special case for Connect_ID
            if col_lower == "connect_id":
                output_name = "Connect_ID"
            else:
                output_name = col_lower
                
            coalesce_parts = []
            for alias in aliases:
                # Use the original case from each table
                original_col = case_maps[alias][col_lower]
                coalesce_parts.append(f"{alias}.{original_col}")
                
            select_clauses.append(f"COALESCE({', '.join(coalesce_parts)}) AS {output_name}")
            processed_columns_lower.add(col_lower)
    
    # Handle unique columns per table
    for alias in aliases:
        # Find columns not already processed
        unique_cols = [col for col in table_columns[alias] 
                      if col.lower() not in processed_columns_lower]
        
        if unique_cols:
            select_clauses.append(f"-- Unique columns from {alias}")
            for col in sorted(unique_cols):
                # Standardize case in output name
                if col == "Connect_ID":
                    output_name = "Connect_ID"
                else:
                    output_name = col.lower()
                
                select_clauses.append(f"{alias}.{col} AS {output_name}")
                processed_columns_lower.add(col.lower())
    
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

    # Format the SQL for better readability
    final_query = utils.format_sql_query(final_query)

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

def build_one_off_renames_clauses(client: bigquery.Client, source_table: str, processed_columns: set) -> tuple[list, set]:
    """
    Builds SELECT clauses for one-off column renames from constants.
    
    Args:
        client: BigQuery client
        source_table: Source table name
        processed_columns: Set of already processed column names (lowercase)
        
    Returns:
        tuple: (list of SELECT clauses, updated set of processed columns)
    """
    select_clauses = []
    
    # Get table identifier for mappings lookup
    parts = source_table.split('.')
    _, _, table = utils.parse_fq_table(source_table)
    full_table_identifier = '.'.join(parts[1:]) if len(parts) >= 2 else table
    
    # Get mappings for this table
    mappings = constants.ONE_OFF_COLUMN_RENAME_MAPPINGS.get(full_table_identifier, [])
    
    if not mappings:
        utils.logger.info(f"No one-off column mappings for table {full_table_identifier}")
        return select_clauses, processed_columns
    
    utils.logger.info(f"Found {len(mappings)} one-off column mappings")
    
    # Get all columns from the source table
    columns = utils.get_column_names(client, source_table)
    
    # Convert to lowercase for case-insensitive matching
    columns_lower = [col.lower() for col in columns]
    
    # Create a lookup dictionary for original column names
    col_case_map = {col.lower(): col for col in columns}
    
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
    
    # Add coalesce groups to select clauses
    for target_col, source_cols in coalesce_groups.items():
        # Get the properly cased target column name
        target_col_cased = next((mapping['target'] for mapping in mappings 
                               if mapping['target'].lower() == target_col), target_col)
        
        # Convert variable names to lower case (except Connect_ID)
        target_col = utils.standardize_column_case(target_col)
            
        # Skip if already processed
        if target_col in processed_columns:
            continue
            
        if len(source_cols) == 1:
            # No need to coalesce if there's only one source
            select_clauses.append(f"{source_cols[0]} AS {target_col_cased}")
            utils.logger.info(f"Renaming: {source_cols[0]} AS {target_col_cased}")
        else:
            # Coalesce multiple source columns
            coalesce_expr = f"COALESCE({', '.join(source_cols)}) AS {target_col_cased}"
            select_clauses.append(coalesce_expr)
            utils.logger.info(f"Coalescing: {coalesce_expr}")
        
        # Mark as processed
        processed_columns.add(target_col)
        for col in source_cols:
            processed_columns.add(col.lower())
    
    return select_clauses, processed_columns

def build_substring_removal_clauses(client: bigquery.Client, source_table: str, processed_columns: set) -> tuple[list, set]:
    """
    Builds SELECT clauses for removing substrings defined in constants.SUBSTRINGS_TO_FIX.
    
    Args:
        client: BigQuery client
        source_table: Source table name
        processed_columns: Set of already processed column names (lowercase)
        
    Returns:
        tuple: (list of SELECT clauses, updated set of processed columns)
    """
    select_clauses = []
    
    # Get all columns from the source table
    all_columns = utils.get_column_names(client, source_table)
    
    # Identify columns that need substring removal
    subset_columns = []
    for col in all_columns:
        # Skip already processed columns
        if col.lower() in processed_columns:
            continue
            
        # Check if any substring from constants.SUBSTRINGS_TO_FIX is in the column name
        if any(substring in col for substring in constants.SUBSTRINGS_TO_FIX):
            subset_columns.append(col)
    
    utils.logger.info(f"Found {len(subset_columns)} columns with substrings to remove")
    
    # If no columns with substrings to remove, return empty list
    if not subset_columns:
        utils.logger.info(f"No columns with substrings to remove found in {source_table}")
        return select_clauses, processed_columns
    
    # Group columns by what they would be after substring removal to handle duplicates
    column_groups = {}
    for col in all_columns:
        # Skip already processed columns
        if col.lower() in processed_columns:
            continue
            
        # Apply substring removal to get the new column name
        new_col = utils.excise_substrings(col, constants.SUBSTRINGS_TO_FIX)

        # Add standardization:
        # Standardize case for the new variable name
        new_col = utils.standardize_column_case(new_col)
        
        # Group columns by their new name to identify duplicates
        if new_col not in column_groups:
            column_groups[new_col] = []
        column_groups[new_col].append(col)
    
    # Process each column group
    for new_col, cols in column_groups.items():
        # Skip if new column name would collide with processed column
        if new_col.lower() in processed_columns:
            utils.logger.info(f"Skipping column group for {new_col} as it would create a duplicate")
            continue
            
        if len(cols) == 1:
            # No duplicate - simple rename or keep as is
            col = cols[0]
            if col != new_col:  # Only add AS clause if the name actually changes
                select_clauses.append(f"{col} AS {new_col}")
            else:
                select_clauses.append(col)
        else:
            # Handle duplicate with COALESCE
            # Sort by priority (columns with fewer substrings first)
            def priority_key(col):
                # Count how many substrings from SUBSTRINGS_TO_FIX are in the column name
                return sum(1 for substring in constants.SUBSTRINGS_TO_FIX if substring in col)
                
            sorted_cols = sorted(cols, key=priority_key)
            
            select_clauses.append(f"COALESCE({', '.join(sorted_cols)}) AS {new_col}")
            utils.logger.info(f"Using COALESCE for duplicate columns: {sorted_cols} -> {new_col}")
        
        # Mark as processed
        processed_columns.add(new_col.lower())
        for col in cols:
            processed_columns.add(col.lower())
    
    return select_clauses, processed_columns

def build_custom_transform_clauses(client: bigquery.Client, source_table: str, processed_columns: set) -> tuple[list, set]:
    """
    Builds SELECT clauses for custom column transformations defined in constants.CUSTOM_TRANSFORMS.
    
    Args:
        client: BigQuery client
        source_table: Source table name
        processed_columns: Set of already processed column names (lowercase)
        
    Returns:
        tuple: (list of SELECT clauses, updated set of processed columns)
    """
    
    
    select_clauses = []
    
    # Get table identifier for transforms lookup
    project, dataset, table = utils.parse_fq_table(source_table)
    # Look up both with and without project prefix since constants might have either format
    table_identifiers = [
        f"{dataset}.{table}",  # dataset.table format
        source_table  # full project.dataset.table format
    ]
    
    # Check if we have any transforms for this table
    transform_dict = {}
    for table_id in table_identifiers:
        if table_id in constants.CUSTOM_TRANSFORMS:
            transform_dict = {table_id: constants.CUSTOM_TRANSFORMS[table_id]}
            utils.logger.info(f"Found {len(constants.CUSTOM_TRANSFORMS[table_id])} custom transforms for {table_id}")
            break
    
    if not transform_dict:
        utils.logger.info(f"No custom transforms found for {source_table}")
        return select_clauses, processed_columns
    
    try:
        # Render the SQL expressions for the transforms
        rendered_sql = transform_renderer.render_transforms(transform_dict)
        
        # Process each rendered SQL expression
        for table_id, sql_expressions in rendered_sql.items():
            for expr in sql_expressions:
                # Extract target column name from the expression (assuming it ends with "AS column_name")
                target_match = re.search(r'AS\s+([^\s,]+)\s*$', expr)
                if target_match:
                    target_column = target_match.group(1)
                    
                    # Skip if target column would create a duplicate
                    if target_column.lower() in processed_columns:
                        utils.logger.info(f"Skipping custom transform for {target_column} as it would create a duplicate")
                        continue
                    
                    # Add the expression to our clauses
                    select_clauses.append(expr)
                    processed_columns.add(target_column.lower())
                else:
                    utils.logger.warning(f"Could not extract target column from custom transform: {expr}")
    except Exception as e:
        utils.logger.error(f"Error rendering custom transforms: {e}")
        # Continue with the pipeline even if there's an error with custom transforms
    
    return select_clauses, processed_columns

def build_loop_variable_clauses(client: bigquery.Client, source_table: str, processed_columns: set) -> tuple[list, set]:
    """
    Builds SELECT clauses for loop variable processing.
    
    Args:
        client: BigQuery client
        source_table: Source table name
        processed_columns: Set of already processed column names (lowercase)
        
    Returns:
        tuple: (list of SELECT clauses, updated set of processed columns)
    """
    select_clauses = []
    
    # Get valid columns that haven't been processed yet
    all_columns = utils.get_column_names(client, source_table)
    remaining_columns = [col for col in all_columns if col.lower() not in processed_columns]
    
    # Apply validation
    for var in remaining_columns:
        if not utils.is_pure_variable(var):
            utils.logger.warning(f"Variable {var} is not pure. Skipping loop variable processing.")
            # Add to processed to avoid including it later
            processed_columns.add(var.lower())
    
    # Get valid remaining columns
    valid_columns = [col for col in remaining_columns if col.lower() not in processed_columns 
                     and utils.is_pure_variable(col)]
    
    # Group loop variables
    grouped_loop_vars = utils.group_vars_by_cid_and_loop_num(valid_columns)

    # Find non-loop variables (all variables except those in the grouped loop vars)
    all_loop_vars = []
    for var_list in grouped_loop_vars.values():
        all_loop_vars.extend(var_list)

    non_loop_vars = [var for var in valid_columns if var not in all_loop_vars]

    # Process loop variables
    for key, var_list in grouped_loop_vars.items():
        concept_ids, loop_number, version_suffix = key
        
        # Get the first variable for reference - only needed for concept ID ordering
        first_var = var_list[0]
        
        # Use the cleaned version (without version suffix) to get ordered concept IDs
        cleaned_var = utils.excise_version_from_column_name(first_var)
        ordered_ids = utils.extract_ordered_concept_ids(cleaned_var)
        
        # Construct raw variable name using ordered concept IDs, loop number, and version
        # Then remove fixed substrings to standardize the output name
        raw_name = "_".join(f"d_{cid}" for cid in ordered_ids) + f"_{loop_number}" + version_suffix
        new_var_name = utils.excise_substrings(raw_name, constants.SUBSTRINGS_TO_FIX)
        
        # Standardize case for the new variable name
        new_var_name = utils.standardize_column_case(new_var_name)
        
        # Skip this variable if it would create a duplicate
        if new_var_name.lower() in processed_columns:
            utils.logger.info(f"Skipping output column {new_var_name} as it would create a duplicate")
            continue
        
        if len(var_list) == 1:
            clause = f"{var_list[0]} AS {new_var_name}"
        else:
            clause = f"COALESCE({', '.join(var_list)}) AS {new_var_name}"
            
        select_clauses.append(clause)
        processed_columns.add(new_var_name.lower())
        for var in var_list:
            processed_columns.add(var.lower())
    
    # Include non-loop variables in the SELECT clause
    # Also remove fixed substrings from their names for consistency
    for var in non_loop_vars:
        new_var_name = utils.excise_substrings(var, constants.SUBSTRINGS_TO_FIX)
        
        # Standardize case for the new variable name
        new_var_name = utils.standardize_column_case(new_var_name)
        
        # Skip this variable if it would create a duplicate
        if new_var_name.lower() in processed_columns:
            utils.logger.info(f"Skipping output column {new_var_name} as it would create a duplicate")
            continue
            
        if var != new_var_name:
            select_clauses.append(f"{var} AS {new_var_name}")
        else:
            select_clauses.append(var)
            
        processed_columns.add(new_var_name.lower())
        processed_columns.add(var.lower())
    
    return select_clauses, processed_columns

def process_columns(source_table: str, destination_table: str) -> dict:
    """
    Processes columns from source_table to destination_table using a single efficient SQL query
    that combines all transformation steps:
    1. One-off column renames
    2. Substring removal (state_, _num, etc.)
    3. Custom column transformations
    4. Loop variable processing
    
    Args:
        source_table (str): A fully qualified BigQuery table (e.g., "project.dataset.table").
        destination_table (str): A fully qualified destination table.
        
    Returns:
        dict: A dictionary with status information.
    """
    project, dataset, table = utils.parse_fq_table(source_table)
    client = bigquery.Client(project=project)
    
    try:
        processed_columns = set()
        all_columns = utils.get_column_names(client, source_table)
        connect_id_clause = []
        
        # Step 0: Always include Connect_ID first if it exists
        if "Connect_ID" in all_columns:
            connect_id_clause = ["Connect_ID"]
            processed_columns.add("connect_id")
        
        # Step 1: Build clauses for one-off column renames
        utils.logger.info("Step 1: Building one-off column rename clauses")
        one_off_clauses, processed_columns = build_one_off_renames_clauses(
            client, source_table, processed_columns)
        
        # Step 2: Build clauses for substring removal
        utils.logger.info(f"Step 2: Building clauses for removing substrings from {constants.SUBSTRINGS_TO_FIX}")
        substring_clauses, processed_columns = build_substring_removal_clauses(
            client, source_table, processed_columns)
        
        # Step 3: Build clauses for custom column transformations
        utils.logger.info("Step 3: Building custom transformation clauses")
        custom_transform_clauses, processed_columns = build_custom_transform_clauses(
            client, source_table, processed_columns)
        
        # Step 4: Build clauses for loop variable processing
        utils.logger.info("Step 3: Building loop variable processing clauses")
        loop_clauses, processed_columns = build_loop_variable_clauses(
            client, source_table, processed_columns)
        
        # Combine all clauses with appropriate comments
        select_parts = []
        
        # Add Connect_ID first
        if connect_id_clause:
            select_parts.append("-- Connect_ID (always preserved)")
            select_parts.append(connect_id_clause[0])
        
        # Add one-off renames with comment
        if one_off_clauses:
            select_parts.append("\n        -- Step 1: One-off column renames from constants")
            select_parts.extend(one_off_clauses)
        
        # Add substring removal clauses with comment
        if substring_clauses:
            select_parts.append("\n        -- Step 2: Substring removal (state_, _num, etc.)")
            select_parts.extend(substring_clauses)

        # Add custom transformation clauses with comment
        if custom_transform_clauses:
            select_parts.append("\n        -- Step 3: Custom column transformations")
            select_parts.extend(custom_transform_clauses)
        
        # Add loop variable clauses with comment
        if loop_clauses:
            select_parts.append("\n        -- Step 4: Loop variable processing")
            select_parts.extend(loop_clauses)
        
        # Create the final SQL query
        joined_select_parts = ",\n        ".join(select_parts)
        sql = f"""
        /* Combined transformation query for {source_table} -> {destination_table} */
         
        CREATE OR REPLACE TABLE `{destination_table}` AS
        SELECT
            {joined_select_parts}
        FROM `{source_table}`
        """

        # Format the SQL for better readability
        sql = utils.format_sql_query(sql)

        # Save the SQL to GCS for audit purposes
        try:
            gcs_client = storage.Client()
            gcs_path = f"{constants.OUTPUT_SQL_PATH}{destination_table}.sql"
            utils.save_sql_string(sql=sql, path=gcs_path, storage_client=gcs_client)
        except Exception as e:
            utils.logger.exception(f"Error saving SQL: {e}")
            raise e
        
        # Execute the SQL
        try:
            query_job = client.query(sql)
            query_job.result()  # Wait for the query to finish
            status = f"Table {destination_table} successfully created with all transformations applied"
            utils.logger.info(status)
            return {
                "status": status,
                "submitted_sql_path": constants.OUTPUT_SQL_PATH
            }
        except Exception as e:
            utils.logger.exception(f"Error executing SQL: {e}")
            raise e
    except Exception as e:
        utils.logger.exception(f"Error in process_columns: {e}")
        raise e