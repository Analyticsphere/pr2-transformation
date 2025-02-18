import textwrap

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

# Example usage:
if __name__ == "__main__":
    
    # Test unbracketing of false arrays ==============================================================
    project = "nih-nci-dceg-connect-stg-5519"
    dataset = "FlatConnect"
    table = "module3_v1_JP"
    col_names = ["D_276575533_D_276575533", "D_517100968_D_517100968", "D_933417196_D_933417196", "D_585819411_D_585819411"]

    # Generate the before-column assignments
    before_expressions = ",\n\t".join([f"{col} AS {col}_before" for col in col_names])

    # Generate the individual SQL expressions for the SELECT clause.
    unbracket_expressions = [render_unwrap_singleton_expression(col, default_value="0") for col in col_names]

    # Join the expressions into a comma-separated string with proper indentation.
    select_expressions = ",\n\n\t".join(unbracket_expressions)

    # Generate the WHERE conditions for each column.
    where_conditions = "\n\tOR ".join([f"{col} IS NOT NULL" for col in col_names])

    # Construct the full query.
    query = f"""
    SELECT
    
        # Variables BEFORE uwrapping
        {before_expressions},
        
        # Variables AFTER unwrapping
        {select_expressions}
        
    FROM `{project}.{dataset}.{table}`
    WHERE {where_conditions};
    """
    print(f"The rendered query looks like this: \n\t{query}")
