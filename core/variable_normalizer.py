import re

def fix_impure_variable(var: str, exception_map: dict) -> str:
    """
    Fixes a variable name that contains exception tokens by replacing them 
    with the appropriate concept id according to exception_map.
    
    The exception_map is a dictionary mapping tokens (e.g. "SIBCANC3O")
    to their replacement concept id (e.g. "123456789").
    
    For example, if exception_map = {"SIBCANC3O": "123456789"}, then:
    
        >>> fix_non_pure_variable("D_259089008_1_1_SIBCANC3O_D_962468280_1", exception_map)
        "D_259089008_1_1_D_123456789_1_1_D_962468280_1"
    
    Args:
        var (str): The original variable name.
        exception_map (dict): A mapping of exception tokens to replacement concept IDs.
    
    Returns:
        str: The fixed variable name.
    """
    # Tokenize the variable name by underscores.
    tokens = var.split('_')
    fixed_tokens = []
    
    # Process each token:
    for token in tokens:
        # If the token is an exception (and appears in the exception_map), replace it.
        if token in exception_map:
            fixed_tokens.append(f"D_{exception_map[token]}")
        else:
            fixed_tokens.append(token)
    
    # Rejoin tokens using underscores.
    fixed_var = "_".join(fixed_tokens)
    return fixed_var

def fix_all_variables(variable_list: list, exception_map: dict) -> list:
    """
    Processes a list of variable names, replacing any exceptions based on the exception_map.
    
    Before fixing variables, it checks each variable for any token that is not allowed (i.e. not:
      - The literal "D" (or "d"),
      - A 9-digit number, or
      - A single digit)
    If any such token is found and is not present in the exception_map, a ValueError is raised.
    
    Args:
        variable_list (list): A list of variable names.
        exception_map (dict): A mapping of exception tokens to replacement concept IDs.
    
    Returns:
        list: A new list of fixed variable names.
        
    Raises:
        ValueError: If any non-pure tokens are found that do not have a mapping in exception_map.
    """
    missing_tokens = set()
    # Define allowed tokens:
    # Allowed tokens are: "D" (or "d"), 9-digit numbers, or a single digit.
    for var in variable_list:
        tokens = var.split('_')
        for token in tokens:
            token = token.strip()
            if not token:
                continue
            if token.upper() == 'D':
                continue
            if token.isdigit() and (len(token) == 9 or len(token) == 1):
                continue
            # If token doesn't match the allowed conditions, it is an exception.
            if token not in exception_map:
                missing_tokens.add(token)
    
    if missing_tokens:
        raise ValueError(
            f"Missing exception mapping for tokens: {', '.join(sorted(missing_tokens))}. "
            "Please add these tokens to the exception_map."
        )
    
    # Now fix the variables.
    fixed_vars = []
    for var in variable_list:
        # We assume that if the variable contains any token that is impure,
        # then fix_impure_variable should be applied.
        # (You may customize the logic below based on your token patterns.)
        if re.search(r'[A-Z]{2,}', var) and not re.search(r'[dD]_\d{9}', var):
            fixed_vars.append(fix_impure_variable(var, exception_map))
        else:
            fixed_vars.append(var)
    return fixed_vars

# Example usage:
if __name__ == "__main__":
    # Example exception map:
    exception_map = {
        "SIBCANC3O": "261863326",  # Map the token SIBCANC3O to concept id "261863326"
        # Add additional mappings as needed.
    }
    
    variable_list = [
        "D_259089008_1_1_SIBCANC3O_D_962468280_1",
        "D_812370563_1_1_D_812370563_1_1_D_665036297"
    ]
    
    try:
        fixed_variables = fix_all_variables(variable_list, exception_map)
        print("Fixed variables:", fixed_variables)
    except ValueError as e:
        print("Error:", e)
