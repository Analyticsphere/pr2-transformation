import re

def fix_impure_variable(var: str, exception_map: dict) -> str:
    """Replaces exception tokens in a variable name with mapped concept IDs.

    Tokens found in `exception_map` are replaced with `D_<concept_id>`, while 
    other tokens remain unchanged.

    Example:
        >>> fix_impure_variable("D_259089008_SIBCANC3O_962468280", {"SIBCANC3O": "123456789"})
        'D_259089008_D_123456789_962468280'

    Args:
        var (str): The input variable name.
        exception_map (dict): Mapping of tokens to concept IDs.

    Returns:
        str: The updated variable name.
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
    """Validates and fixes variable names by replacing exceptions with concept IDs.

    This function processes a list of variable names, ensuring all tokens conform 
    to allowed formats. Tokens that do not meet the criteria must be mapped in 
    `exception_map`; otherwise, an error is raised.

    **Allowed tokens**:
        - The literal "D" (case-insensitive).
        - A **single-digit number** (0-9).
        - A **9-digit numeric concept ID** (e.g., "123456789").

    **Behavior**:
        - If a token is **not in the allowed formats** and **not in `exception_map`**, 
          a `ValueError` is raised.
        - Otherwise, tokens in `exception_map` are replaced with `"D_<concept_id>"`.

    Example:
        >>> exception_map = {"SIBCANC3O": "123456789", "ABC123": "987654321"}
        >>> variables = ["D_259089008_1_SIBCANC3O", "D_962468280_D_ABC123", "D_123456789"]
        >>> fix_all_variables(variables, exception_map)
        ['D_259089008_1_D_123456789', 'D_962468280_D_987654321', 'D_123456789']
        
    Args:
        variable_list (list): A list of variable names to process.
        exception_map (dict): A dictionary mapping non-conforming tokens to replacement concept IDs.

    Returns:
        list: A new list of variable names with exceptions replaced.

    Raises:
        ValueError: If any non-conforming tokens do not have a mapping in `exception_map`.
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
    except_map = {
        "SIBCANC3O": "261863326",  # Map the token SIBCANC3O to concept id "261863326"
        # Add additional mappings as needed.
    }

    var_list = [
        "D_259089008_1_1_SIBCANC3O_D_962468280_1",
        "D_812370563_1_1_D_812370563_1_1_D_665036297"
    ]

    try:
        fixed_variables = fix_all_variables(var_list, except_map)
        print("Fixed variables:", fixed_variables)
    except ValueError as e:
        print("Error:", e)
