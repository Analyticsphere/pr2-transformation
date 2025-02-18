import re
from collections import defaultdict

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

if __name__ == "__main__":
    
    ##########################
    ## FOR DEVELOPMENT ONLY ##
    ##########################
    
    # Test get cids from variable name =====================================================
    if True:
        print("\nTesting extract_cids_from_varname():\n")
        print(extract_ordered_concept_ids("d_123456789_d_987654321"))           # ['123456789', '987654321']
        print(extract_ordered_concept_ids("D_123456789_987654321"))             # ['123456789']
        print(extract_ordered_concept_ids("D_123412349_1_1_D_987654321_1_1"))   # ['123412349', '987654321']
            
        # Test get binary field names ==========================================================
        print("\nTesting extract_loop_number():\n")
        print(extract_loop_number("d_123456789_1_1_d_987654321_1_1"))          # 1
        print(extract_loop_number("d_123456789_2_2_d_987654321_2_2"))          # 2
        print(extract_loop_number("d_111111111_1_1_d_222222222_1_1"))          # 1
        print(extract_loop_number("d_123456789_9_9_d_987654321_9_9"))          # 9
        print(extract_loop_number("d_123456789_9_9_d_987654321_9_9_9_9_9_9"))  # 9
        print(extract_loop_number("d_123456789_5_5"))                          # 5
        print(extract_loop_number("d_123456789"))                              # None (no loop number)
    
    # Test group vars by cid and loop number =====================================================
    if True:
        var_list = [
            "d_123456789_1_1_d_987654321_1_1",
            "d_123456789_2_2_d_987654321_2_2",
            "d_111111111_1_1_d_222222222_1_1",
            "d_123456789_9_9_d_987654321_9_9",
            "d_123456789_9_9_d_987654321_9_9_9_9_9_9",
            "d_123456789_5_5",
            "d_123456789"  # No loop number, should be ignored
        ]

        grouped_vars = group_vars_by_cid_and_loop_num(var_list)

        for key, vars in grouped_vars.items():
            concept_ids, loop_number = key
            print(f"Concept IDs: {sorted(concept_ids)}, Loop Number: {loop_number}, Variables: {vars}")

        # Output:
        # Concept IDs: ['123456789', '987654321'], Loop Number: 1, Variables: ['d_123456789_1_1_d_987654321_1_1']
        # Concept IDs: ['123456789', '987654321'], Loop Number: 2, Variables: ['d_123456789_2_2_d_987654321_2_2']
        # Concept IDs: ['111111111', '222222222'], Loop Number: 1, Variables: ['d_111111111_1_1_d_222222222_1_1']
        # Concept IDs: ['123456789', '987654321'], Loop Number: 9, Variables: ['d_123456789_9_9_d_987654321_9_9', 'd_123456789_9_9_d_987654321_9_9_9_9_9_9']
        # Concept IDs: ['123456789'], Loop Number: 5, Variables: ['d_123456789_5_5']
    
        print(grouped_vars)
    
        # {(frozenset({'123456789', '987654321'}), 1): ['d_123456789_1_1_d_987654321_1_1'], 
        #  (frozenset({'123456789', '987654321'}), 2): ['d_123456789_2_2_d_987654321_2_2'], 
        #  (frozenset({'222222222', '111111111'}), 1): ['d_111111111_1_1_d_222222222_1_1'], 
        #  (frozenset({'123456789', '987654321'}), 9): ['d_123456789_9_9_d_987654321_9_9', 'd_123456789_9_9_d_987654321_9_9_9_9_9_9'], 
        #  (frozenset({'123456789'}), 5): ['d_123456789_5_5']}