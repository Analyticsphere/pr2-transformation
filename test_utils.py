import core.utils as utils

if __name__ == "__main__":
    
    # ==========================================================================================
    # TEST: utils.get_binary_yes_no_fields 
    # ==========================================================================================
    if True:
        project = "nih-nci-dceg-connect-dev"
        dataset = "FlatConnect"
        table = "module1_v1_JP"
        query, fields = utils.get_binary_yes_no_fields(project, dataset, table)
        print(fields)
        print(f"Query:\n\n{query}")
    
    
    # ==========================================================================================
    # TEST: utils.extract_ordered_concept_ids
    # ==========================================================================================
    if True:
        print("\nTesting extract_cids_from_varname():\n")
        print(utils.extract_ordered_concept_ids("d_123456789_d_987654321"))           # ['123456789', '987654321']
        print(utils.extract_ordered_concept_ids("D_123456789_987654321"))             # ['123456789']
        print(utils.extract_ordered_concept_ids("D_123412349_1_1_D_987654321_1_1"))   # ['123412349', '987654321']
        
        
    # ==========================================================================================
    # TEST: utils.extract_loop_number
    # ==========================================================================================
    if True:  
        print("\nTesting extract_loop_number():\n")
        print(utils.extract_loop_number("d_123456789_1_1_d_987654321_1_1"))          # 1
        print(utils.extract_loop_number("d_123456789_2_2_d_987654321_2_2"))          # 2
        print(utils.extract_loop_number("d_111111111_1_1_d_222222222_1_1"))          # 1
        print(utils.extract_loop_number("d_123456789_9_9_d_987654321_9_9"))          # 9
        print(utils.extract_loop_number("d_123456789_9_9_d_987654321_9_9_9_9_9_9"))  # 9
        print(utils.extract_loop_number("d_123456789_5_5"))                          # 5
        print(utils.extract_loop_number("d_123456789"))                              # None (no loop number)
    

    # ==========================================================================================
    # TEST: utils.group_vars_by_cid_and_loop_num
    # ==========================================================================================
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

        grouped_vars = utils.group_vars_by_cid_and_loop_num(var_list)

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
        
        
    # ==========================================================================================
    # TEST: utils.render_unwrap_singleton_expression
    # ==========================================================================================
    if True:
            
        project = "nih-nci-dceg-connect-stg-5519"
        dataset = "FlatConnect"
        table = "module3_v1_JP"
        col_names = ["D_276575533_D_276575533", "D_517100968_D_517100968", "D_933417196_D_933417196", "D_585819411_D_585819411"]

        # Generate the before-column assignments
        before_expressions = ",\n\t".join([f"{col} AS {col}_before" for col in col_names])

        # Generate the individual SQL expressions for the SELECT clause.
        unbracket_expressions = [utils.render_unwrap_singleton_expression(col, default_value="0") for col in col_names]

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
        
        
    # ==========================================================================================
    # TEST: utils.is_false_array
    # ==========================================================================================
    if True:
         # Create a sample DataFrame similar to your R example.
        data = {
            "col1": [None, "[]", "[178420302]", "[178420302]", "[178420302]", None],
            "col2": ["[]", "[]", "invalid", "[987654321]", "[327986541]", "[]"],
            "col3": ["[123456789]", "[123456789]", "[123456789]", "[987654321]", "[327986541]", "[123456789]"],
            "col4": ["[]", None, "[958239616]", None, "[958239616]", "[958239616]"],
            "col5": ["[]", None, "[958239616]", None, "[178420302]", "[958239616]"]
        }
        df = pd.DataFrame(data)
        
        # Define allowed valid values (here, None represents missing values)
        valid_values = [None, "[]", "[178420302]", "[958239616]"]
        
        # Apply the function to each column.
        results = {col: utils.is_false_array(df[col].tolist(), valid_values) for col in df.columns}
        
        print(results)
        # Expected output:
        # {'col1': True, 'col2': False, 'col3': False, 'col4': True, 'col5': False}
