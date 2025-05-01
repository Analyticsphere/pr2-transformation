import re
from typing import List, Dict, Any

# Column name transformation configurations
COLUMN_NAME_TRANSFORMATIONS = [
    {
        "name": "remove_u25_required",
        "pattern": ".*u25_20required$",
        "operation": "remove",
        "description": "Remove 'u25_20required' suffix from the end of column names",
        "examples": [{"input": "customer_name_u25_20required", "output": "customer_name"}]
    }
    # Additional transformations can be added here
]

class ColumnTransformer:
    def __init__(self, transformations=None):
        """Initialize with a list of transformations or use default."""
        self.transformations = []
        transformations = transformations or COLUMN_NAME_TRANSFORMATIONS
        self._compile_transformations(transformations)
    
    def _compile_transformations(self, transformations):
        """Compile regex patterns for all transformations."""
        for transform in transformations:
            transform_copy = transform.copy()
            transform_copy['compiled_pattern'] = re.compile(transform['pattern'])
            self.transformations.append(transform_copy)
    
    def transform_column_name(self, column_name: str) -> str:
        """Apply all defined transformations to a column name."""
        result = column_name
        
        for transform in self.transformations:
            pattern = transform['compiled_pattern']
            operation = transform['operation']
            
            if pattern.match(result):
                if operation == "remove":
                    result = self._remove_pattern(result, pattern, transform)
                elif operation == "replace":
                    result = self._replace_pattern(result, pattern, transform)
        
        return result
    
    def transform_column_names(self, column_list: List[str]) -> List[str]:
        """Transform multiple column names."""
        return [self.transform_column_name(col) for col in column_list]
    
    def _remove_pattern(self, column_name: str, pattern, transform: Dict) -> str:
        """Remove the matching pattern from column name."""
        match = pattern.match(column_name)
        if not match:
            return column_name
            
        # For patterns with capture groups, join the groups
        if len(match.groups()) > 0:
            return ''.join(group for group in match.groups() if group is not None)
        
        # For patterns without capture groups, just remove the matched part
        matched_part = match.group(0)
        if pattern.pattern.endswith('$'):
            # If pattern ends with $, it's matching a suffix
            prefix_length = len(column_name) - len(matched_part)
            return column_name[:prefix_length]
        elif pattern.pattern.startswith('^'):
            # If pattern starts with ^, it's matching a prefix
            suffix_length = len(column_name) - len(matched_part)
            return column_name[-suffix_length:] if suffix_length > 0 else ""
        else:
            # For other patterns, use substitution
            return pattern.sub('', column_name)
        
    def _replace_pattern(self, column_name: str, pattern, transform: Dict) -> str:
        """Replace matched pattern according to parameters."""
        replace_with = transform.get('parameters', {}).get('replace_with', '')
        return pattern.sub(replace_with, column_name)