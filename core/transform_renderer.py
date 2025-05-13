'''Module for rendering SQL transform templates.'''

import core.utils as utils
import core.constants as constants

def validate_transform_dict(transform_dict):
    """
    Validates the structure of a transform dictionary.

    Args:
        transform_dict (dict): Dictionary to validate.

    Raises:
        ValueError: If the structure is invalid.
    """
    if not isinstance(transform_dict, dict):
        raise ValueError("Top-level object must be a dictionary")

    for table, transforms in transform_dict.items():
        if not isinstance(transforms, list):
            raise ValueError(f"Value for key '{table}' must be a list of transformations")

        for i, transform in enumerate(transforms):
            if not isinstance(transform, dict):
                raise ValueError(f"Transform #{i} in '{table}' must be a dictionary")

            for key in ("source", "target", "transform_template"):
                if key not in transform:
                    raise ValueError(f"Missing required key '{key}' in transform #{i} of '{table}'")

            source = transform["source"]
            target = transform["target"]
            template = transform["transform_template"]

            if not (isinstance(source, str) or (isinstance(source, list) and all(isinstance(s, str) for s in source))):
                raise ValueError(f"'source' in transform #{i} of '{table}' must be a string or list of strings")

            if not (isinstance(target, str) or (isinstance(target, list) and all(isinstance(t, str) for t in target))):
                raise ValueError(f"'target' in transform #{i} of '{table}' must be a string or list of strings")

            if not callable(template):
                raise ValueError(f"'transform_template' in transform #{i} of '{table}' must be a callable")

def render_transforms(transform_dict):
    """
    Render SQL expressions from the transform dictionary.

    Handles 1:1, many:1, and 1:many mappings.

    Args:
        transform_dict (dict): Keys are table names, values are lists of transformation dicts.

    Returns:
        dict: {table_name: [sql_expressions]}
    """
    try:
        validate_transform_dict(transform_dict)
        utils.logger.info("Transform dictionary is valid!")
    except ValueError as e:
        utils.logger.error(f"Validation of transform dictionary failed: {e}")
        raise

    rendered_sql = {}

    for table, transforms in transform_dict.items():
        rendered_sql[table] = []
        for transform in transforms:
            source = transform["source"]
            target = transform["target"]
            
            try:
                sql = transform["transform_template"](source, target)
                if isinstance(sql, str):
                    rendered_sql[table].append(sql.strip())
                elif isinstance(sql, list):
                    rendered_sql[table].extend([line.strip() for line in sql])
                else:
                    utils.logger.error(f"Invalid return type from transform_template in {table}: must be string or list")
                    raise ValueError("transform_template must return a string or list of strings")
            except Exception as e:
                utils.logger.error(f"Error rendering transform in {table} for {source} -> {target}: {e}")
                raise

    return rendered_sql

