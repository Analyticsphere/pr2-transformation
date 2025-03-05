from flask import abort

def extract_source_and_destination(mapping: dict) -> tuple:
    """
    Extracts the 'source' and 'destination' fields from the provided 'mapping'.
    If either field is missing or empty, aborts the request with a 400 error.
    """
    source = mapping.get("source")
    destination = mapping.get("destination")
    if not source or not destination:
        abort(400, description="Missing required parameter(s): 'source' and/or 'destination'.")
    return source, destination