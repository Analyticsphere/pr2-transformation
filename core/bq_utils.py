import logging
from google.cloud import bigquery
import bq_utils

def get_column_names(client: str, project: str, dataset: str, table_name: str) -> set:
    """
    Fetches column names of a given BigQuery table.

    Args:
        client (bigquery.Client): BigQuery client instance.
        project (str): Google Cloud project ID.
        dataset (str): BigQuery dataset name.
        table_name (str): Table name.

    Returns:
        set: A set of column names from the table.
    """
    query = f"""
    SELECT column_name 
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` 
    WHERE table_name = '{table_name}';
    """
    try:
        result = client.query(query).to_dataframe()
        return set(result["column_name"])
    except Exception as e:
        logging.error(f"Failed to fetch schema for {table_name}: {e}")
        return set()
    
def get_binary_yes_no_fields(project_id: str, dataset_id: str, table_id: str) -> tuple:
    """
    Identifies columns in a BigQuery table that contain only:
        - '0' and '1'
        - '0', '1', and NULL

    Args:
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.

    Returns:
        tuple: (Formatted SQL query string, list of column names that meet criteria).
    """
    client = bigquery.Client()

    try:
        # Get table schema
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        table = client.get_table(table_ref)

        # Extract column names for STRING and INTEGER types
        column_names = [field.name for field in table.schema if field.field_type in ("STRING", "INTEGER")]

        if not column_names:
            raise ValueError(f"No STRING or INTEGER columns found in `{table_ref}`.")

        # Construct query using COUNTIF for efficiency
        formatted_conditions = ",\n    ".join(
            f"""
            (COUNTIF({col} NOT IN ('0', '1') AND {col} IS NOT NULL) = 0) AS `{col}`
            """.strip()
            for col in column_names
        )

        # Format the query for better readability
        query = f"""
        SELECT 
            {formatted_conditions}
        FROM `{table_ref}`
        """.strip()

        # Execute the query
        query_job = client.query(query)
        result = query_job.result().to_dataframe()

        # Extract columns that meet the criteria
        binary_columns = [col for col in result.columns if result.iloc[0][col]]

        return query, binary_columns

    except Exception as e:
        logging.ERROR(f"Error getting binary fields: {e}")
        return None, []

if __name__ == "__main__":
    
    project = "nih-nci-dceg-connect-dev"
    dataset = "FlatConnect"
    table = "module1_v1_JP"
    
    # Test get binary field names ==========================================================
    query, fields = bq_utils.get_binary_yes_no_fields(project, dataset, table)
    print(fields)
    print(f"Query:\n\n{query}")