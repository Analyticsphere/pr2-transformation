import pytest
from unittest.mock import MagicMock, patch
import re
from google.cloud import bigquery, storage

# Import with patch to prevent actual import execution side effects
with patch("google.cloud.bigquery.Client"):
    with patch("google.cloud.storage.Client"):
        from core.query_composition import compose_coalesce_loop_variable_query
        import core.utils as utils
        import core.constants as constants


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client."""
    client = MagicMock(spec=bigquery.Client)
    # Add a mock query method that returns a mock job
    query_job = MagicMock()
    query_job.result.return_value = None
    client.query.return_value = query_job
    return client


@pytest.fixture
def mock_storage_client():
    """Create a mock Storage client."""
    client = MagicMock(spec=storage.Client)
    # Mock bucket and blob methods
    bucket = MagicMock()
    blob = MagicMock()
    client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    return client


@pytest.mark.parametrize(
    "column_names,expected_sql_pattern",
    [
        # Test case 1: Basic loop variable transformation
        (
            ["Connect_ID", "d_123456789_1_1"],
            r"SELECT\s+Connect_ID,\s+d_123456789_1_1 AS d_123456789_1\s+FROM"
        ),
        # Test case 2: Multiple versions of the same variable
        (
            ["Connect_ID", "d_123456789_2_2", "d_123456789_2_2_2_2"],
            r"COALESCE\(d_123456789_2_2, d_123456789_2_2_2_2\) AS d_123456789_2"
        ),
        # Test case 3: Version handling
        (
            ["Connect_ID", "d_123456789_1_1", "d_123456789_v2_1_1"],
            r"d_123456789_1_1 AS d_123456789_1.*d_123456789_v2_1_1 AS d_123456789_1_v2"
        ),
        # Test case 4: Multiple concept IDs
        (
            ["Connect_ID", "d_123456789_3_3_d_987654321_3_3"],
            r"d_123456789_3_3_d_987654321_3_3 AS d_123456789_d_987654321_3"
        ),
        # Test case 5: Non-loop variables
        (
            ["Connect_ID", "d_123456789", "d_987654321"],
            r"d_123456789.*d_987654321"
        ),
        # Test case 6: Mixed loop and non-loop variables
        (
            ["Connect_ID", "d_123456789_4_4", "d_987654321"],
            r"d_123456789_4_4 AS d_123456789_4.*d_987654321"
        ),
        # Test case 7: Complex case with multiple loop variables for both versioned and unversioned columns
        (
            [
                "Connect_ID", 
                "d_123456789_5_5", "d_123456789_5_5_5_5",  # Unversioned with same CID, loop 5
                "d_123456789_v2_5_5", "d_123456789_v2_5_5_5_5",  # v2 with same CID, loop 5
                "d_123456789_v3_5_5", "d_123456789_v3_5_5_5_5",  # v3 with same CID, loop 5
                "d_987654321_5_5", "d_987654321_5_5_5_5"  # Different CID, same loop 5
            ],
            r"COALESCE\(d_123456789_5_5, d_123456789_5_5_5_5\) AS d_123456789_5.*" +
            r"COALESCE\(d_123456789_v2_5_5, d_123456789_v2_5_5_5_5\) AS d_123456789_5_v2.*" +
            r"COALESCE\(d_123456789_v3_5_5, d_123456789_v3_5_5_5_5\) AS d_123456789_5_v3.*" +
            r"COALESCE\(d_987654321_5_5, d_987654321_5_5_5_5\) AS d_987654321_5"
        ),
    ]
)
def test_compose_coalesce_loop_variable_query(
    column_names, expected_sql_pattern, monkeypatch
):
    """Test that compose_coalesce_loop_variable_query generates the expected SQL query."""
    
    # Create mock clients
    mock_bq_client = MagicMock(spec=bigquery.Client)
    mock_gcs_client = MagicMock(spec=storage.Client)
    
    # Set up query job mock
    query_job = MagicMock()
    query_job.result.return_value = None
    mock_bq_client.query.return_value = query_job
    
    # Apply patches for external dependencies
    with patch("google.cloud.bigquery.Client", return_value=mock_bq_client):
        with patch("google.cloud.storage.Client", return_value=mock_gcs_client):
            # Mock the get_valid_column_names function to return our test column names
            monkeypatch.setattr(
                utils, "get_valid_column_names", 
                lambda client, fq_table: column_names
            )
            
            # Mock functions that interact with external services
            monkeypatch.setattr(utils, "validate_column_names", lambda client, fq_table: None)
            monkeypatch.setattr(utils, "save_sql_string", lambda sql, path, storage_client: None)
            
            # Mock the parse_fq_table function to return a fixed project
            monkeypatch.setattr(
                utils, "parse_fq_table", 
                lambda table: ("test-project", "test-dataset", "test-table")
            )
            
            # Set a constant value for OUTPUT_SQL_PATH
            monkeypatch.setattr(constants, "OUTPUT_SQL_PATH", "gs://test-bucket/sql/")
            
            # Call the function
            result = compose_coalesce_loop_variable_query(
                "test-project.test-dataset.source-table",
                "test-project.test-dataset.destination-table"
            )
            
            # Check that the BigQuery client's query method was called
            mock_bq_client.query.assert_called_once()
            
            # Get the SQL query that was passed to the query method
            sql_query = mock_bq_client.query.call_args[0][0]
            
            # Use regex to check if the expected pattern is in the SQL query
            assert re.search(expected_sql_pattern, sql_query, re.DOTALL | re.MULTILINE), \
                f"Expected pattern '{expected_sql_pattern}' not found in SQL query: {sql_query}"
            
            # Check that the function returns the expected result
            assert result["status"] == "Table test-project.test-dataset.destination-table successfully created or replaced."
            assert result["submitted_sql_path"] == "gs://test-bucket/sql/"

def test_version_placement_debug(monkeypatch):
    """
    Debug test to examine exactly how version placement works in multi-concept-ID columns.
    """
    # Problem column examples
    problem_columns = [
        "d_899251483_v2_d_812107266_6_6",  # Version in the middle
        "d_71558179_v2_5_5",               # Version followed by loop numbers
        "d_899251483_v2_d_812107266"       # Version in the middle, no loop numbers
    ]
    
    # Create mock clients
    mock_bq_client = MagicMock(spec=bigquery.Client)
    mock_gcs_client = MagicMock(spec=storage.Client)
    
    # Set up query job mock
    query_job = MagicMock()
    query_job.result.return_value = None
    mock_bq_client.query.return_value = query_job
    
    # Apply patches for external dependencies
    with patch("google.cloud.bigquery.Client", return_value=mock_bq_client):
        with patch("google.cloud.storage.Client", return_value=mock_gcs_client):
            # Mock the get_valid_column_names function
            full_columns = ["Connect_ID"] + problem_columns
            monkeypatch.setattr(
                utils, "get_valid_column_names", 
                lambda client, fq_table: full_columns
            )
            
            # Mock functions that interact with external services
            monkeypatch.setattr(utils, "validate_column_names", lambda client, fq_table: None)
            monkeypatch.setattr(utils, "save_sql_string", lambda sql, path, storage_client: None)
            
            # Mock the parse_fq_table function
            monkeypatch.setattr(
                utils, "parse_fq_table", 
                lambda table: ("test-project", "test-dataset", "test-table")
            )
            
            # Set a constant value for OUTPUT_SQL_PATH
            monkeypatch.setattr(constants, "OUTPUT_SQL_PATH", "gs://test-bucket/sql/")
            
            # Capture debug info
            debug_info = {}
            
            # Hook into the key functions to capture intermediate results
            original_extract_version = utils.extract_version_suffix
            original_excise_version = utils.excise_version_from_column_name
            original_extract_ordered_ids = utils.extract_ordered_concept_ids
            
            def debug_extract_version(var_name):
                result = original_extract_version(var_name)
                debug_info[f"extract_version_{var_name}"] = result
                return result
            
            def debug_excise_version(column_name):
                result = original_excise_version(column_name)
                debug_info[f"excise_version_{column_name}"] = result
                return result
            
            def debug_extract_ordered_ids(var):
                result = original_extract_ordered_ids(var)
                debug_info[f"extract_ordered_ids_{var}"] = result
                return result
            
            monkeypatch.setattr(utils, "extract_version_suffix", debug_extract_version)
            monkeypatch.setattr(utils, "excise_version_from_column_name", debug_excise_version)
            monkeypatch.setattr(utils, "extract_ordered_concept_ids", debug_extract_ordered_ids)
            
            # Call the function
            compose_coalesce_loop_variable_query(
                "test-project.test-dataset.source-table",
                "test-project.test-dataset.destination-table"
            )
            
            # Get the SQL query
            sql_query = mock_bq_client.query.call_args[0][0]
            
            # Extract the column transformations from the SQL
            transformations = {}
            for col in problem_columns:
                # Find the transformation for this column
                pattern = re.compile(f"{re.escape(col)} AS ([^ ,\n]+)")
                match = pattern.search(sql_query)
                if match:
                    transformations[col] = match.group(1)
                else:
                    transformations[col] = "NOT_FOUND"
            
            # Print debug info
            for col in problem_columns:
                print(f"\nColumn: {col}")
                print(f"  Extract Version: {debug_info.get(f'extract_version_{col}', 'N/A')}")
                print(f"  Excise Version: {debug_info.get(f'excise_version_{col}', 'N/A')}")
                excised = debug_info.get(f'excise_version_{col}', col)
                print(f"  Extract Ordered IDs: {debug_info.get(f'extract_ordered_ids_{excised}', 'N/A')}")
                print(f"  Final transformation: {col} -> {transformations.get(col, 'N/A')}")
            
            # Assertions to check specific transformations
            assert transformations["d_899251483_v2_d_812107266_6_6"] == "d_899251483_d_812107266_6_v2", \
                f"Expected d_899251483_d_812107266_6_v2, got {transformations['d_899251483_v2_d_812107266_6_6']}"
