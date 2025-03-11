import os
from enum import Enum

# -----------------------------------------------------------------------------
# Global Constants
# -----------------------------------------------------------------------------

SERVICE_NAME = "pr2-transformation"
ARTIFACT_GCS_BUCKET = os.environ.get(ARTIFACT_GCS_BUCKET)
OUTPUT_SQL_PATH = f"{ARTIFACT_GCS_BUCKET}sql"  # Can be local path or GCS Bucket, e.g., "gs://my-bucket/queries"

# In production, the project should be set via an environment variable.
PROJECT = os.environ.get("PROJECT_ID")

# Datasets for the different stages of your ETL pipeline.
SOURCE_DATASET = "FlatConnect"
STAGING_DATASET = "ForTestingOnly"
CLEAN_DATASET = "CleanConnect"

# -----------------------------------------------------------------------------
# Custom Base Enum
# -----------------------------------------------------------------------------
# Define a custom base class that inherits from both str and Enum.
# This ensures that each enum member behaves like a string and can be
# printed or interpolated directly without using .value.
class StrEnum(str, Enum):
    def __str__(self):
        return f"{self.value}"

# -----------------------------------------------------------------------------
# Enums for Table Names
# -----------------------------------------------------------------------------
# We create separate enums for source, staging, and clean tables.
# Each enum sets its dataset once as a class variable (_DATASET) so that
# changes to the dataset require an update in only one place.

class SourceTables(StrEnum):
    """
    Enum for source tables.
    Uses the SOURCE_DATASET defined globally.
    """
    _DATASET = SOURCE_DATASET
    MODULE1_V1 = f"{PROJECT}.{_DATASET}.module1_v1_JP"
    MODULE1_V2 = f"{PROJECT}.{_DATASET}.module1_v2_JP"
    MODULE2_V1 = f"{PROJECT}.{_DATASET}.module2_v1_JP"
    MODULE2_V2 = f"{PROJECT}.{_DATASET}.module2_v2_JP"
    MODULE3_V1 = f"{PROJECT}.{_DATASET}.module3_v1_JP"
    MODULE4_V1 = f"{PROJECT}.{_DATASET}.module4_v1_JP"
    BIOSURVEY_V1 = f"{PROJECT}.{_DATASET}.bioSurvey_v1_JP"
    CLINICALBIOSURVEY_V1 = f"{PROJECT}.{_DATASET}.clinicalBioSurvey_v1_JP"
    COVID19SURVEY_V1 = f"{PROJECT}.{_DATASET}.covid19Survey_v1_JP"
    MENSTRUALSURVEY_V1 = f"{PROJECT}.{_DATASET}.mestrualSurvey_v1_JP"
    EXPERIENCE2024 = f"{PROJECT}.{_DATASET}.experience2024_JP"

class StagingTables(StrEnum):
    """
    Enum for staging tables.
    Uses the STAGING_DATASET defined globally.
    """
    _DATASET = STAGING_DATASET
    MODULE1_V1_CLEANED_LOOPS = f"{PROJECT}.{_DATASET}.module1_v1_with_cleaned_loop_vars"
    MODULE1_V2_CLEANED_LOOPS = f"{PROJECT}.{_DATASET}.module1_v2_with_cleaned_loop_vars"
    MODULE2_V1_CLEANED_LOOPS = f"{PROJECT}.{_DATASET}.module2_v1_with_cleaned_loop_vars"
    MODULE2_V2_CLEANED_LOOPS = f"{PROJECT}.{_DATASET}.module2_v2_with_cleaned_loop_vars"
    BIOSURVEY_V1_CLEANED_LOOPS = f"{PROJECT}.{_DATASET}.bioSurvey_v1_with_cleaned_loop_vars"
    CLINICALBIOSURVEY_V1_CLEANED_LOOPS = f"{PROJECT}.{_DATASET}.clinicalBioSurvey_v1_with_cleaned_loop_vars"
    COVID19SURVEY_V1_CLEANED_LOOPS = f"{PROJECT}.{_DATASET}.covid19Survey_v1_with_cleaned_loop_vars"

class CleanTables(StrEnum):
    """
    Enum for clean tables.
    Uses the CLEAN_DATASET defined globally.
    """
    _DATASET = CLEAN_DATASET
    MODULE1 = f"{PROJECT}.{_DATASET}.module1"
    MODULE2 = f"{PROJECT}.{_DATASET}.module2"
    MODULE3 = f"{PROJECT}.{_DATASET}.module3"
    MODULE4 = f"{PROJECT}.{_DATASET}.module4"
    BIOSURVEY = f"{PROJECT}.{_DATASET}.bioSurvey"
    CLINICALBIOSURVEY = f"{PROJECT}.{_DATASET}.clinicalBioSurvey"
    COVID19SURVEY = f"{PROJECT}.{_DATASET}.covid19Survey"
    MENSTRUALSURVEY = f"{PROJECT}.{_DATASET}.mestrualSurvey"
    EXPERIENCE2024 = f"{PROJECT}.{_DATASET}.experience2024"

# -----------------------------------------------------------------------------
# Transformation Configuration
# -----------------------------------------------------------------------------
# The transformation configuration maps transformation group names to
# a dictionary containing a "mappings" key. Each mapping specifies the source
# table(s) and destination table(s) for a transformation.
# Using enums here helps ensure consistency and provides autocomplete.

TRANSFORM_CONFIG = {
    "fix_loop_variables": {
        "mappings": [
            {"source": SourceTables.MODULE1_V1, "destination": StagingTables.MODULE1_V1_CLEANED_LOOPS},
            {"source": SourceTables.MODULE1_V2, "destination": StagingTables.MODULE1_V2_CLEANED_LOOPS},
            {"source": SourceTables.MODULE2_V1, "destination": StagingTables.MODULE2_V1_CLEANED_LOOPS},
            {"source": SourceTables.MODULE2_V2, "destination": StagingTables.MODULE2_V2_CLEANED_LOOPS},
            {"source": SourceTables.BIOSURVEY_V1, "destination": StagingTables.BIOSURVEY_V1_CLEANED_LOOPS},
            {"source": SourceTables.CLINICALBIOSURVEY_V1, "destination": StagingTables.CLINICALBIOSURVEY_V1_CLEANED_LOOPS},
            {"source": SourceTables.COVID19SURVEY_V1, "destination": StagingTables.COVID19SURVEY_V1_CLEANED_LOOPS}
        ]
    },
    "merge_table_versions": {
        "mappings": [
            {
                "source": [StagingTables.MODULE1_V1_CLEANED_LOOPS, StagingTables.MODULE1_V2_CLEANED_LOOPS], 
                "destination": CleanTables.MODULE1
            },
            {
                "source": [StagingTables.MODULE2_V1_CLEANED_LOOPS, StagingTables.MODULE2_V2_CLEANED_LOOPS],
                "destination": CleanTables.MODULE2
            }
        ]
    }
}


# -----------------------------------------------------------------------------
# Example Usage
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Iterate through the mappings of the "fix_loop_variables" transformation group.
    print("Fix loop variables mappings:\n")
    fix_loop_group = TRANSFORM_CONFIG["fix_loop_variables"]
    for mapping in fix_loop_group["mappings"]:
        print(f"\nSource Table: {mapping['source']}")
        print(f"Destination Table: {mapping['destination']}\n")
        print("-" * 80)
    