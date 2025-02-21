"""Constants to be used throughout the pr2-transformation code base."""

import os
from enum import Enum

# Define the default project. In production, this should be set via an environment variable.
PROJECT = os.environ.get("GCP_PROJECT", "nih-nci-dceg-connect-prod-6d04")
SOURCE_DATASET = "FlatConnect"
DESTINATION_DATASET = "CleanConnect"

class SourceTables(str, Enum):
    """Enum to store source table names from the source dataset."""  
    MODULE1_V1 = f"{PROJECT}.{SOURCE_DATASET}.module1_v1_JP"
    MODULE1_V2 = f"{PROJECT}.{SOURCE_DATASET}.module1_v2_JP"
    MODULE2_V1 = f"{PROJECT}.{SOURCE_DATASET}.module2_v1_JP"
    MODULE2_V2 = f"{PROJECT}.{SOURCE_DATASET}.module2_v2_JP"
    MODULE3_V1 = f"{PROJECT}.{SOURCE_DATASET}.module3_v1_JP"
    MODULE4_V1 = f"{PROJECT}.{SOURCE_DATASET}.module4_v1_JP"
    BIOSURVEY_V1 = f"{PROJECT}.{SOURCE_DATASET}.biosurvey_v1_JP"
    CLINICALBIOSURVEY_V1 = f"{PROJECT}.{SOURCE_DATASET}.clinicalBioSurvey_v1_JP"
    COVID19SURVEY_V1 = f"{PROJECT}.{SOURCE_DATASET}.covid19Survey_v1_JP"
    MENSTRUALSURVEY_V1 = f"{PROJECT}.{SOURCE_DATASET}.mestrualSurvey_v1_JP"
    EXPERIENCE2024 = f"{PROJECT}.{SOURCE_DATASET}.experience2024_JP"

class DestinationTables(str, Enum):
    """Enum to store source table names from the source dataset."""  
    MODULE1 = f"{PROJECT}.{DESTINATION_DATASET}.module1"
    MODULE2 = f"{PROJECT}.{DESTINATION_DATASET}.module2"
    MODULE3 = f"{PROJECT}.{DESTINATION_DATASET}.module3"
    MODULE4 = f"{PROJECT}.{DESTINATION_DATASET}.module4"
    BIOSURVEY = f"{PROJECT}.{DESTINATION_DATASET}.biosurvey"
    CLINICALBIOSURVEY = f"{PROJECT}.{DESTINATION_DATASET}.clinicalBioSurvey"
    COVID19SURVEY = f"{PROJECT}.{DESTINATION_DATASET}.covid19Survey"
    MENSTRUALSURVEY = f"{PROJECT}.{DESTINATION_DATASET}.mestrualSurvey"
    EXPERIENCE2024 = f"{PROJECT}.{DESTINATION_DATASET}.experience2024"

# Example usage:
if __name__ == "__main__":
    print(DestinationTables.MODULE1.value)