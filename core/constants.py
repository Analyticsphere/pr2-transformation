import os
from enum import Enum

# -----------------------------------------------------------------------------
# Global Constants
# -----------------------------------------------------------------------------

SERVICE_NAME = "pr2-transformation"
ARTIFACT_GCS_BUCKET = os.environ.get('ARTIFACT_GCS_BUCKET')
OUTPUT_SQL_PATH = f"{ARTIFACT_GCS_BUCKET}sql/" 

# In production, the project should be set via an environment variable.
PROJECT = os.environ.get("PROJECT_ID")

# ------------------------------------------------------------------------------
# Define substring categories for filtering ETL column names
# ------------------------------------------------------------------------------

# Allowable string patterns in variable names that are not 9-digit concept IDs (i.e., CIDs)
ALLOWED_NON_CID_VARIABLE_NAMES = ['connect_id']

# Forbidden variable names that will be dropped because they lack research value
FORBIDDEN_NON_CID_VARIABLE_NAMES = ['token', 'uid', 'date', 'sha', 'siteAcronym', 'utm_source', 'verifiedSeen', 
                                    'id', 'pin', 'state_studyId', 'firstSurveyCompletedSeen'] 

# Substrings that need fixing (future updates; drop columns for now)
SUBSTRINGS_TO_FIX = ['num', 'state_']

# Substrings indicating datatype conflicts (to be fixed upstream in Firestore; drop columns for now)
SUBSTRINGS_DATATYPE_CONFLICT = ['provided', 'string', 'integer', 'entity']

# Substrings indicating misnamed variables (exclude permanently from ETL; will be addressed upstream in Firestore)
SUBSTRINGS_MISSNAMED = [
    'sibcanc3d', 'chol', 'momcanc3d', 'sibcanc3o', 'uf', 'dadcanc3k', 'bloodclot', 'depress2',
    'htn', 'append', 'tublig', 'tonsils', 'breastdis', 'dm2',
    '20required'
]

# Combine all substring lists, removing duplicates
EXCLUDED_NON_CID_SUBSTRINGS = list(
    SUBSTRINGS_TO_FIX +
    SUBSTRINGS_DATATYPE_CONFLICT +
    SUBSTRINGS_MISSNAMED
)

# Allowable non-concept id substrings
ALLOWED_NON_CID_SUBSTRINGS = []

