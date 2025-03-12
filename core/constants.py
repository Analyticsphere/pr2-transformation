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

# Allowable string patterns in variable names that are not 9-digit concept IDs (i.e., CIDs)
# NOTE: 'num', 'string', 'integer' and 'provided' are key words that indicate data type 
#       inconsistencies upstream in Firestore. These inconsistencies must be addressed by DevOps.
ALLOWED_NON_CID_VARIABLE_NAMES = ['connect_id', 'token', 'uid']
ALLOWED_NON_CID_SUBSTRINGS = [
    'sibcanc3d', 'chol', 'momcanc3d', 'sibcanc3o', 'uf', 'dadcanc3k', 'bloodclot', 'depress2',
    'dadcanc3k', 'sibcanc3d', 'htn', 'append', 'tublig', 'tonsils', 'breastdis', 'dm2', 'num',
    'provided', 'string', 'entity', 'date', 'v2', 'sha'
]

