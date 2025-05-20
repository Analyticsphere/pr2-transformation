import os
from enum import Enum
import textwrap

# -----------------------------------------------------------------------------
# Global Constants
# -----------------------------------------------------------------------------

SERVICE_NAME = "pr2-transformation"
ARTIFACT_GCS_BUCKET = os.environ.get('ARTIFACT_GCS_BUCKET') # A place to send artifacts for visibility
OUTPUT_SQL_PATH = f"{ARTIFACT_GCS_BUCKET}sql/"              # SQL queries are sent prior to  here for auditing/debugging

# In production, the project should be set via an environment variable.
PROJECT = os.environ.get("PROJECT_ID")

# ------------------------------------------------------------------------------
# Define substring categories for filtering ETL column names
# ------------------------------------------------------------------------------

# Allowable string patterns in variable names that are not 9-digit concept IDs (i.e., CIDs)
ALLOWED_NON_CID_VARIABLE_NAMES = ['connect_id']

# Forbidden variable names that will be dropped because they lack research value
FORBIDDEN_NON_CID_VARIABLE_NAMES = ['token', 'uid', 'date', 'sha', 'siteAcronym', 'utm_source', 'verifiedSeen', 
                                    'id', 'pin', 'state_studyId', 'state_uid', 'firstSurveyCompletedSeen'] 

# Substrings that need to be removed
SUBSTRINGS_TO_FIX = ['_num', 'state_']

# Allowable non-concept id substrings 
ALLOWED_NON_CID_SUBSTRINGS = ['num', 'state'] # TODO Consider removing this now that these substrings are removed in 27

# Substrings indicating datatype conflicts (to be fixed upstream in Firestore; drop columns for now)
SUBSTRINGS_DATATYPE_CONFLICT = ['provided', 'string', 'integer', 'entity']

# Substrings indicating misnamed variables; They are handled by the ONE_OFF_COLUMN_RENAME_MAPPINGS, but excluded from downstream processing.
SUBSTRINGS_MISSNAMED = [
    'sibcanc3d', 'chol', 'momcanc3d', 'sibcanc3o', 'uf', 'dadcanc3k', 'bloodclot', 'depress2',
    'htn', 'append', 'tublig', 'tonsils', 'breastdis', 'dm2',
    '20required'
]

# Combine all substring lists, removing duplicates
EXCLUDED_NON_CID_SUBSTRINGS = list(
    SUBSTRINGS_DATATYPE_CONFLICT +
    SUBSTRINGS_MISSNAMED
)

# -----------------------------------
# Define false array parameters
# -----------------------------------
# Used for 
FALSE_ARRAY_VALUES = [
    "[]",
    "[178420302]",
    "[958239616]",
]

BRACKETED_NINE_DIGIT_PATTERN = r'^\[\d{9}\]$'

# ------------------------------------------------------------------------------
# Define one-off column renames that cannot be handled by generalized columns
# ------------------------------------------------------------------------------

# Table-specific variable name corrections. 
# Variations on this theme of misnamed variables are not expected 
# to occur again so we will fix them explicitly rather than trying to put together a generalized approach.
ONE_OFF_COLUMN_RENAME_MAPPINGS = {
    "FlatConnect.module1_v1_JP": [
        # Issue: https://github.com/Analyticsphere/pr2-documentation/issues/17
        {"source": "D_122887481_TUBLIG_D_232595513", "target": "d_122887481_d_623218391", "description": "Fix Tubal Ligation Age CID"},
        {"source": "D_122887481_TUBLIG_D_614366597", "target": "d_122887481_d_802622485", "description": "Fix Tubal Ligation Year CID"},
        {"source": "D_259089008_1_1_SIBCANC3O_D_230633094_1", "target": "d_259089008_d_206625031_1", "description": "Fix Sibling Cancer Age CID"},
        {"source": "D_259089008_1_1_SIBCANC3O_D_962468280_1", "target": "d_259089008_d_261863326_1", "description": "Fix Sibling Cancer Year CID"},
        {"source": "D_301414575_DEPRESS2_D_479548517", "target": "d_301414575_d_261863326", "description": "Fix Depression Year CID"},
        {"source": "D_301414575_DEPRESS2_D_591959654", "target": "d_301414575_d_206625031", "description": "Fix Depression Age CID"},
        {"source": "D_301679110_DM2_D_166195719", "target": "d_301679110_d_261863326", "description": "Fix Diabetes Year CID"},
        {"source": "D_301679110_DM2_D_861769692", "target": "d_301679110_d_206625031", "description": "Fix Diabetes Age CID"},
        {"source": "D_355472178_BREASTDIS_D_138780721", "target": "d_619481697_d_261863326", "description": "Fix Breast Disease Year CID"},
        {"source": "D_355472178_BREASTDIS_D_162512268", "target": "d_619481697_d_206625031", "description": "Fix Breast Disease Age CID"},
        {"source": "D_367884741_TONSILS_D_300754548", "target": "d_367884741_d_623218391", "description": "Fix Tonsillectomy Age CID"},
        {"source": "D_367884741_TONSILS_D_714712574", "target": "d_367884741_d_802622485", "description": "Fix Tonsillectomy Year CID"},
        {"source": "D_370198527_DADCANC3K_D_260972338", "target": "d_370198527_d_206625031", "description": "Fix Father's Lung Cancer Age CID"},
        {"source": "D_370198527_DADCANC3K_D_331562964", "target": "d_370198527_d_261863326", "description": "Fix Father's Lung Cancer Year CID"},
        {"source": "D_402548942_MOMCANC3D_D_388289687", "target": "d_402548942_d_206625031", "description": "Fix Mother's Breast Cancer Age CID"},
        {"source": "D_402548942_MOMCANC3D_D_734800333", "target": "d_402548942_d_261863326", "description": "Fix Mother's Breast Cancer Year CID"},
        {"source": "D_460062034_BLOODCLOT_D_497018554", "target": "d_460062034_d_206625031", "description": "Fix Blood Clot Age CID"},
        {"source": "D_460062034_BLOODCLOT_D_694594047", "target": "d_460062034_d_261863326", "description": "Fix Blood Clot Year CID"},
        {"source": "D_550075233_APPEND_D_727704681", "target": "d_550075233_d_802622485", "description": "Fix Appendectomy Year CID"},
        {"source": "D_550075233_APPEND_D_919193251", "target": "d_550075233_d_623218391", "description": "Fix Appendectomy Age CID"},
        {"source": "D_836890480_CHOL_D_470282814", "target": "d_836890480_d_261863326", "description": "Fix High Cholesterol Year CID"},
        {"source": "D_836890480_CHOL_D_637556277", "target": "d_836890480_d_206625031", "description": "Fix High Cholesterol Age CID"},
        {"source": "D_846786840_UF_D_351965599", "target": "d_846786840_d_261863326", "description": "Fix Uterine Fibroid Year CID"},
        {"source": "D_846786840_UF_D_895115511", "target": "d_846786840_d_206625031", "description": "Fix Uterine Fibroid Age CID"},
        {"source": "D_884793537_HTN_D_367670682", "target": "d_884793537_d_206625031", "description": "Fix Hypertension Age CID"},
        {"source": "D_884793537_HTN_D_608469482", "target": "d_884793537_d_261863326", "description": "Fix Hypertension Year CID"},
        {"source": "D_907590067_4_4_SIBCANC3O_D_650332509_4", "target": "d_907590067_d_261863326_4", "description": "Fix Sibling Cancer Year CID"},
        {"source": "D_907590067_4_4_SIBCANC3D_D_932489634_4", "target": "d_907590067_d_206625031_4", "description": "Fix Sibling Breast Cancer Age CID"},
        # Issue: https://github.com/Analyticsphere/pr2-documentation/issues/5
        {"source": "D_150352141_D_206625031", "target": "d_150352141_d_623218391", "description": ""},
        {"source": "D_150352141_D_261863326", "target": "d_150352141_d_802622485", "description": ""},
        {"source": "D_122887481_D_206625031", "target": "d_122887481_d_623218391", "description": ""},
        {"source": "D_122887481_D_261863326", "target": "d_122887481_d_802622485", "description": ""},
        {"source": "D_534007917_D_206625031", "target": "d_534007917_d_623218391", "description": ""},
        {"source": "D_534007917_D_261863326", "target": "d_534007917_d_802622485", "description": ""},
        {"source": "D_752636038_D_206625031", "target": "d_752636038_d_623218391", "description": ""},
        {"source": "D_752636038_D_261863326", "target": "d_752636038_d_802622485", "description": ""},
        {"source": "D_518750011_D_206625031", "target": "d_518750011_d_623218391", "description": ""},
        {"source": "D_518750011_D_261863326", "target": "d_518750011_d_802622485", "description": ""},
        {"source": "D_275770221_D_206625031", "target": "d_275770221_d_623218391", "description": ""},
        {"source": "D_275770221_D_261863326", "target": "d_275770221_d_802622485", "description": ""},
        {"source": "D_527057404_D_206625031", "target": "d_527057404_d_623218391", "description": ""},
        {"source": "D_527057404_D_261863326", "target": "d_527057404_d_802622485", "description": ""}
    ],
    "FlatConnect.module1_v2_JP": [
        # Issue: https://github.com/Analyticsphere/pr2-documentation/issues/5
        {"source": "D_150352141_D_206625031", "target": "d_150352141_d_623218391", "description": ""},
        {"source": "D_150352141_D_261863326", "target": "d_150352141_d_802622485", "description": ""},
        {"source": "D_122887481_D_206625031", "target": "d_122887481_d_623218391", "description": ""},
        {"source": "D_122887481_D_261863326", "target": "d_122887481_d_802622485", "description": ""},
        {"source": "D_534007917_D_206625031", "target": "d_534007917_d_623218391", "description": ""},
        {"source": "D_534007917_D_261863326", "target": "d_534007917_d_802622485", "description": ""},
        {"source": "D_752636038_D_206625031", "target": "d_752636038_d_623218391", "description": ""},
        {"source": "D_752636038_D_261863326", "target": "d_752636038_d_802622485", "description": ""},
        {"source": "D_518750011_D_206625031", "target": "d_518750011_d_623218391", "description": ""},
        {"source": "D_518750011_D_261863326", "target": "d_518750011_d_802622485", "description": ""},
        {"source": "D_275770221_D_206625031", "target": "d_275770221_d_623218391", "description": ""},
        {"source": "D_275770221_D_261863326", "target": "d_275770221_d_802622485", "description": ""},
        {"source": "D_527057404_D_206625031", "target": "d_527057404_d_623218391", "description": ""},
        {"source": "D_527057404_D_261863326", "target": "d_527057404_d_802622485", "description": ""}
    ],
    "FlatConnect.covid19Survey_v1_JP": [
        # Issue: https://github.com/Analyticsphere/pr2-documentation/issues/21
        {"source": "d_71558179_v2_1_1", "target": "d_715581797_1_v2", "description": ""},
        {"source": "d_71558179_v2_2_2", "target": "d_715581797_2_v2", "description": ""}, 
        {"source": "d_71558179_v2_3_3", "target": "d_715581797_3_v2", "description": ""}, 
        {"source": "d_71558179_v2_4_4", "target": "d_715581797_4_v2", "description": ""}, 
        {"source": "d_71558179_v2_5_5", "target": "d_715581797_5_v2", "description": ""}, 
        {"source": "d_71558179_v2_6_6", "target": "d_715581797_6_v2", "description": ""}, 
        {"source": "d_71558179_v2_7_7", "target": "d_715581797_7_v2", "description": ""}, 
        {"source": "d_71558179_v2_8_8", "target": "d_715581797_8_v2", "description": ""}, 
        {"source": "d_71558179_v2_9_9", "target": "d_715581797_9_v2", "description": ""}, 
        {"source": "d_71558179_v2_10_10", "target": "d_715581797_10_v2", "description": ""}
    ]
}

#
# Congigure custom transforms and the affected source and target columns 
CUSTOM_TRANSFORMS = {
    # https://github.com/Analyticsphere/pr2-documentation/issues/4
    "FlatConnect.module1_v2_JP": [
        {
            "source": "D_317093647",
            "target": "D_317093647_D_623218391",
            "transform_template": lambda source, target: textwrap.dedent(f"""\
                -- Extract "Age at Surgery" as SrvBOH_MomEsCancAge_v1r0: match 1-3 digits (e.g., 55, 125)
                CASE
                    WHEN REGEXP_CONTAINS({source}, r'^\\d{{1,3}}$')      -- Match 1-3 digits
                        AND CAST({source} AS INT64) BETWEEN 0 AND 125    -- Valid age range
                        THEN CAST({source} AS INT64)                     -- Convert to integer
                    ELSE NULL                                            -- Default to NULL
                END AS {target}
            """)
        },
        {
            "source": "D_317093647",
            "target": "D_317093647_D_802622485",
            "transform_template": lambda source, target: textwrap.dedent(f"""\
                -- Extract "Year at Surgery" as SrvBOH_MomEsCancYr_v1r1: match exactly 4 digits (e.g., 1987)
                CASE
                    WHEN REGEXP_CONTAINS({source}, r'^\\d{{4}}$')       -- Match exactly 4 digits
                        THEN CAST({source} AS INT64)                    -- Convert to integer
                    ELSE NULL                                           -- Default to NULL
                END AS {target}
            """)
        }
    ]
}
