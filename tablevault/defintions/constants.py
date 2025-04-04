TIMEOUT = 5
CHECK_INTERVAL = 0.5
BASE_TABLE_VERSION = 'base'
TEMP_INSTANCE = "TEMP_"

RESTART_LOCK = "restart"
CODE_FOLDER = "code_functions"
PROMPT_FOLDER = "prompts"
METADATA_FOLDER = "metadata"
LOCK_FOLDER = "locks"
TEMP_FOLDER = "_temp"
ARTIFACT_FOLDER = "artifacts"
ILLEGAL_TABLE_NAMES = [PROMPT_FOLDER, CODE_FOLDER, METADATA_FOLDER, LOCK_FOLDER, TEMP_FOLDER, RESTART_LOCK]

TABLE_FILE = "table.csv"
DTYPE_FILE = "dtypes.json"

META_LOG_FILE = "logs.txt"
META_CLOG_FILE = "completed_logs.txt"
META_ALOG_FILE = "active_logs.json"
META_CHIST_FILE = "columns_history.json"
META_THIST_FILE = "tables_history.json"
META_TABLE_FILE = "tables_metadata.json"
META_INSTANCE_FILE = "table_instances_metadata.json"

COPY_FILE_OP = "copy_files"
DELETE_TABLE_OP = "delete_table"
DELETE_INSTANCE_OP = "delete_instance"
EXECUTE_OP = "execute_instance"
SETUP_TEMP_OP = "setup_temp_instance"
SETUP_TEMP_INNER_OP = "setup_temp_instance_inner"
SETUP_TABLE_OP = "setup_table"
SETUP_TABLE_INNER_OP = "setup_table_inner"
COPY_DB_OP = "copy_database_files"
RESTART_OP = "restart_database"
ROLLBACK_OP = "rollback"
VALID_OPS = [COPY_FILE_OP, DELETE_TABLE_OP, DELETE_INSTANCE_OP, EXECUTE_OP, SETUP_TEMP_OP, SETUP_TABLE_OP,
              COPY_DB_OP, RESTART_OP, ROLLBACK_OP, SETUP_TABLE_INNER_OP, SETUP_TEMP_INNER_OP]

TABLE_SELF = "self"
TABLE_INDEX = "index"
PNAME = "name"
INSTANCE_ORIGIN = "origin"
PTYPE = "ptype"
DEPENDENCIES = "dependencies"

EX_CLEAR_TABLE = "clear_table"
EX_NO_UPDATE = "no_update"
EX_MAT = "materialized"
EX_ARTIFACTS = "move artifacts"
EX_UPDATE_PROMPT = "update_prompt"

TABLE_ALLOW_MARTIFACT = 'multiple_artifacts'
TABLE_SIDE_EFFECTS = 'side_effects'