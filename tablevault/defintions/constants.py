TIMEOUT = 5
ARTIFACT_ROWS = 10
CHECK_INTERVAL = 0.5
BASE_TABLE_VERSION = "base"
TEMP_INSTANCE = "TEMP_"
ARTIFACT_DTYPE = "artifact_string"
BUILDER_DEPENDENCIES = "dependencies"
BUILDER_NAME = "name"
BUILDER_TYPE = "builder_type"

RESTART_LOCK = "restart"
CODE_FOLDER = "code_functions"
ARCHIVE_FOLDER = "EXECUTION_ARCHIVE"
DELETION_FOLDER = "ARCHIVED_TRASH"
BUILDER_FOLDER = "builders"
METADATA_FOLDER = "metadata"
LOCK_FOLDER = "locks"
TEMP_FOLDER = "_temp"
ARTIFACT_FOLDER = "artifacts"
ARTIFACT_REFERENCE = "~ARTIFACT_FOLDER~"
ILLEGAL_TABLE_NAMES = [
    BUILDER_FOLDER,
    CODE_FOLDER,
    METADATA_FOLDER,
    LOCK_FOLDER,
    TEMP_FOLDER,
    RESTART_LOCK,
    ARTIFACT_FOLDER,
]

TABLE_FILE = "table.csv"
DTYPE_FILE = "dtypes.json"

META_LOG_FILE = "logs.txt"
META_CLOG_FILE = "completed_logs.txt"
META_ALOG_FILE = "active_logs.json"
META_CHIST_FILE = "columns_history.json"
META_THIST_FILE = "tables_history.json"
META_TABLE_FILE = "tables_metadata.json"
META_INSTANCE_FILE = "table_instances_metadata.json"
META_DESCRIPTION_FILE = "description.yaml"

STOP_PROCESS_OP = "stop_process"
COPY_FILE_OP = "copy_files"
DELETE_TABLE_OP = "delete_table"
DELETE_INSTANCE_OP = "delete_instance"
MAT_OP = "materialize_instance"
WRITE_TABLE_OP = "write_table"
WRITE_TABLE_INNER_OP = "write_table_inner"
EXECUTE_OP = "execute_instance"
EXECUTE_INNER_OP = "execute_instance_inner"
SETUP_TEMP_OP = "setup_temp_instance"
SETUP_TEMP_INNER_OP = "setup_temp_instance_inner"
SETUP_TABLE_OP = "setup_table"
SETUP_TABLE_INNER_OP = "setup_table_inner"
COPY_DB_OP = "copy_database_files"
RESTART_OP = "restart_database"
ROLLBACK_OP = "rollback"
VALID_OPS = [
    STOP_PROCESS_OP,
    COPY_FILE_OP,
    DELETE_TABLE_OP,
    DELETE_INSTANCE_OP,
    EXECUTE_OP,
    SETUP_TEMP_OP,
    SETUP_TABLE_OP,
    COPY_DB_OP,
    RESTART_OP,
    ROLLBACK_OP,
    SETUP_TABLE_INNER_OP,
    SETUP_TEMP_INNER_OP,
    MAT_OP,
    WRITE_TABLE_OP,
    WRITE_TABLE_INNER_OP,
    EXECUTE_INNER_OP,
]

TABLE_SELF = "self"
OUTPUT_SELF = "self"
TABLE_INDEX = "index"


EX_CLEAR_TABLE = "clear_table"
TABLE_ALLOW_MARTIFACT = "multiple_artifacts"
TABLE_SIDE_EFFECTS = "side_effects"

DESCRIPTION_SUMMARY = "summary"
DESCRIPTION_ORIGIN = "origin"
DESCRIPTION_EDIT = "external_edit"

DESCRIPTION_DEPENDENCIES = "dependencies"
DESCRIPTION_BUILDER_DEPENDENCIES = "builder_dependencies"
DESCRIPTION_FUTURE = "future_version"
DESCRIPTION_CHILDREN = "children"
DESCRIPTION_SUCCESS = "success"
