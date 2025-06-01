TIMEOUT = 5
ARTIFACT_ROWS = 10
CHECK_INTERVAL = 0.5
BASE_TABLE_VERSION = "base"
TEMP_INSTANCE = "TEMP_"
ARTIFACT_DTYPE = "artifact_string"
BUILDER_NAME = "name"
BUILDER_TYPE = "builder_type"
INDEX_BUILDER_SUFFIX = "_index"
BUILDER_DEPENDENCIES = "builder_dependencies"

META_LOG_FILE = "logs.txt"
META_CLOG_FILE = "completed_logs.txt"
META_ALOG_FILE = "active_logs.json"
META_CHIST_FILE = "columns_history.json"
META_THIST_FILE = "tables_history.json"
META_TABLE_FILE = "tables_metadata.json"
META_INSTANCE_FILE = "table_instances_metadata.json"
META_DESCRIPTION_FILE = "description.yaml"


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
    META_DESCRIPTION_FILE,
]

TABLE_FILE = "table.csv"
DTYPE_FILE = "dtypes.json"


STOP_PROCESS_OP = "stop_process"
CREATE_BUILDER_FILE_OP = "create_builder"
CREATE_CODE_MODULE_OP = "create_code_module"
DELETE_BUILDER_FILE_OP = "delete_builder"
DELTE_CODE_MODULE_OP = "delete_code_module"
RENAME_TABLE_OP = "rename_table"
DELETE_TABLE_OP = "delete_table"
DELETE_INSTANCE_OP = "delete_instance"
MAT_OP = "materialize_instance"
WRITE_TABLE_OP = "write_table"
WRITE_TABLE_INNER_OP = "write_table_inner"
EXECUTE_OP = "execute_instance"
EXECUTE_INNER_OP = "execute_instance_inner"
CREATE_INSTANCE_OP = "setup_temp_instance"
CREATE_TABLE_OP = "create_table"
RESTART_OP = "restart_database"
VALID_OPS = [
    STOP_PROCESS_OP,
    CREATE_BUILDER_FILE_OP,
    DELETE_BUILDER_FILE_OP,
    CREATE_CODE_MODULE_OP,
    DELTE_CODE_MODULE_OP,
    RENAME_TABLE_OP,
    DELETE_TABLE_OP,
    DELETE_INSTANCE_OP,
    EXECUTE_OP,
    CREATE_INSTANCE_OP,
    CREATE_TABLE_OP,
    RESTART_OP,
    MAT_OP,
    WRITE_TABLE_OP,
    WRITE_TABLE_INNER_OP,
    EXECUTE_INNER_OP,
]

TABLE_SELF = "self"
# OUTPUT_SELF = "self"
TABLE_INDEX = "index"


EX_CLEAR_TABLE = "clear_table"
TABLE_ALLOW_MARTIFACT = "multiple_artifacts"
TABLE_SIDE_EFFECTS = "side_effects"

DESCRIPTION_SUMMARY = "summary"
DESCRIPTION_ORIGIN = "origin"
DESCRIPTION_EDIT = "external_edit"

DESCRIPTION_DEPENDENCIES = "dependencies"
DESCRIPTION_FUTURE = "future_version"
DESCRIPTION_CHILDREN = "children"
DESCRIPTION_SUCCESS = "success"
