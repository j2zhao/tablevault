from tablevault.col_builders.code_execution_builder_type import CodeBuilder
from tablevault.col_builders.gen_code_builder_type import GeneratorBuilder
from tablevault.col_builders.open_ai_threads_builder_type import OAIThreadBuilder
from tablevault.col_builders.base_builder_type import TVBuilder
from tablevault.defintions.tv_errors import TVBuilderError
from tablevault.defintions import constants
BUILDER_TYPE_MAPPING = {
    "CodeBuilder": CodeBuilder,
    "GeneratorBuilder": GeneratorBuilder,
    "OAIThreadBuilder": OAIThreadBuilder,
}

def load_builder(yaml_builder: dict) -> TVBuilder:
    if constants.BUILDER_TYPE not in yaml_builder:
        raise TVBuilderError(f"Builder {yaml_builder[constants.BUILDER_NAME]} doesn't contain required attribute {constants.BUILDER_TYPE}.")
    try:
        builder = BUILDER_TYPE_MAPPING[yaml_builder[constants.BUILDER_TYPE]](**yaml_builder)
        return builder
    except Exception as e:
        raise TVBuilderError(f'Error {e} when generating builder {yaml_builder[constants.BUILDER_NAME]}')