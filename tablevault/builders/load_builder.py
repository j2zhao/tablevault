from tablevault.builders.code_builder_type import CodeBuilder
from tablevault.builders.generator_builder_type import GeneratorBuilder
from tablevault.builders.open_ai_threads_builder_type import OAIThreadBuilder
from tablevault.builders.base_builder_type import TVBuilder
from tablevault.defintions.tv_errors import TVBuilderError
from tablevault.defintions import constants
from tablevault.builders import builder_constants 

BUILDER_TYPE_MAPPING = {
    builder_constants.CODE_BUILDER: CodeBuilder,
    builder_constants.GENERATOR_BUILDER: GeneratorBuilder,
    builder_constants.OAI_THREAD_BUILDER: OAIThreadBuilder,
}


def load_builder(yaml_builder: dict) -> TVBuilder:
    if constants.BUILDER_TYPE not in yaml_builder:
        raise TVBuilderError(
            f"""Builder {yaml_builder[constants.BUILDER_NAME]}
              doesn't contain attribute {constants.BUILDER_TYPE}."""
        )
    try:
        builder = BUILDER_TYPE_MAPPING[yaml_builder[constants.BUILDER_TYPE]](
            **yaml_builder
        )
        return builder
    except Exception as e:
        raise TVBuilderError(
            f"Error {e} when generating builder {yaml_builder[constants.BUILDER_NAME]}"
        )
