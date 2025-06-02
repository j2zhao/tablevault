from tablevault._builders.column_builder_type import ColumnBuilder
from tablevault._builders.index_builder_type import IndexBuilder
from tablevault._builders.base_builder_type import TVBuilder
from tablevault._defintions.tv_errors import TVBuilderError
from tablevault._defintions import constants
from tablevault._builders import builder_constants

BUILDER_TYPE_MAPPING = {
    builder_constants.COLUMN_BUILDER: ColumnBuilder,
    builder_constants.INDEX_BUILDER: IndexBuilder,
}


def load_builder(yaml_builder: dict) -> TVBuilder:
    if constants.BUILDER_TYPE not in yaml_builder:
        raise TVBuilderError(
            f"""Builder {yaml_builder[constants.BUILDER_NAME]}
              doesn't contain attribute {constants.BUILDER_TYPE}."""
        )
    builder = BUILDER_TYPE_MAPPING[yaml_builder[constants.BUILDER_TYPE]](**yaml_builder)
    return builder
