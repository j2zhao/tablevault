from tablevault._utils.file_operations import get_db_prompts
from tablevault._prompt_parsing.types import Prompt
from tablevault._prompt_parsing.prompt_parser_common import topological_sort, parse_dep


def _get_table_dependencies(table_name: str, prompts: dict[str, Prompt]) -> list[str]:
    dependencies = set()
    for pname in prompts:
        for dep in prompts[pname]["dependencies"]:
            table, _, _ = parse_dep(dep)
            if table != "self" and table != table_name:
                dependencies.add(table)
    return list(dependencies)


def get_table_order(db_dir: str) -> list[str]:
    all_prompts = get_db_prompts(db_dir, get_metadata=False)
    all_dependencies = {}
    for table_name in all_prompts:
        all_dependencies[table_name] = _get_table_dependencies(
            table_name, all_prompts[table_name]
        )

    table_names = topological_sort(list(all_dependencies.keys()), all_dependencies)
    return table_names
