from tablevault._utils import file_operations
from tablevault._utils.metadata_store import MetadataStore
from tablevault._prompt_parsing.prompt_parser_table import (
    parse_arg_from_dict,
    parse_val_from_arg,
)
from tablevault._prompt_parsing.types import *
import copy
import re
from tablevault.errors import DVPromptError


def get_changed_columns(prompt: Prompt) -> list[str]:
    if prompt["type"] == "code":
        changed_columns = copy.deepcopy(prompt["changed_columns"])
    elif prompt["type"] == "llm":
        col = prompt["changed_columns"][0]
        changed_columns = []
        for i in range(len(prompt["questions"]) - 1):
            changed_columns.append(col + "_" + str(i + 1))
        if prompt["output_type"] != "freeform":
            changed_columns.append(col + "_" + str(len(prompt["questions"])))
        changed_columns.append(col)
    return changed_columns


def convert_reference(prompt: Prompt) -> Prompt:
    return parse_arg_from_dict(prompt)


def get_table_value(prompt_arg: PromptArg, index: int, cache: Cache) -> str:
    return parse_val_from_arg(prompt_arg, index, cache)


def _topological_sort(items: list, dependencies: dict) -> list:
    # Step 1: Build the graph based on parent -> child dependencies
    graph = {item: [] for item in items}

    for parent, children in dependencies.items():
        for child in children:
            if child not in graph:
                graph[child] = []  # Ensure child exists in graph
            graph[parent].append(child)  # Parent points to its children

    # Step 2: Perform DFS-based topological sorting
    visited = set()  # To track visited nodes
    visiting = set()  # To track the current recursion stack (for cycle detection)
    sorted_order = []

    def dfs(node):
        if node in visiting:
            raise DVPromptError("Cycle detected! Topological sort of prompts not possible.")
        if node in visited:
            return

        visiting.add(node)  # Mark node as visiting
        for child in graph[node]:
            dfs(child)  # Visit children first
        visiting.remove(node)  # Remove from visiting
        visited.add(node)  # Mark node as visited
        sorted_order.append(node)  # Add node after processing children

    # Step 3: Apply DFS to all items
    for item in items:
        if item not in visited:
            dfs(item)
    # Return the sorted order directly
    # (no need to reverse because we're appending after dependencies)
    return sorted_order


def parse_string(input_string: str) -> tuple[str, str, str]:
    # Define the regex pattern for all cases
    pattern = r"^(\w+)(?:\.(\w+))?(?:\((\w+)\))?$"
    match = re.match(pattern, input_string)

    if match:
        # Extract components
        part1 = match.group(1)  # First ALPHANUMERIC
        part2 = match.group(2)  # Second ALPHANUMERIC (optional)
        part3 = match.group(3)  # ALPHANUMERIC inside parentheses (optional)
        return part1, part2, part3
    else:
        raise DVPromptError("Input string does not match the expected format.")


def _parse_dependencies(
        prompts: dict[str, Prompt],
        table_name:str,
        start_time: float,
        db_metadata: MetadataStore) -> tuple[PromptDeps, InternalDeps, ExternalDeps]:
    
    table_generator = ''
    for prompt in prompts:
        if prompt.startswith(f'gen_{table_name}') and table_generator == '':
            table_generator = prompt
        elif prompt.startswith(f'gen_{table_name}') and table_generator != '':
            raise DVPromptError(f'Can only have one prompt that starts with: gen_{table_name}')
    if table_generator == '':
        raise DVPromptError(f'Needs one generator prompt that starts with gen_{table_name}')

    external_deps = {}
    internal_prompt_deps = {}
    internal_deps = {}
    gen_columns = prompts[table_generator]["parsed_changed_columns"]
    for pname in prompts:
        external_deps[pname] = set()
        if pname != table_generator:
            internal_deps[pname] = set(gen_columns)
            internal_prompt_deps[pname] = {table_generator}
        else:
            internal_deps[pname] = set()
            internal_prompt_deps[pname] = set()

        for dep in prompts[pname]["dependencies"]:
            table, column, instance = parse_string(dep)
            if instance is None:
                latest = True
            else:
                latest = False
            if table == "self":
                internal_deps[pname].add(column)  # TODO check this
                for pn in prompts:
                    if column in prompts[pn]["parsed_changed_columns"]:
                        internal_prompt_deps[pname].add(pn)
                continue
            elif not db_metadata.get_table_multiple(table) and instance is not None:
                raise DVPromptError(
                    f"Table dependency ({table}, {column}, {instance}) for prompt {pname} doesn't have versions."  # noqa: E501
                )
            elif column is not None:
                if instance is not None:
                    instance_exists = db_metadata.check_table_existance(
                        table, instance, column
                    )
                    if not instance_exists:
                        mat_time, start_time_ = 0
                    else:
                        mat_time, start_time_ = db_metadata.get_column_times(
                            column, instance, table
                        )
                else:
                    mat_time, start_time_, instance = (
                        db_metadata.get_last_column_update(table, column, start_time)
                    )
                if mat_time == 0 or start_time_ > start_time:
                    raise DVPromptError(
                        f"Table dependency ({table}, {instance},{column}) for {pname} not materialized at {start_time}"  # noqa: E501
                    )
            else:
                if instance is not None:
                    instance_exists = db_metadata.check_table_existance(table, instance)
                    if not instance_exists:
                        mat_time, start_time_ = 0
                    else:
                        mat_time, start_time_ = db_metadata.get_table_times(
                            instance, table
                        )
                else:
                    instance_exists = db_metadata.check_table_existance(table)
                    if not instance_exists:
                        mat_time, start_time_ = 0
                    mat_time, _, instance = db_metadata.get_last_table_update(
                        table, start_time
                    )
                if mat_time == 0 or start_time_ > start_time:
                    raise DVPromptError(
                        f"Table dependency ({table}, {instance}) for prompt {pname} not materialized at {start_time}"  # noqa: E501
                    )
            external_deps[pname].add((table, column, instance, mat_time, latest))
        external_deps[pname] = list(external_deps[pname])
        internal_deps[pname] = list(internal_deps[pname])
        internal_prompt_deps[pname] = list(internal_prompt_deps[pname])
    return internal_prompt_deps, internal_deps, external_deps


def parse_prompts(
    prompts: dict[Prompt],
    db_metadata: MetadataStore,
    start_time: float,
    instance_id: str,
    table_name: str,
    origin_id: str,
) -> tuple[list[str], list[str], list[str], list[str], InternalDeps, ExternalDeps]:
    internal_prompt_deps, internal_deps, external_deps = _parse_dependencies(
        prompts, table_name, start_time, db_metadata
    )
    pnames = list(prompts.keys())
    top_pnames = _topological_sort(pnames, internal_prompt_deps)
    all_columns = []
    to_change_columns = []
    for i, pname in enumerate(top_pnames):
        all_columns += prompts[pname]["parsed_changed_columns"]

    if origin_id != '':
        to_execute = []
        prev_mat_time, _ = db_metadata.get_table_times(origin_id, table_name)
        prev_prompts = file_operations.get_prompt_names(origin_id, table_name, db_metadata.db_dir)

        for pname in top_pnames:
            execute = False
            for dep in internal_prompt_deps[pname]:
                if dep in to_execute and dep != top_pnames[0]:
                    execute = True
                    break
            if not execute:
                for dep in external_deps[pname]:
                    if dep[3] >= prev_mat_time:
                        execute = True
                        break
            if not execute:
                if pname not in prev_prompts:
                    execute = True
                elif not file_operations.check_prompt_equality(
                    pname, instance_id, origin_id, table_name, db_metadata.db_dir
                ):
                    execute = True
            if execute:
                to_execute.append(pname)
                to_change_columns += prompts[pname]["parsed_changed_columns"]
    else:
        for pname in top_pnames:
            to_change_columns += prompts[pname]["parsed_changed_columns"]
    return (
        top_pnames,
        to_change_columns,
        all_columns,
        internal_deps,
        external_deps,
    )
