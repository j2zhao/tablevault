from tablevault._helper import file_operations
from tablevault._helper.metadata_store import MetadataStore
from tablevault._DEPRECATED.prompt_parser_common import topological_sort, parse_dep
from tablevault.defintions.types import (
    Prompt,
    PromptDeps,
    InternalDeps,
    ExternalDeps,
)

from tablevault.defintions.tv_errors import TVPromptError
from tablevault.defintions import prompt_constants

def _parse_dependencies(
    prompts: dict[str, Prompt],
    table_name: str,
    start_time: float,
    db_metadata: MetadataStore,
) -> tuple[PromptDeps, InternalDeps, ExternalDeps]:

    table_generator = ""
    for prompt in prompts:
        if prompt.startswith(f"gen_{table_name}") and table_generator == "":
            table_generator = prompt
        elif prompt.startswith(f"gen_{table_name}") and table_generator != "":
            raise TVPromptError(
                f"Can only have one prompt that starts with: gen_{table_name}"
            )
    if table_generator == "":
        raise TVPromptError(
            f"Needs one generator prompt that starts with gen_{table_name}"
        )

    external_deps = {}
    internal_prompt_deps = {}
    internal_deps = {}
    gen_columns = prompts[table_generator][prompt_constants.CHANGED_COLUMNS]
    for pname in prompts:
        external_deps[pname] = set()
        if pname != table_generator:
            internal_deps[pname] = set(gen_columns)
            internal_prompt_deps[pname] = {table_generator}
        else:
            internal_deps[pname] = set()
            internal_prompt_deps[pname] = set()

        for dep in prompts[pname][prompt_constants.DEPENDENCIES]:
            table, column, instance = parse_dep(dep)
            if instance is None:
                latest = True
            else:
                latest = False
            if table == prompt_constants.TABLE_SELF:
                internal_deps[pname].add(column)  # TODO check this
                for pn in prompts:
                    if column in prompts[pn][prompt_constants.CHANGED_COLUMNS]:
                        internal_prompt_deps[pname].add(pn)
                continue
            elif not db_metadata.get_table_multiple(table) and instance is not None:
                raise TVPromptError(
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
                    raise TVPromptError(
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
                    raise TVPromptError(
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
    top_pnames = topological_sort(pnames, internal_prompt_deps)
    all_columns = []
    to_change_columns = []
    for i, pname in enumerate(top_pnames):
        all_columns += prompts[pname][prompt_constants.CHANGED_COLUMNS]

    if origin_id != "":
        to_execute = []
        prev_mat_time, _ = db_metadata.get_table_times(origin_id, table_name)
        prev_prompts = file_operations.get_prompt_names(
            origin_id, table_name, db_metadata.db_dir
        )

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
                to_change_columns += prompts[pname][prompt_constants.CHANGED_COLUMNS]
    else:
        for pname in top_pnames:
            to_change_columns += prompts[pname][prompt_constants.CHANGED_COLUMNS]
    return (
        top_pnames,
        to_change_columns,
        all_columns,
        internal_deps,
        external_deps,
    )
