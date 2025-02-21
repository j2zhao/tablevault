from typing import Any, Optional, Union
import re
from tablevault._prompt_parsing.types import (
    PromptArg,
    Cache,
    TableReference,
    TableString,
)
from tablevault.errors import DVPromptError

def parse_arg_from_dict(data: PromptArg) -> PromptArg:
    if isinstance(data, dict):
        return {k: parse_arg_from_dict(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [parse_arg_from_dict(v) for v in data]
    elif isinstance(data, str):
        return _parse_arg_from_string(data)
    else:
        return data


def parse_val_from_arg(prompt: PromptArg, index: Optional[int], cache: Cache) -> Any:
    if isinstance(prompt, TableString):
        prompt_ = prompt.text
        for ref in prompt.references:
            ref_ = _read_table_reference(ref, index=index, cache=cache)
            ref_ = str(ref_)
            prompt_ = prompt_.replace("<<>>", ref_, 1)
    elif isinstance(prompt, TableReference):
        prompt_ = _read_table_reference(prompt, index=index, cache=cache)
    elif isinstance(prompt, dict):
        prompt_ = {}
        for key in prompt:
            temp = parse_val_from_arg(prompt[key], index=index, cache=cache)
            prompt_[key] = temp

    elif isinstance(prompt, list):
        prompt_ = []
        for val in prompt:
            temp = parse_val_from_arg(val, index=index, cache=cache)
            prompt_.append(temp)
    else:
        prompt_ = prompt
    return prompt_


def _parse_arg_from_string(val_str: str) -> TableString | TableReference:
    val_str = val_str.strip()
    if val_str.startswith("<<") and val_str.endswith(">>"):
        return _parse_table_reference(val_str[2:-2])
    # Regular expression to match the pattern <<value>>
    pattern = r"<<(.*?)>>"
    # Find all matches
    extracted_values = re.findall(pattern, val_str)
    if len(extracted_values) == 0:
        return val_str
    modified_string = re.sub(pattern, "<<>>", val_str)
    values = []
    for val in extracted_values:
        values.append(_parse_table_reference(val))
    table_string = TableString(modified_string, values)
    return table_string


def _parse_table_reference(s: str) -> TableReference:
    s = s.strip()

    # Pattern: (table_name.column)([ ... ])?
    main_pattern = r"^([A-Za-z0-9_]+)(\([A-Za-z0-9_]*\))?\.([A-Za-z0-9_]+)(\[(.*)\])?$"
    m = re.match(main_pattern, s)
    if not m:
        raise DVPromptError(f"Invalid TableReference string: {s}")

    main_table = m.group(1)
    main_instance = m.group(2)
    main_col = m.group(3)
    inner_content = m.group(5)

    if not inner_content:
        return TableReference(
            table=main_table, column=main_col, instance_id=main_instance, key={}
        )

    pairs = _split_top_level_list(inner_content)

    key_dict = {}
    for pair in pairs:
        pair = pair.strip()
        kv_split = pair.split(":", 1)
        if len(kv_split) != 2:
            raise DVPromptError(f"Invalid key-value pair: {pair}")
        key_col = kv_split[0].strip()
        val_str = kv_split[1].strip()
        # Parse the value
        if val_str.startswith("'") and val_str.ends("'"):
            val = val_str[1:-1]
        else:
            val = _parse_table_reference(val_str)
        key_dict[key_col] = val
    return TableReference(
        table=main_table, column=main_col, instance_id=main_instance, key=key_dict
    )


def _split_top_level_list(s: str) -> list[str]:
    """
    Split a string by commas that are not nested inside square brackets.
    This is to correctly handle multiple key-value pairs.
    """
    pairs = []
    bracket_depth = 0
    current = []
    for char in s:
        if char == "[":
            bracket_depth += 1
            current.append(char)
        elif char == "]":
            bracket_depth -= 1
            current.append(char)
        elif char == "," and bracket_depth == 0:
            # top-level comma
            pairs.append("".join(current))
            current = []
        else:
            current.append(char)
    if current:
        pairs.append("".join(current))
    return pairs


def _read_table_reference(
    ref: TableReference, index: Optional[int], cache: Cache
) -> Union[list, str]:
    if ref.instance_id is not None:
        df = cache[(ref.table, ref.instance_id)]
    else:
        df = cache[ref.table]
    conditions = {}
    if len(ref.key) == 0:
        conditions["index"] = index
    for condition, value in ref.key.items():
        if isinstance(value, TableReference):
            value = _read_table_reference(value, index=index, cache=cache)
        conditions[condition] = value
    query_str = " & ".join([f"{k} == {repr(v)}" for k, v in conditions.items()])
    rows = df.query(query_str)
    result = rows[ref.column].to_list()

    return result
