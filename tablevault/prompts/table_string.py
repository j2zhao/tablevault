from typing import Any
from tablevault.defintions import constants
import re
from typing import Optional
from pydantic.dataclasses import dataclass
from pydantic import Field
from tablevault.defintions.tv_errors import TVPromptError
from tablevault.defintions.types import Cache

@dataclass(init=False)
class DataTable():
    table: str
    column: Optional[str]
    version: str = Field(default="base")

    def __init__(self, args:str) -> None:
        if not isinstance(args, str):
            raise TVPromptError(f'Expected str type for {args}')
        pattern = r"^(\w+)(?:\.(\w+))?(?:\((\w+)\))?$"
        match = re.match(pattern, args)

        if match:
            self.table = match.group(1) 
            self.version = match.group(2)
            self.column = match.group(3) 
        else:
            raise TVPromptError("Input string does not match the expected format.")

        if hasattr(self, '__post_init__'):
            self.__post_init__()
    
    def parse(self, cache: Cache):
        if self.version is not None:
            df = cache[(self.table, self.version)]
        else:
            df = cache[self.table]
        if self.column is not None:
            return df[self.column]
        else:
            return df

@dataclass
class _TableValue():
    table: str
    column: str
    version: Optional[str]
    conditions: dict[str, "_TableValue"| str]

@dataclass(init=False)
class TableReference():
    text: str
    references: list[_TableValue]

    def __init__(self, args:str) -> None:
        if not isinstance(args, str):
            raise TVPromptError(f'Expected str type for {args}')
        self.text, self.references = _parse_arg_from_string(args)
        if hasattr(self, '__post_init__'):
            self.__post_init__()
    
    def parse(self, cache: Cache, index:Optional[int]= None):
        if len(self.references) == 0:
            return self.text
        if self.text == "<<>>" and len(self.references) == 1:
            return _read_table_reference(self.references[0], index=index, cache=cache)
        tstring = self.text
        for ref in self.references:
            ref_ = _read_table_reference(ref, index=index, cache=cache)
            ref_ = str(ref_)
            tstring = tstring.replace("<<>>", ref_, 1)
        return tstring

TableString = DataTable | TableReference | str


def parse_table_string(arg:Any, cache: Cache, index: Optional[int]= None) -> Any:
    if isinstance(arg, str):
        return arg
    elif isinstance(arg, DataTable):
        return arg.parse(cache)
    elif isinstance(arg, TableReference):
        return arg.parse(cache, index)
    elif isinstance(arg, list):
        return [parse_table_string(item) for item in arg]
    elif isinstance(arg, set):
        return set([parse_table_string(item) for item in arg])
    elif isinstance(arg, dict):
        return {
            parse_table_string(k): parse_table_string(v)
            for k, v in arg.items()
        }
    else:
        return arg
    

def apply_table_string(arg):
    if isinstance(arg, str):
        return _get_table_string(arg)
    elif isinstance(arg, list):
        return [apply_table_string(item) for item in arg]
    elif isinstance(arg, set):
        return set([apply_table_string(item) for item in arg])
    elif isinstance(arg, dict):
        return {
            apply_table_string(k): apply_table_string(v)
            for k, v in arg.items()
        }
    elif hasattr(arg, '__dict__'):
        for attr, val in vars(arg).items():
            setattr(arg, attr, apply_table_string(val))
    return arg

def _get_table_string(arg:str)-> TableString:
    if not isinstance(arg, str):
        raise TVPromptError(f'Expected str type for {arg}')
    pattern = r"<<(.*?)>>"
    extracted_values = re.findall(pattern, arg)
    if len(extracted_values) != 0:
        return TableReference(arg)
    elif arg.startswith('[[') and arg.endswith(']]'):
        return DataTable(arg[2:-2])
    else:
        return arg

def _parse_arg_from_string(val_str: str) -> tuple[str, list[_TableValue]]:
    val_str = val_str.strip()
    pattern = r"<<(.*?)>>"
    extracted_values = re.findall(pattern, val_str)
    if len(extracted_values) == 0:
        return val_str, []
    modified_string = re.sub(pattern, "<<>>", val_str)
    values = []
    for val in extracted_values:
        values.append(_parse_table_reference(val))
    return modified_string, values

def _parse_table_reference(s: str) -> _TableValue:
    s = s.strip()

    # Pattern: (table_name.column)([ ... ])?
    main_pattern = r"^([A-Za-z0-9_]+)(\([A-Za-z0-9_]*\))?\.([A-Za-z0-9_]+)?(\[(.*)\])?$"
    m = re.match(main_pattern, s)
    if not m:
        raise TVPromptError(f"Invalid TableValue string: {s}")

    main_table = m.group(1)
    main_instance = m.group(2)
    main_col = m.group(3)
    inner_content = m.group(5)

    if not inner_content:
        return _TableValue(
            table=main_table, column=main_col, instance_id=main_instance, key={}
        )

    pairs = _split_top_level_list(inner_content)
    key_dict = {}
    for pair in pairs:
        pair = pair.strip()
        kv_split = pair.split(":", 1)
        if len(kv_split) != 2:
            raise TVPromptError(f"Invalid key-value pair: {pair}")
        key_col = kv_split[0].strip()
        val_str = kv_split[1].strip()
        # Parse the value
        if val_str.startswith("'") and val_str.ends("'"):
            val = val_str[1:-1]
        else:
            val = _parse_table_reference(val_str)
        key_dict[key_col] = val
    return _TableValue(
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
            pairs.append("".join(current))
            current = []
        else:
            current.append(char)
    if current:
        pairs.append("".join(current))
    return pairs

def _read_table_reference(
    ref: _TableValue, index: Optional[int], cache: Cache
) -> list | str:
    if ref.version is not None:
        df = cache[(ref.table, ref.version)]
    else:
        df = cache[ref.table]
    conditions = {}
    if len(ref.conditions()) == 0:
        conditions["index"] = index
    for condition, value in ref.conditions.items():
        if isinstance(value, TableReference):
            value = _read_table_reference(value, index=index, cache=cache)
        conditions[condition] = value
        if condition == "index" and value == constants.TABLE_SELF:
            conditions[condition] = index
    query_str = " & ".join([f"{k} == {repr(v)}" for k, v in conditions.items()])
    rows = df.query(query_str)
    result = rows[ref.column].to_list()
    if len(result) == 1:
        return result[0]
    else:
        return result