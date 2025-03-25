from typing import Any
from tablevault.defintions import constants
import re
from typing import Optional, Union, get_origin, get_args
from dataclasses import dataclass
from tablevault.defintions.tv_errors import TVPromptError
from tablevault.defintions.types import Cache
import pandas as pd

@dataclass(init=False)
class DataTable():
    """
    A string representation of a data table.

    Examples:

    - "table_name''

    - "table_name.column''

    - "table_name(version).column''

    - "table_name(version)''

    Note: in ambiguous cases, surround with '~'.
    """
    table: str
    column: str
    version: str

    def __init__(self, args:str) -> None:
        if not isinstance(args, str):
            raise TVPromptError(f'Expected str type for {args}')
        if args.startswith('~') and args.endswith('~'):
            args = args[1:-1]
        pattern = r"^(\w+)(?:\((\w+)\))?(?:\.(\w+))?$"
        match = re.match(pattern, args)

        if match:
            self.table = match.group(1) 
            if match.group(2) is not None:
                self.version = match.group(2)
            else:
                self.version = 'base'
            if match.group(3) is not None:
                self.column = match.group(3)
            else:
                self.column = ''
        else:
            raise TVPromptError("Input string does not match the expected format.")

        if hasattr(self, '__post_init__'):
            self.__post_init__()
    
    def parse(self, cache: Cache):
        df = cache[(self.table, self.version)]
        
        if self.column != '':
            df = df[self.column]
        return df
    
    def __repr__(self):
        table_str = self.table
        if self.version != '':
            table_str = table_str + "(" + self.version + ")"
        if self.column != '':
            table_str = table_str + '.' + self.column
        return table_str
    
    def __str__(self):
        table_str = self.table
        if self.version != '':
            table_str = table_str + "(" + self.version + ")"
        if self.column != '':
            table_str = table_str + '.' + self.column
        return table_str

Condition = Union["_TableValue",str,int]

@dataclass
class _TableValue():
    table: str
    column: str 
    version: str 
    conditions: dict[str, list[Condition]]

    def get_data_tables(self)-> list[DataTable]:
        tables = []
        table_str = self.table
        if self.version != '':
            table_str = table_str + "(" + self.version + ")"
        if self.column != '':
            table_str = table_str + '.' + self.column
        tables.append(TableString(table_str))
        for _, vals in self.conditions:
            for val in vals:
                if isinstance(val, _TableValue):
                    tables += val.get_data_tables()
                elif isinstance(val, TableReference):
                    if isinstance(val, _TableValue):
                        tables += val.get_data_tables()
    
    def parse(self, cache: Cache, index:Optional[int]= None):
        return _read_table_reference(self, index, cache) 

@dataclass(init=False)
class TableReference():
    """
    A string representation of data table entries.

    Examples:

    - <<llm_storage.openai_id[paper_name:self.paper_name]>>

    - <<stories.paper_path[paper_name:self.paper_name]>>
    
    - "Test data: <<stories.paper_path[paper_name:self.paper_name]>> "

    Note: Always surround reference with '<<' and '>>'.
    """
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
            return self.references[0].parse(cache, index)
        tstring = self.text
        for ref in self.references:
            ref_ = ref.parse(cache, index)
            tstring = tstring.replace("<<>>", str(ref_), 1)
        return tstring

    def get_data_tables(self)-> list[DataTable]:
        tables = []
        for ref in self.references: 
            tables += ref.get_data_tables()
        return tables

TableString = DataTable | TableReference | str


def apply_table_string(arg:Any, cache: Cache, index: Optional[int]= None) -> Any:
    if isinstance(arg, str):
        return arg
    elif isinstance(arg, DataTable):
        return arg.parse(cache)
    elif isinstance(arg, TableReference):
        return arg.parse(cache, index)
    elif isinstance(arg, list):
        return [apply_table_string(item, cache, index) for item in arg]
    elif isinstance(arg, set):
        return set([apply_table_string(item, cache, index) for item in arg])
    elif isinstance(arg, dict):
        return {
            apply_table_string(k, cache, index): apply_table_string(v, cache, index)
            for k, v in arg.items()
        }
    else:
        return arg

def parse_table_string(annotation, arg):
    if annotation == DataTable:
        return DataTable(arg)
    if isinstance(arg, str):
        return _parse_table_string(arg)
    elif isinstance(arg, list):
        if annotation != None and get_origin(annotation) is list:
            (annotation,) = get_args(annotation)
        else:
            annotation = None
        return [parse_table_string(annotation, item) for item in arg]
    elif isinstance(arg, set):
        if annotation != None and get_origin(annotation) is set:
            (annotation,) = get_args(annotation)
        else:
            annotation = None
        return set([parse_table_string(annotation, item) for item in arg])
    elif isinstance(arg, dict):
        if annotation != None and get_origin(annotation) is dict:
            key_type, val_type = get_args(annotation)
        else:
            key_type  = None
            val_type = None
        return {
            parse_table_string(key_type, k): parse_table_string(val_type, v)
            for k, v in arg.items()
        }
    elif hasattr(arg, '__dict__'):
        if hasattr(annotation, "__annotations__"):
            annotations = annotation.__annotations__.items()
        else:
            annotations = {}
        for attr, val in vars(arg).items():
            if attr in annotations:
                annotation = annotations[attr]
            else:
                annotation = None
            setattr(arg, attr, parse_table_string(annotation, val))
    return arg    


def _parse_table_string(arg:str) -> TableString:
    if not isinstance(arg, str):
        raise TVPromptError(f'Expected str type for {arg}')
    pattern = r"<<(.*?)>>"
    extracted_values = re.findall(pattern, arg)
    if len(extracted_values) != 0:
        return TableReference(arg)
    elif arg.startswith('~') and arg.endswith('~'):
        return DataTable(arg)
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

def _parse_condition(condition_str:str) -> Condition:
    if condition_str.startswith("'") and condition_str[0].endswith("'"):
        val = condition_str
    elif condition_str.startswith("\"") and condition_str[0].endswith("\""):
        val = condition_str[1:-1]
        val = TableReference(val)
    else:
        val = _parse_table_reference(condition_str[0])
    return val

def _parse_table_reference(s: str) -> _TableValue:
    s = s.strip()

    # Pattern: (table_name.column)([ ... ])?
    main_pattern = r"^([A-Za-z0-9_]+)(\([A-Za-z0-9_]*\))?\.([A-Za-z0-9_]+)?(\[(.*)\])?$"
    m = re.match(main_pattern, s)
    if not m:
        raise TVPromptError(f"Invalid TableValue string: {s}")

    main_table = m.group(1)
    main_version = m.group(2)
    if main_version == None:
        main_version = 'base'
    main_col = m.group(3)
    inner_content = m.group(5)

    if not inner_content:
        return _TableValue(
            table=main_table, column=main_col, version=main_version, conditions={}
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
        vals = _split_top_level_list(val_str, splitter_char=':')
        if len(vals) > 2:
            raise TVPromptError(f"too many ':' splits in Table Reference: {vals}")
        for i in range(len(vals)):
            vals[i] = _parse_condition(vals[i])
        key_dict[key_col] = vals
    return _TableValue(
        table=main_table, column=main_col, version=main_version, conditions=key_dict
    )

def _split_top_level_list(s: str, splitter_char=',') -> list[str]:
    """
    Split a string by the splitter character that are not nested inside square brackets
    or quoted strings. Single and double quotes are treated separately:
    once a quoted segment starts with a particular quote type, only that same type can close it.
    
    For example, the string "'test:test':'test:test'" when split with splitter_char=':' 
    will be split into:
        ["'test:test'", "'test:test'"]
    """
    pairs = []
    bracket_depth = 0
    in_quote = None  # Holds the current quote character if inside a quoted string
    escape = False   # Indicates if the current character is escaped
    current = []
    
    for char in s:
        if escape:
            current.append(char)
            escape = False
            continue
        if char == "\\":
            escape = True
            current.append(char)
            continue
        if char in ("'", '"'):
            if in_quote is None:
                in_quote = char
            elif in_quote == char:
                in_quote = None
            current.append(char)
        elif char == "[" and in_quote is None:
            bracket_depth += 1
            current.append(char)
        elif char == "]" and in_quote is None:
            bracket_depth -= 1
            current.append(char)
        elif char == splitter_char and bracket_depth == 0 and in_quote is None:
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
    if ref.table != constants.TABLE_SELF:
        df = cache[(ref.table, ref.version)]
    else:
        df = cache[ref.table]
    conditions = {}
    range_conditions = {}
    if len(ref.conditions) == 0:
        conditions["index"] = index
    for condition, value in ref.conditions.items():
        if len(value)== 1:
            value = value[0]
            if isinstance(value, _TableValue):
                value = _read_table_reference(value, index=index, cache=cache)
            elif isinstance(value, TableReference):
                value = value.parse(cache, index)
            conditions[condition] = value
        else:
            start_val, end_val = value
            if isinstance(start_val, _TableValue):
                start_val = _read_table_reference(start_val, index=index, cache=cache)
            elif isinstance(start_val, TableReference):
                start_val = start_val.parse(cache, index)
            if isinstance(end_val, _TableValue):
                end_val = _read_table_reference(end_val, index=index, cache=cache)
            elif isinstance(end_val, TableReference):
                end_val = end_val.parse(cache, index)
            range_conditions[condition] = (start_val, end_val)
    query_str = " & ".join([f"{k} == {repr(v)}" for k, v in conditions.items()])
    query_str_range = " & ".join([f"{k} >= {repr(v1)} & {k} < {repr(v2)}" for k, (v1,v2) in range_conditions.items()])
    if query_str_range != "":
        query_str = query_str + " & " + query_str_range
    rows = df.query(query_str)
    result = rows[ref.column].to_list()
    if len(result) == 1:
        return result[0]
    else:
        return result