from typing import Any
from tablevault.defintions import constants
import re
from typing import Optional, Union, get_origin, get_args
from dataclasses import dataclass
from tablevault.defintions.tv_errors import TVBuilderError, TableStringError
from tablevault.defintions.types import Cache
import pandas as pd
from pandas.api.types import is_string_dtype

Condition = Union["TableValue", str, "TableReference"]


@dataclass
class TableValue:
    table: str
    columns: list[str]
    version: str
    conditions: dict[str, list[Condition]]

    def get_data_tables(self) -> list["TableValue"]:
        tables = []
        ttable = TableValue(self.table, self.columns, self.version, {})
        tables.append(ttable)
        for key, vals in self.conditions.items():
            ttable = TableValue(self.table, [key], self.version, {})
            tables.append(ttable)
            for val in vals:
                if isinstance(val, TableValue):
                    tables += val.get_data_tables()
                elif isinstance(val, TableReference):
                    tables += val.get_data_tables()
        return tables

    def parse(self, cache: Cache, index: Optional[int] = None):
        return _read_table_reference(self, index, cache)

    @classmethod
    def from_string(cls, args: str) -> "TableValue":
        s = args.strip()
        main_pattern = (
            r"^([-A-Za-z0-9_]+)"  # group 1: object name
            r"(\([-A-Za-z0-9_]*\))?"  # group 2: optional paren-args
            r"(?:\.([-A-Za-z0-9_\{\},\s]+))?"  # group 3: dot + fields
            r"(\[(.*)\])?$"  # group 4: optional […]
        )
        m = re.match(main_pattern, s)
        if not m:
            raise TVBuilderError(f"Invalid TableValue string: {s}")
        main_table = m.group(1)
        main_version = m.group(2)
        main_col = m.group(3)
        if main_version is None:
            main_version = constants.BASE_TABLE_VERSION
        if main_col is not None:
            main_col = main_col.strip()
            if main_col.startswith("{") and main_col.endswith("}"):
                main_col = _split_top_level_list(main_col[1:-1])
            else:
                main_col = [main_col]
        inner_content = m.group(5)
        if not inner_content:
            return cls(
                table=main_table, columns=main_col, version=main_version, conditions={}
            )
        pairs = _split_top_level_list(inner_content)
        key_dict = {}
        for pair in pairs:
            pair = pair.strip()
            kv_split = pair.split("::", 1)
            if len(kv_split) == 1:
                key_dict[kv_split[0]] = []
            else:
                key_col = kv_split[0].strip()
                val_str = kv_split[1].strip()
                vals = _split_top_level_list(val_str, splitter_char=":")
                if len(vals) > 2:
                    raise TVBuilderError(
                        f"too many ':' splits in Table Reference: {vals}"
                    )
                for i in range(len(vals)):
                    if vals[i].startswith("'") and vals[i].endswith("'"):
                        val = vals[i]
                    elif vals[i].startswith('"') and vals[i].endswith('"'):
                        val = vals[i][1:-1]
                        val = TableReference.from_string(val)
                    else:
                        val = cls.from_string(vals[i])
                    vals[i] = val
                key_dict[key_col] = vals
        return cls(
            table=main_table,
            columns=main_col,
            version=main_version,
            conditions=key_dict,
        )


@dataclass
class TableReference:
    """
    A string representation of data table entries.

    Examples:

    - <<llm_storage.openai_id[paper_name:self.paper_name]>>

    - <<stories.paper_path[paper_name:self.paper_name]>>

    - "Test data: <<stories.paper_path[paper_name:self.paper_name]>> "

    Note: Always surround reference with '<<' and '>>'.
    """

    text: str
    references: list[TableValue]

    @classmethod
    def from_string(cls, args: str) -> "TableReference":
        args = args.strip()
        pattern = r"<<(.*?)>>"
        extracted_values = re.findall(pattern, args)
        modified_string = re.sub(pattern, "<<>>", args)
        values = []
        for val in extracted_values:
            values.append(TableValue.from_string(val))
        return cls(modified_string, values)

    def parse(
        self, cache: Cache, index: Optional[int] = None
    ) -> Union[str, "TableReference"]:
        if len(self.references) == 0:
            return self.text
        try:
            if self.text == "<<>>" and len(self.references) == 1:
                return self.references[0].parse(cache, index)
            tstring = self.text
            for ref in self.references:
                ref_ = ref.parse(cache, index)
                tstring = tstring.replace("<<>>", str(ref_), 1)
            return tstring
        except TableStringError:
            return self

    def get_data_tables(self) -> list[TableValue]:
        tables = []
        for ref in self.references:
            tables += ref.get_data_tables()
        return tables


TableString = TableReference | str


def get_dependencies_str(dependencies: list[TableValue]) -> list[str]:
    dep_strs = []
    for dep in dependencies:
        table_str = dep.table
        if isinstance(dep.columns, list):
            cols = ",".join(dep.columns)
        elif dep.columns:
            cols = dep.columns
        else:
            cols = ""
        if dep.version != "":
            table_str = table_str + "(" + dep.version + ")"
        if cols != "":
            table_str = table_str + "." + cols
        dep_strs.append(table_str)
    return dep_strs


def apply_table_string(arg: Any, cache: Cache, index: Optional[int] = None) -> Any:
    if isinstance(arg, str):
        return arg
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
    elif hasattr(arg, "__dict__"):
        for attr, val in vars(arg).items():
            val_ = apply_table_string(val, cache, index)
            setattr(arg, attr, val_)
        return arg
    else:
        return arg


def parse_table_string(annotation, arg):
    if isinstance(arg, str):
        pattern = r"<<(.*?)>>"
        extracted_values = re.findall(pattern, arg)
        if len(extracted_values) != 0:
            return TableReference.from_string(arg)
        else:
            return arg
    elif isinstance(arg, list):
        if annotation is not None and get_origin(annotation) is list:
            (annotation,) = get_args(annotation)
        else:
            annotation = None
        return [parse_table_string(annotation, item) for item in arg]
    elif isinstance(arg, set):
        if annotation is not None and get_origin(annotation) is set:
            (annotation,) = get_args(annotation)
        else:
            annotation = None
        return set([parse_table_string(annotation, item) for item in arg])
    elif isinstance(arg, dict):
        if annotation is not None and get_origin(annotation) is dict:
            key_type, val_type = get_args(annotation)
        else:
            key_type = None
            val_type = None
        return {
            parse_table_string(key_type, k): parse_table_string(val_type, v)
            for k, v in arg.items()
        }
    elif hasattr(arg, "__dict__"):
        if hasattr(annotation, "__annotations__"):
            annotations = annotation.__annotations__
        else:
            annotations = {}
        for attr, val in vars(arg).items():
            attr_annotation = annotations.get(attr, None)
            setattr(arg, attr, parse_table_string(attr_annotation, val))
    return arg


def _read_table_reference(ref: TableValue, index: Optional[int], cache: Cache) -> Any:
    if (
        ref.table == constants.TABLE_SELF
        and ref.columns == [constants.TABLE_INDEX]
        and len(ref.conditions) == 0
    ):
        if index is None:
            raise TableStringError()
        return index

    if ref.table != constants.TABLE_SELF:
        df = cache[(ref.table, ref.version)]
    else:
        df = cache[ref.table]
    if len(ref.conditions) == 0:
        if ref.columns != "":
            if ref.columns is None:
                return df
            if isinstance(ref.columns, str):
                cols = [ref.columns]
            else:
                cols = ref.columns
            return df[cols]
        else:
            return df
    else:
        conditions = {}
        range_conditions = {}
        for condition, value in ref.conditions.items():
            if len(value) == 0:
                if index is None:
                    raise TableStringError()
                else:
                    conditions[condition] = index
            elif len(value) == 1:
                value = value[0]
                if isinstance(value, TableValue):
                    value = _read_table_reference(value, index=index, cache=cache)
                elif isinstance(value, TableReference):
                    value = value.parse(cache, index)
                value = format_query_value(value, df[condition].dtype)
                conditions[condition] = value
            elif len(value) == 2:
                start_val, end_val = value
                if isinstance(start_val, TableValue):
                    start_val = _read_table_reference(
                        start_val, index=index, cache=cache
                    )
                elif isinstance(start_val, TableReference):
                    start_val = start_val.parse(cache, index)
                if isinstance(end_val, TableValue):
                    end_val = _read_table_reference(end_val, index=index, cache=cache)
                elif isinstance(end_val, TableReference):
                    end_val = end_val.parse(cache, index)
                start_val = format_query_value(start_val, df[condition].dtype)
                end_val = format_query_value(end_val, df[condition].dtype)
                range_conditions[condition] = (start_val, end_val)
            else:
                raise TVBuilderError(f"Can't read {condition}: {value}")
        query_str = " & ".join([f"{k} == {v}" for k, v in conditions.items()])
        query_str_range = " & ".join(
            [f"{k} >= {v1} & {k} < {v2}" for k, (v1, v2) in range_conditions.items()]
        )
        if query_str_range != "":
            if query_str != "":
                query_str = query_str + " & " + query_str_range
            else:
                query_str = query_str_range
        rows = df.query(query_str)
        if len(ref.columns) != 0:
            cols = ref.columns
            rows = rows[cols]
        return _simplify_df(rows)


def format_query_value(val, dtype):
    if is_string_dtype(dtype):
        if not (
            (val.startswith("'") and val.endswith("'"))
            or (val.startswith('"') and val.endswith('"'))
        ):
            return f"'{val}'"
        else:
            return val
    else:
        if isinstance(val, str):
            return val.strip("'\"")
        return val


def _simplify_df(df: pd.DataFrame):
    if df.empty:
        return None
    elif df.size == 1:
        return df.iloc[0, 0]
    elif df.shape[1] == 1:
        return df.iloc[:, 0].tolist()
    else:
        return df.to_dict(orient="list")


def _split_top_level_list(s: str, splitter_char=",") -> list[str]:
    pairs = []
    bracket_depth = 0
    in_quote = None  # Holds the current quote character if inside a quoted string
    escape = False  # Indicates if the current character is escaped
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
            pairs.append("".join(current).strip())
            current = []
        else:
            current.append(char)

    if current:
        pairs.append("".join(current).strip())
    if bracket_depth != 0:
        raise TVBuilderError("Unbalanced brackets in string: " + s)
    return pairs
