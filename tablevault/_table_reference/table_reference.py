from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Union, Optional, Any, get_origin, get_args
from tablevault._defintions import tv_errors, constants
import pandas as pd
from pandas.api.types import is_string_dtype
from tablevault._defintions.types import Cache


# ───────────────────────────────────────────── helpers ──
def _find_matching(text: str, pos: int, open_sym: str, close_sym: str) -> int:
    """Return index *after* the matching close_sym for the opener at *pos*."""
    depth, i = 0, pos
    while i < len(text):
        if text.startswith(open_sym, i):
            depth += 1
            i += len(open_sym)
            continue
        if text.startswith(close_sym, i):
            depth -= 1
            i += len(close_sym)
            if depth == 0:
                return i
            continue
        i += 1
    raise tv_errors.TableReferenceError("Unbalanced symbols while scanning.")


def _format_query_value(val, dtype):
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


# forward reference
Condition = Union[str, "TableReference"]


# ───────────────────────────────────────────── dataclasses ──
@dataclass
class TableValue:
    table: Condition
    columns: Optional[list[Condition]] = None
    version: Optional[Condition] = None
    conditions: Optional[
        dict[Condition, list[Union[Condition, tuple[Condition, Condition]]]]
    ] = None

    def get_data_tables(self) -> Optional[list["TableValue"]]:
        tables = []
        if isinstance(self.table, TableReference):
            return None
        if self.columns is not None:
            for col in self.columns:
                if isinstance(col, TableReference):
                    return None
        if isinstance(self.version, TableReference):
            return None
        ttable = TableValue(self.table, self.columns, self.version, {})

        for key, vals in self.conditions.items():
            if isinstance(key, str):
                if ttable.columns is not None:
                    ttable.columns.append(key)
            else:
                ttable.columns = None
            for val in vals:
                if isinstance(val, TableReference):
                    tables_ = val.get_data_tables()
                    if tables_ is None:
                        return None
                    tables.append(tables_)
        tables.append(ttable)
        return tables

    def parse(self, cache: Cache, index: Optional[int] = None):
        return _read_table_reference(self, index, cache)

    # ---------------------------- factory ----------------------------

    @classmethod
    def from_string(cls, arg: str) -> "TableValue":
        """
        Parse one DSL fragment such as:
            table(ver).{c1, c2}[c1::0:5]
            table.COL
            table
        Nesting via << … >> is supported in every token position.
        """
        arg = arg.strip()

        # Decide between raw identifier and nested <<…>> reference
        def _cond(token: str) -> Condition:
            token = token.strip()
            if token.startswith("<<") and token.endswith(">>"):
                return TableReference.from_string(token)
            return token

        # 1) table name ------------------------------------------------
        cursor = 0
        m = re.match(r"[A-Za-z0-9_-]+", arg)
        if not m:
            raise tv_errors.TableReferenceError(f"Illegal table name in '{arg}'")
        table = _cond(m.group(0))
        cursor = m.end()

        # 2) (version) -------------------------------------------------
        version = None
        if cursor < len(arg) and arg[cursor] == "(":
            start = cursor
            cursor = _find_matching(arg, cursor, "(", ")")
            version = _cond(arg[start + 1 : cursor - 1])

        # 3) .{c1,c2}  or  .COL  --------------------------------------
        columns = None
        if cursor < len(arg) and arg[cursor] == ".":
            cursor += 1
            if arg[cursor] == "{":  # brace-list
                end = _find_matching(arg, cursor, "{", "}")
                cols_txt = arg[cursor + 1 : end - 1]
                columns = [_cond(t) for t in cols_txt.split(",") if t.strip()]
                cursor = end
            else:  # single-column
                if arg.startswith("<<", cursor):
                    end = _find_matching(arg, cursor, "<<", ">>")
                    token = arg[cursor:end]
                    cursor = end
                else:
                    m = re.match(r"[A-Za-z0-9_-]+", arg[cursor:])
                    if not m:
                        raise tv_errors.TableReferenceError(
                            f"Illegal column identifier after '.' in '{arg}'"
                        )
                    token = m.group(0)
                    cursor += len(token)
                columns = [_cond(token)]

        # 4) [col::idx] / [col::a:b] ----------------------------------
        conds = None
        if cursor < len(arg) and arg[cursor] == "[":
            start = cursor
            cursor = _find_matching(arg, cursor, "[", "]")
            raw = arg[start + 1 : cursor - 1]
            conds = {}
            for part in filter(None, map(str.strip, raw.split(","))):
                if "::" not in part:
                    raise tv_errors.TableReferenceError(
                        f"Missing '::' in condition '{part}'"
                    )
                key_tok, idx_tok = map(str.strip, part.split("::", 1))
                key = _cond(key_tok)
                # idx  vs  start:end
                if ":" in idx_tok:
                    a, b = map(str.strip, idx_tok.split(":", 1))
                    val: Union[Condition, tuple[Condition, Condition]] = (
                        _cond(a),
                        _cond(b),
                    )
                else:
                    val = _cond(idx_tok)
                conds.setdefault(key, []).append(val)

        # 5) any leftover means malformed string ----------------------
        if arg[cursor:].strip():
            raise tv_errors.TableReferenceError(
                f"Unparsed tail in '{arg}': '{arg[cursor:]}'"
            )

        return cls(table=table, version=version, columns=columns, conditions=conds)


@dataclass
class TableReference:
    text: str  # original string (possibly with free text)
    references: list[TableValue]  # every << … >> inside *text*

    def get_data_tables(self) -> list[TableValue]:  # TODO FIX
        tables = []
        for ref in self.references:
            table = ref.get_data_tables()
            if table is None:
                return None
            tables += tables
        return tables

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
        except tv_errors.TableReferenceError:
            return self

    # ---------------------------- factory ----------------------------
    @classmethod
    def from_string(cls, arg: str) -> "TableReference":
        """
        Parse *any* string that can embed one or more << … >> blocks.
        If the whole argument is exactly one << … >> pair, we still
        wrap it in a TableReference for uniformity.
        """
        # special-case fully wrapped form
        if arg.startswith("<<") and arg.endswith(">>") and arg.count("<<") == 1:
            inner = arg[2:-2].strip()
            return cls(text=arg, references=[TableValue.from_string(inner)])

        # general scan for top-level << … >> pairs
        refs: list[TableValue] = []
        i = 0
        while i < len(arg):
            if arg.startswith("<<", i):
                start = i + 2
                end = _find_matching(arg, i, "<<", ">>")
                token = arg[start : end - 2].strip()
                refs.append(TableValue.from_string(token))
                i = end
            else:
                i += 1
        return cls(text=arg, references=refs)


# ---------------------------- functions ----------------------------
def get_table_result(arg: Any, cache: Cache, index: Optional[int] = None) -> Any:
    if isinstance(arg, str):
        return arg
    elif isinstance(arg, TableReference):
        return arg.parse(cache, index)
    elif isinstance(arg, list):
        return [get_table_result(item, cache, index) for item in arg]
    elif isinstance(arg, set):
        return set([get_table_result(item, cache, index) for item in arg])
    elif isinstance(arg, dict):
        return {
            get_table_result(k, cache, index): get_table_result(v, cache, index)
            for k, v in arg.items()
        }
    elif hasattr(arg, "__dict__"):
        for attr, val in vars(arg).items():
            val_ = get_table_result(val, cache, index)
            setattr(arg, attr, val_)
        return arg
    else:
        return arg


def table_reference_from_string(annotation, arg):
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
        return [table_reference_from_string(annotation, item) for item in arg]
    elif isinstance(arg, set):
        if annotation is not None and get_origin(annotation) is set:
            (annotation,) = get_args(annotation)
        else:
            annotation = None
        return set([table_reference_from_string(annotation, item) for item in arg])
    elif isinstance(arg, dict):
        if annotation is not None and get_origin(annotation) is dict:
            key_type, val_type = get_args(annotation)
        else:
            key_type = None
            val_type = None
        return {
            table_reference_from_string(key_type, k): table_reference_from_string(
                val_type, v
            )
            for k, v in arg.items()
        }
    elif hasattr(arg, "__dict__"):
        if hasattr(annotation, "__annotations__"):
            annotations = annotation.__annotations__
        else:
            annotations = {}
        for attr, val in vars(arg).items():
            attr_annotation = annotations.get(attr, None)
            setattr(arg, attr, table_reference_from_string(attr_annotation, val))
    return arg


def _read_table_reference(ref: TableValue, index: Optional[int], cache: Cache) -> Any:
    # recursive parsing
    if isinstance(ref.table, TableReference):
        ref.table = _read_table_reference(ref.table, index, cache)
    if ref.columns is not None:
        columns = []
        for col in ref.columns:
            if isinstance(col, TableReference):
                columns.append(_read_table_reference(col, index, cache))
            else:
                columns.append(col)
        ref.columns = columns
    if isinstance(ref.version, TableReference):
        ref.version = _read_table_reference(ref.version, index, cache)

    if ref.conditions is not None:
        conditions = {}
        for key, vals in ref.conditions.items():
            if isinstance(key, TableReference):
                key = _read_table_reference(key, index, cache)
            vals_ = []
            for val in vals:
                if isinstance(val, tuple):
                    if isinstance(val[0], TableReference):
                        val_0 = _read_table_reference(val[0], index, cache)
                    else:
                        val_0 = val[0]
                    if isinstance(val[1], TableReference):
                        val_1 = _read_table_reference(val[1], index, cache)
                    else:
                        val_1 = val[1]
                    vals_.append((val_0, val_1))
                else:
                    if isinstance(val, TableReference):
                        vals_.append(_read_table_reference(val, index, cache))
                    else:
                        vals_.append(val)
            conditions[key] = vals_
        ref.conditions = conditions

    # get query params
    if (
        ref.table == constants.TABLE_SELF
        and ref.columns == [constants.TABLE_INDEX]
        and len(ref.conditions) == 0
    ):
        if index is None:
            raise tv_errors.TableReferenceError()
        return index
    if ref.table != constants.TABLE_SELF:
        df = cache[(ref.table, ref.version)]
    else:
        df = cache[ref.table]

    if len(ref.conditions) == 0:
        if len(ref.columns) != 0:
            return df[ref.columns]
        else:
            return df
    conditions = {}
    range_conditions = {}
    for key, value in ref.conditions.items():
        if len(value) == 0:
            if index is None:
                raise tv_errors.TableReferenceError()
            else:
                conditions[key] = index
        elif not isinstance(value, tuple):
            value = _format_query_value(value, df[key].dtype)
            conditions[key] = value
        elif len(value) == 2:
            start_val, end_val = value
            start_val = _format_query_value(start_val, df[key].dtype)
            end_val = _format_query_value(end_val, df[key].dtype)
            range_conditions[key] = (start_val, end_val)

    # format query
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
