from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Union, Optional, Any, get_origin, get_args
from tablevault._defintions import tv_errors, constants
import pandas as pd
from pandas.api.types import is_string_dtype
from tablevault._defintions.types import Cache
import ast


# ───────────────────────────────────────────── helpers ──
def _find_matching(text: str, pos: int, open_sym: str, close_sym: str) -> int:
    """Return index *after* the matching close_sym for the opener at
    *pos*."""
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
        return df


# forward reference
Condition = Union[str, "TableReference"]


# ───────────────────────────────────────────── dataclasses ──
@dataclass
class TableValue:
    table: Condition
    columns: Optional[list[Condition]] = None
    version: Optional[Condition] = None
    conditions: Optional[
        dict[Condition, Union[tuple[Condition], tuple[Condition, Condition], None]]
    ] = None

    def get_data_tables(self) -> Optional[list["TableValue"]]:
        tables: list[TableValue] = []

        # If any of table/columns/version is a TableReference, bail out
        if isinstance(self.table, TableReference):
            return None
        if self.columns is not None:
            for col in self.columns:
                if isinstance(col, TableReference):
                    return None
        if isinstance(self.version, TableReference):
            return None

        # Make a (shallow) copy of columns so we never modify self.columns
        new_columns = list(self.columns) if self.columns is not None else None

        # Start with an empty dict for conditions in the copy
        new_conditions_dict: dict[
            Condition, Union[tuple[Condition], tuple[Condition, Condition], None]
        ] = {}

        ttable = TableValue(self.table, new_columns, self.version, new_conditions_dict)

        if self.conditions is not None:
            for key, vals in self.conditions.items():
                if isinstance(key, str):
                    if ttable.columns is not None:
                        ttable.columns.append(key)
                else:
                    # If the key isn’t a string, drop columns entirely
                    ttable.columns = None

                if vals is not None:
                    for val in vals:
                        if isinstance(val, TableReference):
                            tables_ = val.get_data_tables()
                            if tables_ is None:
                                return None
                            tables += tables_

        tables.append(ttable)
        return tables

    def parse(self, cache: Cache, index: Optional[int] = None):
        return _read_table_reference(self, cache, index)

    # ---------------------------- factory ----------------------------
    @classmethod
    def from_string(cls, arg: str) -> "TableValue":
        """
        Parse one DSL fragment such as
            table(ver).{c1, c2}[c1::0:5]
            table.COL
            table[col]                <-- NEW: bare-key condition (= None)
            table[c1::idx]            <-- NEW: 1-tuple condition
            table[c1::a:b]            <-- 2-tuple condition
        Nesting via << … >> is supported in every token position.
        """
        arg = arg.strip()

        # ─────────────────────────── helpers ────────────────────────────
        def _cond(token: str) -> Condition:
            token = token.strip()
            return (
                TableReference.from_string(token)
                if token.startswith("<<") and token.endswith(">>")
                else token
            )

        # 1) table name --------------------------------------------------
        cursor = 0
        m = re.match(r"[A-Za-z0-9_-]+", arg)
        if not m:
            raise tv_errors.TableReferenceError(f"Illegal table name in '{arg}'")
        table = _cond(m.group(0))
        cursor = m.end()

        # 2) (version) ---------------------------------------------------
        version = None
        if cursor < len(arg) and arg[cursor] == "(":
            start = cursor
            cursor = _find_matching(arg, cursor, "(", ")")
            version = _cond(arg[start + 1 : cursor - 1])

        # 3) .{c1,c2}  or  .COL  ----------------------------------------
        columns = None
        if cursor < len(arg) and arg[cursor] == ".":
            cursor += 1
            if arg[cursor] == "{":  # brace-list
                end = _find_matching(arg, cursor, "{", "}")
                cols_txt = arg[cursor + 1 : end - 1]
                columns = [_cond(t) for t in cols_txt.split(",") if t.strip()]
                cursor = end
            else:  # single column
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

        # 4) [ … ]  (optional conditions) -------------------------------
        conds: Optional[
            dict[
                Condition,
                Union[tuple[Condition], tuple[Condition, Condition], None],
            ]
        ] = None

        if cursor < len(arg) and arg[cursor] == "[":
            start = cursor
            cursor = _find_matching(arg, cursor, "[", "]")
            raw = arg[start + 1 : cursor - 1]
            conds = {}
            for part in filter(None, map(str.strip, raw.split(","))):
                # <key>           -> value = None
                # <key>::idx      -> value = (idx,)
                # <key>::a:b      -> value = (a, b)
                if "::" in part:
                    key_tok, idx_tok = map(str.strip, part.split("::", 1))
                else:
                    key_tok, idx_tok = part, ""  # no value

                key = _cond(key_tok)

                if idx_tok == "":
                    value = None  # bare key
                elif ":" in idx_tok:
                    a, b = map(str.strip, idx_tok.split(":", 1))
                    value = (_cond(a), _cond(b))  # 2-tuple
                else:
                    value = (_cond(idx_tok),)  # 1-tuple

                if key in conds:
                    raise tv_errors.TableReferenceError(
                        f"Duplicate condition key '{key_tok}' in '{arg}'"
                    )
                conds[key] = value

        # 5) any leftover means malformed string ------------------------
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
            tables += table
        return tables

    def parse(
        self, cache: Cache, index: Optional[int] = None, raise_error=False
    ) -> Union[str, "TableReference"]:
        # no embedded references → nothing to do
        if not self.references:
            return self.text
        try:
            # fast-path: the whole thing is exactly one << … >> pair
            if (
                self.text.startswith("<<")
                and self.text.endswith(">>")
                and self.text.count("<<") == 1
                and len(self.references) == 1
            ):
                return self.references[0].parse(cache, index)

            pieces: list[str] = []
            i = 0  # cursor in self.text
            r = 0  # index in self.references

            while i < len(self.text):
                # hit the start of a top-level << … >> block
                if self.text.startswith("<<", i):
                    end = _find_matching(self.text, i, "<<", ">>")  # index AFTER ">>"
                    # substitute the parsed value
                    replacement = self.references[r].parse(cache, index)
                    pieces.append(str(replacement))
                    r += 1
                    i = end  # jump past the block
                else:
                    pieces.append(self.text[i])
                    i += 1

            output = "".join(pieces)
            try:
                output = ast.literal_eval(output)
            except Exception:
                pass
            return output

        except tv_errors.TableReferenceError:
            if raise_error:
                raise tv_errors.TableReferenceError()
            else:
                # propagate “can’t resolve yet” by returning the reference object itself
                return self
        except Exception as e:
            raise tv_errors.TVArgumentError(f"Couldn't parse TableReference: {e}")

    # ---------------------------- factory ----------------------------
    @classmethod
    def from_string(cls, arg: str) -> "TableReference":
        """Parse *any* string that can embed one or more << … >> blocks.

        If the whole argument is exactly one << … >> pair, we still wrap it in a
        TableReference for uniformity.
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
        tr = cls(text=arg, references=refs)
        return tr


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


def _read_table_reference(ref: TableValue, cache: Cache, index: Optional[int]) -> Any:
    if isinstance(ref.table, TableReference):
        table_name = ref.table.parse(cache, index, raise_error=True)
    else:
        table_name = ref.table
    table_columns = []
    if ref.columns is not None:
        for col in ref.columns:
            if isinstance(col, TableReference):
                table_columns.append(col.parse(cache, index, raise_error=True))
            else:
                table_columns.append(col)
    if table_name == constants.TABLE_SELF:
        for col in table_columns:
            if col not in cache[table_name].columns:
                raise tv_errors.TableReferenceError()
    if isinstance(ref.version, TableReference):
        table_version = ref.version.parse(cache, index, raise_error=True)
    else:
        table_version = ref.version
    table_conditions = {}
    if ref.conditions is not None:
        for key, vals in ref.conditions.items():
            if isinstance(key, TableReference):
                key = key.parse(cache, index, raise_error=True)
            vals_ = []
            if vals is not None:
                for val in vals:
                    if isinstance(val, TableReference):
                        vals_.append(val.parse(cache, index, raise_error=True))
                    else:
                        vals_.append(val)
                vals_ = tuple(vals_)
            else:
                vals_ = None
            table_conditions[key] = vals_
    if (
        table_name == constants.TABLE_SELF
        and table_columns == [constants.TABLE_INDEX]
        and len(table_conditions) == 0
    ):
        if index is None:
            raise tv_errors.TableReferenceError()
        return index
    if table_name != constants.TABLE_SELF:
        df = cache[(table_name, table_version)]
    else:
        df = cache[table_name]

    if len(table_conditions) == 0:
        if len(table_columns) != 0:
            return df[table_columns]
        else:
            return df
    conditions = {}
    range_conditions = {}
    for key, value in table_conditions.items():
        if value is None:
            if index is None:
                raise tv_errors.TableReferenceError()
            else:
                conditions[key] = index
        elif len(value) == 1:
            value = _format_query_value(value[0], df[key].dtype)
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
    if len(table_columns) != 0:
        rows = rows[table_columns]
    return _simplify_df(rows)


if __name__ == "__main__":
    tv = TableReference.from_string("<<openai_store.paperId[index::0:10]>> ")
