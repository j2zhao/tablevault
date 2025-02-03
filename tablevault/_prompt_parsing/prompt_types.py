from typing import Any, Optional, Union
from dataclasses import dataclass
import pandas as pd


@dataclass
class TableReference:
    table: str
    column: str
    instance_id: Optional[str]
    key: dict[str, Union["TableReference", str]]


@dataclass
class TableString:
    text: str
    references: list[TableReference]


Prompt = dict[Any, Any]
PromptArg = Any
Cache = dict[Union[str, tuple[str, str]], pd.DataFrame]
