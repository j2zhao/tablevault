from typing import Any, Optional, Union
from dataclasses import dataclass
import pandas as pd
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from tablevault._defintions import step_constants
import json

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


InternalDeps = dict[str, list[str]]
ExternalDeps = dict[str, list[tuple[str, str, str, float, bool]]]
PromptDeps = dict[str, list[str]]

@dataclass_json
@dataclass
class ProcessLog:
    process_id: str
    author: str
    start_time: float
    log_time: float
    operation: str
    complete_steps: list[str]
    step_times = list[float]
    data: dict[str, Any]
    execution_success: Optional[bool]
    start_success: Optional[bool]
    error: Optional[tuple[str, str]]
    pid: int
    def __str__(self) -> str:
        obj_dict = self.to_dict()
        obj_dict.pop("data", None)
        return json.dumps(obj_dict, indent=4)
    # def get_completed_step(self):
    #     last_index = 0
    #     for i in range(len(self.complete_steps) - 1, -1, -1):
    #         if self.complete_steps[i] == step_constants.EX_RESTART_FORCED:
    #             last_index = i
    #             break
    #     return self.complete_steps[last_index:]


ColumnHistoryDict = dict[str, dict[str, dict[str, dict[str, float]]]]
TableHistoryDict = dict[str, dict[str, tuple[float, float]]]
TableMultipleDict = dict[str, bool]
ActiveProcessDict = dict[str, ProcessLog]

SETUP_OUTPUT =  dict[str, Any]
