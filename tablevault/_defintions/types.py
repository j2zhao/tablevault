from typing import Any, Optional
import pandas as pd
from dataclasses import dataclass
from dataclasses_json import dataclass_json
import json


Cache = dict[str | tuple[str, str], pd.DataFrame]

InternalDeps = dict[str, list[str]]
ExternalDeps = dict[str, list[tuple[str, str, str, float, bool]]]
BuilderDeps = dict[str, list[str]]
CodeDeps = dict[str, Optional[str]]


@dataclass_json
@dataclass
class ProcessLog:
    process_id: str
    author: str
    start_time: float
    log_time: float
    operation: str
    complete_steps: list[str]
    step_times: list[float]
    data: dict[str, Any]
    execution_success: Optional[bool]
    start_success: Optional[bool]
    error: Optional[tuple[str, str]]
    pid: Optional[int]
    force_takedown: bool

    def __str__(self) -> str:
        obj_dict = self.to_dict()
        obj_dict.pop("data", None)
        return json.dumps(obj_dict, indent=4)


ColumnHistoryDict = dict[str, dict[str, dict[str, float]]]
TableHistoryDict = dict[str, dict[str, tuple[float, float, Optional[float], bool]]]
TableTempDict = dict[str, list[str]]

ActiveProcessDict = dict[str, ProcessLog]

SETUP_OUTPUT = dict[str, Any]
