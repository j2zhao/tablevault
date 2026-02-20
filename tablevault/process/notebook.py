from typing import Any, Optional

from arango.database import StandardDatabase
from IPython import get_ipython
from IPython.core.interactiveshell import InteractiveShell

from tablevault.database import process_collection


class ProcessNotebook:
    def __init__(self, db: StandardDatabase, name: str, user_id: str, parent_process_name: str, parent_process_index: int) -> None:
        self.ip: InteractiveShell = get_ipython()
        if self.ip is None:
            raise RuntimeError(
                "ProcessNotebook requires an IPython/Jupyter environment; get_ipython() returned None."
            )
        self.name: str = name
        self.db: StandardDatabase = db
        self._installed: bool = True
        self.user_id: str = user_id
        process_collection.create_process(db, name, user_id, "notebook", parent_process_name, parent_process_index)
        self.ip.events.register("pre_run_cell", self.pre_run_cell)
        self.ip.events.register("post_run_cell", self.post_run_cell)
        self.current_index: Optional[int] = None

    def pre_run_cell(self, info: Any) -> None:
        self.current_index = process_collection.process_add_code_start(
            self.db, self.name, info.raw_cell, "", 0
        )
        print("\n---[ TableVault Record ]---")

    def post_run_cell(self, result: Any) -> None:
        if self.current_index is None:
            return
        err = result.error_before_exec or result.error_in_exec
        if err is None:
            err_msg = ""
        else:
            err_msg = str(err)
        process_collection.process_add_code_end(
            self.db, self.name, self.current_index, error=err_msg
        )
        print("---[ TableVault Record ]---\n")
