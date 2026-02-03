from typing import Any, Optional

from arango.database import StandardDatabase
from IPython import get_ipython
from IPython.core.interactiveshell import InteractiveShell

from ml_vault.database import session_collection


class SessionNotebook:
    def __init__(self, db: StandardDatabase, name: str, user_id: str) -> None:
        self.ip: InteractiveShell = get_ipython()
        if self.ip is None:
            raise RuntimeError("Not running inside IPython / Jupyter")
        self.name: str = name
        self.db: StandardDatabase = db
        self._installed: bool = True
        self.user_id: str = user_id
        session_collection.create_session(db, name, user_id, "notebook")
        self.ip.events.register("pre_run_cell", self.pre_run_cell)
        self.ip.events.register("post_run_cell", self.post_run_cell)
        self.current_index: Optional[int] = None

    def pre_run_cell(self, info: Any) -> None:
        self.current_index = session_collection.session_add_code_start(
            self.db, self.name, info.raw_cell, "", 0
        )
        print("\n---[ ML_Vault Record ]---")

    def post_run_cell(self, result: Any) -> None:
        if self.current_index is None:
            return
        err = result.error_before_exec or result.error_in_exec
        if err is None:
            err_msg = ""
        else:
            err_msg = str(err)
        session_collection.session_add_code_end(
            self.db, self.name, self.current_index, error=err_msg
        )
        print("---[ ML_Vault Record ]---\n")
