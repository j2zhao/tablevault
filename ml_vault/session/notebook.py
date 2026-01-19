from IPython import get_ipython
import time
from ml_vault.database import session_collection

class SessionNotebook:
    def __init__(self, db, name, user_id):
        self.ip = get_ipython()
        if self.ip is None:
            raise RuntimeError("Not running inside IPython / Jupyter")
        self.name = name
        self.db = db
        self._installed = True
        session_collection.create_session(db, name, user_id)
        self.ip.events.register("pre_run_cell", self.pre_run_cell)
        self.ip.events.register("post_run_cell", self.post_run_cell)
        self.current_index = None

    def pre_run_cell(self, info):
        session_collection.session_add_code_start(self.db, self.name, self.user_id, info.raw_cell)
        print(f"\n---[ Cell Recorded ]---")

    def post_run_cell(self, result):
        err = result.error_before_exec or result.error_in_exec
        if err is None:
            err_msg = ""
        else:
            err_msg = str(err)
        session_collection.session_add_code_end(db, self.name, )
        print(f"---[ Cell Recorded ]---\n")

    
    # def install(self):
    #     if self._installed:
    #         return
    #     self.ip.events.register("pre_run_cell", self.pre_run_cell)
    #     self.ip.events.register("post_run_cell", self.post_run_cell)
    #     self._installed = True
    #     print("Session Started")

    # def uninstall(self):
    #     if not self._installed:
    #         return
    #     self.ip.events.unregister("pre_run_cell", self.pre_run_cell)
    #     self.ip.events.unregister("post_run_cell", self.post_run_cell)
    #     self._installed = False
    #     print("Session Ended")
