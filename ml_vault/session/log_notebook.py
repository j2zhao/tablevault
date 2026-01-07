from IPython import get_ipython
import time

class CellAnnotator:
    def __init__(self):
        self.ip = get_ipython()
        if self.ip is None:
            raise RuntimeError("Not running inside IPython / Jupyter")
        self.cell_index = 0
        self.start_time = None
        self._installed = False

    def pre_run_cell(self, info):
        self.cell_index += 1
        self.start_time = time.time()
        print(f"\n---[ Cell {self.cell_index} START ]---")

    def post_run_cell(self, result):
        if self.start_time is None:
            return
        elapsed = time.time() - self.start_time
        print(f"---[ Cell {self.cell_index} END | {elapsed:.3f}s ]---\n")

    def install(self):
        if self._installed:
            return
        self.ip.events.register("pre_run_cell", self.pre_run_cell)
        self.ip.events.register("post_run_cell", self.post_run_cell)
        self._installed = True
        print("Cell annotations installed ✅")

    def uninstall(self):
        if not self._installed:
            return
        self.ip.events.unregister("pre_run_cell", self.pre_run_cell)
        self.ip.events.unregister("post_run_cell", self.post_run_cell)
        self._installed = False
        print("Cell annotations uninstalled ❌")
