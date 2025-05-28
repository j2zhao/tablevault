import runpy
from helper import clean_up_open_ai

scripts = [
    "test_basic_api.py",
    "test_versioning.py",
    "test_multiprocessing.py",
    "test_external_edit.py",
    "test_exceptions.py",
    "test_restart_and_stops.py",
    "test_stop_and_materialize.py"
]

for path in scripts:
    print(f"▶️ Running {path}")
    runpy.run_path(path, run_name="__main__")
clean_up_open_ai()