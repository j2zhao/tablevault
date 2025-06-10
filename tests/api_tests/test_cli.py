import subprocess
from pathlib import Path
from . import helper


def basic_function_cli(db_dir: str, author: str) -> None:
    """
    Recreates `basic_function` but drives the *tablevault-cli* instead of
    calling TableVault methods directly.

    Parameters
    ----------
    db_dir : str
        Path to the TableVault directory.
    author : str
        Author name recorded in the CLI calls.

    Notes
    -----
    • Requires `tablevault-cli` in your shell PATH
    • Commands raise `subprocess.CalledProcessError` on failure.
    • Builder-file paths are kept identical to the original example.
    """

    def run_cli(*args: str) -> None:
        """Invoke tablevault-cli with shared --db-dir / --author."""
        cmd = [
            "tablevault",
            "--db-dir",
            db_dir,
            "--author",
            author,
            *args,
        ]
        subprocess.run(cmd, check=True)

    # ---------- 1. code module & table setup ---------------------------------
    run_cli("create-code-module", "--module-name", "test")
    run_cli("create-table", "stories_TEST")  # no --multiple-artifacts flag → False
    run_cli("rename-table", "stories_TEST", "stories")
    run_cli("create-table", "llm_storage", "--side-effects")
    run_cli("create-table", "llm_questions")

    # ---------- 2. create instances ------------------------------------------
    run_cli("create-instance", "stories")
    run_cli("create-instance", "llm_storage", "--builder", "upload_openai")
    run_cli(
        "create-instance",
        "llm_questions",
        "--builder",
        "question_1",
        "--builder",
        "question_2",
        "--builder",
        "question_3",
    )

    # ---------- 3. builder files ---------------------------------------------
    tests_dir = Path("tests/test_data")
    run_cli(
        "create-builder-file",
        "stories",
        "--copy-dir",
        tests_dir / "test_data_db/stories/stories_index.yaml",
    )
    run_cli(
        "create-builder-file",
        "llm_storage",
        "--copy-dir",
        tests_dir / "test_data_db_selected/llm_storage",
    )
    run_cli(
        "create-builder-file",
        "llm_questions",
        "--copy-dir",
        tests_dir / "test_data_db_selected/llm_questions",
    )

    # ---------- 4. execute instances -----------------------------------------
    run_cli("execute-instance", "stories")
    run_cli("execute-instance", "llm_storage")
    run_cli("execute-instance", "llm_questions")


def test_basic_cli(tablevault):
    basic_function_cli("example_tv", "jinjin")
    helper.evaluate_operation_logging([])
