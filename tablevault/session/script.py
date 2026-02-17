import atexit
import inspect
import sys
import traceback
from dataclasses import dataclass
from typing import Optional, Type
from tablevault.database import session_collection


@dataclass
class Uncaught:
    exc_type: Type[BaseException]
    exc: BaseException
    tb: object  # traceback object


def try_get_main_source() -> str:
    main_mod = sys.modules.get("__main__")
    if main_mod is None:
        return ""
    try:
        return inspect.getsource(main_mod)
    except (OSError, TypeError):
        return ""


class SessionScript:
    def __init__(self, db, name: str, user_id: str, parent_session_name: str, parent_session_index: int, code_text: Optional[str] = None):
        self.name = name
        self.db = db
        self.user_id = user_id

        self.current_index = None
        self._uncaught: Optional[Uncaught] = None
        self._prev_excepthook = sys.excepthook

        session_collection.create_session(db, name, user_id, "script", parent_session_name, parent_session_index)
        sys.excepthook = self._excepthook
        atexit.register(self._atexit_finalize)
        self.pre_run_script(
            code_text=code_text,
        )

    def pre_run_script(
        self,
        code_text: Optional[str] = None,
        fallback_stub: Optional[str] = None,
    ):
        if code_text is not None:
            final_code = code_text
        else:
            final_code = try_get_main_source()
            if not final_code:
                final_code = fallback_stub or ""

        self.current_index = session_collection.session_add_code_start(
            self.db,
            self.name,
            final_code,
            "",
            0,
        )
        print("\n---[ TableVault Record ]---")

    def _excepthook(self, exc_type, exc, tb):
        self._uncaught = Uncaught(exc_type, exc, tb)
        self._prev_excepthook(exc_type, exc, tb)

    def _atexit_finalize(self):
        if self.current_index is None:
            return

        if self._uncaught is None:
            err_msg = ""
        else:
            err_msg = "".join(
                traceback.format_exception(
                    self._uncaught.exc_type, self._uncaught.exc, self._uncaught.tb
                )
            )
        session_collection.session_add_code_end(
            self.db,
            self.name,
            self.current_index,
            error=err_msg,
        )
        print("---[ TableVault Record ]---\n")
