# Package

t.b.d.

# Setup

- download from github
- pip install .
- Add open_ai key to examples/open_ai_key (to test)

# Example

**LOOK HERE**: See examples/run_basic_script.ipynb and examples/run_basic_object.ipynb for a walkthrough of basic functionalities.


# Package Improvements

- Write up better documentation
- Run formatting Passes (i.e. type checking/code formatter)
- rework testing so that it works
- some things are slow -> figure why it is slow? (it was faster in the non-package version) -> suspect it might be due to file locking...
- debugging print statements
- **IMPORTANT**: "import_module" doesn't work (I suspect it is actually a Python error). Need work around since this breaks code executions (hacky solution right now).


**Note:** Current testing code is from before package conversion -> but all functionality should remain the same, and works

**Advanced ToDos -> too complex for current project**

- Convert tests to pytests? -> Only if testing becomes a pain (!) -> depends on how much 
- Consider CI/CD setup -> mainly so that I can learn how to do CI/CD...

