# Package

t.b.d.

# Setup

- download from github
- pip install .
- Add open_ai key to examples/open_ai_key (to test)

# Example

**LOOK HERE**: See examples/run_basic_script.ipynb and examples/run_basic_object.ipynb for a walkthrough of basic functionalities.

**Advanced ToDos**
- Write up better documentation (TODO)
- rework testing so that it works (pytest)

**Notes**

- DEAL WITH FAILURE
Quality of Life Benefits?

- given a directory of folders of yamls -> generate database
- Given a folder of yamls -> generate table
- Given a folder of yamls -> generate temp table?
- Given a folder of yamls -> transfer to table
- Given one yaml -> transfer to table (!)

- On create database -> allow from folders
- Combine -> Given a folder of yamls/given one yaml -> transfer to table (and generate)
- Generate temp table from all yamls -> (and execute)


- Downsides: since we don't have external locking, this means that the move operation could fail + be unexpected due to malicious actors on the input folders -> What happens if it fails??? It can fuck up all my operations? 
- I should deal with failures then!
- What about rollbacks? (!) -> On failure operations should roll back (!)
- What happens if someone deletes the folder?? UGH I r

**Other Feature to ADD**

QoL: OOO data type extraction or something related to that -> specification of data types (!) for columns