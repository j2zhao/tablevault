# TESTS TO RUN

## Single Process

- [x] Create a Vault Object in Jupyter notebook
- [x] Create two Vault Objects in one file
  - [x] One with the same settings (return the previous object)
  - [x] One with different settings
- [x] Create each Artifact object type
- [x] Append data to each Artifact type
- [x] Add description to each Artifact type
- [x] Query each Artifact metadata query
- Query Each Artifact collection
  - [x] With empty collections
  - [x] With one item
  - [x] with description text
  - [x] With description embedding
  - [x] with process text
- [x] Try deletion process (FIX DELETION PROCESS)
- [x] Create a Vault Object in Python file.

## Multiple Process and Data
- Run two concurrent processes
  - Have each process -> create an artifact
    - [ ] Test that concurrent creates don't work
    - [x] Test that sequential creates work
  - Have each process -> append to an object
    - [x] Test that concurrent appends don't work
    - [x] Test that sequential appends work
    - [ ] Test that concurrent append/create don't work
- [x] Test that queries work in both processes independently

## Multiple Processes and Process Control
- [x] Try Stop and Checkpointing
- [x] Try Pause and Checkpointing
- [x] Try Multiple Stops and Checkpointing
- [x] Try Multiple Pauses and Checkpointing
- [x] Try Pause Checkpoint Restart
- [x] Try Stop Checkpoint Restart
- [x] Try Restart (without Pause)
- [x] Try Pause Restart (without Checkpoint)

## Process Failure Restart
- [ ] create_artifact_list -> List doesn't exist
- [ ] append_artifact -> Item not appended
- [ ] add_description_inner -> Do not add item to description
- [ ] delete_artifact_list -> Delete Artifact Anyways
- [ ] process_add_code_end -> Adds code to the end?
- [ ] process_resume_request -> Just commits properly?
