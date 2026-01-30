# TESTS TO RUN

## Single Session

- [x] Create a Vault Object in Jupyter notebook 
- [x] Create two Vault Objects in one file
  - [x] One with the same settings (return the previous object)
  - [x] One with different settings
- [x] Create each Artifact object type
- [x] Append data to each Artifact type
- [x] Add description to each Artifact type
- [x] Query each Artifact metadata query
- Query Each Artifact collection 
  - with no context
  - with description text
  - With description embedding
  - with session text
- [x] Try deletion session (FIX DELETION SESSION)
- [x] Create a Vault Object in Python file.

## Multiple Session and Data
- Run two concurrent sessions
  - Have each session -> create an artifact 
    - Test that concurrent creates don't work
    - Test that sequential creates work
  - Have each session -> append to an object
    - Test that concurrent appends don't work
    - Test that sequential appends work
    - Test that concurrent append/create don't work
- Test that queries work in both sessions independently
  
## Multiple Sessions and Session Control
- Try Stop and Checkpointing
- Try Pause and Checkpointing
- Try Multiple Stops and Checkpointing
- Try Multiple Pauses and Checkpointing
- Try Pause Checkpoint Restart
- Try Stop Checkpoint Restart
- Try Restart (without Pause)
- Try Pause Restart (without Checkpoint)

## Session Failure
- For each session consider different fail points and check that one restart works. Check that one restart is the same as two restarts.

## Session Failure Checkpoints
- Check that failures during checkpoints are properly supported.