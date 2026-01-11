

class ArtifactIterator():
    def __init__(self, db, artifact_name, start_position, end_position, chunk, chunk_end):
        pass

class ArtifactCollection():
    # indices -> [1, 4, 6] or [start: 1, end: 3, gaps:]
    def __init__(self, db, artifact_name, start_position, end_position):
        pass

    def get_first(self, limit = -1):
        pass

    def get_position(self, start_position, end_position = None):
        pass

    def __iter__(self):
        pass

# we can make something either an artifact list or a list
class ArtifactList():
    def __init__(self, db, query, experiment_names, child_names, parent_names, input_names, output_names, user_names, descriptions):
        self.query = query # given query return an artifact

    def __iter__(self):
        if
