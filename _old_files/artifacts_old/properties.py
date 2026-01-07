
from arango import ArangoClient, ArangoError

data_types = [
    "experiment",
    "collection",
    "user",
    "file",
    "array", 
    "document", 
    "document_chunk",
    "record_list",
    "record",
]

prop_types = {
    "string",
    "numeric",
    "bool",
    "string_list"
    "searchable_string_with_embeddings",
    "searchable_string",
    "timestamp",
    "json_record",
    "value_field",
}
search_parameters = {
    "string": ["REGEX","CONTAINS", "EQUALS"],
    "numeric": ["GT","LT", "GTE", "LTE", "EQUALS"],
    "bool": ["EQUALS"],
    "string_list": ["REGEX","CONTAINS", "EQUALS"],
    "searchable_string_with_embeddings": ["REGEX","CONTAINS", "EQUALS", "FUZZZY"], 
    "searchable_string": ["REGEX","CONTAINS", "EQUALS"], 
    "array_with_embeddings": ["FUZZZY"],
    "timestamp": ["LATEST_BEFORE", "ALL_BEFORE"],
    "json_record": ["MATCHES"],
    "value_field": ["GT","LT", "GTE", "LTE", "EQUALS", "REGEX","CONTAINS", "MATCHES"]
}

collection_search_properties = {
    "experiment": {
        "name": "string",
        "timestamp": "timestamp",
        "current_pid": "numeric",
        "current_status": "string",
        "code": "searchable_string",
        "description": ["searchable_string_with_embeddings", "embeddings"],
        "OTHER": "value_field",
    },
    "collection" : {
        "name": "string",
        "timestamp": "timestamp",
        "n_items": "numeric",
        "description": ["searchable_string_with_embeddings", "embeddings"]
    },
    "user" : {
        "name": "string",
        "timestamp": "timestamp",
        "type": "string",
        "description": ["searchable_string_with_embeddings", "embeddings"]
    },
    "file": {
        "name": "string",
        "timestamp": "timestamp",
        "extention": "string",
        "description": [["searchable_string_with_embeddings", "embeddings"]]
    },
    "array": {
        "name": "string",
        "timestamp": "timestamp",
        "ndim": "numeric",
        "dim_[INT]": "numeric", # allow multiple dimensions
        "description": ["searchable_string_with_embeddings", "embeddings"],
    },
    "document": {
        "name": "string",
        "timestamp": "timestamp",
        "n_chunks": "numeric",
        "max_chunk_size": "numeric",
        "description": ["searchable_string_with_embeddings", "embeddings"],
    },
    "document_chunk": {
        "timestamp": "timestamp",
        "text": ["searchable_string_with_embeddings", "text_embeddings"],
        "index": "numeric",
    },
    "record_list": {
        "name": "string",
        "timestamp": "timestamp",
        "dim": "numeric",
        "properties": "string_list",
        "all_properties": "boolean",
        "description": ["searchable_string_with_embeddings", "embeddings"],
    },
    "record": {
        "timestamp": "timestamp",
        "value": "json_record",
        "index": "numeric",
    },
}

collection_edges = {
    "experiment": {
        "read_artifacts": "many_all",
        "write_artifacts": "many_all",
        "user": "user",
        # "previous_ts": "experiment",
        # "next_ts": "experiment",
    },
    "collection": {
        "items": "many_all",
        "read_experiments": "many_experiments",
        "write_experiments": "many_experiments",
        # "previous_ts": "collection",
        # "next_ts": "collection",
    },
    "user": {
        "read_experiments": "many_experiments",
    },
    "file": {
        "read_experiments": "many_experiments",
        "write_experiments": "many_experiments",
        # "previous_ts": "file",
        # "next_ts": "file",
    },
    "array": {
        "read_experiments": "many_experiments",
        "write_experiments": "many_experiments",
        # "previous_ts": "array",
        # "next_ts": "array",
    },
    "document": {
        "read_experiments": "many_experiments",
        "write_experiments": "many_experiments",
        # "previous_ts": "document",
        # "next_ts": "document",
    },
    "document_chunk": {
        "document": "document"
    },
    "record_list": {
        "read_experiments": "many_experiments",
        "write_experiments": "many_experiments",
        # "previous_ts": "record_list",
        # "next_ts": "record_list",
    },
    "record": {
        "record_list": "record_list"
    },
}

timestamp_condition = {
    "committed_before_last",
    "committed_after_last",
    "committed_before",
    "committed_after",
    "committed_together",
}

def parse_code_files(code_artifacts, goal, collection): 
    # we have information about this code outside of the code file itself
    # if we use a LLM here -> is this valid?
    # return a list of code artifacts
    pass
    # we want to sort out our code to see if we can adhere to the pattern


def code_to_artifacts(db, collection_name, code_ids, props, ts, strict):
    pass
    # we can search for ids -> 
    # props = [property_name, condition, value, timestamp_condition]
    # we can consider property_name
    # search over code bases 
    # figure out action -> plan

def artifacts_to_code(db, artifact_ids, inverse):
    pass

def get_artifact(db, artifact_id, variable_name, ):
    # log a read 
    pass

class ArtifactCollection():
    def __init__(self, db, keys, variable_name, n_chunks = 1):
        self.keys = keys
        self.current_index = 0
        self.variable_name = variable_name
        self.n = len(self.keys)

    def __iter__(self):
        self.current_index = 0
        return self
    
    def __next__(self):
        if self.current_index < self.n:
            key = self.keys[self.current_index]
            artifact = get_artifact(key, self.variable_name)
            self.current_index += 1
            return artifact
        else:
            raise StopIteration