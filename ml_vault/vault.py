
from ml_vault.database import create_database, session_collection, artifact_collection, description_collection, query_artifact_simple, query_collection_simple
from ml_vault.session.notebook import SessionNotebook

import os

class Vault():
    def __init__(self,
            user_id,
            session_name, 
            arango_url = "http://localhost:8529",
            arango_db = "ml_vault",
            arango_username = "mlvault_user",
            arango_password = "mlvault_password",
            new_arango_db = True,
            arango_root_username = "root",
            arango_root_password = "123abc",
            file_location = "./ml_vault", 
            description_embedding_size = 1024,
            ):
            
            self.name  = name
            self.user_id = user_id
            
            self.db = create_database.get_arango_db(arango_db, 
                            arango_url, 
                            arango_username,
                            arango_password,
                            arango_root_username,
                            arango_root_password,
                            new_arango_db)
            if new_arango_db:
                create_database.create_ml_vault_db(db, file_location, description_embedding_size)
            
            session_collection.create_session(self.db, self.name, self.user_id)
            self.notebook_session = SessionNotebook(self.db, self.name, self.user_id)
    
    def checkpoint_execution(self):
        session_collection.session_checkpoint(self.db, self.name)

    
    def create_file_list(self, item_name):
        artifact_collection.create_file_list(self.db, item_name, self.name)
    
    def append_file(self, item_name, location, input_artifacts = {}, index = None):
        if index != None: 
            end_position = index + 1
        else:
            end_position = None
        artifact_collection.append_file(db, item_name, location, self.name, index, index, end_position, input_artifacts)
    
    def create_document_list(self, item_name):
        artifact_collection.create_document_list(self.db, item_name, self.name)

    def append_document(self, item_name, text, input_artifacts = {}, index = None, start_position = None):
        if index == None and start_position != None or index != None and start_position == None:
            raise ValueError("Start Position and Index must both be given")
        if start_position != None: 
            end_position = start_position + len(text)
        else:
            end_position = None
        artifact_collection.append_document(db, item_name, text, self.name, index, start_position, end_position, input_artifacts)

    def create_embedding_list(self, item_name, ndim):
        artifact_collection.create_embedding_list(self.db, item_name, self.name, n_dim)
    
    def append_embedding(self, item_name, embedding, input_artifacts = {}, index = None):
        if index != None: 
            end_position = index + 1
        else:
            end_position = None
        artifact_collection.append_document(db, item_name, embedding, self.name, index, index, end_position, input_artifacts)


    def create_record_list(self, item_name, column_names):
        artifact_collection.create_record_list(self.db, item_name, self.name, column_names)

    def append_record(item_name, record, input_artifacts = {}, index = None):
        if index != None: 
            end_position = index + 1
        else:
            end_position = None
        artifact_collection.append_document(db, item_name, record, self.name, index, index, end_position, input_artifacts)

    def create_description(self, item_name, description, embedding):
        description_collection.add_description(self.db, item_name, self.name, description, embedding)

    def pause_execution(self, session_name) -> bool:
        session_collection.session_stop_pause_request(self.db, session_name, "pause", self.name, os.getpid())

    def stop_execution(self, session_name) -> bool:
        session_collection.session_stop_pause_request(self.db, session_name, "stop", self.name, os.getpid())

    def resume_execution(self, session_name):
        session_collection.session_resume_request(self.db,session_name, self.name, os.getpid())

    def query_session_collection(self, code_text = None, desciption_embedding = None, desciption_text = None, filtered = []):
        return query_collection_simple.query_session(
                self.db,
                record_text,
                desciption_embedding,
                desciption_text,
                filtered = filtered,  # list of file.name strings
            )

    def query_embedding_collection(self, embedding, desciption_embedding = None, desciption_text = None, code_text = None, filtered = [], use_idx=False):
        if not use_idx:
            return query_collection_simple.query_embedding_no_idx(
                    self.db,
                    embedding,
                    desciption_embedding,
                    desciption_text,
                    code_text,
                    filtered = filtered,
                )
        else:
            return query_collection_simple.query_embedding(
                    self.db,
                    embedding,
                    desciption_embedding,
                    desciption_text,
                    code_text,
                    filtered = filtered,
                )


    def query_record_collection(self, record_text, desciption_embedding = None, desciption_text = None, code_text = None, filtered = []):
        return query_collection_simple.query_record(
                self.db,
                record_text,
                desciption_embedding,
                desciption_text,
                code_text,
                filtered = filtered,
            )

    def query_document_collection(self, document_text, desciption_embedding = None, desciption_text = None, code_text = None, filtered = []):
        return query_collection_simple.query_document(
                self.db,
                document_text,
                desciption_embedding,
                desciption_text,
                code_text,
                filtered = filtered,
            )

    def query_file_collection(self, desciption_embedding = None, desciption_text = None, code_text = None, filtered = []):
        return query_collection_simple.query_file(
                self.db,
                desciption_embedding,
                desciption_text,
                code_text,
                filtered = filtered,
            )

    def query_item_input(self, item_name, start_position = None, end_position = None):
        return query_artifact_simple.query_artifact_input(self.db, item_name, start_position, end_position)

    def query_item_children(self, item_name, start_position = None, end_position = None):
        return query_artifact_simple.query_artifact_output(self.db, item_name, start_position, end_position)

    def query_item_content(self, item_name, start_position = None, end_position = None):
        return query_artifact_simple.query_artifact(self.db, start_position, end_position)
    
    def query_item_list(self, item_name):
        return query_artifact_simple.query_artifact_list(db, item_name)

    def query_item_descriptions(self, item_name):
        return query_artifact_simple.query_artifact_description(self.db, item_name)

    def query_item_sessions(self, item_name, start_position = None, end_position = None):
        return query_artifact_simple.query_artifact_session(self.db, item_name, start_position, end_position)

    def query_session_items(self, session_name):
        return query_artifact_simple.query_session_artifact(self.db, session_name)
