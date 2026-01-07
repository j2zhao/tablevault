
from ml_vault.database import create_ml_vault_db, get_arango_db

class Vault():
    def __init__(self, 
            user_id,
            session_id, 
            arango_url = "http://localhost:8529",
            arango_db = "ml_vault",
            arango_username = "mlvault_user",
            arango_password = "mlvault_password",
            new_arango_db = True,
            arango_root_username = "root",
            arango_root_password = "123abc",
            file_location = "./ml_vault", 
            description_embedding_size = 1024,
            openai_key = None,
            ):
            db = get_arango_db(arango_db, 
                            arango_url, 
                            arango_username,
                            arango_root_username,
                            arango_root_password,
                            new_arango_db)

            create_ml_vault_db(db, file_location, description_embedding_size, openai_key)

    def create_user(self, user_id):
        pass

    def checkpoint_execution(self):
        pass

    def create_record(self, item_id, description = False):
        pass
    
    def create_file_list(self, item_id, description = False):
        pass

    def create_embeddings(self, item_id, description = False):
        pass

    def create_document(self, item_id, description = False):
        pass

    def create_record_list(self, item_id, description = False):
        pass

    def add_item(self, item, name, type, parent_names = {}):
        pass

    def pause_execution(self, session_name) -> bool:
        pass

    def stop_execution(self, session_name) -> bool:
        pass

    def resume_execution(self, session_name):
        pass

    def search_experiments(self, query, input_names, output_names, user_names):
        pass

    def search_file_list(self, query, experiment_names, child_names, parent_names):
        pass

    def search_embeddings(self, query, experiment_names, child_names, parent_names):
        pass

    def search_document(self, query, experiment_names, child_names, parent_names):
        pass

    def search_record_list(self, query, experiment_names, child_names, parent_names):
        pass

