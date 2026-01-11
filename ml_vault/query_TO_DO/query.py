
# for child/parent dependencies -> only consider iteration
# otherwise -> we will implicitly let the cod etake care of it


def search_file_list(file_name,
    timestamp,
    dependencies):
    for dependent in dependencies:


# def search_embeddings(embedding,
#     top_k,
#     n_items,
#     timestamp):
#     pass

# def search_session(code, 
#     descriptions, 
#     execution_type, 
#     n_items,
#     user_ids,
#     read_artifacts,
#     write_artifacts,
#     timestamp):
#     pass