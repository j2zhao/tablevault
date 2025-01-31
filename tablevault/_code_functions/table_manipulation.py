
def generate_categories():
    pass



# import os
# from database_operations import clean_table_artifacts
# import shutil



# def revert_table(time: str, table:str, db_dir:str, other_files: bool = True, replace: bool = False):
#     table_dir = os.path.join(db_dir, table)
#     table_path = os.path.join(table_dir, 'table.csv')
#     # table_dir = os.path.join(db_dir, table)
#     archive = os.path.join(table_dir, 'archive')
#     archive_dir = os.path.join(archive, time)
#     if not os.path.isdir(archive_dir):
#         raise FileExistsError('There is not a previous save at {time}')
    
#     archive_path = os.path.join(archive, 'table.csv')
#     shutil.copy2(archive_path, table_path)