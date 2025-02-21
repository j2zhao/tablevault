from tablevault import _tablevault
from tablevault._utils.metadata_store import ActiveProcessDict
from tablevault._utils.utils import gen_process_id

class TableVault:

    def __init__(
        self,
        db_dir: str,
        author: str,
        create: bool = False,
        restart: bool = False,
        allow_multiple_tables: list[str] = [],
        yaml_dir: str = '',
        code_dir:str = '',
        execute: bool = False
    ) -> None:
        """Creates a TableVault object that interfaces with a TableVault directory.

        This function can create the directory, copy information from different folders
        and execute tables.

        Args:
            db_dir (str): The directory where the database files are stored.
            author (str): The name of the user or system initiating the operation.
            create (bool, optional): If True, a new database will be created at db_dir. Defaults to False.
            restart (bool, optional): If True, the current active processes will be restarted. Defaults to False.
            allow_multiple_tables (list[str], optional): A list of tables that are allowed to have 
                                                        multiple valid versions. Defaults to an empty list.
            yaml_dir (str, optional): The directory containing YAML configuration files for different tables. 
                                        Defaults to an empty string.
            code_dir (str, optional): The directory containing code functions to execute. Defaults to an empty string.
            execute (bool, optional): If True, materializes the tables (TODO: fix order doesn't work right now). Defaults to False.

        Returns:
            None
    """
        self.db_dir = db_dir
        self.author = author
        if create:
            _tablevault.setup_database(db_dir=db_dir, replace=True)
            if yaml_dir != '' or code_dir != '':
                _tablevault.copy_database_files(author=self.author,
                                                yaml_dir=yaml_dir,
                                                code_dir=code_dir,
                                                execute=execute,
                                                allow_multiple_tables=allow_multiple_tables,
                                                db_dir=db_dir
                                                )
        elif restart:
            _tablevault.restart_database(author=self.author, db_dir=self.db_dir)

    def active_processes(self) -> ActiveProcessDict:
        """
        Return a dictionary of currently active processes.
        """
        return _tablevault.active_processes()

    def list_instances(self, table_name: str, version: str = "") -> list[str]:
        """
        Return a list of materialized instance names for a table.

        Args:
            table_name (str): table name
            version (str): specify a version of the table

        """
        return _tablevault.list_instances(table_name=table_name,
                                          db_dir=self.db_dir,
                                          version = version)
    def stop_process(self, process_id):
        """
        Stop a currently active process and release all of its locks.

        Args:
            process_id (str): currently active process

        """
        _tablevault.stop_process(process_id=process_id,
                                 db_dir=self.db_dir)
    def copy_table_files(self, 
                    table_name: str, 
                    prompt_dir:str,
                    process_id:str = '') -> None:
        
        """
        Copy prompt files into a table.

        Args:
             table_name (str): table name to copy into
             prompt_dir (str): directory of files to copy from
             process_id (str): Optional identifier for the process (to re-execute)
        
        """
        
        _tablevault.copy_table_files(author = self.author,
                                     table_name=table_name,
                                     prompt_dir=prompt_dir,
                                     process_id=process_id,
                                     db_dir = self.db_dir)

    def delete_table(self, table_name: str, process_id:str = '') -> None:
        _tablevault.delete_table(author = self.author,
                                 table_name=table_name,
                                 process_id=process_id,
                                 db_dir = self.db_dir)  
        """
        Delete a table.

        Args:
             table_name (str): table name to delete
             process_id (str): Optional identifier for the process (to re-execute)
        
        """
           
    
    
    def delete_instance(self, 
                        instance_id: str, 
                        table_name: str, 
                        process_id:str = '') -> None:
        _tablevault.delete_instance(author = self.author,
                                    table_name=table_name,
                                    instance_id= instance_id,
                                    process_id= process_id,
                                    db_dir = self.db_dir
                                    )
        """
        Delete a materialized table instance.

        Args:
             instance_id (str): instance identifier
             table_name (str): table name
             process_id (str): Optional identifier for the process (to re-execute)
        
        """

    def execute_instance(self, table_name: str, 
                         version: str = "", 
                         force_restart:bool =False,
                         force_execute:bool = False,
                         process_id:str = "") -> None:
        _tablevault.execute_instance(author = self.author,
                                     table_name=table_name,
                                     version=version,
                                     force_restart=force_restart,
                                     force_execute=force_execute,
                                     process_id=process_id,
                                     db_dir = self.db_dir)
        """
        Materialize a table instance from YAML prompts.

        Args:
             version (str): version of table
             table_name (str): table name
             force_restart (bool): If True completely re-execute everything on restart of process. 
                Default False.
             force_execute (bool): If True, completely execute whole table. Otherwise, attempts to copy
                from previous instances of same version.
             process_id (str): Optional identifier for the process (to re-execute)
        
        """

    def setup_temp_instance(
        self,
        table_name: str,
        version: str = "",
        prev_id: str = "",
        copy_previous: bool = False,
        prompts: list[str] = [],
        execute: bool = False,
        process_id:str = ''
    ) -> None:
        """
        Setup temporary instance to execute.

        Note: Only at most one of prev_id, copy_previous, prompts
        should be given.

        Args:
             version (str): version of table
             table_name (str): table name
             prev_id (str): Optionally previous instance ID to copy prompts from
             copy_previous (bool): If true, copy prompts from latest materialized version
             prompts (list[str]): If given, copies list of prompts from table prompts 
             execute (bool): If True, executes instance after creation.
             process_id (str): Optional identifier for the process (to re-execute)

        """
        _tablevault.setup_temp_instance(self.author,
                                        version= version,
                                        table_name=table_name,
                                        prev_id=prev_id,
                                        copy_previous=copy_previous,
                                        prompts=prompts,
                                        execute=execute,
                                        process_id=process_id,
                                        db_dir = self.db_dir)
    def setup_table(self, 
                    table_name: str, 
                    execute:bool = False,
                    create_temp:bool = False,
                    allow_multiple: bool = False,
                    prompt_dir:str = '',
                    process_id:str = '') -> None:
        _tablevault.setup_table(author =self.author,
                                table_name=table_name,
                                prompt_dir=prompt_dir,
                                create_temp=create_temp,
                                execute=execute,
                                allow_multiple=allow_multiple,
                                process_id=process_id,
                                db_dir = self.db_dir)
        """
        Setup new table.

        Args:
             table_name (str): table name
             execute (bool): If True, executes instance after creation. 
             create_temp (bool): If True, copies all prompts from table prompts to create
                temporary instance.
             allow_multiple (bool): If True, table is allowed multiple versions and valid instances
             prompt_dir (str): Optionally, a directory of prompts to copy from.
             process_id (str): Optional identifier for the process (to re-execute)

        """
        
    def gen_process_id(self)-> str:
        """
        Generates new valid process id.

        Returns:
             Process Id (str)
        """
        return gen_process_id()
    

    

    

    
    