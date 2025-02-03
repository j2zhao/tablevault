from tablevault import _table_operations
from tablevault._metadata_store import MetadataStore, ActiveProcessDict


class TableVault:

    def __init__(
        self,
        db_dir: str,
        author: str,
        create: bool = False,
        restart: bool = False,
        excluded: list[str] = [],
    ) -> None:
        self.db_dir = db_dir
        self.author = author
        if create:
            _table_operations.setup_database(db_dir, replace=True)
        elif restart:
            _table_operations.restart_database(
                author, db_dir, excluded_processes=excluded
            )

    def active_processes(self, all_info: bool = False) -> ActiveProcessDict:
        db_metadata = MetadataStore(self.db_dir)
        return db_metadata.get_active_processes(all_info, to_print=False)

    def list_instances(self, table_name: str, version: str = "") -> list[str]:
        db_metadata = MetadataStore(self.db_dir)
        return db_metadata.get_table_instances(table_name, version, to_print=False)

    def setup_table(self, table_name: str, allow_multiple: bool = False) -> None:
        _table_operations.setup_table(
            table_name, self.db_dir, self.author, allow_multiple
        )

    def setup_temp(
        self,
        table_name: str,
        version: str = "",
        prev_id: str = "",
        prompts: list[str] = [],
        gen_prompt: str = "",
    ) -> None:
        _table_operations.setup_table_instance(
            version,
            table_name,
            self.db_dir,
            self.author,
            prev_id,
            list(prompts),
            gen_prompt,
        )

    def delete_table(self, table_name: str) -> None:
        _table_operations.delete_table(table_name, self.db_dir, self.author)

    def delete_instance(self, instance_id: str, table_name: str) -> None:
        _table_operations.delete_table_instance(
            instance_id, table_name, self.db_dir, self.author
        )

    def execute(self, table_name: str, version: str = "", force=False) -> None:
        _table_operations.execute_table(
            table_name, self.db_dir, self.author, version, force
        )
