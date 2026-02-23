from typing import Any, Dict, List, Optional, Tuple

from arango.database import StandardDatabase
from tablevault.types import InputItems
from tablevault.utils.errors import ValidationError, NotFoundError

from tablevault.database import (
    create_database,
    process_collection,
    item_collection,
    description_collection,
    query_item_simple,
    query_collection_simple,
    database_restart,
)
from tablevault.process.notebook import ProcessNotebook
from tablevault.process.script import ProcessScript

import threading


def is_ipython() -> bool:
    """Check if running inside an IPython/Jupyter environment."""
    try:
        from IPython import get_ipython

        return get_ipython() is not None
    except Exception:
        return False


class Vault:
    """
    Vault for tracking ML items and their lineage.

    The Vault provides methods for creating and querying various item types
    (files, documents, embeddings, records) and tracking their relationships
    and provenance through processes.

    Once active in a notebook or Python script, all subsequently executed code is tracked.

    Only one vault can be active in one process. Subsequent calls (with same initialization
    parameters return the same Vault object)
    """
    _instance: Optional["Vault"] = None
    _lock: threading.Lock = threading.Lock()
    _allowed_key: Optional[Tuple[str, str, str, str]] = None

    def __new__(
        cls,
        user_id: str,
        process_name: str,
        parent_process_name: str = "",
        parent_process_index: int = 0,
        arango_url: str = "http://localhost:8529",
        arango_db: str = "tablevault",
        arango_username: str = "tablevault_user",
        arango_password: str = "tablevault_password",
        new_arango_db: bool = True,
        arango_root_username: str = "root",
        arango_root_password: str = "passwd",
        description_embedding_size: int = 1024,
        log_file_location: str = "~/.tablevault/logs/",
    ) -> "Vault":
        key = (user_id, process_name, arango_db, arango_url)
        with cls._lock:
            if cls._instance is None:
                obj = super().__new__(cls)
                cls._instance = obj
                cls._allowed_key = key
                obj._initialized = False
                return obj

            if key != cls._allowed_key:
                raise RuntimeError(
                    f"{cls.__name__} already exists with a different process.\n"
                    f"allowed_key={cls._allowed_key!r}\n"
                    f"new_key={key!r}"
                )
            return cls._instance

    def __init__(
        self,
        user_id: str,
        process_name: str,
        parent_process_name: str = "",
        parent_process_index: int = 0,
        arango_url: str = "http://localhost:8529",
        arango_db: str = "tablevault",
        arango_username: str = "tablevault_user",
        arango_password: str = "tablevault_password",
        new_arango_db: bool = True,
        arango_root_username: str = "root",
        arango_root_password: str = "passwd",
        description_embedding_size: int = 1024,
        log_file_location: str = "~/.tablevault/logs/",
    ) -> None:
        """
        Initialize the Vault singleton.

        Note: Arango database must be active with matching sign-in information.

        Args:
            user_id: Unique identifier for the user.
            process_name: Name for this process.
            parent_process_name: Name of generating process (if exists).
            parent_process_index: Index of generating process (if exists).
            arango_url: URL of the ArangoDB server.
            arango_db: Name of the database to use.
            arango_username: Username for database access.
            arango_password: Password for database access.
            new_arango_db: If True, create a new database (drops existing).
            arango_root_username: Root username for database creation.
            arango_root_password: Root password for database creation.
            description_embedding_size: Dimension of description embeddings.
            log_file_location: Directory for log files.
        """
        self.name: str = process_name
        self.user_id: str = user_id
        if getattr(self, "_initialized", True):
            return
        self._initialized: bool = True
        self.db: StandardDatabase = create_database.get_arango_db(
            arango_db,
            arango_url,
            arango_username,
            arango_password,
            arango_root_username,
            arango_root_password,
            new_arango_db,
        )
        if new_arango_db:
            create_database.create_tablevault_db(
                self.db, log_file_location, description_embedding_size
            )
        if is_ipython():
            self.process = ProcessNotebook(self.db, self.name, self.user_id, parent_process_name, parent_process_index)
        else:
            self.process = ProcessScript(self.db, self.name, self.user_id, parent_process_name, parent_process_index)

    def get_current_operations(self) -> Dict[str, Any]:
        """Get all currently active operations."""
        metadata = self.db.collection("metadata")
        doc = metadata.get("global")
        return doc["active_timestamps"]

    def _ensure_item_exists(self, item_name: str, *, operation: str) -> None:
        items = self.db.collection("items")
        if items.get(item_name) is None:
            raise NotFoundError(
                f"Item '{item_name}' not found in 'items' collection.",
                operation=operation,
                collection="items",
                key=item_name,
            )

    def _ensure_process_exists(self, process_name: str, *, operation: str) -> None:
        processes = self.db.collection("process_list")
        if processes.get(process_name) is None:
            raise NotFoundError(
                f"Process '{process_name}' not found in 'process_list'.",
                operation=operation,
                collection="process_list",
                key=process_name,
            )

    def vault_cleanup(self, interval: int = 60, selected_timestamps: Optional[List[int]] = None) -> None:
        """
        Clean up stale operations that have exceeded the interval.

        Args:
            interval: Time in seconds after which an operation is considered stale.
            selected_timestamps: If provided, only clean up these specific timestamps.
        """
        database_restart.function_restart(
            self.db, interval, self.name, selected_timestamps
        )

    def delete_list(self, item_name: str) -> None:
        """
        Delete an item list's content.

        Args:
            item_name: Name of the item list to delete.
        """
        item_collection.delete_item_list(
            self.db, item_name, self.name, self.process.current_index
        )

    def create_file_list(self, item_name: str) -> None:
        """
        Create a new file list item.

        Args:
            item_name: Unique name for the file list.
        """
        item_collection.create_file_list(
            self.db, item_name, self.name, self.process.current_index
        )

    def append_file(self, item_name: str, location: str, input_items: Optional[InputItems] = None, index: Optional[int] = None) -> None:
        """
        Append a file reference to a file list.

        Args:
            item_name: Name of the file list to append to.
            location: File path or location string.
            input_items: Mapping of dependency item key -> [start_position, end_position] (two integers).
            index: Specific index to insert at (appends to end if None).
        """
        if index is not None:
            end_position = index + 1
        else:
            end_position = None
        item_collection.append_file(
            self.db,
            item_name,
            location,
            self.name,
            self.process.current_index,
            index,
            index,
            end_position,
            input_items,
        )

    def create_document_list(self, item_name: str) -> None:
        """
        Create a new document list item.

        Args:
            item_name: Unique name for the document list.
        """
        item_collection.create_document_list(
            self.db, item_name, self.name, self.process.current_index
        )

    def append_document(
        self, item_name: str, text: str, input_items: Optional[InputItems] = None, index: Optional[int] = None, start_position: Optional[int] = None
    ) -> None:
        """
        Append a text chunk to a document list.

        Args:
            item_name: Name of the document list to append to.
            text: Text content of the document.
            input_items: Mapping of dependency item key -> [start_position, end_position] (two integers).
            index: Specific index to insert at (appends to end if None).
            start_position: Character position within the document stream.
        """
        if (index is None and start_position is not None) or (
            index is not None and start_position is None
        ):
            raise ValidationError(
                "Both 'index' and 'start_position' must be provided together when specifying manual positions.",
                operation="append_document",
                collection="document_list",
                key=item_name,
            )
        if start_position is not None:
            end_position = start_position + len(text)
        else:
            end_position = None
        item_collection.append_document(
            self.db,
            item_name,
            text,
            self.name,
            self.process.current_index,
            index,
            start_position,
            end_position,
            input_items,
        )

    def create_embedding_list(self, item_name: str, ndim: int) -> None:
        """
        Create a new embedding list.

        Args:
            item_name: Unique name for the embedding list.
            ndim: Dimensionality of the embeddings in this list.
        """
        item_collection.create_embedding_list(
            self.db, item_name, self.name, self.process.current_index, ndim
        )

    def append_embedding(
        self,
        item_name: str,
        embedding: List[float],
        input_items: Optional[InputItems] = None,
        index: Optional[int] = None,
        build_idx: bool = True,
        index_rebuild_count: int = 10000,
    ) -> None:
        """
        Append an embedding vector to an embedding list.

        Args:
            item_name: Name of the embedding list to append to.
            embedding: The embedding vector to store.
            input_items: Mapping of dependency item key -> [start_position, end_position] (two integers).
            index: Specific index to insert at (appends to end if None).
            build_idx: Whether to rebuild the vector index.
            index_rebuild_count: Threshold for triggering index rebuild.
        """
        if index is not None:
            end_position = index + 1
        else:
            end_position = None
        item_collection.append_embedding(
            self.db,
            item_name,
            embedding,
            self.name,
            self.process.current_index,
            index,
            index,
            end_position,
            input_items,
            build_idx,
            index_rebuild_count,
        )

    def create_record_list(self, item_name: str, column_names: List[str]) -> None:
        """
        Create a new record list with specified column names.

        Args:
            item_name: Unique name for the record list.
            column_names: List of column names for records in this list.
        """
        item_collection.create_record_list(
            self.db, item_name, self.name, self.process.current_index, column_names
        )

    def append_record(self, item_name: str, record: Dict[str, Any], input_items: Optional[InputItems] = None, index: Optional[int] = None) -> None:
        """
        Append a record (row) to a record list.

        Note: top-level dictionary keys must match initial column name.

        Args:
            item_name: Name of the record list to append to.
            record: Dictionary with column names as keys and values.
            input_items: Mapping of dependency item key -> [start_position, end_position] (two integers).
            index: Specific index to insert at (appends to end if None).
        """
        if index is not None:
            end_position = index + 1
        else:
            end_position = None
        item_collection.append_record(
            self.db,
            item_name,
            record,
            self.name,
            self.process.current_index,
            index,
            index,
            end_position,
            input_items,
        )

    def create_description(
        self, item_name: str, description: str, embedding: List[float], description_name: str = "BASE"
    ) -> None:
        """
        Adds a joint text and embedding description to an item list.

        Args:
            item_name: Name of the item to describe.
            description: Text description of the item.
            embedding: Embedding vector for the description.
            description_name: Label for this description (default "BASE").
        """
        description_collection.add_description(
            self.db,
            description_name,
            item_name,
            self.name,
            self.process.current_index,
            description,
            embedding,
        )

    def checkpoint_execution(self) -> None:
        """
        Identify safe checkpoint in code where stop and pause requests can be executed.

        Using this avoids stopping during undesirable conditions (e.g. while still waiting for outgoing API calls).
        """
        process_collection.process_checkpoint(self.db, self.name)

    def pause_execution(self, process_name: str) -> None:
        """
        Request to pause another process list's current execution.

        Args:
            process_name: Name of the process to pause.
        """
        process_collection.process_stop_pause_request(
            self.db, process_name, "pause", self.name
        )

    def stop_execution(self, process_name: str) -> None:
        """
        Request to stop another process list's current execution.

        Args:
            process_name: Name of the process to stop.
        """
        process_collection.process_stop_pause_request(
            self.db, process_name, "stop", self.name
        )

    def resume_execution(self, process_name: str) -> None:
        """
        Resume a paused process list's current process by name.

        Note: Only work in single machine/container case currently.

        Args:
            process_name: Name of the process list to resume.
        """
        process_collection.process_resume_request(self.db, process_name, self.name)

    def has_vector_index(self, ndim: int) -> bool:
        """
        Check if a vector index exists for embeddings of a given dimension.

        Args:
            ndim: Dimensionality of the embeddings.

        Returns:
            True if a vector index exists for this dimension.
        """
        col = self.db.collection("embedding")
        field = "embedding_" + str(ndim)
        for idx in col.indexes():
            if idx.get("type") == "vector" and field in (idx.get("fields") or []):
                return True
        return False

    def query_process_list(
        self,
        code_text: Optional[str] = None,
        parent_code_text: Optional[str] = None,
        description_embedding: Optional[List[float]] = None,
        description_text: Optional[str] = None,
        filtered: Optional[List[str]] = None,
    ) -> List[Any]:
        """
        Query process items. Can optionally filter by descriptions and parent process.

        Args:
            code_text: Text to search in process code.
            parent_code_text: Text to search in parent process code.
            description_embedding: Embedding vector for similarity search.
            description_text: Text to search in descriptions.
            filtered: List of process names to restrict search to.

        Returns:
            List of matching process results.
        """
        return query_collection_simple.query_process(
            self.db,
            code_text=code_text,
            parent_code_text=parent_code_text,
            description_embedding=description_embedding,
            description_text=description_text,
            filtered=filtered or [],  # list of file.name strings
        )

    def query_embedding_list(
        self,
        embedding: List[float],
        description_embedding: Optional[List[float]] = None,
        description_text: Optional[str] = None,
        code_text: Optional[str] = None,
        filtered: Optional[List[str]] = None,
        use_approx: bool = False,
    ) -> List[Any]:
        """
        Query embedding items. Can optionally filter by descriptions and parent process.

        Args:
            embedding: Query embedding vector for similarity search.
            description_embedding: Embedding for description similarity.
            description_text: Text to search in descriptions.
            code_text: Text to search in process code.
            filtered: List of embedding names to restrict search to.
            use_approx: Use approximate (faster) similarity search.

        Returns:
            List of matching embedding results.
        """
        return query_collection_simple.query_embedding(
            self.db,
            embedding,
            description_embedding,
            description_text,
            code_text,
            filtered=filtered or [],
            use_approx=use_approx,
        )

    def query_record_list(
        self,
        record_text: str,
        description_embedding: Optional[List[float]] = None,
        description_text: Optional[str] = None,
        code_text: Optional[str] = None,
        filtered: Optional[List[str]] = None,
    ) -> List[Any]:
        """
        Query record items. Can optionally filter by descriptions and parent process.

        Args:
            record_text: Text to search in record data.
            description_embedding: Embedding for description similarity.
            description_text: Text to search in descriptions.
            code_text: Text to search in process code.
            filtered: List of record names to restrict search to.

        Returns:
            List of matching record results.
        """
        return query_collection_simple.query_record(
            self.db,
            record_text,
            description_embedding,
            description_text,
            code_text,
            filtered=filtered or [],
        )

    def query_document_list(
        self,
        document_text: str,
        description_embedding: Optional[List[float]] = None,
        description_text: Optional[str] = None,
        code_text: Optional[str] = None,
        filtered: Optional[List[str]] = None,
    ) -> List[Any]:
        """
        Query document items. Can optionally filter by descriptions and parent process.

        Args:
            document_text: Text to search in document content.
            description_embedding: Embedding for description similarity.
            description_text: Text to search in descriptions.
            code_text: Text to search in process code.
            filtered: List of document names to restrict search to.

        Returns:
            List of matching document item results.
        """
        return query_collection_simple.query_document(
            self.db,
            document_text,
            description_embedding,
            description_text,
            code_text,
            filtered=filtered or [],
        )

    def query_file_list(
        self,
        description_embedding: Optional[List[float]] = None,
        description_text: Optional[str] = None,
        code_text: Optional[str] = None,
        filtered: Optional[List[str]] = None,
    ) -> List[Any]:
        """
        Query file items. Can optionally filter by descriptions and parent process.

        Args:
            description_embedding: Embedding for description similarity.
            description_text: Text to search in descriptions.
            code_text: Text to search in process code.
            filtered: List of file names to restrict search to.

        Returns:
            List of matching file item results.
        """
        return query_collection_simple.query_file(
            self.db,
            description_embedding,
            description_text,
            code_text,
            filtered=filtered or [],
        )

    def query_item_content(self, item_name: str, index: Optional[int] = None, start_position: Optional[int] = None, end_position: Optional[int] = None) -> Any:
        """
        Query the content of an item list by index chunk or position range.

        Args:
            item_name: Name of the item list to query.
            index: Specific index chunk to retrieve.
            start_position: Start of position range (if index not specified).
            end_position: End of position range (if index not specified).

        Returns:
            The item content at the specified index or position range.
        """
        self._ensure_item_exists(item_name, operation="query_item_content")
        if index is not None:
            return query_item_simple.query_item_index(
            self.db, item_name, index
        )
        return query_item_simple.query_item(
            self.db, item_name, start_position, end_position
        )

    def query_item_list(self, item_name: str) -> Dict[str, Any]:
        """
        Get metadata for an item list.

        Args:
            item_name: Name of the item list.

        Returns:
            Dictionary with list metadata (n_items, length, etc.).
        """
        self._ensure_item_exists(item_name, operation="query_item_list")
        return query_item_simple.query_item_list(self.db, item_name)

    def query_item_parent(self, item_name: str, start_position: Optional[int] = None, end_position: Optional[int] = None) -> List[Any]:
        """
        Query input dependencies of an item list. Allows optional position filtering.

        Args:
            item_name: Name of the item list.
            start_position: Filter by start position.
            end_position: Filter by end position.

        Returns:
            List of item list information.
        """
        self._ensure_item_exists(item_name, operation="query_item_parent")
        return query_item_simple.query_item_input(
            self.db, item_name, start_position, end_position
        )

    def query_item_child(self, item_name: str, start_position: Optional[int] = None, end_position: Optional[int] = None) -> List[Any]:
        """
        Query items that depend on an item list. Allows optional position filtering.

        Args:
            item_name: Name of the item list.
            start_position: Filter by start position.
            end_position: Filter by end position.

        Returns:
            List of child item information.
        """
        self._ensure_item_exists(item_name, operation="query_item_child")
        return query_item_simple.query_item_output(
            self.db, item_name, start_position, end_position
        )

    def query_item_description(self, item_name: str) -> List[str]:
        """
        Get descriptions associated with an item list.

        Args:
            item_name: Name of the item list.

        Returns:
            List of description texts.
        """
        self._ensure_item_exists(item_name, operation="query_item_description")
        return query_item_simple.query_item_description(self.db, item_name)

    def query_item_creation_process(self, item_name: str) -> List[Dict[str, Any]]:
        """
        Get the process that created an item list.

        Args:
            item_name: Name of the item list.

        Returns:
            List of process information with process_id and index.
        """
        self._ensure_item_exists(item_name, operation="query_item_creation_process")
        return query_item_simple.query_item_creation_process(self.db, item_name)

    def query_item_process(self, item_name: str, start_position: Optional[int] = None, end_position: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get processes that modified an item list (given name). Can filter by interval range within the list.

        Args:
            item_name: Name of the item list.
            start_position: Filter by start position.
            end_position: Filter by end position.

        Returns:
            List of process info dicts with process name and index.
        """
        self._ensure_item_exists(item_name, operation="query_item_process")
        return query_item_simple.query_item_process(
            self.db, item_name, start_position, end_position
        )

    def query_process_item(self, process_name: str) -> List[Dict[str, Any]]:
        """
        Get all items created or modified by a given process name.

        Args:
            process_name: Name of the process list.

        Returns:
            List of item dictionaries with name and position range.
        """
        self._ensure_process_exists(process_name, operation="query_process_item")
        return query_item_simple.query_process_item(self.db, process_name)
