from pydantic import Field, BaseModel
from tablevault.prompts.base_ptype import TVPrompt
from tablevault.defintions.types import Cache
from tablevault.prompts.utils import utils, table_operations
from tablevault.prompts.utils.table_string import apply_table_string
from tablevault.defintions import constants
from tablevault.helper import file_operations
from typing import Any, Optional

class LoadArtifact(BaseModel):
    artifact_name: str = Field("Column name of artifact name.")
    artifact_path: str = Field("Column name of original artifact path.")

class GeneratorPrompt(TVPrompt):
    is_custom: bool = Field(description="Custom to database.")
    code_module: str = Field(description="Module of function.") 
    python_function: str = Field(description="Function to execute.")
    arguments: dict[str, Any] = Field(description="Function Arguments. DataTable and TableStrings are valid.")
    load_artifact: Optional[LoadArtifact] = Field(default=None)
    
    def execute(
        self,
        cache: Cache,
        instance_id: str,
        table_name: str,
        db_dir: str,
    ) -> bool:
        if not self.is_custom:
            funct = utils.get_function_from_module(
                self.code_module, self.python_function
            )
        else:
            funct, _ = utils.load_function_from_file(self.code_module, self.python_function, db_dir)

        kwargs = {}
        kwargs = apply_table_string(self.arguments, cache)
        results = funct(**kwargs)
        merged_df, diff_flag = table_operations.merge_columns(
            self.changed_columns, results, cache[constants.TABLE_SELF]
        )
        if self.load_artifact is not None:
            for _, row in merged_df.iterrows():
                path_name = row[self.load_artifact.artifact_path]
                artifact_name = row[self.load_artifact.artifact_name]
                file_operations.upload_artifact(artifact_name, path_name, db_dir, table_name, instance_id) 
        if diff_flag:
            cache[constants.TABLE_SELF] = merged_df
            table_operations.write_table(merged_df, instance_id, table_name, db_dir)
            return True
        else:
            return False
        

        
    