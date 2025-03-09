from tablevault.prompts.code_execution_ptype import CodePrompt
from tablevault.prompts.gen_code_ptype import GeneratorPrompt
from tablevault.prompts.open_ai_threads_ptype import OAIThreadPrompt
from tablevault.prompts.base_ptype import TVPrompt
from tablevault.defintions.tv_errors import TVPromptError
from tablevault.defintions import constants
PTYPE_MAPPING = {
    "CodePrompt": CodePrompt,
    "GeneratorPrompt": GeneratorPrompt,
    "OAIThreadPrompt": OAIThreadPrompt,
}

def load_prompt(yaml_prompt: dict) -> TVPrompt:
    if constants.PTYPE not in yaml_prompt:
        raise TVPromptError(f"Prompt {yaml_prompt[constants.PNAME]} doesn't contain required attribute {constants.PTYPE}.")
    try:
        PTYPE_MAPPING[yaml_prompt["ptype"]](yaml_prompt)
    except Exception as e:
        raise TVPromptError(f'Error {e} when generating prompt {yaml_prompt[constants.PNAME]}')