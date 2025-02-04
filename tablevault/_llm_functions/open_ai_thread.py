import os
import openai
from typing import Optional

from tablevault._llm_functions import llm_prompts
from time import sleep


def _set_up_thread(
    client, model, temperature, name, response_format, instructions, uses_files
):
    thread = client.beta.threads.create()

    if response_format:
        response_format = {
            "type": "json_schema",
            "json_schema": {"name": "emotions", "schema": response_format},
        }
    if uses_files:
        tools = [{"type": "file_search"}]
    else:
        tools = None
    assistant = client.beta.assistants.create(
        name=name,
        instructions=instructions,
        model=model,  # gpt-4o-mini
        tools=tools,
        temperature=temperature,
        response_format=response_format,
    )
    return assistant, thread


def add_open_ai_secret(secret):
    os.environ["OPENAI_API_KEY"] = secret


class Open_AI_Thread:
    def __init__(
        self,
        name,
        model,
        temperature: float = 0.2,
        retry: int = 10,
        instructions: Optional[str] = None,
        response_format=None,
        client: Optional[openai.OpenAI] = None,
        uses_files: bool = True,
    ):
        if not client:
            self.client = openai.OpenAI()
        else:
            self.client = client
        self.retry = retry
        self.name = name
        self.assistant = None
        self.thread = None
        for i in range(self.retry):
            try:
                self.assistant, self.thread = _set_up_thread(
                    self.client,
                    model,
                    temperature,
                    self.name,
                    response_format,
                    instructions,
                    uses_files,
                )
                self.success = True
                return
            except Exception as e:
                print(f"Error Calling LLM for Setup: {self.name}")
                print(e)
            sleep(1)
        self.success = False

    def run_query(self):
        for i in range(self.retry):
            try:
                run = self.client.beta.threads.runs.create_and_poll(
                    thread_id=self.thread.id, assistant_id=self.assistant.id
                )
                if run.status == "completed":
                    messages = self.client.beta.threads.messages.list(
                        thread_id=self.thread.id
                    )
                    msg = messages.data[0].content[0].text.value
                    return msg
                else:
                    print(f"Run Status Error: {self.name}")
                    print(run.last_error)
                    print(run.status)
                    sleep(1)
            except Exception as e:
                print(f"Error Calling LLM for {self.name} Run: {e}")
            sleep(1)
        return None

    def add_message(self, message, role="user", file_ids=None):
        for i in range(self.retry):
            try:
                attachments = []
                if file_ids is not None:
                    for file_id in file_ids:
                        att = {"file_id": file_id, "tools": [{"type": "file_search"}]}
                        attachments.append(att)
                self.client.beta.threads.messages.create(
                    self.thread.id,
                    role="user",
                    content=message,
                    attachments=attachments,
                )
                return True
            except Exception as e:
                print(f"Error Calling LLM for {self.name} Message Adding: {e}")
            sleep(1)
        return False

    def delete_assistant(self):
        try:
            self.client.beta.assistants.delete(self.assistant.id)
            self.assistant = None
            return True
        except Exception as e:
            print(f"Failed to delete assistant for {self.name}")
            print(e)
            return False
