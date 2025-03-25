from importlib import import_module
from typing import Any, Callable
import os
from tablevault.defintions import constants
from tablevault.defintions import tv_errors
 

def load_function_from_file(module_name: str, function_name: str, db_dir: str) -> tuple[Callable, Any]:
    file_path_ = os.path.join(db_dir, constants.CODE_FOLDER, module_name + '.py')
    try:
        namespace = {}
        with open(file_path_, "r") as file:
            exec(file.read(), namespace)
        if function_name in namespace:
            return namespace[function_name], namespace
        else:
            raise tv_errors.TVPromptError(f"Function '{function_name}' not found in '{file_path}'")
    except:
        raise tv_errors.TVPromptError(f"Function '{function_name}' not found in '{file_path}'")

def get_function_from_module(module_name: str, function_name: str, is_tablevault:bool=True) -> Callable:
    if is_tablevault:
        module_name = f"tablevault.code_functions.{module_name}"
    # Import the module by its absolute name
    module = import_module(module_name)
    # Retrieve the attribute (which should be a function)
    func = getattr(module, function_name, None)
    # Validate it's actually callable
    if not callable(func):
        raise TypeError(f"'{function_name}' in '{module_name}' is not a callable.")

    return func

def topological_sort(items: list, dependencies: dict) -> list:
    graph = {item: [] for item in items}

    for parent, children in dependencies.items():
        for child in children:
            if child not in graph:
                graph[child] = []
            graph[parent].append(child)

    visited = set()
    visiting = set()
    sorted_order = []

    def dfs(node):
        if node in visiting:
            raise tv_errors.TVPromptError(
                "Cycle detected! Topological sort of prompts not possible."
            )
        if node in visited:
            return

        visiting.add(node)  # Mark node as visiting
        for child in graph[node]:
            dfs(child)  # Visit children first
        visiting.remove(node)  # Remove from visiting
        visited.add(node)  # Mark node as visited
        sorted_order.append(node)  # Add node after processing children

    for item in items:
        if item not in visited:
            dfs(item)
    return sorted_order
