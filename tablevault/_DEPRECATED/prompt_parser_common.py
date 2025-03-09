from tablevault.defintions.tv_errors import TVPromptError
import re


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
            raise TVPromptError(
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


# def parse_dep(input_string: str) -> tuple[str, str, str]:
#     pattern = r"^(\w+)(?:\.(\w+))?(?:\((\w+)\))?$"
#     match = re.match(pattern, input_string)

#     if match:
#         part1 = match.group(1)  # First ALPHANUMERIC
#         part2 = match.group(2)  # Second ALPHANUMERIC (optional)
#         part3 = match.group(3)  # ALPHANUMERIC inside parentheses (optional)
#         return part1, part2, part3
#     else:
#         raise TVPromptError("Input string does not match the expected format.")
