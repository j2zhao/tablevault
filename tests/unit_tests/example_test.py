def find_shortest_prefix_strings(string_list):
    """
    Filters a list of underscore-separated strings to find the shortest
    prefix strings.

    For any group of strings that share a common prefix (e.g., "PREFIX" is a
    prefix of "PREFIX_SUFFIX"), this function returns only the shortest ones.

    For example:
    - If ["STRING1", "STRING1_STRING2"] is the input, it returns ["STRING1"].
    - If ["STRING1_STRING2", "STRING1_STRING4"] is the input (and "STRING1" is
      not), it returns ["STRING1_STRING2", "STRING1_STRING4"].

    Args:
        string_list: A list of strings, where substrings are joined by '_'.

    Returns:
        A list of strings containing only the shortest prefix strings from the
        input list.
    """
    # Use a set for faster lookups of existing strings.
    string_set = set(string_list)
    result = []

    for s in string_list:
        # Split the string into its component parts.
        parts = s.split('_')
        # Assume the current string is the shortest until a shorter prefix is found.
        is_shortest_prefix = True
        # Check for the existence of any shorter prefix.
        # We iterate through the parts to build prefixes (e.g., "A", "A_B").
        for i in range(1, len(parts)):
            # Create a prefix by joining the parts.
            prefix = "_".join(parts[:i])
            # If this shorter prefix already exists in our original list,
            # then the current string 's' is not one of the shortest.
            if prefix in string_set:
                is_shortest_prefix = False
                # No need to check other prefixes for this string; we can break.
                break
        
        # If, after checking all possible shorter prefixes, none were found
        # in the original list, then this string is a "shortest" one.
        if is_shortest_prefix:
            result.append(s)

    return result

# --- Examples ---

# Example 1: Basic case where a shorter prefix exists.
list1 = ["STRING1_STRING2_STRING3", "STRING1_STRING2", "STRING1"]
print(f"Input: {list1}")
print(f"Result: {find_shortest_prefix_strings(list1)}\n")
# Expected Output: ['STRING1']

# Example 2: The case where two strings share a prefix, but the root prefix isn't in the list.
list2 = ["STRING1_STRING2", "STRING1_STRING4"]
print(f"Input: {list2}")
print(f"Result: {find_shortest_prefix_strings(list2)}\n")
# Expected Output: ['STRING1_STRING2', 'STRING1_STRING4']

# Example 3: A more complex list with multiple distinct prefixes.
list3 = ["A_B_C", "A_B", "A_D", "X_Y", "X_Z", "W"]
print(f"Input: {list3}")
print(f"Result: {find_shortest_prefix_strings(list3)}\n")
# Expected Output: ['A_B', 'A_D', 'X_Y', 'X_Z', 'W']

# Example 4: A list with no shared prefixes.
list4 = ["ALPHA", "BETA", "GAMMA"]
print(f"Input: {list4}")
print(f"Result: {find_shortest_prefix_strings(list4)}\n")
# Expected Output: ['ALPHA', 'BETA', 'GAMMA']