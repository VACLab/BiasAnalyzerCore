import logging
import re

import numpy as np
from ipytree import Node

logger = logging.getLogger(__name__)


def notify_users(message: str, level: str = "info"):
    """
    Notify users via both print and logging.
    :param message: message to show
    :param level: Logging level: 'info', 'warning', 'error'
    :return:
    """

    print(message)

    log_func = {
        "info": logger.info,
        "warning": logger.warning,
        "error": logger.error,
        "debug": logger.debug,
    }.get(level.lower(), logger.info)

    log_func(message)


def get_direction_arrow(tree_type):
    # the two unicodes are for up and down arrows
    return "\U0001f53c" if tree_type == "parents" else "\U0001f53d"


def clean_string(text):
    # replace newlines and tabs with a space
    text = re.sub(r"[\n\t]", " ", text)
    # Replace multiple spaces with a single space
    text = re.sub(r"\s+", " ", text)
    # Strip leading and trailing spaces
    return text.strip()


def hellinger_distance(p, q):
    """
    Compute the Hellinger distance between two probability distributions.
    """
    p = np.array(p)
    q = np.array(q)

    # Ensure the distributions are normalized
    p = p / np.sum(p)
    q = q / np.sum(q)

    return np.sqrt(0.5 * np.sum((np.sqrt(p) - np.sqrt(q)) ** 2))


def build_concept_hierarchy(
    df, parent_col="ancestor_concept_id", child_col="descendant_concept_id", details_col="details"
):
    """
    Builds a hierarchy using only direct parent-child relationships to remove duplicate branches.
    """
    grouped = df.groupby(parent_col)
    hierarchy = {parent: list(zip(group[child_col], group[details_col])) for parent, group in grouped}
    return hierarchy


def build_concept_tree(concept_tree: dict, tree_type: str) -> Node:
    """
    Recursively builds an ipytree Node for a given concept tree.
    """
    # Extract concept details
    details = concept_tree.get("details", {})
    concept_name = details.get("concept_name", "Unknown Concept")
    concept_id = details.get("concept_id", "")
    concept_code = details.get("concept_code", "")
    direction_arrow = get_direction_arrow(tree_type)
    # Create a label for the current concept
    label_text = f"{direction_arrow} {concept_name} (ID: {concept_id}, Code: {concept_code})"
    node = Node(label_text)

    # Recursively add child nodes
    for child in concept_tree.get(tree_type, []):
        child_node = build_concept_tree(child, tree_type)
        node.add_node(child_node)

    return node


def find_roots(df, parent_col="ancestor_concept_id", child_col="descendant_concept_id"):
    """
    Finds root nodes in the hierarchy. Roots are nodes that are parents
    but never appear as children.
    """
    # Get unique parent and child IDs
    parents = set(df[parent_col].dropna())  # Exclude None values
    children = set(df[child_col])
    # Roots are parents that are not children
    roots = parents - children
    return list(roots)


def print_hierarchy(hierarchy, parent=None, level=0, parent_details=None):
    # Print the hierarchy in an indented format
    if parent not in hierarchy:
        return
    if level == 0 and parent_details is not None:
        print(parent_details)
        level += 1
    for child, details in hierarchy[parent]:
        print("  " * level + details)
        print_hierarchy(hierarchy, parent=child, level=level + 1)
