import numpy as np

def get_direction_arrow(tree_type):
    # the two unicodes are for up and down arrows
    return "\U0001F53C" if tree_type == 'parents' else "\U0001F53D"


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