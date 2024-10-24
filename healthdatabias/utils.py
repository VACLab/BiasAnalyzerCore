import numpy as np


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