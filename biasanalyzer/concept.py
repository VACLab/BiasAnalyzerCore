from collections import defaultdict
import networkx as nx
from typing import List, Any, Dict


class ConceptNode:
    def __init__(self, concept_id: int, graph: "ConceptHierarchy"):
        self.id = concept_id
        self._ch = graph  # reference back to ConceptHierarchy

    @property
    def name(self) -> str:
        return self._ch.graph.nodes[self.id]["concept_name"]

    @property
    def code(self) -> str:
        return self._ch.graph.nodes[self.id]["concept_code"]

    def get_metrics(self, cohort_id: int) -> dict:
        metrics = self._ch.graph.nodes[self.id].get("metrics", {})
        return metrics.get(cohort_id, {})

    def get_union_metrics(self) -> dict:
        # simple aggregation example
        metrics = self._ch.graph.nodes[self.id].get("metrics", {})
        counts = [m["count"] for m in metrics.values()]
        prevalences = [m["prevalence"] for m in metrics.values()]
        return {
            "count": sum(counts),
            "prevalence": sum(prevalences) / len(prevalences) if prevalences else 0.0,
        }

    def parents(self) -> List["ConceptNode"]:
        return [ConceptNode(p, self._ch) for p in self._ch.graph.predecessors(self.id)]

    def children(self) -> List["ConceptNode"]:
        return [ConceptNode(c, self._ch) for c in self._ch.graph.successors(self.id)]


class ConceptHierarchy:
    def __init__(self, input_g: nx.DiGraph):
        self.graph = input_g

    def get_node(self, concept_id: int):
        if concept_id in self.graph.nodes:
            return ConceptNode(concept_id, self)
        return None

    def get_root_nodes(self) -> List[ConceptNode]:
        roots = [n for n in self.graph.nodes if self.graph.in_degree(n) == 0]
        return [ConceptNode(r, self) for r in roots]

    def subtree(self, concept_id: int):
        """Yield all nodes in the subtree rooted at concept_id."""
        descendants = nx.descendants(self.graph, concept_id) | {concept_id}
        for d in descendants:
            yield ConceptNode(d, self)

    def union(self, other: "ConceptHierarchy") -> "ConceptHierarchy":
        """Merge two hierarchies into a new one, aggregating metrics."""
        composed_graph = nx.compose(self.graph, other.graph)
        # merge node metrics
        for n in composed_graph.nodes:
            metrics_self = self.graph.nodes.get(n, {}).get("metrics", {})
            metrics_other = other.graph.nodes.get(n, {}).get("metrics", {})
            merged = {**metrics_self, **metrics_other}
            composed_graph.nodes[n]["metrics"] = merged
        return ConceptHierarchy(composed_graph)

    def to_dict(self) -> dict:
        roots = self.get_root_nodes()
        return {"roots": [self._node_to_dict(r) for r in roots]}

    def _node_to_dict(self, node: ConceptNode) -> dict:
        data = {
            "concept_id": node.id,
            "concept_name": node.name,
            "concept_code": node.code,
            "metrics": {
                "union": node.get_union_metrics(),
                "cohorts": self.graph.nodes[node.id].get("metrics", {}),
                },
            "children": [
                self._node_to_dict(c) for c in node.children()
                ]
        }
        return data


def build_concept_hierarchy_from_results(results, cohort_id: int) -> ConceptHierarchy:
    """
    build concept hierarchy tree managed by networkx from list of dicts returned from the concept prevalence SQL
    :param results: list of dicts from prevalence SQL
    :param cohort_id: cohort id to get concept hierarchy for
    :return: ConceptHierarchy object
    """
    # node metrics
    metrics_by_concept = defaultdict(lambda: {"count": 0, "prevalence": 0.0})
    node_metadata = {}

    for row in results:
        cid = row["descendant_concept_id"]
        if cid not in node_metadata:
            node_metadata[cid] = {
                "concept_name": row["concept_name"],
                "concept_code": row["concept_code"],
            }
            metrics_by_concept[cid] = {
                "count": row["count_in_cohort"],
                "prevalence": row["prevalence"],
            }

    graph = nx.DiGraph()
    # add nodes with metadata + metrics
    for cid, meta in node_metadata.items():
        graph.add_node(cid, **meta, metrics={cohort_id: metrics_by_concept[cid]})

    # add parent-child edges
    for row in results:
        anc = row["ancestor_concept_id"]
        desc = row["descendant_concept_id"]
        if anc and desc and anc != desc:
            graph.add_edge(anc, desc)

    return ConceptHierarchy(graph)
