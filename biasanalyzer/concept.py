from collections import defaultdict
import networkx as nx
from typing import List, Optional
from _collections import deque


class ConceptNode:
    def __init__(self, concept_id: int, ch: "ConceptHierarchy"):
        self.id = concept_id
        self._ch = ch  # reference back to ConceptHierarchy

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

    def to_dict(self, include_children: bool = True) -> dict:
        """
        Serialize this node into a dict. Optionally include nested children.
        """
        data = {
            "concept_id": self.id,
            "concept_name": self.name,
            "concept_code": self.code,
            "metrics": {
                "union": self.get_union_metrics(),
                "cohorts": self._ch.graph.nodes[self.id].get("metrics", {}),
            },
            "parent_ids": list(self._ch.graph.predecessors(self.id)),
        }
        if include_children:
            data["children"] = [c.to_dict(include_children=True) for c in self.children()]
        return data


class ConceptHierarchy:
    _graph_cache = {}

    def __init__(self, input_g: nx.DiGraph, cohort_id: int):
        self.graph = input_g
        self.cohort_id = cohort_id

    @classmethod
    def build_concept_hierarchy_from_results(cls, cohort_id: int, results: List[dict]):
        """
        build concept hierarchy tree managed by networkx from list of dicts returned from the concept prevalence SQL
        with cache management
        :param results: list of dicts from prevalence SQL
        :param cohort_id: cohort id to get concept hierarchy for
        :return: ConceptHierarchy object
        """
        if cohort_id in cls._graph_cache:
            return cls._graph_cache[cohort_id]

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

        hierarchy = ConceptHierarchy(graph, cohort_id)
        cls._graph_cache[cohort_id] = hierarchy
        return hierarchy

    @classmethod
    def clear_cache(cls):
        cls._graph_cache.clear()

    def get_node(self, concept_id: int, serialization: bool = False):
        concept_node = ConceptNode(concept_id, self) if concept_id in self.graph.nodes else None
        return concept_node.to_dict(include_children=False) if serialization else concept_node

    def get_root_nodes(self, serialization: bool = False) -> List:
        roots = [n for n in self.graph.nodes if self.graph.in_degree(n) == 0]
        root_nodes = [ConceptNode(r, self) for r in roots]
        if serialization:
            return [rn.to_dict(include_children=False)  for rn in root_nodes]
        else:
            return root_nodes

    def get_leaf_nodes(self, serialization: bool = False) -> List:
        leaves = [n for n in self.graph.nodes if self.graph.out_degree(n) == 0]
        leave_nodes = [ConceptNode(l, self) for l in leaves]
        if serialization:
            return [rl.to_dict(include_children=False) for rl in leave_nodes]
        else:
            return leave_nodes

    def iter_nodes(self, root_id: int, order: str = "bfs", include_root: bool = True,
                   serialization: bool = False):
        """Iterate nodes in BFS or DFS order from a given root."""
        if root_id not in self.graph:
            raise ValueError(f"Root node {root_id} not found in graph.")

        if order == "bfs":
            queue = deque([root_id])
            while queue:
                node = queue.popleft()
                if not include_root and node == root_id:
                    queue.extend(self.graph.successors(node))
                    continue
                if serialization:
                    yield ConceptNode(node, self).to_dict(include_children=False)
                else:
                    yield ConceptNode(node, self)
                queue.extend(self.graph.successors(node))
        elif order == "dfs":
            stack = [root_id]
            while stack:
                node = stack.pop()
                if not include_root and node == root_id:
                    stack.extend(self.graph.successors(node))
                    continue
                if serialization:
                    yield ConceptNode(node, self).to_dict(include_children=False)
                else:
                    yield ConceptNode(node, self)
                stack.extend(self.graph.successors(node))
        else:
            raise ValueError("order must be 'bfs' or 'dfs'")

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

    def to_dict(self, root_id: Optional[int] = None) -> dict:
        """
        Convert the concept hierarchy or a sub-hierarchy to a nested dict structure
        :param root_id: if provided, return the sub-hierarchy rooted at this concept_id;
        if None, return the whole hierarchy with all roots.
        :return: nested dict representation of the hierarchy or sub-hierarchy
        """
        if root_id is not None:
            if root_id not in self.graph:
                raise ValueError(f"Input concept id {root_id} not found in the concept hierarchy graph")
            return {"hierarchy": [ConceptNode(root_id, self).to_dict()]}

        return {"hierarchy": [r.to_dict() for r in self.get_root_nodes()]}
